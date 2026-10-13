package testdb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ayranoliveira1/pokelocation-server/internal/db"
)

const (
	redisDBs = 64
	// Folgado: com `go test ./...` cada pacote sobe os seus containers ao mesmo tempo (e outros agentes também), e o
	// padrão de 60 s do testcontainers estourou no Docker Desktop.
	startTimeout = 3 * time.Minute
)

var (
	dockerOnce sync.Once
	dockerErr  error

	pgOnce  sync.Once
	pgC     *postgres.PostgresContainer
	pgURL   string // banco "tmpl": o template migrado
	pgAdmin *pgxpool.Pool
	pgErr   error
	pgSeq   atomic.Int64
	cloneMu sync.Mutex

	rdOnce sync.Once
	rdC    *tcredis.RedisContainer
	rdAddr string
	rdFree chan int // DBs lógicos livres
	rdErr  error
)

// Pool devolve um pool para um banco novo, clonado do template já migrado. Fechado e apagado no fim do teste.
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	needDocker(t)
	pgOnce.Do(startPostgres)
	if pgErr != nil {
		t.Fatal("testdb: postgres:", pgErr)
	}
	ctx := context.Background()
	name := fmt.Sprintf("t%d", pgSeq.Add(1))
	// ponytail: clones em série; CREATE DATABASE concorrente do mesmo template pode recusar ("being accessed").
	cloneMu.Lock()
	_, err := pgAdmin.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE tmpl")
	cloneMu.Unlock()
	if err != nil {
		t.Fatal("testdb: clone:", err)
	}
	pool, err := db.Open(ctx, withDB(pgURL, name))
	if err != nil {
		t.Fatal("testdb:", err)
	}
	t.Cleanup(func() {
		pool.Close()
		// FORCE derruba conexões que sobraram (ex.: servidor subido pelo teste).
		if _, err := pgAdmin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Error("testdb: drop:", err)
		}
	})
	return pool
}

func startPostgres() {
	ctx := context.Background()
	pgC, pgErr = postgres.Run(ctx, "postgres:17-alpine", // o módulo já sobe com fsync=off
		postgres.WithDatabase("tmpl"), postgres.WithUsername("t"), postgres.WithPassword("t"),
		testcontainers.WithWaitStrategyAndDeadline(startTimeout,
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(startTimeout)))
	if pgErr != nil {
		return
	}
	if pgURL, pgErr = pgC.ConnectionString(ctx, "sslmode=disable"); pgErr != nil {
		return
	}
	tmpl, err := db.Open(ctx, pgURL)
	if err != nil {
		pgErr = err
		return
	}
	pgErr = db.Migrate(ctx, tmpl)
	tmpl.Close() // template não pode ter conexão aberta na hora do clone
	if pgErr != nil {
		return
	}
	pgAdmin, pgErr = db.Open(ctx, withDB(pgURL, "postgres"))
}

func withDB(raw, name string) string {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err) // a URL vem do testcontainers
	}
	u.Path = "/" + name
	return u.String()
}

// Redis devolve um cliente para um DB lógico só deste teste, vazio. Pub/sub é global: canais com nome único.
// URL para subir o binário: fmt.Sprintf("redis://%s/%d", c.Options().Addr, c.Options().DB).
func Redis(t testing.TB) *redis.Client {
	t.Helper()
	needDocker(t)
	rdOnce.Do(startRedis)
	if rdErr != nil {
		t.Fatal("testdb: redis:", rdErr)
	}
	n := <-rdFree
	c := redis.NewClient(&redis.Options{Addr: rdAddr, DB: n})
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		_ = c.Close()
		rdFree <- n
		t.Fatal("testdb: flushdb:", err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		rdFree <- n
	})
	return c
}

func startRedis() {
	ctx := context.Background()
	if rdC, rdErr = runRedis(ctx); rdErr != nil {
		return
	}
	if rdAddr, rdErr = rdC.PortEndpoint(ctx, "6379/tcp", ""); rdErr != nil {
		return
	}
	rdFree = make(chan int, redisDBs)
	for i := range redisDBs {
		rdFree <- i
	}
}

// Sem persistência, como em produção (plano G/D1).
func runRedis(ctx context.Context) (*tcredis.RedisContainer, error) {
	return tcredis.Run(ctx, "redis:7-alpine",
		testcontainers.WithCmdArgs("--databases", fmt.Sprint(redisDBs), "--save", "", "--appendonly", "no"),
		testcontainers.WithWaitStrategyAndDeadline(startTimeout,
			wait.ForLog("Ready to accept connections").WithStartupTimeout(startTimeout)))
}

var errRedisStopped = errors.New("testdb: redis parado (dial recusado)")

// RedisCtl derruba e religa o Redis de StoppableRedis.
type RedisCtl struct {
	c    *tcredis.RedisContainer
	addr atomic.Pointer[string] // nil = parado
}

// StoppableRedis sobe um Redis só deste teste (segundos a mais) e devolve um cliente que segue o container entre
// Stop/Start. Só para código no mesmo processo: o endereço muda a cada Start.
func StoppableRedis(t testing.TB) (*redis.Client, *RedisCtl) {
	t.Helper()
	needDocker(t)
	ctx := context.Background()
	c, err := runRedis(ctx)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal("testdb: redis:", err)
	}
	r := &RedisCtl{c: c}
	if err := r.refresh(ctx); err != nil {
		t.Fatal("testdb:", err)
	}
	cl := redis.NewClient(&redis.Options{
		Addr: "testdb-redis", // ignorado: o Dialer usa o endereço atual
		Dialer: func(ctx context.Context, network, _ string) (net.Conn, error) {
			a := r.addr.Load()
			if a == nil {
				return nil, errRedisStopped
			}
			var d net.Dialer
			return d.DialContext(ctx, network, *a)
		},
	})
	t.Cleanup(func() { _ = cl.Close() })
	return cl, r
}

// Stop mata o Redis na hora (SIGKILL), como uma queda: até o Start, toda conexão nova é recusada.
func (r *RedisCtl) Stop(t testing.TB) {
	t.Helper()
	// Esquece o endereço antes de parar: a porta liberada no host pode ir para o container de outro teste, e o cliente
	// falaria com o Redis errado.
	r.addr.Store(nil)
	var now time.Duration
	if err := r.c.Stop(context.Background(), &now); err != nil {
		t.Fatal("testdb: stop:", err)
	}
}

// Start religa e espera responder PING. O Redis volta vazio.
func (r *RedisCtl) Start(t testing.TB) {
	t.Helper()
	ctx := context.Background()
	if err := r.c.Start(ctx); err != nil {
		t.Fatal("testdb: start:", err)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal("testdb:", err)
	}
	probe := redis.NewClient(&redis.Options{Addr: *r.addr.Load(), MaxRetries: -1})
	defer probe.Close()
	for deadline := time.Now().Add(15 * time.Second); probe.Ping(ctx).Err() != nil; {
		if time.Now().After(deadline) {
			t.Fatal("testdb: redis não voltou")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (r *RedisCtl) refresh(ctx context.Context) error {
	a, err := r.c.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		return err
	}
	r.addr.Store(&a)
	return nil
}

// Main roda os testes e remove os containers compartilhados. Só faz falta com o Ryuk desligado.
func Main(m *testing.M) {
	code := m.Run()
	if pgAdmin != nil {
		pgAdmin.Close()
	}
	_ = testcontainers.TerminateContainer(pgC)
	_ = testcontainers.TerminateContainer(rdC)
	os.Exit(code)
}

// Igual a testcontainers.SkipIfProviderIsNotHealthy (que só aceita *testing.T), checado uma vez por binário.
func needDocker(t testing.TB) {
	t.Helper()
	dockerOnce.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				dockerErr = fmt.Errorf("%v", r)
			}
		}()
		p, err := testcontainers.ProviderDocker.GetProvider()
		if err == nil {
			err = p.Health(context.Background())
		}
		dockerErr = err
	})
	if dockerErr != nil {
		t.Skip("testdb: sem Docker:", dockerErr)
	}
}
