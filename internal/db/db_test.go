package db

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	pgOnce sync.Once
	pgURL  string
	pgErr  error
	dbSeq  atomic.Int32
)

// newDB: um container por binário de teste (o ryuk o derruba no fim) e um banco novo por teste.
func newDB(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	pgOnce.Do(func() {
		pg, err := postgres.Run(ctx, "postgres:17-alpine",
			postgres.WithDatabase("t"), postgres.WithUsername("t"), postgres.WithPassword("t"),
			// o deadline padrão do WithWaitStrategy é 60 s: pouco com o Docker carregado pelos outros testes
			testcontainers.WithWaitStrategyAndDeadline(3*time.Minute, wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(3*time.Minute)))
		if err != nil {
			pgErr = err
			return
		}
		pgURL, pgErr = pg.ConnectionString(ctx, "sslmode=disable")
	})
	if pgErr != nil {
		t.Fatal(pgErr)
	}
	admin, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	name := fmt.Sprintf("m%d", dbSeq.Add(1))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	return strings.Replace(pgURL, "/t?", "/"+name+"?", 1)
}

func openDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := Open(context.Background(), newDB(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func version(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var v int
	if err := pool.QueryRow(context.Background(), `SELECT coalesce(max(version), 0) FROM schema_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// indexValid: -1 = não existe, 0 = INVALID, 1 = válido.
func indexValid(t *testing.T, pool *pgxpool.Pool, name string) int {
	t.Helper()
	var valid *bool
	err := pool.QueryRow(context.Background(), `SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)`,
		name).Scan(&valid)
	switch {
	case err == pgx.ErrNoRows:
		return -1
	case err != nil:
		t.Fatal(err)
	case *valid:
		return 1
	}
	return 0
}

func noTx(sql string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(noTxMarker + "\n" + sql)} }
func tx(sql string) *fstest.MapFile   { return &fstest.MapFile{Data: []byte(sql)} }

const table = `CREATE TABLE t (id serial PRIMARY KEY, v int); INSERT INTO t (v) SELECT g FROM generate_series(1, 1000) g;`

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	url := newDB(t)
	if p, err := Open(ctx, url+"&statement_timeout=1500"); err != nil {
		t.Fatal(err)
	} else {
		var got string
		_ = p.QueryRow(ctx, "SHOW statement_timeout").Scan(&got)
		p.Close()
		if got != "1500ms" {
			t.Errorf("statement_timeout do DSN ignorado: %q", got)
		}
	}
	pool, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	for name, want := range map[string]string{"statement_timeout": statementTimeout,
		"idle_in_transaction_session_timeout": idleInTxTimeout} {
		var got string
		if err := pool.QueryRow(ctx, "SHOW "+name).Scan(&got); err != nil || got != want {
			t.Errorf("%s = %q (%v), want %q", name, got, err, want)
		}
	}
	for range 2 {
		if err := Migrate(ctx, pool); err != nil {
			t.Fatal(err)
		}
	}
	var n, v int
	if err := pool.QueryRow(ctx, `SELECT count(*), max(version) FROM schema_version`).Scan(&n, &v); err != nil {
		t.Fatal(err)
	}
	files, _ := fs.Glob(migrations, "migrations/*.sql")
	if n != len(files) || v != len(files) {
		t.Fatalf("schema_version: %d linhas, versão %d; want %d", n, v, len(files))
	}

	var a, b string
	ins := `INSERT INTO users (email, friend_code) VALUES ($1, $2) RETURNING id`
	if err := pool.QueryRow(ctx, ins, "a@x", "0123ABCD").Scan(&a); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, ins, "b@x", "0123ABCE").Scan(&b); err != nil {
		t.Fatal(err)
	}
	for name, q := range map[string]string{
		"friend_code fora do Crockford": `INSERT INTO users (email, friend_code) VALUES ('c@x', '0123ABCI')`,
		"usuário sem email nem google":  `INSERT INTO users (friend_code) VALUES ('0123ABCF')`,
		"amizade consigo":               `INSERT INTO friendships (requester_id, addressee_id, status) VALUES ('` + a + `', '` + a + `', 'pending')`,
		"kyber sem assinatura":          `INSERT INTO one_time_prekeys (user_id, kind, key_id, public_key) VALUES ('` + a + `', 'kyber', 1, '\x08')`,
	} {
		if _, err := pool.Exec(ctx, q); err == nil {
			t.Errorf("%s: aceito", name)
		}
	}
	pair := `INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'pending')`
	if _, err := pool.Exec(ctx, pair, a, b); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, pair, b, a); err == nil {
		t.Error("par de amizade duplicado na direção inversa: aceito")
	}
}

// Com o índice em construção (preso esperando uma transação antiga), outra escrita na tabela passa na hora. Um
// CREATE INDEX comum estaria na fila do lock e a escrita ficaria atrás dele.
func TestNoTxDoesNotBlockWrites(t *testing.T) {
	ctx := context.Background()
	pool := openDB(t)
	mfs := fstest.MapFS{"migrations/0001_t.sql": tx(table)}
	if err := migrate(ctx, pool, mfs); err != nil {
		t.Fatal(err)
	}
	old, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(ctx, `INSERT INTO t (v) VALUES (0)`); err != nil {
		t.Fatal(err)
	}
	mfs["migrations/0002_i.sql"] = noTx("CREATE INDEX CONCURRENTLY IF NOT EXISTS t_v ON t (v);\n")
	done := make(chan error, 1)
	go func() { done <- migrate(ctx, pool, mfs) }()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE query LIKE 'CREATE INDEX CONCURRENTLY%' AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("o CREATE INDEX CONCURRENTLY não chegou a esperar a transação antiga")
		}
	}
	wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := pool.Exec(wctx, `INSERT INTO t (v) VALUES (1)`); err != nil {
		t.Fatalf("escrita bloqueada durante a construção do índice: %v", err)
	}
	if err := old.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if indexValid(t, pool, "t_v") != 1 || version(t, pool) != 2 {
		t.Fatal("índice ou versão não ficaram certos")
	}
}

func TestConcurrentMigrators(t *testing.T) {
	ctx := context.Background()
	pool := openDB(t)
	mfs := fstest.MapFS{
		"migrations/0001_t.sql": tx(table),
		"migrations/0002_i.sql": noTx("CREATE INDEX CONCURRENTLY IF NOT EXISTS t_v ON t (v);\n"),
	}
	errs := make(chan error, 2)
	for range 2 {
		go func() { errs <- migrate(ctx, pool, mfs) }()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_version`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("schema_version com %d linhas (%v), want 2", n, err)
	}
	if indexValid(t, pool, "t_v") != 1 {
		t.Fatal("índice não ficou válido")
	}
}

// Um CONCURRENTLY que falha deixa o índice INVALID e não grava a versão; na repetição, ele é apagado e recriado.
func TestInvalidIndexRecreated(t *testing.T) {
	ctx := context.Background()
	pool := openDB(t)
	mfs := fstest.MapFS{
		"migrations/0001_t.sql": tx(`CREATE TABLE t (v int); INSERT INTO t VALUES (1), (1);`),
		"migrations/0002_u.sql": noTx("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS t_u ON t (v);\n"),
	}
	if err := migrate(ctx, pool, mfs); err == nil {
		t.Fatal("índice único sobre duplicata: aceito")
	}
	if v, valid := version(t, pool), indexValid(t, pool, "t_u"); v != 1 || valid != 0 {
		t.Fatalf("depois da falha: versão %d e índice %d; want 1 e INVALID", v, valid)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM t WHERE ctid = (SELECT ctid FROM t LIMIT 1)`); err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, pool, mfs); err != nil {
		t.Fatal(err)
	}
	if v, valid := version(t, pool), indexValid(t, pool, "t_u"); v != 2 || valid != 1 {
		t.Fatalf("depois de repetir: versão %d e índice %d; want 2 e válido", v, valid)
	}
}

func TestNoTxRequiresIfNotExists(t *testing.T) {
	ctx := context.Background()
	pool := openDB(t)
	mfs := fstest.MapFS{
		"migrations/0001_t.sql": tx(table),
		"migrations/0002_i.sql": noTx("CREATE INDEX CONCURRENTLY t_v ON t (v);\n"),
	}
	if err := migrate(ctx, pool, mfs); err == nil || !strings.Contains(err.Error(), "IF NOT EXISTS") {
		t.Fatalf("CONCURRENTLY sem IF NOT EXISTS: %v", err)
	}
	if v := version(t, pool); v != 1 {
		t.Fatalf("versão %d, want 1", v)
	}
}

func TestStatements(t *testing.T) {
	got := statements(noTxMarker + "\n-- comentário\nCREATE INDEX a\n  ON t (v);\n\nCREATE INDEX b ON t (w);")
	if len(got) != 2 || !strings.Contains(got[0], "ON t (v);") || !strings.Contains(got[1], "b ON t (w)") {
		t.Fatalf("%q", got)
	}
}
