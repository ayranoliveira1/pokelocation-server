package db

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Postgres real: a migração sobe, é idempotente e as constraints do schema valem.
func TestMigrate(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("t"), postgres.WithUsername("t"), postgres.WithPassword("t"),
		// o deadline padrão do WithWaitStrategy é 60 s: pouco com o Docker carregado pelos outros testes
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute, wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(3*time.Minute)))
	testcontainers.CleanupContainer(t, pg)
	if err != nil {
		t.Fatal(err)
	}
	url, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

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
