package testdb_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestPoolIsolado(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	a, b := testdb.Pool(t), testdb.Pool(t)

	var v int
	if err := a.QueryRow(ctx, `SELECT max(version) FROM schema_version`).Scan(&v); err != nil || v < 1 {
		t.Fatalf("template sem migração: v=%d err=%v", v, err)
	}
	if _, err := a.Exec(ctx, `INSERT INTO users (email, friend_code) VALUES ('a@x', '0123ABCD')`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := b.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("banco b vê %d usuários (err=%v)", n, err)
	}
}

func TestRedisIsolado(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	a, b := testdb.Redis(t), testdb.Redis(t)
	if err := a.Set(ctx, "loc:x", "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := b.Get(ctx, "loc:x").Err(); err != redis.Nil {
		t.Fatalf("redis b vê a chave do a: %v", err)
	}
}

func TestStoppableRedis(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c, ctl := testdb.StoppableRedis(t)
	if err := c.Set(ctx, "loc:x", "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	ctl.Stop(t)
	if err := c.Get(ctx, "loc:x").Err(); err == nil || err == redis.Nil {
		t.Fatalf("redis fora respondeu: %v", err)
	}
	ctl.Start(t)
	if err := c.Get(ctx, "loc:x").Err(); err != redis.Nil {
		t.Fatalf("depois do Start: %v (esperado vazio)", err)
	}
	if err := c.Set(ctx, "loc:x", "2", 0).Err(); err != nil {
		t.Fatal(err)
	}
}
