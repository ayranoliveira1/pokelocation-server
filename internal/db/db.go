// Package db: pool do Postgres e migrações SQL embutidas.
package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// lockID serializa as migrações de instâncias subindo juntas.
const lockID = 0x706f6b656c6f63

// maxConns: o padrão do pgx (max(4, NumCPU)) não aguenta uma onda de reconexão depois de um deploy (todo cliente faz
// o Auth do WS, 4 consultas cada, ao mesmo tempo) e os Auths estouram o prazo. pool_max_conns no DSN tem precedência.
const maxConns = 32

// Teto no servidor para query presa ou transação esquecida aberta segurar conexão do pool sem limite. Os handlers
// têm prazo próprio, menor; isto é a rede de segurança.
const (
	statementTimeout = "10s"
	idleInTxTimeout  = "30s"
)

func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(url, "pool_max_conns") {
		cfg.MaxConns = maxConns
	}
	rp := cfg.ConnConfig.RuntimeParams
	for k, v := range map[string]string{"statement_timeout": statementTimeout,
		"idle_in_transaction_session_timeout": idleInTxTimeout} {
		if _, ok := rp[k]; !ok { // o DSN tem precedência
			rp[k] = v
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// Migrate aplica as migrations/NNNN_nome.sql acima da versão em schema_version, todas numa transação só.
// ponytail: por isso CREATE INDEX vai sem CONCURRENTLY e trava escrita na tabela durante a construção; ok sem
// produção. Com dados reais, índice novo vai como CONCURRENTLY, fora desta transação.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := fs.Glob(migrations, "migrations/*.sql") // ordem lexical = numérica (4 dígitos)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// antes do lock: criar índice em tabela grande passa do statement_timeout, e a instância que espera o lock
		// esperaria o mesmo tempo
		if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = 0`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_version (
			version integer PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		var cur int
		if err := tx.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_version`).Scan(&cur); err != nil {
			return err
		}
		prev := 0
		for _, f := range files {
			v, err := strconv.Atoi(strings.SplitN(path.Base(f), "_", 2)[0])
			if err != nil || v <= prev {
				return fmt.Errorf("migração %s: nome deve ser NNNN_nome.sql com número crescente", f)
			}
			prev = v
			if v <= cur {
				continue
			}
			sql, err := migrations.ReadFile(f)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil { // sem args = protocolo simples, que aceita vários comandos
				return fmt.Errorf("migração %s: %w", f, err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO schema_version (version) VALUES ($1)`, v); err != nil {
				return err
			}
		}
		return nil
	})
}
