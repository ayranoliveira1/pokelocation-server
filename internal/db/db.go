// Package db: pool do Postgres e migrações SQL embutidas.
package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

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

// noTxMarker na 1ª linha: a migração roda fora de transação, um comando por vez (exigido por CREATE INDEX
// CONCURRENTLY). Índice novo em tabela que já existe vai sempre assim, para não travar escrita durante a construção.
// A versão só é gravada no fim, então todo comando tem de ser idempotente (a migração se repete depois de uma falha).
const noTxMarker = "-- migrate:no-transaction"

// Migrate aplica as migrations/NNNN_nome.sql acima da versão em schema_version, cada uma na sua transação (ou sem
// transação, com noTxMarker). A versão só é gravada depois que a migração inteira deu certo.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error { return migrate(ctx, pool, migrations) }

func migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) error {
	files, err := fs.Glob(fsys, "migrations/*.sql") // ordem lexical = numérica (4 dígitos)
	if err != nil {
		return err
	}
	pc, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// conexão própria, fechada no fim: o statement_timeout = 0 e o lock de sessão não voltam ao pool
	conn := pc.Hijack()
	defer conn.Close(context.WithoutCancel(ctx))
	// antes do lock: quem espera outra instância construir um índice esperaria além do prazo
	if _, err := conn.Exec(ctx, `SET statement_timeout = 0`); err != nil {
		return err
	}
	if err := lock(ctx, conn); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_version (
		version integer PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	var cur int
	if err := conn.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_version`).Scan(&cur); err != nil {
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
		b, err := fs.ReadFile(fsys, f)
		if err != nil {
			return err
		}
		const record = `INSERT INTO schema_version (version) VALUES ($1)`
		sql := string(b)
		if strings.HasPrefix(sql, noTxMarker) {
			// sem transação, a versão vem depois: os comandos são idempotentes e a repetição é segura
			if err = applyNoTx(ctx, conn, sql); err == nil {
				_, err = conn.Exec(ctx, record, v)
			}
		} else {
			err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, sql); err != nil { // sem args = protocolo simples, que aceita vários comandos
					return err
				}
				_, err := tx.Exec(ctx, record, v)
				return err
			})
		}
		if err != nil {
			return fmt.Errorf("migração %s: %w", f, err)
		}
	}
	return nil
}

var createConcurrently = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+(IF\s+NOT\s+EXISTS\s+)?([\w.]+)`)

// applyNoTx exige IF NOT EXISTS nos índices, para a migração poder ser repetida depois de falhar no meio. Um
// CONCURRENTLY que falha deixa o índice INVALID, que o IF NOT EXISTS manteria para sempre: é apagado e recriado.
func applyNoTx(ctx context.Context, conn *pgx.Conn, sql string) error {
	for _, stmt := range statements(sql) {
		if m := createConcurrently.FindStringSubmatch(stmt); m != nil {
			if m[1] == "" {
				return fmt.Errorf("CREATE INDEX CONCURRENTLY sem IF NOT EXISTS: %s", m[2])
			}
			var invalid bool
			err := conn.QueryRow(ctx, `SELECT NOT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)`,
				m[2]).Scan(&invalid)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if invalid {
				drop := `DROP INDEX CONCURRENTLY IF EXISTS ` + pgx.Identifier(strings.Split(m[2], ".")).Sanitize()
				if _, err := conn.Exec(ctx, drop); err != nil { // #nosec G202 -- identificador escapado
					return err
				}
			}
		}
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// statements separa por ';' no fim da linha e descarta linhas de comentário: migração sem transação não pode ter
// ';' no meio de um comando.
func statements(sql string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
		if strings.HasSuffix(t, ";") {
			out = append(out, cur.String())
			cur.Reset()
		}
	}
	if strings.TrimSpace(cur.String()) != "" {
		out = append(out, cur.String())
	}
	return out
}

// lock pega o lock de sessão em tentativas curtas. Esperar dentro de pg_advisory_lock deixaria um snapshot aberto,
// e o CREATE INDEX CONCURRENTLY da instância que tem o lock espera todo snapshot antigo terminar: deadlock.
func lock(ctx context.Context, conn *pgx.Conn) error {
	for {
		var ok bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockID).Scan(&ok); err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}
