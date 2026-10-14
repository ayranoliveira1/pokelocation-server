package testdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PlanNode é um nó do EXPLAIN, achatado (Plan devolve a árvore em pré-ordem).
type PlanNode struct {
	Type     string     `json:"Node Type"`
	Relation string     `json:"Relation Name"`
	Index    string     `json:"Index Name"`
	Plans    []PlanNode `json:"Plans"`
}

// Pools vindos de Seed: planejador de verdade. Os demais (banco vazio) planejam com seqscan desligado, porque num banco
// vazio o Postgres escolhe Seq Scan de qualquer jeito; assim só sobra Seq Scan se nenhum índice serve à query.
var seeded sync.Map // *pgxpool.Pool → []string (tabelas grandes)

// Seed devolve o banco de loadtest/seed (S12_DATABASE_URL) ou pula o teste.
func Seed(t testing.TB) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("S12_DATABASE_URL")
	if url == "" {
		t.Skip("sem S12_DATABASE_URL (seed de loadtest/seed)")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal("testdb:", err)
	}
	t.Cleanup(pool.Close)
	rows, _ := pool.Query(ctx, `SELECT relname FROM pg_class WHERE relkind = 'r'
		AND relnamespace = 'public'::regnamespace AND reltuples >= 10000`)
	big, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal("testdb:", err)
	}
	seeded.Store(pool, big)
	return pool
}

// Plan devolve o plano de sql com os valores dados.
func Plan(t testing.TB, pool *pgxpool.Pool, sql string, args ...any) []PlanNode {
	t.Helper()
	custom, _, err := plans(pool, "", sql, args, true)
	if err != nil {
		t.Fatal("testdb:", err)
	}
	return custom
}

// GenericPlan devolve o plano genérico, o que o Postgres passa a usar para prepared statements (o pgx prepara tudo).
func GenericPlan(t testing.TB, pool *pgxpool.Pool, sql string) []PlanNode {
	t.Helper()
	_, generic, err := plans(pool, "", sql, nil, false)
	if err != nil {
		t.Fatal("testdb:", err)
	}
	return generic
}

// RequireIndex reprova se, no plano com os valores ou no genérico, nenhum dos índices aparece ou há Seq Scan (no seed,
// só em tabela grande). Para exigir vários índices juntos, chame uma vez por índice.
func RequireIndex(t testing.TB, pool *pgxpool.Pool, sql string, args []any, anyOf ...string) {
	t.Helper()
	for _, p := range problems(pool, "", sql, args, false, anyOf) {
		t.Error(p)
	}
}

// RequireIndexNoSort: RequireIndex e, além disso, a ordem tem de vir do índice (queries paginadas, ORDER BY … LIMIT).
func RequireIndexNoSort(t testing.TB, pool *pgxpool.Pool, sql string, args []any, anyOf ...string) {
	t.Helper()
	for _, p := range problems(pool, "", sql, args, true, anyOf) {
		t.Error(p)
	}
}

// problems devolve o que reprova a query; setup roda na mesma transação antes do EXPLAIN (o teste de sanidade derruba
// um índice ali).
func problems(pool *pgxpool.Pool, setup, sql string, args []any, noSort bool, anyOf []string) []string {
	// Sem Sort, a pergunta é se o índice entrega a ordem. Num banco vazio, com seqscan off, o planejador prefere
	// Bitmap Index Scan + Sort (ordenar 0 linhas custa nada), e bitmap nunca entrega ordem: desliga também. No
	// RequireIndex não, porque as queries com OR precisam do BitmapOr; no seed também não (planejador de verdade).
	big, isSeed := seeded.Load(pool)
	if noSort && !isSeed {
		setup = "SET LOCAL enable_bitmapscan = off; " + setup
	}
	custom, generic, err := plans(pool, setup, sql, args, true)
	if err != nil {
		return []string{"testdb: " + err.Error()}
	}
	var out []string
	for kind, nodes := range map[string][]PlanNode{"plano com os valores": custom, "plano genérico": generic} {
		var used []string
		for _, n := range nodes {
			if n.Type == "Seq Scan" && (!isSeed || slices.Contains(big.([]string), n.Relation)) {
				out = append(out, fmt.Sprintf("%s: Seq Scan em %s", kind, n.Relation))
			}
			if noSort && IsSort(n) {
				out = append(out, fmt.Sprintf("%s: %s, a ordem não veio do índice", kind, n.Type))
			}
			if n.Index != "" {
				used = append(used, n.Index)
			}
		}
		if !slices.ContainsFunc(anyOf, func(i string) bool { return slices.Contains(used, i) }) {
			out = append(out, fmt.Sprintf("%s: nenhum de %v (usados: %v)", kind, anyOf, used))
		}
	}
	sort.Strings(out)
	return out
}

// plans faz os dois EXPLAIN numa transação desfeita: DELETE e FOR UPDATE só são planejados, e o SET LOCAL e o setup
// morrem junto. Só o SQL do store vai concatenado ao EXPLAIN; os valores seguem como parâmetros do pgx.
func plans(pool *pgxpool.Pool, setup, sql string, args []any, withCustom bool) (custom, generic []PlanNode, err error) {
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, isSeed := seeded.Load(pool); !isSeed {
		if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
			return nil, nil, err
		}
	}
	if setup != "" {
		if _, err := tx.Exec(ctx, setup); err != nil {
			return nil, nil, err
		}
	}
	if withCustom {
		var out []byte
		if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&out); err != nil {
			return nil, nil, fmt.Errorf("EXPLAIN: %w", err)
		}
		if custom, err = flatten(out); err != nil {
			return nil, nil, err
		}
	}
	// GENERIC_PLAN deixa os $n sem valor: vai cru pelo PgConn, porque o pgx tentaria preenchê-los.
	res, err := tx.Conn().PgConn().Exec(ctx, "EXPLAIN (FORMAT JSON, GENERIC_PLAN) "+sql).ReadAll()
	if err != nil || len(res) == 0 || len(res[0].Rows) == 0 {
		return nil, nil, fmt.Errorf("EXPLAIN genérico: %v", err)
	}
	generic, err = flatten(res[0].Rows[0][0])
	return custom, generic, err
}

func flatten(out []byte) ([]PlanNode, error) {
	var doc []struct {
		Plan PlanNode `json:"Plan"`
	}
	if err := json.Unmarshal(out, &doc); err != nil || len(doc) == 0 {
		return nil, fmt.Errorf("plano ilegível: %v %s", err, out)
	}
	var all []PlanNode
	var walk func(n PlanNode)
	walk = func(n PlanNode) {
		all = append(all, n)
		for _, c := range n.Plans {
			walk(c)
		}
	}
	walk(doc[0].Plan)
	return all, nil
}

// Sample: valores que tornam o plano representativo. No seed, o usuário com mais amigos e o destinatário com a maior
// fila (onde um índice ruim mais pesa); num banco vazio, valores que não existem, e o plano sai igual.
type Sample struct {
	User, Friend, Email, GoogleSub, FriendCode, Family, Recipient, Sender, MsgID, KeyUser string
	TokenHash                                                                             []byte
}

var samples sync.Map // *pgxpool.Pool → Sample

func SampleOf(t testing.TB, pool *pgxpool.Pool) Sample {
	t.Helper()
	if s, ok := samples.Load(pool); ok {
		return s.(Sample)
	}
	const none = "00000000-0000-0000-0000-000000000001"
	var s Sample
	err := pool.QueryRow(context.Background(), `
		WITH top AS (SELECT u, count(*) FROM (SELECT requester_id u FROM friendships UNION ALL
		             SELECT addressee_id FROM friendships) x GROUP BY u ORDER BY 2 DESC LIMIT 1),
		     q AS (SELECT recipient_id, max(sender_id::text) s, max(client_msg_id) m FROM envelopes
		           GROUP BY 1 ORDER BY count(*) DESC LIMIT 1)
		SELECT coalesce((SELECT u::text FROM top), $1),
		       coalesce((SELECT CASE WHEN requester_id = top.u THEN addressee_id ELSE requester_id END::text
		                 FROM friendships, top WHERE requester_id = top.u OR addressee_id = top.u LIMIT 1), $1),
		       coalesce((SELECT email FROM users, top WHERE id = top.u), 'x@y'),
		       coalesce((SELECT google_sub FROM users WHERE google_sub IS NOT NULL LIMIT 1), 'g'),
		       coalesce((SELECT friend_code FROM users, top WHERE id = top.u), '0123ABCD'),
		       coalesce((SELECT family_id::text FROM refresh_tokens LIMIT 1), $1),
		       coalesce((SELECT recipient_id::text FROM q), $1), coalesce((SELECT s FROM q), $1),
		       coalesce((SELECT m FROM q), 'm'), coalesce((SELECT user_id::text FROM one_time_prekeys LIMIT 1), $1),
		       coalesce((SELECT token_hash FROM refresh_tokens LIMIT 1), '\x01'::bytea)`, none).Scan(
		&s.User, &s.Friend, &s.Email, &s.GoogleSub, &s.FriendCode, &s.Family, &s.Recipient, &s.Sender, &s.MsgID,
		&s.KeyUser, &s.TokenHash)
	if err != nil {
		t.Fatal("testdb: amostra:", err)
	}
	samples.Store(pool, s)
	return s
}

// IsSort: nó de ordenação (Sort, Incremental Sort).
func IsSort(n PlanNode) bool { return strings.HasSuffix(n.Type, "Sort") }
