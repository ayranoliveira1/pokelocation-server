package pogo

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestIndices(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	checkIndices(t, pool)
	// qQueueUsers (reconstrução) lê todas as regras ativas: varre o índice parcial inteiro, sem Index Cond.
	for _, q := range []string{qQueueUsers, qQueueUsersShared} {
		partial := false
		for _, n := range append(testdb.Plan(t, pool, q), testdb.GenericPlan(t, pool, q)...) {
			if n.Type == "Seq Scan" || testdb.IsSort(n) {
				t.Errorf("%s: %s em %s", q, n.Type, n.Relation)
			}
			partial = partial || n.Index == "pogo_rules_enabled"
		}
		if !partial {
			t.Errorf("%s não usa pogo_rules_enabled", q)
		}
	}
}

func TestIndicesSeed(t *testing.T) { checkIndices(t, testdb.Seed(t)) }

// qUpsertAccount e qInsertRule ficam de fora: INSERT de VALUES não lê tabela (o ON CONFLICT usa a PK).
func checkIndices(t *testing.T, pool *pgxpool.Pool) {
	s := testdb.SampleOf(t, pool)
	enc := []byte{1}
	testdb.RequireIndex(t, pool, qAccount, []any{s.User}, "pogo_accounts_pkey")
	testdb.RequireIndex(t, pool, qDeleteAccount, []any{s.User}, "pogo_accounts_pkey")
	testdb.RequireIndex(t, pool, qDeleteToken, []any{s.User, enc}, "pogo_accounts_pkey")
	testdb.RequireIndex(t, pool, qUpdateProfile, []any{s.User, enc, "n", []byte(`[]`)}, "pogo_accounts_pkey")
	const anyRule = "00000000-0000-4000-8000-000000000000"
	testdb.RequireIndex(t, pool, qCountRules, []any{s.User}, "pogo_rules_user")
	testdb.RequireIndexNoSort(t, pool, qListRules, []any{s.User, maxRules}, "pogo_rules_user")
	testdb.RequireIndex(t, pool, qUpdateRule, []any{anyRule, s.User, "n", []byte(`{}`), "world", nil, nil, nil, true},
		"pogo_rules_pkey")
	testdb.RequireIndex(t, pool, qDeleteRule, []any{anyRule, s.User}, "pogo_rules_pkey")
	testdb.RequireIndex(t, pool, qDisableRule, []any{anyRule, s.User, "r", []byte(`{}`)}, "pogo_rules_pkey",
		"pogo_rules_enabled")
	testdb.RequireIndex(t, pool, qTrainers, []any{s.User}, "pogo_accounts_pkey")
	testdb.RequireIndex(t, pool, qUpdateTrainers, []any{s.User, enc, []byte(`[]`)}, "pogo_accounts_pkey")
	for _, idx := range []string{"pogo_accounts_pkey", "devices_pkey", "pogo_rules_enabled"} {
		testdb.RequireIndexNoSort(t, pool, qUserRules, []any{s.User}, idx)
	}
}
