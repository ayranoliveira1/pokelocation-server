package testdb_test

import (
	"slices"
	"testing"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestPlan(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	s := testdb.SampleOf(t, pool)
	testdb.RequireIndex(t, pool, `SELECT id FROM users WHERE email = $1`, []any{s.Email}, "users_email_key")

	// Sem índice que sirva, nem com seqscan desligado o plano sai do Seq Scan: é assim que um índice sumido reprova.
	seq := func(n testdb.PlanNode) bool { return n.Type == "Seq Scan" }
	if !slices.ContainsFunc(testdb.Plan(t, pool, `SELECT id FROM users WHERE password_hash = $1`, "x"), seq) {
		t.Error("query sem índice não deu Seq Scan")
	}
	if !slices.ContainsFunc(testdb.GenericPlan(t, pool, `SELECT id FROM users WHERE password_hash = $1`), seq) {
		t.Error("plano genérico de query sem índice não deu Seq Scan")
	}
	testdb.RequireIndexNoSort(t, pool, `SELECT id FROM envelopes WHERE recipient_id = $1 AND id > $2 ORDER BY id LIMIT $3`,
		[]any{s.Recipient, 0, 50}, "envelopes_recipient")
	testdb.RequireIndexNoSort(t, pool, `DELETE FROM envelopes WHERE id = ANY(ARRAY(SELECT id FROM envelopes
		WHERE created_at < now() - interval '30 days' ORDER BY created_at LIMIT $1))`, []any{5000}, "envelopes_created_at")
}

func TestPlanSeed(t *testing.T) {
	pool := testdb.Seed(t)
	s := testdb.SampleOf(t, pool)
	testdb.RequireIndex(t, pool, `SELECT id FROM users WHERE email = $1`, []any{s.Email}, "users_email_key")
	testdb.RequireIndex(t, pool, `SELECT (CASE WHEN requester_id = $1 THEN addressee_id ELSE requester_id END)::text
		FROM friendships WHERE status = 'accepted' AND (requester_id = $1 OR addressee_id = $1)`, []any{s.User},
		"friendships_requester", "friendships_addressee")
}
