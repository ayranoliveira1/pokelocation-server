package testdb

import "testing"

func TestPlanReprovaSemIndice(t *testing.T) {
	t.Parallel()
	pool := Pool(t)
	const none = "00000000-0000-0000-0000-000000000001"
	page := `SELECT id FROM envelopes WHERE recipient_id = $1 AND id > $2 ORDER BY id LIMIT $3`
	friends := `SELECT requester_id FROM friendships WHERE status = 'accepted' AND (requester_id = $1 OR addressee_id = $1)`
	login := `SELECT id FROM users WHERE email = $1`
	for _, c := range []struct {
		name, setup, sql string
		args             []any
		noSort           bool
		want             []string
	}{
		{"índice comum", "DROP INDEX envelopes_recipient", page, []any{none, 0, 50}, true, []string{"envelopes_recipient"}},
		{"um lado do OR", "DROP INDEX friendships_addressee", friends, []any{none}, false,
			[]string{"friendships_requester", "friendships_addressee"}},
		{"índice de UNIQUE", "ALTER TABLE users DROP CONSTRAINT users_email_key", login, []any{"x@y"}, false,
			[]string{"users_email_key"}},
		// O cast tira o predicado do índice: o planejador varre users_pkey inteiro com Filter.
		{"índice varrido com Filter", "", `SELECT id FROM users WHERE id = $1`, []any{none}, false, []string{"users_pkey"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if p := problems(pool, "", c.sql, c.args, c.noSort, c.want); len(p) != 0 {
				t.Fatalf("com o índice: %v", p)
			}
			bad, setup := c.sql, c.setup
			if setup == "" {
				bad = `SELECT id FROM users WHERE id::text = $1`
			}
			if p := problems(pool, setup, bad, c.args, c.noSort, c.want); len(p) == 0 {
				t.Errorf("sem buscar pelo índice (%s): passou", c.name)
			} else {
				t.Logf("sem buscar pelo índice: %v", p)
			}
		})
	}
}
