package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func (e *env) exec(q string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), q, args...); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) deleteMe(access string, body map[string]string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(body)
	return e.req("DELETE", "/v1/me", string(b), access)
}

// leftovers conta, em toda coluna com FK para users, as linhas que ainda apontam para id: tabela nova entra sozinha.
func (e *env) leftovers(id string) map[string]int {
	e.t.Helper()
	ctx := context.Background()
	rows, err := e.pool.Query(ctx, `SELECT c.conrelid::regclass::text, a.attname FROM pg_constraint c
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
		WHERE c.contype = 'f' AND c.confrelid = 'users'::regclass`)
	if err != nil {
		e.t.Fatal(err)
	}
	type fk struct{ Table, Col string }
	fks, err := pgx.CollectRows(rows, pgx.RowToStructByPos[fk])
	if err != nil || len(fks) < 10 {
		e.t.Fatalf("FKs para users: %v %v", fks, err)
	}
	fks = append(fks, fk{"users", "id"})
	out := map[string]int{}
	for _, f := range fks {
		var n int
		q := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = $1", pgx.Identifier{f.Table}.Sanitize(),
			pgx.Identifier{f.Col}.Sanitize())
		if err := e.pool.QueryRow(ctx, q, id).Scan(&n); err != nil {
			e.t.Fatal(err)
		}
		if n > 0 {
			out[f.Table+"."+f.Col] = n
		}
	}
	return out
}

type hooks struct {
	deleted, revoked []string
	ex               []string
	friendsChanged   [][2]string
	groups           map[string][]string
}

func (e *env) record() *hooks {
	h := &hooks{groups: map[string][]string{}}
	e.h.OnDelete = func(u string, ex []string) { h.deleted, h.ex = append(h.deleted, u), ex }
	e.h.OnRevoke = func(u string) { h.revoked = append(h.revoked, u) }
	e.h.OnFriendsChange = func(a, b string) { h.friendsChanged = append(h.friendsChanged, [2]string{a, b}) }
	e.h.OnGroupsChange = func(g string, users []string) { h.groups[g] = users }
	return h
}

func TestDeleteAccount(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.register("ana@ex.com")
	sessao2 := wantTokens(t, e.post("/v1/auth/login", cred("ana@ex.com", "senha-forte-1")), 200)
	m := e.me(a.Access)
	id := m.ID
	if !e.meHasPassword(a.Access) {
		t.Fatal("has_password = false numa conta com senha")
	}
	b, c, d, f := e.me(e.register("bia@ex.com").Access).ID, e.me(e.register("caio@ex.com").Access).ID,
		e.me(e.register("davi@ex.com").Access).ID, e.me(e.register("fabi@ex.com").Access).ID

	e.exec(`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'accepted'), ($1, $3, 'pending'),
		($2, $4, 'accepted')`, id, b, c, f)
	e.exec(`INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2), ($3, $1)`, id, d, f)
	e.exec(`INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_sig,
		kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig, fcm_token)
		VALUES ($1, 1, '\x05', 1, '\x05', '\x00', 1, '\x08', '\x00', 'fcm-da-ana')`, id)
	e.exec(`INSERT INTO one_time_prekeys (user_id, kind, key_id, public_key) VALUES ($1, 'ec', 1, '\x05')`, id)
	e.exec(`INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload) VALUES ($1, $2, 'm1', '\x01'),
		($2, $1, 'm2', '\x01'), ($2, $3, 'm3', '\x01')`, id, b, f)
	var shared, alone string
	ctx := context.Background()
	if err := e.pool.QueryRow(ctx, `INSERT INTO groups (owner_id) VALUES ($1) RETURNING id`, id).Scan(&shared); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `INSERT INTO groups (owner_id) VALUES ($1) RETURNING id`, id).Scan(&alone); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	e.exec(`INSERT INTO group_members (group_id, user_id, joined_at) VALUES ($1, $2, $5), ($1, $3, $6), ($1, $4, $7),
		($8, $2, $5)`, shared, id, b, f, now.Add(-3*time.Hour), now.Add(-2*time.Hour), now.Add(-time.Hour), alone)
	e.rdb.Set(ctx, "loc:"+id, "cifrada", time.Minute)
	h := e.record()

	wantErr(t, e.deleteMe(a.Access, nil), 403, "reauth_required")
	wantErr(t, e.deleteMe(a.Access, map[string]string{"reauth_token": "qualquer"}), 403, "reauth_required")
	wantErr(t, e.deleteMe(a.Access, map[string]string{"password": "errada-123"}), 401, "invalid_credentials")
	wantErr(t, e.req("DELETE", "/v1/me", `nada`, a.Access), 400, "bad_request")
	wantErr(t, e.deleteMe("", map[string]string{"password": "senha-forte-1"}), 401, "unauthorized")
	if len(h.deleted)+len(h.revoked) != 0 || len(e.leftovers(id)) == 0 {
		t.Fatal("tentativa recusada apagou ou avisou")
	}

	if w := e.deleteMe(a.Access, map[string]string{"password": "senha-forte-1"}); w.Code != 204 {
		t.Fatalf("DELETE: %d %s", w.Code, w.Body.String())
	}
	if left := e.leftovers(id); len(left) != 0 {
		t.Fatalf("sobrou: %v", left)
	}

	slices.Sort(h.ex)
	want := []string{b, c}
	slices.Sort(want)
	if !slices.Equal(h.deleted, []string{id}) || !slices.Equal(h.revoked, []string{id}) || !slices.Equal(h.ex, want) {
		t.Fatalf("avisos: deleted %v revoked %v ex %v", h.deleted, h.revoked, h.ex)
	}
	if len(h.friendsChanged) != 2 {
		t.Fatalf("recargas de amigos: %v", h.friendsChanged)
	}
	if u := h.groups[shared]; !slices.Contains(u, b) || !slices.Contains(u, f) || !slices.Contains(u, id) {
		t.Fatalf("GroupChanged do grupo que fica: %v (todos %v)", u, h.groups)
	}

	var owner string
	var members int
	if err := e.pool.QueryRow(ctx, `SELECT owner_id::text, (SELECT count(*) FROM group_members WHERE group_id = $1)
		FROM groups WHERE id = $1`, shared).Scan(&owner, &members); err != nil || owner != b || members != 2 {
		t.Fatalf("grupo que fica: dono %s, %d membros, %v", owner, members, err)
	}
	if n := e.count(`SELECT count(*) FROM groups WHERE id = $1`, alone); n != 0 {
		t.Fatal("grupo só da conta apagada sobrou")
	}
	if e.count(`SELECT count(*) FROM friendships WHERE requester_id = $1 AND addressee_id = $2`, b, f) != 1 ||
		e.count(`SELECT count(*) FROM envelopes WHERE sender_id = $1`, f) != 1 {
		t.Fatal("apagou dados dos outros")
	}
	e.me(e.login("bia@ex.com").Access)

	wantErr(t, e.deleteMe(a.Access, map[string]string{"password": "senha-forte-1"}), 410, "account_deleted")
	wantErr(t, e.req("GET", "/v1/me", "", sessao2.Access), 410, "account_deleted")
	rb, _ := json.Marshal(map[string]string{"refresh_token": sessao2.Refresh})
	wantErr(t, e.post("/v1/auth/refresh", string(rb)), 410, "account_deleted")
	wantErr(t, e.post("/v1/auth/refresh", `{"refresh_token":"nunca-existiu"}`), 401, "unauthorized")
	if ttl := e.rdb.TTL(ctx, goneKey(sha(sessao2.Refresh))).Val(); ttl < 29*24*time.Hour {
		t.Fatalf("TTL da lápide do refresh: %v", ttl)
	}
	wantTokens(t, e.post("/v1/auth/register", credName("ana@ex.com", "outra-senha-1", *m.Username)), 201)
}

func (e *env) login(email string) tokenResp {
	e.t.Helper()
	return wantTokens(e.t, e.post("/v1/auth/login", cred(email, "senha-forte-1")), 200)
}

func (e *env) count(q string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) meHasPassword(access string) bool {
	e.t.Helper()
	var m struct {
		HasPassword *bool `json:"has_password"`
	}
	w := e.req("GET", "/v1/me", "", access)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &m) != nil || m.HasPassword == nil {
		e.t.Fatalf("me: %d %s", w.Code, w.Body.String())
	}
	return *m.HasPassword
}

func (e *env) reauthExchange(code, verifier, bearer string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(map[string]any{"code": code, "code_verifier": verifier, "reauth": true})
	return e.req("POST", "/v1/auth/google/exchange", string(b), bearer)
}

func (e *env) reauth(googleCode, bearer string) *httptest.ResponseRecorder {
	e.t.Helper()
	v, ch := pkce()
	back := e.googleFlow("&reauth=1", googleCode, ch, fmt.Sprintf("app-reauth-%08d", ipSeq.Add(1)))
	return e.reauthExchange(back.Query().Get("code"), v, bearer)
}

func reauthToken(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var r struct {
		Token     string `json:"reauth_token"`
		ExpiresIn int    `json:"expires_in"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &r) != nil || r.Token == "" || r.ExpiresIn != 300 {
		t.Fatalf("reauth: %d %s", w.Code, w.Body.String())
	}
	return r.Token
}

func TestDeleteGoogleAccount(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.h.svc.google = fakeGoogle
	ctx := context.Background()
	gil := func(code string) tokenResp {
		v, ch := pkce()
		back := e.googleCallback(code, ch, fmt.Sprintf("app-login-%08d", ipSeq.Add(1)))
		return wantTokens(t, e.exchange(back.Query().Get("code"), v), 200)
	}
	g := gil("sub-gil|gil@ex.com")
	id := e.me(g.Access).ID
	if e.meHasPassword(g.Access) {
		t.Fatal("has_password = true numa conta só-Google")
	}
	outra := gil("sub-iris|iris@ex.com")
	users := e.count(`SELECT count(*) FROM users`)

	wantErr(t, e.deleteMe(g.Access, nil), 403, "reauth_required")
	wantErr(t, e.deleteMe(g.Access, map[string]string{"password": "qualquer-1"}), 403, "reauth_required")
	wantErr(t, e.reauth("sub-gil|gil@ex.com", ""), 401, "unauthorized")
	wantErr(t, e.reauth("sub-iris|iris@ex.com", g.Access), 403, "reauth_mismatch")
	wantErr(t, e.reauth("sub-novo|novo@ex.com", g.Access), 403, "reauth_mismatch")
	if n := e.count(`SELECT count(*) FROM users`); n != users {
		t.Fatalf("o callback de reauth criou conta: %d → %d", users, n)
	}

	v, ch := pkce()
	back := e.googleCallback("sub-gil|gil@ex.com", ch, "app-login-cruzado1")
	wantErr(t, e.reauthExchange(back.Query().Get("code"), v, g.Access), 400, "invalid_grant")
	v, ch = pkce()
	back = e.googleFlow("&reauth=1", "sub-gil|gil@ex.com", ch, "app-reauth-cruzado")
	wantErr(t, e.exchange(back.Query().Get("code"), v), 400, "invalid_grant")
	if w := e.req("GET", "/v1/auth/google/start?code_challenge="+ch+"&code_challenge_method=S256&state=app-reauth-ruim-1"+
		"&reauth=sim", "", ""); w.Code != 400 {
		t.Fatalf("reauth=sim: %d", w.Code)
	}

	deOutra := reauthToken(t, e.reauth("sub-iris|iris@ex.com", outra.Access))
	wantErr(t, e.deleteMe(g.Access, map[string]string{"reauth_token": deOutra}), 403, "reauth_required")
	wantErr(t, e.deleteMe(outra.Access, map[string]string{"reauth_token": deOutra}), 403, "reauth_required")

	tok := reauthToken(t, e.reauth("sub-gil|gil@ex.com", g.Access))
	if ttl := e.rdb.TTL(ctx, "reauth:"+tok).Val(); ttl <= 290*time.Second || ttl > 300*time.Second {
		t.Fatalf("TTL do reauth_token: %v", ttl)
	}
	if w := e.deleteMe(g.Access, map[string]string{"reauth_token": tok}); w.Code != 204 {
		t.Fatalf("DELETE: %d %s", w.Code, w.Body.String())
	}
	if left := e.leftovers(id); len(left) != 0 {
		t.Fatalf("sobrou: %v", left)
	}
	if n := e.rdb.Exists(ctx, "reauth:"+tok).Val(); n != 0 {
		t.Fatal("reauth_token sobrou no Redis")
	}
	wantErr(t, e.deleteMe(g.Access, map[string]string{"reauth_token": tok}), 410, "account_deleted")
	e.me(outra.Access)
	if e.me(gil("sub-gil|gil@ex.com").Access).ID == id {
		t.Fatal("conta apagada voltou")
	}
}

func TestDeleteRateLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.register("rui@ex.com")
	for i := range 5 {
		wantErr(t, e.deleteMe(a.Access, map[string]string{"password": fmt.Sprintf("errada-%d-x", i)}), 401,
			"invalid_credentials")
	}
	wantErr(t, e.deleteMe(a.Access, map[string]string{"password": "senha-forte-1"}), 429, "rate_limited")
	e.me(a.Access)
}
