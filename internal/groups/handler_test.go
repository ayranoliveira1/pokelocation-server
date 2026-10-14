package groups

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/friends"
	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

const nobody = "00000000-0000-4000-8000-000000000000"

type change struct {
	group string
	users []string
}

type env struct {
	t       *testing.T
	pool    *pgxpool.Pool
	mux     *http.ServeMux
	h       *Handler
	mu      sync.Mutex
	changes []change
}

func newEnv(t *testing.T) *env {
	t.Helper()
	t.Parallel()
	e := &env{t: t, pool: testdb.Pool(t)}
	e.h = New(e.pool)
	e.h.OnChange = func(g string, us []string) {
		us = slices.Clone(us)
		slices.Sort(us)
		e.mu.Lock()
		e.changes = append(e.changes, change{g, us})
		e.mu.Unlock()
	}
	e.routes()
	return e
}

func (e *env) routes() {
	e.mux = http.NewServeMux()
	e.h.Routes(e.mux, func(h http.HandlerFunc) http.Handler { return h })
}

func (e *env) user() string {
	e.t.Helper()
	var id string
	err := e.pool.QueryRow(context.Background(),
		`INSERT INTO users (email, friend_code) VALUES (gen_random_uuid()::text || '@x', $1) RETURNING id`,
		friends.NewCode()).Scan(&id)
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *env) exec(q string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), q, args...); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) friends(a string, bs ...string) {
	e.t.Helper()
	for _, b := range bs {
		e.exec(`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'accepted')`, a, b)
	}
}

func (e *env) friendsBulk(u string, n int) []string {
	e.t.Helper()
	rows, err := e.pool.Query(context.Background(), `WITH x AS (
		INSERT INTO users (email, friend_code)
		SELECT gen_random_uuid()::text || '@x', upper(substr(md5(random()::text), 1, 8)) FROM generate_series(1, $1)
		RETURNING id),
		f AS (INSERT INTO friendships (requester_id, addressee_id, status) SELECT id, $2, 'accepted' FROM x)
		SELECT id::text FROM x`, n, u)
	if err != nil {
		e.t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			e.t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func (e *env) inGroups(u string, n int) {
	e.exec(`WITH g AS (INSERT INTO groups (owner_id) SELECT $1 FROM generate_series(1, $2) RETURNING id)
		INSERT INTO group_members (group_id, user_id) SELECT id, $1 FROM g`, u, n)
}

func (e *env) do(user, method, path, body string) (int, map[string]any) {
	e.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req = req.WithContext(httpx.WithUser(req.Context(), user))
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	var out map[string]any
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			e.t.Fatalf("%s %s: corpo não é JSON: %q", method, path, w.Body.String())
		}
	}
	return w.Code, out
}

func (e *env) want(user, method, path, body string, status int, code string) map[string]any {
	e.t.Helper()
	got, out := e.do(user, method, path, body)
	if got != status || (code != "" && out["error"] != code) {
		e.t.Fatalf("%s %s: %d %v, want %d %q", method, path, got, out, status, code)
	}
	return out
}

func members(ids ...string) string {
	b, _ := json.Marshal(map[string][]string{"members": ids})
	return string(b)
}

func (e *env) create(owner string, ms ...string) string {
	e.t.Helper()
	return e.want(owner, "POST", "/v1/groups", members(ms...), 201, "")["id"].(string)
}

func (e *env) add(by, group, user string, status int, code string) {
	e.t.Helper()
	e.want(by, "POST", "/v1/groups/"+group+"/members", `{"user_id":"`+user+`"}`, status, code)
}

type group struct {
	ID        string   `json:"id"`
	OwnerID   string   `json:"owner_id"`
	Members   []member `json:"members"`
	CreatedAt int64    `json:"created_at"`
}

type member struct {
	UserID   string  `json:"user_id"`
	Username *string `json:"username"`
}

func (g group) ids() []string {
	out := make([]string, len(g.Members))
	for i, m := range g.Members {
		out[i] = m.UserID
	}
	return out
}

func (e *env) get(user, id string) group {
	e.t.Helper()
	out := e.want(user, "GET", "/v1/groups/"+id, "", 200, "")
	b, _ := json.Marshal(out)
	var g group
	json.Unmarshal(b, &g)
	return g
}

func (e *env) takeChanges() []change {
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.changes
	e.changes = nil
	return c
}

func sorted(ids ...string) []string {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return ids
}

func (e *env) wantChange(group string, users ...string) {
	e.t.Helper()
	c := e.takeChanges()
	if len(c) != 1 || c[0].group != group || !slices.Equal(c[0].users, sorted(users...)) {
		e.t.Fatalf("OnChange: %+v, want 1 com %s %v", c, group, sorted(users...))
	}
}

func (e *env) wantNoChange() {
	e.t.Helper()
	if c := e.takeChanges(); len(c) != 0 {
		e.t.Fatalf("OnChange inesperado: %+v", c)
	}
}

func TestCreateAndGet(t *testing.T) {
	e := newEnv(t)
	a, b, c, d := e.user(), e.user(), e.user(), e.user()
	e.friends(a, b, c)

	e.exec(`UPDATE users SET username = 'bob_1' WHERE id = $1`, b)
	id := e.create(a, b, c, b, a) // repetidos e o próprio dono não duplicam
	e.wantChange(id, a, b, c)
	g := e.get(b, id)
	if g.ID != id || g.OwnerID != a || !slices.Equal(sorted(g.ids()...), sorted(a, b, c)) || g.CreatedAt < 1e12 {
		t.Fatalf("GET: %+v", g)
	}
	for _, m := range g.Members {
		if (m.UserID == b) != (m.Username != nil && *m.Username == "bob_1") {
			t.Fatalf("username do membro %s: %v", m.UserID, m.Username)
		}
	}
	if g.Members[0].UserID != a {
		t.Fatalf("membros fora da ordem de entrada (dono primeiro): %v", g.Members)
	}

	var list struct{ Groups []group }
	_, out := e.do(c, "GET", "/v1/groups", "")
	raw, _ := json.Marshal(out)
	json.Unmarshal(raw, &list)
	if len(list.Groups) != 1 || list.Groups[0].ID != id || len(list.Groups[0].Members) != 3 {
		t.Fatalf("GET /v1/groups de c: %s", raw)
	}
	if _, out := e.do(d, "GET", "/v1/groups", ""); !strings.Contains(mustJSON(out), `"groups":[]`) {
		t.Fatalf("GET /v1/groups de quem não tem grupo: %v", out)
	}

	for _, gid := range []string{id, nobody, "x"} { // não-membro, inexistente e malformado: 404 sem revelar
		e.want(d, "GET", "/v1/groups/"+gid, "", 404, "not_found")
	}

	solo := e.create(d)
	if g := e.get(d, solo); !slices.Equal(g.ids(), []string{d}) {
		t.Fatalf("grupo só com o dono: %+v", g)
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

func TestCreateRules(t *testing.T) {
	e := newEnv(t)
	a, friend, stranger, pending, blocked := e.user(), e.user(), e.user(), e.user(), e.user()
	e.friends(a, friend, blocked)
	e.exec(`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'pending')`, a, pending)
	e.exec(`INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2)`, blocked, a)

	for name, m := range map[string]string{"estranho": stranger, "pedido pendente": pending, "bloqueio": blocked, "inexistente": nobody} {
		t.Run(name, func(t *testing.T) {
			e.want(a, "POST", "/v1/groups", members(friend, m), 403, "forbidden")
		})
	}
	e.want(a, "POST", "/v1/groups", members("x"), 400, "bad_request")
	e.want(a, "POST", "/v1/groups", `não é json`, 400, "bad_request")
	e.want(a, "POST", "/v1/groups", `{}`, 201, "")
	e.takeChanges()

	many := e.friendsBulk(a, maxMembers)
	e.want(a, "POST", "/v1/groups", members(many...), 409, "too_many_members")
	e.want(a, "POST", "/v1/groups", members(many[:maxMembers-1]...), 201, "")
}

func TestTooManyGroups(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	e.inGroups(b, maxGroups)
	e.want(a, "POST", "/v1/groups", members(b), 409, "too_many_groups")
	e.inGroups(a, maxGroups-1)
	e.create(a)
	e.want(a, "POST", "/v1/groups", members(), 409, "too_many_groups")
	e.wantChange(e.lastGroup(a), a)
}

func (e *env) lastGroup(owner string) string {
	var id string
	e.pool.QueryRow(context.Background(), `SELECT id FROM groups WHERE owner_id = $1 ORDER BY created_at DESC, id LIMIT 1`,
		owner).Scan(&id)
	return id
}

func TestAddMember(t *testing.T) {
	e := newEnv(t)
	a, b, d, stranger, blocked, outsider := e.user(), e.user(), e.user(), e.user(), e.user(), e.user()
	e.friends(a, b)
	e.friends(b, d, blocked) // d é amigo de b, não do dono
	e.exec(`INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2)`, b, blocked)
	id := e.create(a, b)
	e.takeChanges()

	e.add(b, id, d, 204, "")
	e.wantChange(id, a, b, d)
	e.add(b, id, d, 204, "")
	e.wantNoChange()
	e.add(b, id, stranger, 403, "forbidden")
	e.add(b, id, blocked, 403, "forbidden")
	e.add(b, id, nobody, 403, "forbidden")
	e.add(outsider, id, stranger, 404, "not_found")
	e.add(b, nobody, d, 404, "not_found")
	e.want(b, "POST", "/v1/groups/"+id+"/members", `{"user_id":"x"}`, 400, "bad_request")
	e.wantNoChange()

	many := e.friendsBulk(a, maxMembers-3)
	for _, m := range many {
		e.add(a, id, m, 204, "")
	}
	extra := e.friendsBulk(a, 1)[0]
	e.add(a, id, extra, 409, "too_many_members")

	e2 := e.user()
	e.friends(b, e2)
	e.inGroups(e2, maxGroups)
	small := e.create(b)
	e.add(b, small, e2, 409, "too_many_groups")
}

func TestRemoveAndLeave(t *testing.T) {
	e := newEnv(t)
	a, b, c, d := e.user(), e.user(), e.user(), e.user()
	e.friends(a, b, c, d)
	id := e.create(a, b, c, d)
	// ordem de entrada explícita: b entrou antes de c e d
	e.exec(`UPDATE group_members SET joined_at = now() - interval '1 hour' WHERE group_id = $1 AND user_id = $2`, id, b)
	e.exec(`UPDATE group_members SET joined_at = now() - interval '2 hour' WHERE group_id = $1 AND user_id = $2`, id, a)
	e.takeChanges()

	e.want(b, "DELETE", "/v1/groups/"+id+"/members/"+c, "", 403, "forbidden")
	e.want(e.user(), "DELETE", "/v1/groups/"+id+"/members/"+c, "", 404, "not_found")
	e.wantNoChange()

	e.want(a, "DELETE", "/v1/groups/"+id+"/members/"+c, "", 204, "")
	e.wantChange(id, a, b, d, c) // os atuais e quem saiu
	e.want(c, "GET", "/v1/groups/"+id, "", 404, "not_found")
	e.want(a, "DELETE", "/v1/groups/"+id+"/members/"+c, "", 204, "")
	e.wantNoChange()

	e.want(a, "DELETE", "/v1/groups/"+id+"/members/"+a, "", 204, "") // o dono sai: herda o mais antigo (b)
	e.wantChange(id, b, d, a)
	if g := e.get(b, id); g.OwnerID != b || !slices.Equal(sorted(g.ids()...), sorted(b, d)) {
		t.Fatalf("depois de o dono sair: %+v", g)
	}

	e.want(d, "DELETE", "/v1/groups/"+id+"/members/"+d, "", 204, "")
	e.wantChange(id, b, d)
	e.want(b, "DELETE", "/v1/groups/"+id+"/members/"+b, "", 204, "")
	e.wantChange(id, b)
	var n int
	e.pool.QueryRow(context.Background(), `SELECT count(*) FROM groups WHERE id = $1`, id).Scan(&n)
	if n != 0 {
		t.Fatal("grupo sem membros continua no banco")
	}
	e.want(b, "DELETE", "/v1/groups/"+id+"/members/"+b, "", 404, "not_found")
}

// Sem abrir as conexões antes, abrir conexão demora mais que a transação e a corrida vira fila.
func (e *env) warm(n int) {
	e.t.Helper()
	conns := make([]*pgxpool.Conn, n)
	for i := range conns {
		c, err := e.pool.Acquire(context.Background())
		if err != nil {
			e.t.Fatal(err)
		}
		conns[i] = c
	}
	for _, c := range conns {
		c.Release()
	}
}

func TestConcurrentLimits(t *testing.T) {
	e := newEnv(t)
	e.warm(24)
	a := e.user()
	base := e.friendsBulk(a, maxMembers-2)
	id := e.create(a, base...) // 49 membros: 1 vaga
	cands := e.friendsBulk(a, 20)
	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[int]int{}
	start := make(chan struct{})
	for _, c := range cands {
		wg.Go(func() {
			<-start
			s, _ := e.do(a, "POST", "/v1/groups/"+id+"/members", `{"user_id":"`+c+`"}`)
			mu.Lock()
			codes[s]++
			mu.Unlock()
		})
	}
	close(start)
	wg.Wait()
	if codes[204] != 1 || codes[409] != len(cands)-1 {
		t.Fatalf("adds no grupo com 1 vaga: %v, want 1×204 e o resto 409", codes)
	}

	x := e.user()
	owners := make([]string, 4)
	for i := range owners {
		owners[i] = e.user()
		e.friends(owners[i], x)
	}
	e.inGroups(x, maxGroups-1)
	codes = map[int]int{}
	for _, o := range owners {
		wg.Go(func() {
			s, _ := e.do(o, "POST", "/v1/groups", members(x))
			mu.Lock()
			codes[s]++
			mu.Unlock()
		})
	}
	wg.Wait()
	if codes[201] != 1 || codes[409] != 3 {
		t.Fatalf("criações com x em 24 grupos: %v, want 1×201 e 3×409", codes)
	}
}

func TestDeadline(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	e.h.reqWait = time.Nanosecond
	e.routes()
	e.want(a, "GET", "/v1/groups", "", 503, "unavailable")
	e.want(a, "POST", "/v1/groups", members(), 503, "unavailable")
}

func TestOnChangeNil(t *testing.T) {
	e := newEnv(t)
	e.h.OnChange = nil
	a := e.user()
	id := e.create(a)
	e.want(a, "DELETE", "/v1/groups/"+id+"/members/"+a, "", 204, "")
}

func TestLeaveAllTx(t *testing.T) {
	e := newEnv(t)
	u, x, y := e.user(), e.user(), e.user()
	e.friends(u, x)
	e.friends(y, u)
	owned := e.create(u, x)
	member := e.create(y, u)
	alone := e.create(u)
	e.exec(`UPDATE group_members SET joined_at = now() - interval '1 hour' WHERE user_id = $1`, u)
	e.takeChanges()

	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changes, err := LeaveAllTx(ctx, tx, u)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	e.wantNoChange() // o aviso é de quem chama, depois do commit

	got := map[string][]string{}
	for _, c := range changes {
		got[c.Group] = sorted(c.Users...)
	}
	want := map[string][]string{owned: sorted(x, u), member: sorted(y, u), alone: {u}}
	if len(got) != 3 || !slices.Equal(got[owned], want[owned]) || !slices.Equal(got[member], want[member]) ||
		!slices.Equal(got[alone], want[alone]) {
		t.Fatalf("changes: %v, want %v", got, want)
	}
	if g := e.get(x, owned); g.OwnerID != x || !slices.Equal(g.ids(), []string{x}) {
		t.Fatalf("grupo de que u era dono: %+v", g)
	}
	if g := e.get(y, member); !slices.Equal(g.ids(), []string{y}) {
		t.Fatalf("grupo de que u era membro: %+v", g)
	}
	var n int
	e.pool.QueryRow(ctx, `SELECT count(*) FROM group_members WHERE user_id = $1`, u).Scan(&n)
	if n != 0 {
		t.Fatalf("u ainda em %d grupos", n)
	}
	e.pool.QueryRow(ctx, `SELECT count(*) FROM groups WHERE id = $1`, alone).Scan(&n)
	if n != 0 {
		t.Fatal("grupo só com u continua")
	}

	if changes, err := e.leaveAll(u); err != nil || len(changes) != 0 {
		t.Fatalf("de novo: %v %v", changes, err)
	}
}

func (e *env) leaveAll(u string) ([]Change, error) {
	var out []Change
	err := pgx.BeginFunc(context.Background(), e.pool, func(tx pgx.Tx) error {
		var err error
		out, err = LeaveAllTx(context.Background(), tx, u)
		return err
	})
	return out, err
}

func TestListSize(t *testing.T) {
	e := newEnv(t)
	u := e.user()
	ctx := context.Background()
	others := e.friendsBulk(u, maxMembers-1)
	e.exec(`UPDATE users SET username = 'u' || lpad(n::text, 19, '0')
		FROM (SELECT id, row_number() OVER () n FROM users WHERE id = ANY($1::uuid[])) x WHERE users.id = x.id`, others)
	e.exec(`UPDATE users SET username = 'dono' || lpad('', 16, '0') WHERE id = $1`, u)
	seed := func(n int) {
		e.exec(`WITH g AS (INSERT INTO groups (owner_id) SELECT $1 FROM generate_series(1, $2) RETURNING id)
			INSERT INTO group_members (group_id, user_id)
			SELECT g.id, m FROM g, unnest($3::uuid[]) m`, u, n, append([]string{u}, others...))
	}
	seed(maxGroups)
	req := httptest.NewRequest("GET", "/v1/groups", nil).WithContext(httpx.WithUser(ctx, u))
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	if w.Code != 200 || w.Body.Len() > maxJSON {
		t.Fatalf("pior caso dos tetos: %d, %d bytes (limite %d)", w.Code, w.Body.Len(), maxJSON)
	}
	seed(maxGroups) // 50 grupos: só o banco deixa
	w = httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	if w.Code != 413 || !strings.Contains(w.Body.String(), `"too_large"`) {
		t.Fatalf("acima do teto: %d %.100s", w.Code, w.Body)
	}
}
