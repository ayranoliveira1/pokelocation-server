package friends

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

const nobody = "00000000-0000-4000-8000-000000000000" // UUID válido que não existe

type env struct {
	t    *testing.T
	pool *pgxpool.Pool
	mux  *http.ServeMux
}

type user struct{ id, code string }

func newEnv(t *testing.T) *env {
	t.Helper()
	t.Parallel()
	pool := testdb.Pool(t)
	mux := http.NewServeMux()
	New(pool).Routes(mux, func(h http.HandlerFunc) http.Handler { return h }) // usuário vem do contexto (httpx.WithUser)
	return &env{t, pool, mux}
}

func (e *env) user() user {
	e.t.Helper()
	var u user
	err := e.pool.QueryRow(context.Background(),
		`INSERT INTO users (email, friend_code) VALUES (gen_random_uuid()::text || '@x', $1) RETURNING id, friend_code`,
		NewCode()).Scan(&u.id, &u.code)
	if err != nil {
		e.t.Fatal(err)
	}
	return u
}

func (e *env) exec(q string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), q, args...); err != nil {
		e.t.Fatal(err)
	}
}

// do chama a rota como u e devolve status e corpo decodificado (nil se vazio).
func (e *env) do(u user, method, path, body string) (int, map[string]any) {
	e.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req = req.WithContext(httpx.WithUser(req.Context(), u.id))
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

func (e *env) want(u user, method, path, body string, status int, code string) map[string]any {
	e.t.Helper()
	got, out := e.do(u, method, path, body)
	if got != status || (code != "" && out["error"] != code) {
		e.t.Fatalf("%s %s: %d %v, want %d %q", method, path, got, out, status, code)
	}
	return out
}

func (e *env) request(from, to user) map[string]any {
	e.t.Helper()
	status, out := e.do(from, "POST", "/v1/friends/requests", `{"code":"`+to.code+`"}`)
	if status != 200 && status != 201 {
		e.t.Fatalf("pedido: %d %v", status, out)
	}
	return out
}

type listed struct {
	friends            map[string]string // user_id → friend_code
	incoming, outgoing map[string]map[string]any
}

func (e *env) list(u user) listed {
	e.t.Helper()
	req := httptest.NewRequest("GET", "/v1/friends", nil)
	req = req.WithContext(httpx.WithUser(req.Context(), u.id))
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	if w.Code != 200 {
		e.t.Fatalf("GET /v1/friends: %d %s", w.Code, w.Body)
	}
	var raw struct {
		Friends            []map[string]any `json:"friends"`
		Incoming, Outgoing []map[string]any
	}
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		e.t.Fatal(err)
	}
	for _, k := range []string{`"friends":[`, `"incoming":[`, `"outgoing":[`} { // nunca null
		if !strings.Contains(w.Body.String(), k) {
			e.t.Fatalf("GET /v1/friends sem %s: %s", k, w.Body)
		}
	}
	l := listed{map[string]string{}, map[string]map[string]any{}, map[string]map[string]any{}}
	for _, f := range raw.Friends {
		l.friends[f["user_id"].(string)] = f["friend_code"].(string)
	}
	for _, r := range raw.Incoming {
		l.incoming[r["user_id"].(string)] = r
	}
	for _, r := range raw.Outgoing {
		l.outgoing[r["user_id"].(string)] = r
	}
	return l
}

func TestRequestAccept(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()

	lower := strings.ToLower(b.code[:4]) + "-" + b.code[4:] // normalizado como no app
	out := e.want(a, "POST", "/v1/friends/requests", `{"code":"`+lower+`"}`, 201, "")
	id, _ := out["id"].(string)
	if id == "" || out["status"] != "pending" {
		t.Fatalf("pedido: %v", out)
	}
	again := e.want(a, "POST", "/v1/friends/requests", `{"code":"`+b.code+`"}`, 200, "") // idempotente
	if again["id"] != id || again["status"] != "pending" {
		t.Fatalf("pedido repetido: %v, want id %s pending", again, id)
	}

	la, lb := e.list(a), e.list(b)
	if r := la.outgoing[b.id]; r == nil || r["id"] != id || r["friend_code"] != b.code || r["created_at"].(float64) < 1e12 {
		t.Fatalf("outgoing de A: %v", la.outgoing)
	}
	if r := lb.incoming[a.id]; r == nil || r["id"] != id || r["friend_code"] != a.code {
		t.Fatalf("incoming de B: %v", lb.incoming)
	}
	if len(la.friends)+len(la.incoming)+len(lb.friends)+len(lb.outgoing) != 0 {
		t.Fatalf("listas extras: A %+v B %+v", la, lb)
	}

	e.want(a, "POST", "/v1/friends/requests/"+id+"/accept", "", 404, "not_found") // quem pediu não aceita
	e.want(b, "POST", "/v1/friends/requests/"+id+"/accept", "", 204, "")
	e.want(b, "POST", "/v1/friends/requests/"+id+"/accept", "", 404, "not_found") // já aceito

	la, lb = e.list(a), e.list(b)
	if la.friends[b.id] != b.code || lb.friends[a.id] != a.code || len(la.outgoing)+len(lb.incoming) != 0 {
		t.Fatalf("depois do aceite: A %+v B %+v", la, lb)
	}
	e.want(a, "POST", "/v1/friends/requests", `{"code":"`+b.code+`"}`, 409, "already_friends")
	e.want(b, "POST", "/v1/friends/requests", `{"code":"`+a.code+`"}`, 409, "already_friends")
}

func TestRequestErrors(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	for _, tc := range []struct {
		body   string
		status int
		code   string
	}{
		{`{"code":"` + a.code + `"}`, 400, "self_request"},
		{`{"code":"ZZZZZZZZ"}`, 404, "code_not_found"},
		{`{"code":"curto"}`, 404, "code_not_found"},
		{`{"code":""}`, 404, "code_not_found"},
		{`{}`, 404, "code_not_found"},
		{`{"code":1}`, 400, "bad_request"},
		{`não é json`, 400, "bad_request"},
		{`{"code":"ZZZZZZZZ"} {}`, 400, "bad_request"},
		{`{"code":"` + strings.Repeat("A", 5000) + `"}`, 400, "bad_request"}, // > 4 KiB
	} {
		e.want(a, "POST", "/v1/friends/requests", tc.body, tc.status, tc.code)
	}
}

// Pedido cruzado: B pede A com o pedido de A pendente → aceita na hora, mesmo id.
func TestRequestCrossAccepts(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	id := e.request(a, b)["id"]
	out := e.want(b, "POST", "/v1/friends/requests", `{"code":"`+a.code+`"}`, 200, "")
	if out["id"] != id || out["status"] != "accepted" {
		t.Fatalf("pedido cruzado: %v, want id %v accepted", out, id)
	}
	if l := e.list(a); l.friends[b.id] != b.code {
		t.Fatalf("A não tem B como amigo: %+v", l)
	}
}

func TestDecline(t *testing.T) {
	e := newEnv(t)
	a, b, c := e.user(), e.user(), e.user()
	id := e.request(a, b)["id"].(string)
	for _, path := range []string{"/accept", "/decline"} {
		e.want(c, "POST", "/v1/friends/requests/"+id+path, "", 404, "not_found") // terceiro
	}
	e.want(b, "POST", "/v1/friends/requests/"+id+"/decline", "", 204, "")
	e.want(b, "POST", "/v1/friends/requests/"+id+"/decline", "", 404, "not_found")
	if l := e.list(a); len(l.outgoing) != 0 {
		t.Fatalf("pedido recusado continua: %+v", l)
	}

	id = e.request(a, b)["id"].(string)
	e.want(a, "POST", "/v1/friends/requests/"+id+"/decline", "", 204, "") // quem pediu cancela
	if l := e.list(b); len(l.incoming) != 0 {
		t.Fatalf("pedido cancelado continua: %+v", l)
	}

	e.request(a, b)
	e.want(b, "POST", "/v1/friends/requests", `{"code":"`+a.code+`"}`, 200, "")
	var accepted string
	if err := e.pool.QueryRow(context.Background(), `SELECT id FROM friendships`).Scan(&accepted); err != nil {
		t.Fatal(err)
	}
	e.want(b, "POST", "/v1/friends/requests/"+accepted+"/decline", "", 404, "not_found") // amizade não é pedido

	for _, bad := range []string{"x", nobody, "00000000-0000-0000-0000-00000000000g"} {
		e.want(b, "POST", "/v1/friends/requests/"+bad+"/accept", "", 404, "not_found")
		e.want(b, "POST", "/v1/friends/requests/"+bad+"/decline", "", 404, "not_found")
	}
}

func TestRemove(t *testing.T) {
	e := newEnv(t)
	a, b, c := e.user(), e.user(), e.user()
	e.request(a, b)
	e.request(b, a) // amigos
	e.request(c, a) // pedido pendente

	e.want(a, "DELETE", "/v1/friends/"+b.id, "", 204, "")
	e.want(a, "DELETE", "/v1/friends/"+c.id, "", 204, "") // também desfaz pedido
	if l := e.list(a); len(l.friends)+len(l.incoming) != 0 {
		t.Fatalf("depois da remoção: %+v", l)
	}
	if l := e.list(b); len(l.friends) != 0 {
		t.Fatalf("B ainda tem A: %+v", l)
	}
	for _, id := range []string{b.id, nobody, "x"} { // sempre 204
		e.want(a, "DELETE", "/v1/friends/"+id, "", 204, "")
	}
}

func TestBlock(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.request(a, b)
	e.request(b, a) // amigos

	e.want(a, "POST", "/v1/blocks/"+b.id, "", 204, "")
	e.want(a, "POST", "/v1/blocks/"+b.id, "", 204, "") // idempotente
	if l := e.list(a); len(l.friends) != 0 {
		t.Fatalf("bloqueio não desfez a amizade: %+v", l)
	}
	e.want(a, "POST", "/v1/friends/requests", `{"code":"`+b.code+`"}`, 403, "blocked")
	e.want(b, "POST", "/v1/friends/requests", `{"code":"`+a.code+`"}`, 404, "code_not_found") // nunca revela

	e.want(b, "POST", "/v1/blocks/"+a.id, "", 204, "") // os dois bloqueiam
	e.want(a, "DELETE", "/v1/blocks/"+b.id, "", 204, "")
	e.want(a, "POST", "/v1/friends/requests", `{"code":"`+b.code+`"}`, 404, "code_not_found") // B ainda bloqueia A
	e.want(b, "DELETE", "/v1/blocks/"+a.id, "", 204, "")
	e.want(a, "POST", "/v1/friends/requests", `{"code":"`+b.code+`"}`, 201, "") // desbloqueio não refaz amizade

	e.want(a, "POST", "/v1/blocks/"+nobody, "", 404, "not_found")
	e.want(a, "POST", "/v1/blocks/x", "", 404, "not_found")
	e.want(a, "POST", "/v1/blocks/"+a.id, "", 400, "bad_request")
	for _, id := range []string{nobody, "x"} {
		e.want(a, "DELETE", "/v1/blocks/"+id, "", 204, "")
	}
}

func TestLimits(t *testing.T) {
	e := newEnv(t)
	a, b, c := e.user(), e.user(), e.user()
	fill := func(n int, q string, u user) {
		e.exec(`WITH x AS (
			INSERT INTO users (email, friend_code)
			SELECT gen_random_uuid()::text || '@x', upper(substr(md5(random()::text), 1, 8)) FROM generate_series(1, $1)
			RETURNING id)
			`+q, n, u.id) // hex maiúsculo é Crockford válido

	}

	// teto de pedidos enviados por A: o próximo é recusado
	fill(maxPending, `INSERT INTO friendships (requester_id, addressee_id, status) SELECT $2, id, 'pending' FROM x`, a)
	e.want(a, "POST", "/v1/friends/requests", `{"code":"`+c.code+`"}`, 429, "rate_limited")

	// teto de pedidos recebidos por B: ninguém mais pede
	fill(maxPending, `INSERT INTO friendships (requester_id, addressee_id, status) SELECT id, $2, 'pending' FROM x`, b)
	e.want(c, "POST", "/v1/friends/requests", `{"code":"`+b.code+`"}`, 429, "rate_limited")

	// pedidos pendentes com C criados antes de ele chegar a 500 amigos
	d, h, k, f, g := e.user(), e.user(), e.user(), e.user(), e.user()
	fromD := e.request(d, c)["id"].(string)
	e.request(c, h)
	toK := e.request(c, k)["id"].(string)
	fill(maxFriends, `INSERT INTO friendships (requester_id, addressee_id, status) SELECT id, $2, 'accepted' FROM x`, c)

	// 500 amigos de C: nenhuma amizade nova, por nenhum caminho
	e.want(c, "POST", "/v1/friends/requests/"+fromD+"/accept", "", 429, "rate_limited")     // C aceita
	e.want(k, "POST", "/v1/friends/requests/"+toK+"/accept", "", 429, "rate_limited")       // aceitam o de C
	e.want(h, "POST", "/v1/friends/requests", `{"code":"`+c.code+`"}`, 429, "rate_limited") // cruzado
	e.want(c, "POST", "/v1/friends/requests", `{"code":"`+f.code+`"}`, 429, "rate_limited") // C pede
	e.want(g, "POST", "/v1/friends/requests", `{"code":"`+c.code+`"}`, 429, "rate_limited") // pedem a C
}

// Pior caso dentro dos tetos: a resposta cabe no limite de leitura do app (64 KiB).
func TestListFitsApp(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	ins := func(n int, q string) {
		e.exec(`WITH x AS (
			INSERT INTO users (email, friend_code)
			SELECT gen_random_uuid()::text || '@x', upper(substr(md5(random()::text), 1, 8)) FROM generate_series(1, $1)
			RETURNING id)
			`+q, n, a.id)
	}
	ins(maxFriends, `INSERT INTO friendships (requester_id, addressee_id, status) SELECT id, $2, 'accepted' FROM x`)
	ins(maxPending, `INSERT INTO friendships (requester_id, addressee_id, status) SELECT id, $2, 'pending' FROM x`)
	ins(maxPending, `INSERT INTO friendships (requester_id, addressee_id, status) SELECT $2, id, 'pending' FROM x`)

	req := httptest.NewRequest("GET", "/v1/friends", nil).WithContext(httpx.WithUser(context.Background(), a.id))
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	if w.Code != 200 || w.Body.Len() > 64<<10 {
		t.Fatalf("GET /v1/friends no teto: %d, %d bytes (limite %d)", w.Code, w.Body.Len(), 64<<10)
	}
}

func TestEmptyList(t *testing.T) {
	e := newEnv(t)
	l := e.list(e.user()) // list confere "[...]" nas três listas
	if len(l.friends)+len(l.incoming)+len(l.outgoing) != 0 {
		t.Fatalf("lista de usuário novo: %+v", l)
	}
}

// Banco fora → 503 unavailable, sem vazar nada.
func TestUnavailable(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	pool, err := pgxpool.New(context.Background(), e.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	mux := http.NewServeMux()
	New(pool).Routes(mux, func(h http.HandlerFunc) http.Handler { return h })
	e.mux = mux
	e.want(a, "GET", "/v1/friends", "", 503, "unavailable")
	e.want(a, "POST", "/v1/friends/requests", `{"code":"ZZZZZZZZ"}`, 503, "unavailable")
}

// 10 pedidos por minuto por usuário, contando os erros: varrer códigos esbarra no limite.
func TestRequestRateLimit(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	for range 10 {
		e.want(a, "POST", "/v1/friends/requests", `{"code":"ZZZZZZZZ"}`, 404, "code_not_found")
	}
	e.want(a, "POST", "/v1/friends/requests", `{"code":"`+b.code+`"}`, 429, "rate_limited")
	e.want(a, "POST", "/v1/friends/requests", `não é json`, 429, "rate_limited")
	e.want(b, "POST", "/v1/friends/requests", `{"code":"`+a.code+`"}`, 201, "") // outro usuário não é afetado
}
