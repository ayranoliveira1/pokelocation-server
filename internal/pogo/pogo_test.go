package pogo

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func newKey(t *testing.T) string {
	t.Helper()
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	return base64.StdEncoding.EncodeToString(k)
}

var seq atomic.Int32

func newUser(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	n := seq.Add(1)
	var id string
	if err := pool.QueryRow(context.Background(), `INSERT INTO users (email, friend_code) VALUES ($1, $2) RETURNING id`,
		fmt.Sprintf("pg%d@ex.com", n), fmt.Sprintf("PG%06d", n)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// upstream falso: rotas do PoGo trocáveis por teste.
func fakeUpstream(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func newHandler(t *testing.T, pool *pgxpool.Pool, upstream string) *Handler {
	t.Helper()
	h, err := New(Config{TokenKey: newKey(t), Upstream: upstream}, pool, testdb.Redis(t))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func serve(h *Handler, user, method, path, body string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	h.Routes(mux, func(next http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next(w, r.WithContext(httpx.WithUser(r.Context(), user)))
		})
	})
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(method, path, rd))
	return w
}

func link(t *testing.T, h *Handler, user, token string, trainers ...string) {
	t.Helper()
	p := &Profile{Trainers: []json.RawMessage{}}
	for _, tr := range trainers {
		p.Trainers = append(p.Trainers, json.RawMessage(tr))
	}
	if err := h.svc.store.upsert(context.Background(), user, "google", h.svc.seal(user, token), p); err != nil {
		t.Fatal(err)
	}
}

func linked(t *testing.T, pool *pgxpool.Pool, user string) bool {
	t.Helper()
	var ok bool
	if err := pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pogo_accounts WHERE user_id = $1)`,
		user).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

func wantError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var e struct{ Error string }
	_ = json.Unmarshal(w.Body.Bytes(), &e)
	if w.Code != status || e.Error != code {
		t.Fatalf("esperava %d %s, veio %d %s", status, code, w.Code, w.Body.String())
	}
}

func TestSeal(t *testing.T) {
	s, err := newService(Config{TokenKey: newKey(t)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, b := s.seal("u1", "tok"), s.seal("u1", "tok")
	if bytes.Equal(a, b) || bytes.Contains(a, []byte("tok")) {
		t.Fatal("nonce repetido ou texto aberto")
	}
	if got, err := s.open("u1", a); err != nil || got != "tok" {
		t.Fatalf("open: %q %v", got, err)
	}
	if _, err := s.open("u2", a); err == nil {
		t.Fatal("abriu com outro user_id")
	}
	if _, err := s.open("u1", a[:5]); err == nil {
		t.Fatal("abriu cifra curta")
	}
	for _, bad := range []string{"0", "65", "x", "-1"} {
		if _, err := New(Config{Workers: bad}, nil, nil); err == nil {
			t.Errorf("POGO_WORKERS=%q aceito", bad)
		}
	}
	if h, err := New(Config{Workers: "64"}, nil, nil); err != nil || h.workers != 64 {
		t.Fatalf("POGO_WORKERS=64: %v", err)
	}
	if h, _ := New(Config{}, nil, nil); h.workers != 32 || h.interval != 3*time.Minute {
		t.Fatalf("padrão: %d workers, %v", h.workers, h.interval)
	}
	for _, bad := range []string{"1m59s", "10m1s", "3", "x", "-3m"} {
		if _, err := New(Config{AlertInterval: bad}, nil, nil); err == nil {
			t.Errorf("POGO_ALERT_INTERVAL=%q aceito", bad)
		}
	}
	for in, want := range map[string]time.Duration{"2m": 2 * time.Minute, "10m": 10 * time.Minute, "5m30s": 330 * time.Second} {
		if h, err := New(Config{AlertInterval: in}, nil, nil); err != nil || h.interval != want {
			t.Errorf("POGO_ALERT_INTERVAL=%q: %v %v", in, h, err)
		}
	}
	for _, bad := range []string{"x", base64.StdEncoding.EncodeToString(make([]byte, 16)), "não-base64!"} {
		if _, err := New(Config{TokenKey: bad}, nil, nil); err == nil {
			t.Errorf("chave %q aceita", bad)
		}
	}
}

func TestParseProfile(t *testing.T) {
	p, err := parseProfile("discord", []byte(`{"display_name":null,"discord_username":"dd","google_email":"a@b.com",
		"trainers":[{"trainer_code":"1","active":true},{"trainer_code":"2","active":false},{"trainer_code":"3"}],
		"trainer_cap":2,"vip_tier":"vip"}`))
	if err != nil {
		t.Fatal(err)
	}
	out, _ := json.Marshal(p)
	const want = `{"provider":"discord","display_name":"dd","trainers":[{"trainer_code":"1","active":true},` +
		`{"trainer_code":"3"}],"trainer_cap":2,"vip_tier":"vip"}`
	if string(out) != want {
		t.Fatalf("%s", out)
	}
	p, _ = parseProfile("google", []byte(`{"display_name":"N","trainer":{"trainer_code":"9"}}`))
	if out, _ := json.Marshal(p); string(out) != `{"provider":"google","display_name":"N","trainers":[{"trainer_code":"9"}],"trainer_cap":0,"vip_tier":null}` {
		t.Fatalf("legado: %s", out)
	}
	p, _ = parseProfile("google", []byte(`{"trainer":null,"trainers":null}`))
	if out, _ := json.Marshal(p); !strings.Contains(string(out), `"trainers":[]`) {
		t.Fatalf("sem treinador: %s", out)
	}
}

func TestClassify(t *testing.T) {
	for _, c := range []struct {
		status int
		header string
		body   string
		want   string
	}{
		{200, "", `{}`, "<nil>"},
		{401, "", `{"detail":"Invalid token"}`, errUnauthorized.Error()},
		{500, "", ``, ErrUnavailable.Error()},
		{302, "", ``, ErrUnavailable.Error()},
		{429, "30", `{"detail":{"error":"rate_limited","retry_after":46}}`, `429 {"error":"rate_limited","retry_after":46} 46`},
		{429, "30", `{"detail":"x"}`, `429 "x" 30`},
		{429, "", `<html>`, `429 null 0`},
		{403, "", `{"detail":{"error":"vip_required"}}`, `403 {"error":"vip_required"} 0`},
	} {
		err := classify(c.status, c.header, []byte(c.body))
		got := fmt.Sprint(err)
		if ue, ok := err.(*UpstreamError); ok {
			got = fmt.Sprintf("%d %s %d", ue.Status, ue.Detail, ue.RetryAfter)
		}
		if got != c.want {
			t.Errorf("%d %s: %s", c.status, c.body, got)
		}
	}
}

func TestDisabled(t *testing.T) {
	h, err := New(Config{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]string{{"POST", "/v1/pogo/login"}, {"DELETE", "/v1/pogo/login"}, {"GET", "/v1/pogo/me"},
		{"POST", "/v1/pogo/lookup"}, {"GET", "/v1/pogo/reveal/abc"}, {"POST", "/v1/pogo/trainer/confirm"}} {
		wantError(t, serve(h, "u", r[0], r[1], "{}"), 503, "pogo_disabled")
	}
}

const secretTok = "tok-segredo-1"

func TestLogin(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	up := http.NewServeMux()
	up.HandleFunc("POST /auth/{provider}/callback", func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Code string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		if r.Header.Get("Authorization") != "" || in.Code != "bom-"+r.PathValue("provider") {
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"detail":"Google token exchange failed: invalid_grant"}`)
			return
		}
		_, _ = io.WriteString(w, `{"token":"`+secretTok+`"}`)
	})
	up.HandleFunc("GET /users/me", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+secretTok || r.Header.Get("User-Agent") != userAgent {
			w.WriteHeader(401)
			return
		}
		_, _ = io.WriteString(w, `{"display_name":"Fulano","google_email":"x@y.com","trainers":[
			{"trainer_code":"521284665027","trainer_name":"T","active":true},{"trainer_code":"1","active":false}],
			"trainer_cap":1,"vip_tier":null}`)
	})
	h := newHandler(t, pool, fakeUpstream(t, up))
	u := newUser(t, pool)

	w := serve(h, u, "POST", "/v1/pogo/login", `{"provider":"google","code":"bom-google"}`)
	if w.Code != 200 || strings.Contains(w.Body.String(), "x@y.com") || strings.Contains(w.Body.String(), secretTok) ||
		!strings.Contains(w.Body.String(), `"trainers":[{"trainer_code":"521284665027","trainer_name":"T","active":true}]`) ||
		!strings.Contains(w.Body.String(), `"display_name":"Fulano"`) {
		t.Fatalf("login: %d %s", w.Code, w.Body.String())
	}
	var enc []byte
	var provider, trainers string
	if err := pool.QueryRow(context.Background(), `SELECT provider, token_enc, trainers::text FROM pogo_accounts
		WHERE user_id = $1`, u).Scan(&provider, &enc, &trainers); err != nil {
		t.Fatal(err)
	}
	if provider != "google" || bytes.Contains(enc, []byte(secretTok)) || !strings.Contains(trainers, "521284665027") {
		t.Fatalf("gravado: %s %s", provider, trainers)
	}

	wantError(t, serve(h, u, "POST", "/v1/pogo/login", `{"provider":"discord","code":"ruim"}`), 400, "invalid_grant")
	if !linked(t, pool, u) {
		t.Fatal("login recusado apagou o vínculo anterior")
	}
	if w := serve(h, u, "POST", "/v1/pogo/login", `{"provider":"discord","code":"bom-discord"}`); w.Code != 200 ||
		!strings.Contains(w.Body.String(), `"provider":"discord"`) {
		t.Fatalf("relogin discord: %d %s", w.Code, w.Body.String())
	}
	for _, bad := range []string{`{"provider":"github","code":"a"}`, `{"provider":"google","code":""}`,
		`{"provider":"google","code":"` + strings.Repeat("a", maxCode+1) + `"}`, `[]`} {
		wantError(t, serve(h, newUser(t, pool), "POST", "/v1/pogo/login", bad), 400, "bad_request")
	}

	if w := serve(h, u, "GET", "/v1/pogo/me", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"Fulano"`) {
		t.Fatalf("me: %d %s", w.Code, w.Body.String())
	}
	for range 2 { // idempotente
		if w := serve(h, u, "DELETE", "/v1/pogo/login", ""); w.Code != 204 {
			t.Fatalf("delete: %d", w.Code)
		}
	}
	if linked(t, pool, u) {
		t.Fatal("DELETE não apagou")
	}
	wantError(t, serve(h, u, "GET", "/v1/pogo/me", ""), 409, "pogo_relogin")
}

func TestProxy(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	var status atomic.Int32 // 0 = 200
	up := http.NewServeMux()
	echo := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+secretTok {
			w.WriteHeader(401)
			return
		}
		switch st := int(status.Load()); st {
		case 0:
		case 429:
			w.Header().Set("Retry-After", "50")
			w.WriteHeader(429)
			_, _ = io.WriteString(w, `{"detail":{"error":"rate_limited","retry_after":46}}`)
			return
		case 999: // acima do teto
			_, _ = io.WriteString(w, `{"x":"`+strings.Repeat("a", 300<<10)+`"}`)
			return
		default:
			w.WriteHeader(st)
			_, _ = io.WriteString(w, `{"detail":{"error":"trainer_not_linked","message":"m"}}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, `{"method":%q,"path":%q,"body":%q}`, r.Method, r.URL.EscapedPath(), body)
	}
	for _, p := range []string{"POST /map/lookup", "POST /search", "GET /reveal/{code}", "GET /trainer",
		"POST /trainer/lookup", "POST /trainer/confirm", "POST /rare-candy/search"} {
		up.HandleFunc(p, echo)
	}
	h := newHandler(t, pool, fakeUpstream(t, up))
	u := newUser(t, pool)
	wantError(t, serve(h, u, "POST", "/v1/pogo/lookup", `{}`), 409, "pogo_relogin")
	link(t, h, u, secretTok)

	for _, c := range [][4]string{
		{"POST", "/v1/pogo/lookup", `{"mode":"pokemon","n_lat":1}`, `{"method":"POST","path":"/map/lookup","body":"{\"mode\":\"pokemon\",\"n_lat\":1}"}`},
		{"POST", "/v1/pogo/search", ` {"pokemon_ids":[147]}`, `{"method":"POST","path":"/search","body":"{\"pokemon_ids\":[147]}"}`},
		{"GET", "/v1/pogo/reveal/Ab%2Bc%3D", ``, `{"method":"GET","path":"/reveal/Ab+c=","body":""}`},
		{"GET", "/v1/pogo/trainer", ``, `{"method":"GET","path":"/trainer","body":""}`},
		{"POST", "/v1/pogo/trainer/lookup", `{"trainer_code":"123456789012"}`, `{"method":"POST","path":"/trainer/lookup","body":"{\"trainer_code\":\"123456789012\"}"}`},
		{"POST", "/v1/pogo/trainer/confirm", `{"trainer_code":"1"}`, `{"method":"POST","path":"/trainer/confirm","body":"{\"trainer_code\":\"1\"}"}`},
		{"POST", "/v1/pogo/rare-candy", `{"center_lat":1,"center_lng":2,"radius_km":5}`, `{"method":"POST","path":"/rare-candy/search","body":"{\"center_lat\":1,\"center_lng\":2,\"radius_km\":5}"}`},
	} {
		w := serve(h, u, c[0], c[1], c[2])
		if w.Code != 200 || w.Body.String() != c[3] || w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("%s %s: %d %s", c[0], c[1], w.Code, w.Body.String())
		}
	}
	for _, bad := range []string{`[]`, `"x"`, `null`, `{`, `{} {}`} {
		wantError(t, serve(h, u, "POST", "/v1/pogo/search", bad), 400, "bad_request")
	}
	wantError(t, serve(h, u, "GET", "/v1/pogo/reveal/"+strings.Repeat("a", maxReveal+1), ""), 400, "bad_request")
	for _, bad := range []string{"a%0Ab", "%2E%2E", "%2E", "a%2Fb"} {
		wantError(t, serve(h, u, "GET", "/v1/pogo/reveal/"+bad, ""), 400, "bad_request")
	}

	status.Store(429)
	w := serve(h, u, "POST", "/v1/pogo/lookup", `{}`)
	if w.Code != 429 || w.Header().Get("Retry-After") != "46" || w.Body.String() !=
		`{"error":"upstream","status":429,"detail":{"error":"rate_limited","retry_after":46},"retry_after":46}`+"\n" {
		t.Fatalf("429: %d %q %s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
	status.Store(403)
	if w := serve(h, u, "POST", "/v1/pogo/lookup", `{}`); w.Code != 403 || w.Body.String() !=
		`{"error":"upstream","status":403,"detail":{"error":"trainer_not_linked","message":"m"}}`+"\n" {
		t.Fatalf("403: %d %s", w.Code, w.Body.String())
	}
	status.Store(503)
	wantError(t, serve(h, u, "POST", "/v1/pogo/lookup", `{}`), 502, "upstream_unavailable")
	status.Store(999)
	wantError(t, serve(h, u, "POST", "/v1/pogo/search", `{}`), 502, "upstream_unavailable") // 256 KiB
	if w := serve(h, u, "POST", "/v1/pogo/lookup", `{}`); w.Code != 200 {                   // 1 MiB
		t.Fatalf("lookup grande: %d", w.Code)
	}
	status.Store(0)

	link(t, h, u, "outro-token") // o PoGo recusa: 401 apaga o vínculo
	wantError(t, serve(h, u, "GET", "/v1/pogo/trainer", ""), 409, "pogo_relogin")
	if linked(t, pool, u) {
		t.Fatal("401 não apagou o vínculo")
	}
}

func TestKeyChanged(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	old, u := newHandler(t, pool, "http://127.0.0.1:1"), newUser(t, pool)
	link(t, old, u, secretTok)
	wantError(t, serve(newHandler(t, pool, "http://127.0.0.1:1"), u, "GET", "/v1/pogo/trainer", ""), 409, "pogo_relogin")
	if linked(t, pool, u) {
		t.Fatal("token ilegível ficou no banco")
	}
}

func TestRateLimit(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	up := http.NewServeMux()
	up.HandleFunc("POST /search", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{}`) })
	h := newHandler(t, pool, fakeUpstream(t, up))
	u, other := newUser(t, pool), newUser(t, pool)
	link(t, h, u, secretTok)
	for i := range 10 {
		if w := serve(h, u, "POST", "/v1/pogo/search", `{}`); w.Code != 200 {
			t.Fatalf("%d: %d", i, w.Code)
		}
	}
	wantError(t, serve(h, u, "POST", "/v1/pogo/search", `{}`), 429, "rate_limited")
	wantError(t, serve(h, other, "POST", "/v1/pogo/search", `{}`), 409, "pogo_relogin") // por usuário
}

func TestRateLimitCandy(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	up := http.NewServeMux()
	up.HandleFunc("POST /rare-candy/search", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{}`) })
	h := newHandler(t, pool, fakeUpstream(t, up))
	u, other := newUser(t, pool), newUser(t, pool)
	link(t, h, u, secretTok)
	for i := range 10 {
		if w := serve(h, u, "POST", "/v1/pogo/rare-candy", `{}`); w.Code != 200 {
			t.Fatalf("%d: %d", i, w.Code)
		}
	}
	wantError(t, serve(h, u, "POST", "/v1/pogo/rare-candy", `{}`), 429, "rate_limited")
	wantError(t, serve(h, other, "POST", "/v1/pogo/rare-candy", `{}`), 409, "pogo_relogin")
}

func TestProxyGzip(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	zip := func(s string) []byte {
		var b bytes.Buffer
		zw := gzip.NewWriter(&b)
		_, _ = io.WriteString(zw, s)
		_ = zw.Close()
		return b.Bytes()
	}
	var status atomic.Int32
	big := `{"x":"` + strings.Repeat("a", 300<<10) + `"}` // comprimido é pequeno; aberto passa do teto do search
	up := http.NewServeMux()
	up.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		body := `{"results":[1,2,3]}`
		switch status.Load() {
		case 429:
			w.Header().Set("Content-Encoding", "gzip")
			w.WriteHeader(429)
			_, _ = w.Write(zip(`{"detail":{"error":"rate_limited","retry_after":7}}`))
			return
		case 1:
			body = big
		}
		if r.Header.Get("Accept-Encoding") != "gzip" {
			_, _ = io.WriteString(w, body)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(zip(body))
	})
	h := newHandler(t, pool, fakeUpstream(t, up))
	u := newUser(t, pool)
	link(t, h, u, secretTok)
	call := func(path string, gz bool) *httptest.ResponseRecorder {
		mux := http.NewServeMux()
		h.Routes(mux, func(next http.HandlerFunc) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next(w, r.WithContext(httpx.WithUser(r.Context(), u)))
			})
		})
		r := httptest.NewRequest("POST", path, strings.NewReader(`{}`))
		if gz {
			r.Header.Set("Accept-Encoding", "gzip")
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}

	w := call("/v1/pogo/lookup", true)
	zr, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatalf("não veio comprimido: %d %q", w.Code, w.Body.String())
	}
	if out, _ := io.ReadAll(zr); w.Code != 200 || string(out) != `{"results":[1,2,3]}` ||
		w.Header().Get("Content-Encoding") != "gzip" || w.Header().Get("Vary") != "Accept-Encoding" {
		t.Fatalf("gzip: %d %s %v", w.Code, out, w.Header())
	}
	if w := call("/v1/pogo/lookup", false); w.Body.String() != `{"results":[1,2,3]}` || w.Header().Get("Content-Encoding") != "" {
		t.Fatalf("sem gzip: %q %v", w.Body.String(), w.Header())
	}
	status.Store(1)
	wantError(t, call("/v1/pogo/search", true), 502, "upstream_unavailable") // o teto vale aberto
	status.Store(429)
	if w := call("/v1/pogo/search", true); w.Code != 429 || w.Header().Get("Content-Encoding") != "" ||
		w.Body.String() != `{"error":"upstream","status":429,"detail":{"error":"rate_limited","retry_after":7},"retry_after":7}`+"\n" {
		t.Fatalf("erro comprimido: %d %q", w.Code, w.Body.String())
	}
}

// Prazo estourado no PoGo é 504 upstream_timeout; o PoGo fora (conexão recusada) segue 502.
func TestProxyTimeout(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	up := http.NewServeMux()
	up.HandleFunc("POST /map/lookup", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) // sem ler o corpo, o servidor não percebe o cliente indo embora
		select {                  // o PoGo lento além do prazo
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	h := newHandler(t, pool, fakeUpstream(t, up))
	u := newUser(t, pool)
	link(t, h, u, secretTok)
	mux := http.NewServeMux()
	h.Routes(mux, func(next http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next(w, r.WithContext(httpx.WithUser(r.Context(), u)))
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond) // o Timed respeita o prazo menor
	defer cancel()
	r := httptest.NewRequest("POST", "/v1/pogo/lookup", strings.NewReader(`{}`)).WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	wantError(t, w, 504, "upstream_timeout")

	down := newHandler(t, pool, "http://127.0.0.1:1")
	link(t, down, u, secretTok)
	wantError(t, serve(down, u, "POST", "/v1/pogo/lookup", `{}`), 502, "upstream_unavailable")
}
