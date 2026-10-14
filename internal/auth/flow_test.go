package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

type env struct {
	t    *testing.T
	h    *Handler
	mux  *http.ServeMux
	pool *pgxpool.Pool
	rdb  *redis.Client
	addr string // RemoteAddr fixo; "" = um IP novo por requisição (não esbarra no limite por IP)
}

var ipSeq atomic.Int32

func newEnv(t *testing.T) *env {
	t.Helper()
	pool, rdb := testdb.Pool(t), testdb.Redis(t)
	h, err := New(Config{SigningKey: testKey(), GoogleClientID: "cid", GoogleClientSecret: "sec",
		PublicURL: "https://api.test"}, pool, rdb)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	h.Routes(mux, h.Authed)
	return &env{t: t, h: h, mux: mux, pool: pool, rdb: rdb}
}

func (e *env) req(method, path, body, bearer string) *httptest.ResponseRecorder {
	e.t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.RemoteAddr = e.addr
	if r.RemoteAddr == "" {
		n := ipSeq.Add(1)
		r.RemoteAddr = fmt.Sprintf("198.18.%d.%d:4000", n>>8&255, n&255) // 198.18/15: não é privado
	}
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, r)
	return w
}

func (e *env) post(path, body string) *httptest.ResponseRecorder {
	return e.req("POST", path, body, "")
}

func cred(email, pw string) string {
	b, _ := json.Marshal(map[string]string{"email": email, "password": pw})
	return string(b)
}

type tokenResp struct {
	Access    string `json:"access_token"`
	Refresh   string `json:"refresh_token"`
	ExpiresIn int    `json:"expires_in"`
}

func wantErr(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if w.Code != status || w.Body.String() != `{"error":"`+code+"\"}\n" {
		t.Fatalf("quero %d %s, veio %d %s", status, code, w.Code, w.Body.String())
	}
}

func wantTokens(t *testing.T, w *httptest.ResponseRecorder, status int) tokenResp {
	t.Helper()
	var tr tokenResp
	if w.Code != status {
		t.Fatalf("status %d (%s), quero %d", w.Code, w.Body.String(), status)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &tr); err != nil || tr.Access == "" || tr.ExpiresIn != 900 {
		t.Fatalf("resposta %s: %v", w.Body.String(), err)
	}
	if raw, err := base64.RawURLEncoding.DecodeString(tr.Refresh); err != nil || len(raw) != 32 {
		t.Fatalf("refresh %q não é base64url de 32 bytes", tr.Refresh)
	}
	return tr
}

func (e *env) register(email string) tokenResp {
	e.t.Helper()
	return wantTokens(e.t, e.post("/v1/auth/register", cred(email, "senha-forte-1")), 201)
}

type me struct {
	ID         string  `json:"id"`
	Email      *string `json:"email"`
	FriendCode string  `json:"friend_code"`
}

func (e *env) me(access string) me {
	e.t.Helper()
	w := e.req("GET", "/v1/me", "", access)
	var m me
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &m) != nil {
		e.t.Fatalf("me: %d %s", w.Code, w.Body.String())
	}
	return m
}

var crockford = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{8}$`)

func TestRegister(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tr := e.register("  Ana@Ex.COM ")
	m := e.me(tr.Access)
	if m.Email == nil || *m.Email != "ana@ex.com" || !crockford.MatchString(m.FriendCode) || len(m.ID) != 36 {
		t.Fatalf("me = %+v", m)
	}
	var phc string
	if err := e.pool.QueryRow(context.Background(), `SELECT password_hash FROM users WHERE id = $1`, m.ID).Scan(&phc); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(phc, "$argon2id$v=19$m=19456,t=2,p=1$") {
		t.Fatalf("hash %q", phc)
	}

	wantErr(t, e.post("/v1/auth/register", cred("ANA@ex.com", "outra-senha")), 409, "email_taken")

	for _, bad := range []string{"", "a", "@b", "a@", "a@@b", "a@b@c", strings.Repeat("a", 250) + "@b.co"} {
		wantErr(t, e.post("/v1/auth/register", cred(bad, "12345678")), 400, "invalid_email")
	}
	for _, pw := range []string{"", "1234567", strings.Repeat("a", 129)} {
		wantErr(t, e.post("/v1/auth/register", cred("pw@ex.com", pw)), 400, "weak_password")
	}
	wantTokens(t, e.post("/v1/auth/register", cred("utf8@ex.com", "áéíóúãõç")), 201) // 8 caracteres, 16 bytes
	wantTokens(t, e.post("/v1/auth/register", cred("max@ex.com", strings.Repeat("ç", 128))), 201)

	for _, body := range []string{"{", cred("x@ex.com", "12345678") + " {}", `{"email":"x@ex.com","password":"` +
		strings.Repeat("a", 5000) + `"}`} {
		wantErr(t, e.post("/v1/auth/register", body), 400, "bad_request")
	}
}

func TestLogin(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	reg := e.register("bia@ex.com")
	w := e.post("/v1/auth/login", cred("BIA@ex.com", "senha-forte-1"))
	tr := wantTokens(t, w, 200)
	if tr.Refresh == reg.Refresh || e.me(tr.Access).ID != e.me(reg.Access).ID {
		t.Fatal("login não deu tokens novos do mesmo usuário")
	}

	if _, err := e.pool.Exec(context.Background(),
		`INSERT INTO users (email, google_sub, friend_code) VALUES ('g@ex.com', 'sub-g', '0000AAAA')`); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"senha errada": cred("bia@ex.com", "senha-errada"),
		"sem conta":    cred("ninguem@ex.com", "senha-forte-1"),
		"só Google":    cred("g@ex.com", "senha-forte-1"),
		"senha vazia":  cred("bia@ex.com", ""),
	} {
		t.Run(name, func(t *testing.T) {
			wantErr(t, e.post("/v1/auth/login", body), 401, "invalid_credentials")
		})
	}
	wantErr(t, e.post("/v1/auth/login", `{"email":`), 400, "bad_request")
}

func TestRateLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	t0 := time.Now() // relógio parado: o argon2 sob -race leva segundos e o balde recarregaria
	e.h.svc.now = func() time.Time { return t0 }
	for i := range 5 {
		if w := e.post("/v1/auth/login", cred("alvo@ex.com", "x")); w.Code != 401 {
			t.Fatalf("tentativa %d: %d", i+1, w.Code)
		}
	}
	wantErr(t, e.post("/v1/auth/login", cred("ALVO@ex.com", "x")), 429, "rate_limited")

	// register e login contam juntos no mesmo IP
	e.addr = "203.0.113.7:1000"
	for i := range 3 {
		e.post("/v1/auth/login", cred(fmt.Sprintf("u%d@ex.com", i), "x"))
	}
	e.post("/v1/auth/register", cred("novo1@ex.com", "12345678"))
	e.post("/v1/auth/register", cred("novo2@ex.com", "12345678"))
	wantErr(t, e.post("/v1/auth/register", cred("novo3@ex.com", "12345678")), 429, "rate_limited")
	wantErr(t, e.post("/v1/auth/google/exchange", `{"code":"c","code_verifier":"v"}`), 429, "rate_limited")

	// IPv6: o mesmo /64 é um IP só
	for i := range 5 {
		e.addr = fmt.Sprintf("[2001:db8:1:2::%x]:1000", i+1)
		e.post("/v1/auth/login", cred(fmt.Sprintf("v6-%d@ex.com", i), "x"))
	}
	e.addr = "[2001:db8:1:2:ffff::1]:1000"
	wantErr(t, e.post("/v1/auth/login", cred("v6-x@ex.com", "x")), 429, "rate_limited")
}

func TestClientIP(t *testing.T) {
	for _, c := range []struct{ remote, xff, want string }{
		{"198.18.0.1:5", "", "198.18.0.1"},
		{"10.0.0.2:5", "1.2.3.4", "1.2.3.4"},
		{"198.18.0.1:5", "1.2.3.4", "1.2.3.4"},
		{"172.18.0.3:5", "9.9.9.9, 1.2.3.4", "1.2.3.4"},
		{"127.0.0.1:5", "lixo", "127.0.0.1"},
		{"10.0.0.2:5", "", "10.0.0.2"},
		{"[2001:db8:a:b:c::1]:5", "", "2001:db8:a:b::/64"},
		{"[::1]:5", "2001:db8:a:b:c::1", "2001:db8:a:b::/64"},
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = c.remote
		if c.xff != "" {
			r.Header.Set("X-Forwarded-For", c.xff)
		}
		if got := clientIP(r); got != c.want {
			t.Errorf("%s + %q = %q, quero %q", c.remote, c.xff, got, c.want)
		}
	}
}

func TestRefresh(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r1 := e.register("caio@ex.com")
	refresh := func(tok string) *httptest.ResponseRecorder {
		b, _ := json.Marshal(map[string]string{"refresh_token": tok})
		return e.post("/v1/auth/refresh", string(b))
	}
	r2 := wantTokens(t, refresh(r1.Refresh), 200)
	r3 := wantTokens(t, refresh(r2.Refresh), 200)
	if r2.Refresh == r1.Refresh || r3.Refresh == r2.Refresh {
		t.Fatal("refresh não rotacionou")
	}
	if e.me(r3.Access).ID != e.me(r1.Access).ID {
		t.Fatal("refresh trocou de usuário")
	}

	other := wantTokens(t, e.post("/v1/auth/login", cred("caio@ex.com", "senha-forte-1")), 200)

	wantErr(t, refresh(r1.Refresh), 401, "unauthorized") // reuso
	wantErr(t, e.req("GET", "/v1/me", "", r3.Access), 401, "unauthorized")
	wantErr(t, refresh(r3.Refresh), 401, "unauthorized")
	wantTokens(t, refresh(other.Refresh), 200) // outro login, outra família

	wantErr(t, refresh("desconhecido"), 401, "unauthorized")
	wantErr(t, e.post("/v1/auth/refresh", `{}`), 401, "unauthorized")

	r4 := e.register("dani@ex.com")
	if _, err := e.pool.Exec(context.Background(), `UPDATE refresh_tokens SET expires_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	wantErr(t, refresh(r4.Refresh), 401, "unauthorized")
}

func TestLogout(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r1 := e.register("eva@ex.com")
	e.me(r1.Access)
	b, _ := json.Marshal(map[string]string{"refresh_token": r1.Refresh})
	for range 2 {
		if w := e.post("/v1/auth/logout", string(b)); w.Code != 204 {
			t.Fatalf("logout: %d", w.Code)
		}
	}
	wantErr(t, e.post("/v1/auth/refresh", string(b)), 401, "unauthorized")
	wantErr(t, e.req("GET", "/v1/me", "", r1.Access), 401, "unauthorized")
	if w := e.post("/v1/auth/logout", `{"refresh_token":"lixo"}`); w.Code != 204 {
		t.Fatalf("logout lixo: %d", w.Code)
	}
	wantErr(t, e.post("/v1/auth/logout", `nada`), 400, "bad_request")
}

func TestMeDeletedUser(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tr := e.register("fred@ex.com")
	if _, err := e.pool.Exec(context.Background(), `DELETE FROM users`); err != nil {
		t.Fatal(err)
	}
	wantErr(t, e.req("GET", "/v1/me", "", tr.Access), 401, "unauthorized")
	wantErr(t, e.req("GET", "/v1/me", "", ""), 401, "unauthorized")
}

func pkce() (verifier, challenge string) {
	verifier = strings.Repeat("v", 20) + fmt.Sprint(ipSeq.Add(1)) + strings.Repeat("w", 30)
	sum := sha256.Sum256([]byte(verifier))
	return verifier, base64.RawURLEncoding.EncodeToString(sum[:])
}

// fakeGoogle: code "sub|email" → identidade; "falha" → erro.
func fakeGoogle(_ context.Context, code string) (string, string, error) {
	sub, email, ok := strings.Cut(code, "|")
	if !ok {
		return "", "", errors.New("google fora")
	}
	return sub, email, nil
}

func (e *env) googleCallback(googleCode, challenge, appState string) *url.URL {
	e.t.Helper()
	w := e.req("GET", "/v1/auth/google/start?code_challenge="+challenge+"&code_challenge_method=S256&state="+appState, "", "")
	if w.Code != 302 {
		e.t.Fatalf("start: %d %s", w.Code, w.Body.String())
	}
	g, _ := url.Parse(w.Header().Get("Location"))
	q := url.Values{"state": {g.Query().Get("state")}}
	if googleCode != "" {
		q.Set("code", googleCode)
	} else {
		q.Set("error", "access_denied")
	}
	w = e.req("GET", "/v1/auth/google/callback?"+q.Encode(), "", "")
	if w.Code != 302 {
		e.t.Fatalf("callback: %d %s", w.Code, w.Body.String())
	}
	back, _ := url.Parse(w.Header().Get("Location"))
	if back.Scheme != "pokelocationauth" || back.Host != "callback" || back.Query().Get("state") != appState {
		e.t.Fatalf("volta ao app: %s", back)
	}
	return back
}

func (e *env) exchange(code, verifier string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(map[string]string{"code": code, "code_verifier": verifier})
	return e.post("/v1/auth/google/exchange", string(b))
}

func TestGoogleStart(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	_, ch := pkce()
	const st = "estado-do-app-123"
	for name, q := range map[string]string{
		"sem challenge":   "code_challenge_method=S256&state=" + st,
		"challenge curto": "code_challenge=" + ch[:42] + "&code_challenge_method=S256&state=" + st,
		"challenge ruim":  "code_challenge=" + ch[:42] + "+&code_challenge_method=S256&state=" + st,
		"método plain":    "code_challenge=" + ch + "&code_challenge_method=plain&state=" + st,
		"state curto":     "code_challenge=" + ch + "&code_challenge_method=S256&state=" + st[:15],
		"state com ponto": "code_challenge=" + ch + "&code_challenge_method=S256&state=" + st + ".",
		"state 129":       "code_challenge=" + ch + "&code_challenge_method=S256&state=" + strings.Repeat("a", 129),
		"state duplicado": "code_challenge=" + ch + "&code_challenge_method=S256&state=" + st + "&state=" + st,
	} {
		if w := e.req("GET", "/v1/auth/google/start?"+q, "", ""); w.Code != 400 {
			t.Errorf("%s: %d", name, w.Code)
		}
	}

	w := e.req("GET", "/v1/auth/google/start?code_challenge="+ch+"&code_challenge_method=S256&state="+st, "", "")
	g, _ := url.Parse(w.Header().Get("Location"))
	q := g.Query()
	if w.Code != 302 || g.Host != "accounts.google.com" || q.Get("client_id") != "cid" ||
		q.Get("redirect_uri") != "https://api.test/v1/auth/google/callback" || q.Get("response_type") != "code" ||
		q.Get("scope") != "openid email" || q.Get("state") == "" || q.Get("state") == st {
		t.Fatalf("start: %d %s", w.Code, g)
	}
	ttl := e.rdb.TTL(context.Background(), "gstate:"+q.Get("state")).Val()
	if ttl <= 9*60e9 || ttl > 10*60e9 {
		t.Fatalf("TTL do state: %v", ttl)
	}
}

func TestGoogleCallbackErrors(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.h.svc.google = fakeGoogle
	_, ch := pkce()

	if w := e.req("GET", "/v1/auth/google/callback?state=inventado&code=x", "", ""); w.Code != 400 {
		t.Fatalf("state desconhecido: %d", w.Code)
	}
	if back := e.googleCallback("", ch, "app-state-negado-1"); back.Query().Get("error") != "access_denied" ||
		back.Query().Get("code") != "" {
		t.Fatalf("cancelado: %s", back)
	}
	if back := e.googleCallback("falha", ch, "app-state-falhou-1"); back.Query().Get("error") != "google_failed" {
		t.Fatalf("google falhou: %s", back)
	}
}

func TestGoogleLogin(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.h.svc.google = fakeGoogle

	v, ch := pkce()
	back := e.googleCallback("sub-1|Gil@Ex.com", ch, "app-state-0000001")
	code := back.Query().Get("code")
	if raw, err := base64.RawURLEncoding.DecodeString(code); err != nil || len(raw) != 32 {
		t.Fatalf("código %q", code)
	}
	if ttl := e.rdb.TTL(context.Background(), "gcode:"+code).Val(); ttl <= 50e9 || ttl > 60e9 {
		t.Fatalf("TTL do código: %v", ttl)
	}
	tr := wantTokens(t, e.exchange(code, v), 200)
	m := e.me(tr.Access)
	if m.Email == nil || *m.Email != "gil@ex.com" || !crockford.MatchString(m.FriendCode) {
		t.Fatalf("me = %+v", m)
	}
	wantErr(t, e.exchange(code, v), 400, "invalid_grant")

	v2, ch2 := pkce()
	back = e.googleCallback("sub-1|gil@ex.com", ch2, "app-state-0000002")
	if e.me(wantTokens(t, e.exchange(back.Query().Get("code"), v2), 200).Access).ID != m.ID {
		t.Fatal("mesmo sub virou outra conta")
	}

	// verifier errado consome o código
	v3, ch3 := pkce()
	back = e.googleCallback("sub-1|gil@ex.com", ch3, "app-state-0000003")
	wantErr(t, e.exchange(back.Query().Get("code"), v3+"x"), 400, "invalid_grant")
	wantErr(t, e.exchange(back.Query().Get("code"), v3), 400, "invalid_grant")
	wantErr(t, e.exchange("nunca-existiu", v3), 400, "invalid_grant")
	wantErr(t, e.post("/v1/auth/google/exchange", `{"code":"x"}`), 400, "bad_request")
}

// Pre-account hijacking: quem cadastrou o email de outro por senha perde o acesso quando o dono entra pelo Google.
func TestGoogleLinksVerifiedEmail(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.h.svc.google = fakeGoogle
	squatter := e.register("hugo@ex.com")
	id := e.me(squatter.Access).ID
	friend := e.me(e.register("cumplice@ex.com").Access).ID
	ctx := context.Background()
	for _, q := range []string{
		`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'accepted')`,
		`INSERT INTO blocks (blocker_id, blocked_id) VALUES ($2, $1)`,
		`INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_sig,
			kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig) VALUES ($1, 1, '\x05', 1, '\x05', '\x00', 1, '\x08', '\x00')`,
		`INSERT INTO one_time_prekeys (user_id, kind, key_id, public_key) VALUES ($1, 'ec', 1, '\x05')`,
	} {
		args := []any{id}
		if strings.Contains(q, "$2") {
			args = append(args, friend)
		}
		if _, err := e.pool.Exec(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}

	v, ch := pkce()
	back := e.googleCallback("sub-hugo|hugo@ex.com", ch, "app-state-hugo-01")
	tr := wantTokens(t, e.exchange(back.Query().Get("code"), v), 200)
	if e.me(tr.Access).ID != id {
		t.Fatal("não vinculou pelo email")
	}
	wantErr(t, e.req("GET", "/v1/me", "", squatter.Access), 401, "unauthorized")
	b, _ := json.Marshal(map[string]string{"refresh_token": squatter.Refresh})
	wantErr(t, e.post("/v1/auth/refresh", string(b)), 401, "unauthorized")
	wantErr(t, e.post("/v1/auth/login", cred("hugo@ex.com", "senha-forte-1")), 401, "invalid_credentials")
	for _, table := range []string{"friendships WHERE $1 IN (requester_id, addressee_id)",
		"blocks WHERE $1 IN (blocker_id, blocked_id)", "devices WHERE user_id = $1", "one_time_prekeys WHERE user_id = $1"} {
		var n int
		if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM "+table, id).Scan(&n); err != nil || n != 0 {
			t.Errorf("%s: %d linhas (err %v)", table, n, err)
		}
	}
	b, _ = json.Marshal(map[string]string{"refresh_token": tr.Refresh})
	wantTokens(t, e.post("/v1/auth/refresh", string(b)), 200)

	// email já vinculado a outro sub: nunca sobrescreve
	v, ch = pkce()
	back = e.googleCallback("sub-outro|hugo@ex.com", ch, "app-state-hugo-02")
	if back.Query().Get("error") != "google_failed" {
		t.Fatalf("outro sub: %s", back)
	}
}

// Sem t.Parallel: troca o slog global (os testes paralelos só rodam depois dos em série).
func TestUnavailableAndNoLeak(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)
	e := newEnv(t)
	tr := e.register("ivo@ex.com")
	e.pool.Close()
	_ = e.rdb.Close()
	wantErr(t, e.post("/v1/auth/login", cred("ivo@ex.com", "senha-forte-1")), 503, "unavailable")
	wantErr(t, e.post("/v1/auth/register", cred("ivo2@ex.com", "senha-forte-1")), 503, "unavailable")
	b, _ := json.Marshal(map[string]string{"refresh_token": tr.Refresh})
	wantErr(t, e.post("/v1/auth/refresh", string(b)), 503, "unavailable")
	wantErr(t, e.exchange("algum-codigo", "algum-verifier-com-43-chars-aaaaaaaaaaaaaaaa"), 503, "unavailable")
	wantErr(t, e.req("GET", "/v1/me", "", tr.Access), 503, "unavailable")

	logs := buf.String()
	if logs == "" {
		t.Fatal("sem log de erro")
	}
	for _, secret := range []string{"ivo", "senha-forte-1", tr.Refresh, tr.Access, "algum"} {
		if strings.Contains(logs, secret) {
			t.Errorf("log contém %q:\n%s", secret, logs)
		}
	}
}

func TestCleanup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.register("jo@ex.com")
	e.register("lu@ex.com")
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, `UPDATE refresh_tokens SET expires_at = now() - interval '1 second'
		WHERE user_id = (SELECT id FROM users WHERE email = 'jo@ex.com')`); err != nil {
		t.Fatal(err)
	}
	if err := e.h.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens`).Scan(&n)
	if n != 1 {
		t.Fatalf("sobraram %d tokens", n)
	}
}

// ParseAccess não lê o banco: a versão volta para quem chama comparar com token_version (o WS, depois do register).
func TestParseAccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tr := e.register("vera@ex.com")
	uid, exp, tv, err := e.h.ParseAccess(tr.Access)
	if err != nil || uid != e.me(tr.Access).ID || tv != 0 {
		t.Fatalf("ParseAccess: %q %d %v", uid, tv, err)
	}
	if d := time.Until(exp); d < 14*time.Minute || d > 15*time.Minute {
		t.Fatalf("exp em %v", d)
	}
	b, _ := json.Marshal(map[string]string{"refresh_token": tr.Refresh})
	e.post("/v1/auth/logout", string(b))
	r2 := wantTokens(t, e.post("/v1/auth/login", cred("vera@ex.com", "senha-forte-1")), 200)
	if _, _, tv, err := e.h.ParseAccess(r2.Access); err != nil || tv != 1 {
		t.Fatalf("depois do logout: tv %d %v", tv, err)
	}

	future := jwt.NewNumericDate(time.Now().Add(time.Hour))
	zero := 0
	sign := func(m jwt.SigningMethod, key any, c claims) string {
		s, _ := jwt.NewWithClaims(m, c).SignedString(key)
		return s
	}
	k := e.h.svc.key
	for name, tok := range map[string]string{
		"lixo":        "lixo",
		"alg none":    sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, claims{jwt.RegisteredClaims{Subject: uid, ExpiresAt: future}, &zero}),
		"outra chave": sign(jwt.SigningMethodEdDSA, keyOnlyHandler(t).svc.key, claims{jwt.RegisteredClaims{Subject: uid, ExpiresAt: future}, &zero}),
		"vencido":     sign(jwt.SigningMethodEdDSA, k, claims{jwt.RegisteredClaims{Subject: uid, ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute))}, &zero}),
		"sem exp":     sign(jwt.SigningMethodEdDSA, k, claims{jwt.RegisteredClaims{Subject: uid}, &zero}),
		"sem sub":     sign(jwt.SigningMethodEdDSA, k, claims{jwt.RegisteredClaims{ExpiresAt: future}, &zero}),
		"sem tv":      sign(jwt.SigningMethodEdDSA, k, claims{jwt.RegisteredClaims{Subject: uid, ExpiresAt: future}, nil}),
	} {
		if _, _, _, err := e.h.ParseAccess(tok); err != ErrUnauthorized {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestArgonBusy(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for range cap(e.h.svc.argon) {
		e.h.svc.argon <- struct{}{}
	}
	start := time.Now()
	wantErr(t, e.post("/v1/auth/login", cred("ze@ex.com", "senha-forte-1")), 503, "unavailable")
	if d := time.Since(start); d < argonWait || d > argonWait+5*time.Second {
		t.Fatalf("esperou %v", d)
	}
}

func waitLockWait(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		var n int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
	}
	t.Fatal("ninguém esperou o lock")
}

// rotating segura uma rotação de tok em voo (mesmas travas do rotate, sucessor já inserido) enquanto fire roda; fire
// tem de esperar por ela.
func rotating(t *testing.T, e *env, tok string, fire func()) string {
	t.Helper()
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var user, family string
	if err := tx.QueryRow(ctx, `SELECT user_id FROM refresh_tokens WHERE token_hash = $1`, sha(tok)).Scan(&user); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM users WHERE id = $1 FOR NO KEY UPDATE`, user); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, `SELECT family_id FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE`, sha(tok)).
		Scan(&family); err != nil {
		t.Fatal(err)
	}
	next := randomToken()
	if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET used_at = now() WHERE token_hash = $1`, sha(tok)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
		VALUES ($1, $2, $3, now() + interval '1 day')`, user, family, sha(next)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); fire() }()
	waitLockWait(t, e.pool)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	<-done
	return next
}

func (e *env) refreshCode(tok string) int {
	b, _ := json.Marshal(map[string]string{"refresh_token": tok})
	return e.post("/v1/auth/refresh", string(b)).Code
}

// Logout do dono enquanto o ladrão rotaciona o refresh roubado.
func TestLogoutDuringRotation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r := e.register("rot1@ex.com")
	var code int
	next := rotating(t, e, r.Refresh, func() {
		b, _ := json.Marshal(map[string]string{"refresh_token": r.Refresh})
		code = e.post("/v1/auth/logout", string(b)).Code
	})
	if code != 204 || e.refreshCode(next) != 401 {
		t.Fatalf("logout %d; sucessor ainda vale", code)
	}
}

func TestReuseDuringRotation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r1 := e.register("rot2@ex.com")
	r2 := wantTokens(t, e.post("/v1/auth/refresh", `{"refresh_token":"`+r1.Refresh+`"}`), 200)
	var code int
	next := rotating(t, e, r2.Refresh, func() { code = e.refreshCode(r1.Refresh) })
	if code != 401 || e.refreshCode(next) != 401 {
		t.Fatalf("reuso %d; sucessor ainda vale", code)
	}
}

func TestLinkDuringRotation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r := e.register("rot3@ex.com")
	var err error
	next := rotating(t, e, r.Refresh, func() { _, err = e.h.svc.googleUser(context.Background(), "sub-rot3", "rot3@ex.com") })
	if err != nil || e.refreshCode(next) != 401 {
		t.Fatalf("vínculo: %v; sucessor ainda vale", err)
	}
}

// Revogação em voo com o UPDATE users feito e o DELETE ainda não.
func TestRotationDuringRevoke(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r := e.register("rot4@ex.com")
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE users SET token_version = token_version + 1 WHERE email = 'rot4@ex.com'`); err != nil {
		t.Fatal(err)
	}
	codes := make(chan int, 1)
	go func() { codes <- e.refreshCode(r.Refresh) }()
	waitLockWait(t, e.pool)
	if _, err := tx.Exec(ctx, `DELETE FROM refresh_tokens`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if c := <-codes; c != 401 {
		t.Fatalf("refresh durante a revogação: %d", c)
	}
}

func TestHooks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.h.svc.google = fakeGoogle
	var mu sync.Mutex
	var got []string
	e.h.OnRevoke = func(u string) { mu.Lock(); got = append(got, "revoke "+u); mu.Unlock() }
	e.h.OnFriendsChange = func(a, b string) { mu.Lock(); got = append(got, "friends "+a+" "+b); mu.Unlock() }
	take := func() []string { mu.Lock(); defer mu.Unlock(); g := got; got = nil; return g }

	r := e.register("hk1@ex.com")
	id := e.me(r.Access).ID
	b, _ := json.Marshal(map[string]string{"refresh_token": r.Refresh})
	e.post("/v1/auth/logout", string(b))
	if g := take(); len(g) != 1 || g[0] != "revoke "+id {
		t.Fatalf("logout: %v", g)
	}
	e.post("/v1/auth/logout", string(b))
	if g := take(); len(g) != 0 {
		t.Fatalf("logout repetido: %v", g)
	}

	r = e.register("hk2@ex.com")
	id = e.me(r.Access).ID
	e.refreshCode(r.Refresh)
	e.refreshCode(r.Refresh)
	if g := take(); len(g) != 1 || g[0] != "revoke "+id {
		t.Fatalf("reuso: %v", g)
	}

	r = e.register("hk3@ex.com")
	id = e.me(r.Access).ID
	f1, f2 := e.me(e.register("hk4@ex.com").Access).ID, e.me(e.register("hk5@ex.com").Access).ID
	ctx := context.Background()
	for _, q := range []string{
		`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'accepted')`,
		`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($2, $1, 'pending')`,
		`INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload) VALUES ($1, $2, 'a', 'x'), ($2, $1, 'b', 'x')`,
	} {
		other := f1
		if strings.Contains(q, "pending") {
			other = f2
		}
		if _, err := e.pool.Exec(ctx, q, id, other); err != nil {
			t.Fatal(err)
		}
	}
	take()
	if _, err := e.h.svc.googleUser(ctx, "sub-hk3", "hk3@ex.com"); err != nil {
		t.Fatal(err)
	}
	g := take()
	sort.Strings(g)
	want := []string{"friends " + id + " " + f1, "friends " + id + " " + f2, "revoke " + id}
	sort.Strings(want)
	if fmt.Sprint(g) != fmt.Sprint(want) {
		t.Fatalf("vínculo: %v, quero %v", g, want)
	}
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM envelopes`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("envelopes: %d %v", n, err)
	}
}

// Banco travado: o handler desiste no prazo dele em vez de segurar a requisição.
func TestRequestDeadline(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r := e.register("prazo@ex.com")
	e.h.reqWait = 300 * time.Millisecond // depois do register: o argon2 sob -race passa disso
	e.mux = http.NewServeMux()
	e.h.Routes(e.mux, e.h.Authed)
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE users SET token_version = token_version WHERE email = 'prazo@ex.com'`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	b, _ := json.Marshal(map[string]string{"refresh_token": r.Refresh})
	wantErr(t, e.post("/v1/auth/refresh", string(b)), 503, "unavailable")
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("esperou %v", d)
	}
}

func TestCleanupBatches(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.me(e.register("lote@ex.com").Access).ID
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, `INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
		SELECT $1, gen_random_uuid(), sha256(('lote' || g)::bytea), now() - interval '1 day'
		FROM generate_series(1, $2::int) g`, id, 2*cleanupBatch+500); err != nil {
		t.Fatal(err)
	}
	if err := e.h.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("sobraram %d (%v)", n, err)
	}
}

// Logout e reuso de refresh apagam o token FCM: o aparelho não recebe ping da conta que saiu.
func TestRevocationClearsFCM(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	withDevice := func(email string) (tokenResp, string) {
		r := e.register(email)
		id := e.me(r.Access).ID
		if _, err := e.pool.Exec(ctx, `INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id,
			signed_prekey, signed_prekey_sig, kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig, fcm_token)
			VALUES ($1, 1, '\x05', 1, '\x05', '\x00', 1, '\x08', '\x00', $2)`, id, "tok-"+id); err != nil {
			t.Fatal(err)
		}
		return r, id
	}
	fcm := func(id string) *string {
		var tok *string
		if err := e.pool.QueryRow(ctx, `SELECT fcm_token FROM devices WHERE user_id = $1`, id).Scan(&tok); err != nil {
			t.Fatal(err)
		}
		return tok
	}
	r, id := withDevice("fcm1@ex.com")
	b, _ := json.Marshal(map[string]string{"refresh_token": r.Refresh})
	e.post("/v1/auth/logout", string(b))
	if fcm(id) != nil {
		t.Fatal("logout não apagou o token FCM")
	}
	r, id = withDevice("fcm2@ex.com")
	e.refreshCode(r.Refresh)
	e.refreshCode(r.Refresh) // reuso
	if fcm(id) != nil {
		t.Fatal("reuso não apagou o token FCM")
	}
	_, other := withDevice("fcm3@ex.com")
	if fcm(other) == nil {
		t.Fatal("apagou o token de outra conta")
	}
}

// Vínculo Google tira a conta de todos os grupos (o invasor não fica nos grupos do dono real nem o contrário).
func TestLinkLeavesGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.h.svc.google = fakeGoogle
	ctx := context.Background()
	id := e.me(e.register("grp@ex.com").Access).ID
	f1, other := e.me(e.register("grp1@ex.com").Access).ID, e.me(e.register("grp2@ex.com").Access).ID
	group := func(owner string, members ...string) string {
		var g string
		if err := e.pool.QueryRow(ctx, `INSERT INTO groups (owner_id) VALUES ($1) RETURNING id`, owner).Scan(&g); err != nil {
			t.Fatal(err)
		}
		for i, m := range append([]string{owner}, members...) {
			if _, err := e.pool.Exec(ctx, `INSERT INTO group_members (group_id, user_id, joined_at)
				VALUES ($1, $2, now() + make_interval(secs => $3))`, g, m, i); err != nil {
				t.Fatal(err)
			}
		}
		return g
	}
	owned, joined, alone := group(id, f1), group(other, id), group(id)

	var mu sync.Mutex
	got := map[string][]string{}
	e.h.OnGroupsChange = func(g string, users []string) {
		mu.Lock()
		got[g] = slices.Sorted(slices.Values(users))
		mu.Unlock()
	}
	if _, err := e.h.svc.googleUser(ctx, "sub-grp", "grp@ex.com"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM group_members WHERE user_id = $1`, id).Scan(&n); err != nil || n != 0 {
		t.Fatalf("ainda em %d grupos (%v)", n, err)
	}
	var owner string
	if err := e.pool.QueryRow(ctx, `SELECT owner_id FROM groups WHERE id = $1`, owned).Scan(&owner); err != nil || owner != f1 {
		t.Fatalf("dono do grupo que era dele: %q (%v)", owner, err)
	}
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM groups WHERE id = $1`, alone).Scan(&n); err != nil || n != 0 {
		t.Fatal("grupo só dele não sumiu")
	}
	mu.Lock()
	defer mu.Unlock()
	if want := slices.Sorted(slices.Values([]string{f1, id})); fmt.Sprint(got[owned]) != fmt.Sprint(want) {
		t.Errorf("aviso de %s: %v", owned, got[owned])
	}
	if want := slices.Sorted(slices.Values([]string{other, id})); fmt.Sprint(got[joined]) != fmt.Sprint(want) {
		t.Errorf("aviso de %s: %v", joined, got[joined])
	}
	if fmt.Sprint(got[alone]) != fmt.Sprint([]string{id}) {
		t.Errorf("aviso de %s: %v", alone, got[alone])
	}
}
