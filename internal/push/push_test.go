package push

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

var seq atomic.Int32

func newUser(t *testing.T, pool *pgxpool.Pool, device bool) string {
	t.Helper()
	ctx := context.Background()
	n := seq.Add(1)
	var id string
	if err := pool.QueryRow(ctx, `INSERT INTO users (email, friend_code) VALUES ($1, $2) RETURNING id`,
		fmt.Sprintf("p%d@ex.com", n), fmt.Sprintf("%08d", n)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if device {
		if _, err := pool.Exec(ctx, `INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id,
			signed_prekey, signed_prekey_sig, kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig)
			VALUES ($1, 1, '\x05', 1, '\x05', '\x00', 1, '\x08', '\x00')`, id); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func token(t *testing.T, pool *pgxpool.Pool, user string) *string {
	t.Helper()
	var tok *string
	if err := pool.QueryRow(context.Background(), `SELECT fcm_token FROM devices WHERE user_id = $1`, user).Scan(&tok); err != nil {
		t.Fatal(err)
	}
	return tok
}

func put(h *Handler, user, body string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	h.Routes(mux, func(next http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next(w, r.WithContext(httpx.WithUser(r.Context(), user)))
		})
	})
	r := httptest.NewRequest("PUT", "/v1/devices/fcm", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestPutToken(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	h := NewHandler(pool)
	u := newUser(t, pool, true)
	if w := put(h, u, `{"token":"tok-1"}`); w.Code != 204 {
		t.Fatalf("put: %d %s", w.Code, w.Body.String())
	}
	if tok := token(t, pool, u); tok == nil || *tok != "tok-1" {
		t.Fatalf("token %v", tok)
	}
	if w := put(h, u, `{"token":null}`); w.Code != 204 || token(t, pool, u) != nil {
		t.Fatalf("null não apagou: %d", w.Code)
	}
	for _, bad := range []string{`{"token":""}`, `{}`, `{"token":"` + strings.Repeat("x", 5000) + `"}`, `x`} {
		if w := put(h, u, bad); w.Code != 400 {
			t.Errorf("%.20s: %d", bad, w.Code)
		}
	}
	if w := put(h, newUser(t, pool, false), `{"token":"t"}`); w.Code != 404 || !strings.Contains(w.Body.String(), "not_found") {
		t.Fatalf("sem aparelho: %d %s", w.Code, w.Body.String())
	}
}

type sent struct{ user, token string }

type fakeSender struct {
	mu   sync.Mutex
	got  []sent
	data []Data
	fail func(token string) error
}

func (f *fakeSender) Send(_ context.Context, user, token string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, sent{user, token})
	if f.fail != nil {
		return f.fail(token)
	}
	return nil
}

func (f *fakeSender) SendData(ctx context.Context, user, token string, msgs []Data) error {
	f.mu.Lock()
	f.data = append(f.data, msgs...)
	f.mu.Unlock()
	return f.Send(ctx, user, token)
}

func (f *fakeSender) wait(t *testing.T, n int) []sent {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		f.mu.Lock()
		got := append([]sent(nil), f.got...)
		f.mu.Unlock()
		if len(got) >= n {
			return got
		}
	}
	t.Fatalf("esperava %d envios", n)
	return nil
}

func TestNotify(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	fs := &fakeSender{fail: func(tok string) error {
		if tok == "morto" {
			return ErrInvalidToken
		}
		return nil
	}}
	s := New(pool, fs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	withTok, noTok, dead := newUser(t, pool, true), newUser(t, pool, true), newUser(t, pool, true)
	h := NewHandler(pool)
	put(h, withTok, `{"token":"tok-a"}`)
	put(h, dead, `{"token":"morto"}`)

	s.Notify(noTok) // sem token: nada
	s.Notify(withTok)
	s.Notify(dead)
	got := fs.wait(t, 2)
	time.Sleep(100 * time.Millisecond)
	fs.mu.Lock()
	n := len(fs.got)
	fs.mu.Unlock()
	if n != 2 {
		t.Fatalf("envios: %v", fs.got)
	}
	tokens := map[string]string{}
	for _, g := range got {
		tokens[g.user] = g.token
	}
	if tokens[withTok] != "tok-a" || tokens[dead] != "morto" {
		t.Fatalf("envios: %v", got)
	}
	for deadline := time.Now().Add(5 * time.Second); token(t, pool, dead) != nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("token inválido não foi apagado")
		}
	}
	if token(t, pool, withTok) == nil {
		t.Fatal("token bom apagado")
	}
}

func TestNotifyCoalesces(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	fs := &fakeSender{}
	s := New(pool, fs)
	u := newUser(t, pool, true)
	put(NewHandler(pool), u, `{"token":"tok"}`)
	for range 50 {
		s.Notify(u)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	fs.wait(t, 1)
	time.Sleep(100 * time.Millisecond)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.got) != 1 {
		t.Fatalf("%d envios", len(fs.got))
	}
}

func TestLogSenderNoToken(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(old)
	s, err := NewSender(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), "u-1", "segredo-do-token"); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, `"msg":"fcm ping"`) || !strings.Contains(out, `"user":"u-1"`) ||
		strings.Contains(out, "segredo") {
		t.Fatalf("log: %s", out)
	}
}

func TestNewSenderBadCredentials(t *testing.T) {
	if _, err := NewSender(context.Background(), "/nao/existe.json"); err == nil {
		t.Fatal("credencial inexistente aceita")
	}
}

func TestTokenUnique(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	h := NewHandler(pool)
	u1, u2 := newUser(t, pool, true), newUser(t, pool, true)
	put(h, u1, `{"token":"mesmo"}`)
	if w := put(h, u2, `{"token":"mesmo"}`); w.Code != 204 {
		t.Fatalf("put: %d", w.Code)
	}
	if token(t, pool, u1) != nil {
		t.Fatal("o token ficou nas duas contas")
	}
	if tok := token(t, pool, u2); tok == nil || *tok != "mesmo" {
		t.Fatalf("u2: %v", tok)
	}
	if w := put(h, u2, `{"token":"mesmo"}`); w.Code != 204 { // de novo na mesma conta: idempotente
		t.Fatalf("idempotente: %d", w.Code)
	}
}

func TestNotifyThroughput(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	ctx := context.Background()
	var ids []string
	rows, err := pool.Query(ctx, `WITH u AS (
			INSERT INTO users (email, friend_code) SELECT 'vz' || g || '@ex.com', 'V' || lpad(g::text, 7, '0')
			FROM generate_series(1, 1000) g RETURNING id)
		INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_sig,
			kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig, fcm_token)
		SELECT id, 1, '\x05', 1, '\x05', '\x00', 1, '\x08', '\x00', 'tok-' || id FROM u RETURNING user_id::text`)
	if err != nil {
		t.Fatal(err)
	}
	if ids, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	slow := senderFunc(func(context.Context, string, string) error {
		time.Sleep(50 * time.Millisecond)
		n.Add(1)
		return nil
	})
	s := New(pool, slow)
	for _, id := range ids {
		s.Notify(id)
	}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	go s.Run(rctx)
	for n.Load() < int32(len(ids)) {
		if time.Since(start) > 15*time.Second {
			t.Fatalf("%d de %d em %v", n.Load(), len(ids), time.Since(start))
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("%d avisos em %v", len(ids), time.Since(start))
}

type senderFunc func(ctx context.Context, user, token string) error

func (f senderFunc) Send(ctx context.Context, user, token string) error { return f(ctx, user, token) }
func (f senderFunc) SendData(ctx context.Context, user, token string, _ []Data) error {
	return f(ctx, user, token)
}

func TestTryPush(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	fs := &fakeSender{fail: func(tok string) error {
		if tok == "morto" {
			return ErrInvalidToken
		}
		return nil
	}}
	s := New(pool, fs)
	h := NewHandler(pool)
	ok, dead := newUser(t, pool, true), newUser(t, pool, true)
	put(h, ok, `{"token":"tok-ok"}`)
	put(h, dead, `{"token":"morto"}`)
	msgs := []Data{{Fields: map[string]string{"t": "p"}, Collapse: "p:1", TTL: time.Minute}, {Fields: map[string]string{"t": "p"}}}
	for range maxData / len(msgs) { // o teto é em mensagens, não em envios
		if !s.TryPush(ok, "tok-ok", msgs) {
			t.Fatal("recusou antes do teto")
		}
	}
	if s.TryPush(ok, "tok-ok", msgs[:1]) {
		t.Fatal("passou do teto")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	fs.wait(t, maxData/len(msgs))
	if !s.TryPush(dead, "morto", msgs) {
		t.Fatal("fila não esvaziou")
	}
	for deadline := time.Now().Add(5 * time.Second); token(t, pool, dead) != nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("token inválido não foi apagado")
		}
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.got[0] != (sent{ok, "tok-ok"}) || len(fs.data) != maxData+2 || fs.data[0].Collapse != "p:1" ||
		token(t, pool, ok) == nil {
		t.Fatalf("envios: %d %d", len(fs.got), len(fs.data))
	}
}
