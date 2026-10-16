package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/auth"
	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/ratelimit"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

type env struct {
	t    *testing.T
	hub  *Hub
	srv  *httptest.Server
	pool *pgxpool.Pool
	exp  sync.Map // token → vencimento, quando não é 1 h
	locs chan string
}

var codeSeq atomic.Int32

func newEnv(t *testing.T) *env {
	t.Helper()
	t.Parallel()
	return newEnvPool(t, testdb.Pool(t))
}

// newEnvPool: outro Hub (outra instância) sobre o mesmo banco.
func newEnvPool(t *testing.T, pool *pgxpool.Pool) *env {
	t.Helper()
	e := &env{t: t, pool: pool, locs: make(chan string, 16)}
	parse := func(tok string) (string, time.Time, int, error) {
		if len(tok) != 36 {
			return "", time.Time{}, 0, auth.ErrUnauthorized
		}
		exp := time.Now().Add(time.Hour)
		if v, ok := e.exp.Load(tok); ok {
			exp = v.(time.Time)
		}
		return tok, exp, 0, nil
	}
	e.hub = New(e.pool, parse, func(c *Conn, f wire.Frame) {
		e.locs <- fmt.Sprintf("%s %d %s", c.User(), f.Type, f.Payload)
	})
	e.hub.authWait = 300 * time.Millisecond
	e.hub.chat = ratelimit.New(1 << 20) // rajadas dos testes; TestChatRateLimit põe o limite de verdade
	e.hub.groupRcpt = ratelimit.New(1 << 20)
	e.hub.acks = ratelimit.New(1 << 20)
	mux := http.NewServeMux()
	e.hub.Routes(mux, func(h http.HandlerFunc) http.Handler { // X-User no lugar do Bearer
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h(w, r.WithContext(httpx.WithUser(r.Context(), r.Header.Get("X-User"))))
		})
	})
	e.srv = httptest.NewServer(mux)
	t.Cleanup(e.srv.Close)
	return e
}

func (e *env) user() string {
	e.t.Helper()
	var id string
	n := codeSeq.Add(1)
	code := fmt.Sprintf("%08d", n)
	if err := e.pool.QueryRow(context.Background(), `INSERT INTO users (email, friend_code, username) VALUES ($1, $2, $3)
		RETURNING id`, fmt.Sprintf("u%d@ex.com", n), code, fmt.Sprintf("user%d", n)).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *env) friends(a, b string) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), `INSERT INTO friendships (requester_id, addressee_id, status)
		VALUES ($1, $2, 'accepted')`, a, b); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) count(q string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

type client struct {
	t  *testing.T
	ws *websocket.Conn
}

func (e *env) dialRaw(h http.Header) (*websocket.Conn, *http.Response, error) {
	return websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(e.srv.URL, "http")+"/v1/ws", h)
}

func (e *env) connect(token string) *client {
	e.t.Helper()
	ws, _, err := e.dialRaw(nil)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { ws.Close() })
	c := &client{t: e.t, ws: ws}
	c.send(wire.AppendAuth(nil, []byte(token)))
	return c
}

// online espera o Auth terminar: o servidor não responde ao Auth, mas só responde a um frame inválido depois dele.
func (e *env) online(user string) *client {
	e.t.Helper()
	c := e.connect(user)
	c.send([]byte{0xff})
	c.wantError("bad_frame")
	return c
}

func (e *env) waitOnline(user string, n int) {
	e.t.Helper()
	e.waitOnlineWithin(user, n, 5*time.Second)
}

func (e *env) waitOnlineWithin(user string, n int, d time.Duration) {
	e.t.Helper()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if e.hub.online(user) == n {
			return
		}
	}
	e.t.Fatalf("%s: %d conexões, quero %d", user, e.hub.online(user), n)
}

func (c *client) send(b []byte) {
	c.t.Helper()
	if err := c.ws.WriteMessage(websocket.BinaryMessage, b); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) read() wire.Frame {
	c.t.Helper()
	_ = c.ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, b, err := c.ws.ReadMessage()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	f, err := wire.Decode(b)
	if err != nil {
		c.t.Fatalf("decode: %v", err)
	}
	return f
}

func (c *client) quiet(d time.Duration) {
	c.t.Helper()
	_ = c.ws.SetReadDeadline(time.Now().Add(d))
	if _, b, err := c.ws.ReadMessage(); err == nil {
		c.t.Fatalf("frame inesperado: %v", b)
	}
}

func (c *client) wantError(code string) {
	c.t.Helper()
	if f := c.read(); f.Type != wire.Error || string(f.Code) != code {
		c.t.Fatalf("quero Error %s, veio %d %s", code, f.Type, f.Code)
	}
}

func (c *client) wantClose(code int) {
	c.t.Helper()
	_ = c.ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		_, _, err := c.ws.ReadMessage()
		if err == nil {
			continue
		}
		var ce *websocket.CloseError
		if !errors.As(err, &ce) || ce.Code != code {
			c.t.Fatalf("quero close %d, veio %v", code, err)
		}
		return
	}
}

func envelope(to, id, payload string) []byte {
	return wire.AppendEnvelope(nil, []byte(to), []byte(id), []byte(payload))
}

func TestAuth(t *testing.T) {
	e := newEnv(t)
	a := e.user()

	t.Run("token ruim", func(t *testing.T) {
		c := e.connect("ruim")
		c.wantError("unauthorized")
		c.wantClose(websocket.ClosePolicyViolation)
	})
	t.Run("primeiro frame não é Auth", func(t *testing.T) {
		ws, _, err := e.dialRaw(nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ws.Close()
		c := &client{t: t, ws: ws}
		c.send(envelope(a, "m1", "x"))
		c.wantClose(websocket.ClosePolicyViolation)
	})
	t.Run("sem Auth no prazo", func(t *testing.T) {
		ws, _, err := e.dialRaw(nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ws.Close()
		(&client{t: t, ws: ws}).wantClose(websocket.ClosePolicyViolation)
	})
	t.Run("texto", func(t *testing.T) {
		c := e.online(a)
		if err := c.ws.WriteMessage(websocket.TextMessage, []byte("oi")); err != nil {
			t.Fatal(err)
		}
		c.wantClose(websocket.CloseUnsupportedData)
		e.waitOnline(a, 0)
	})
	t.Run("acima de 64 KiB", func(t *testing.T) {
		c := e.online(a)
		c.send(make([]byte, 64<<10+1))
		c.wantClose(websocket.CloseMessageTooBig)
		e.waitOnline(a, 0)
	})
	t.Run("Origin de navegador", func(t *testing.T) {
		_, res, err := e.dialRaw(http.Header{"Origin": {"https://evil.example"}})
		if err == nil || res == nil || res.StatusCode != http.StatusForbidden {
			t.Fatalf("Origin aceito: %v %v", err, res)
		}
	})
	t.Run("conta apagada", func(t *testing.T) {
		b := e.user()
		if _, err := e.pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, b); err != nil {
			t.Fatal(err)
		}
		c := e.connect(b)
		c.wantError("account_deleted")
		c.wantClose(websocket.ClosePolicyViolation)
	})
	t.Run("vence com o token", func(t *testing.T) {
		b := e.user()
		e.exp.Store(b, time.Now().Add(300*time.Millisecond))
		c := e.online(b)
		c.wantError("unauthorized")
		c.wantClose(websocket.ClosePolicyViolation)
		e.waitOnline(b, 0)
	})
}

func TestMaxConnections(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	first := e.online(a)
	for i := 2; i <= 4; i++ {
		e.connect(a)
		e.waitOnline(a, i)
	}
	e.connect(a)
	first.wantClose(websocket.ClosePolicyViolation)
	e.waitOnline(a, 4)
}

func TestEnvelopeLive(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	ca, cb1, cb2 := e.online(a), e.online(b), e.online(b)

	ca.send(envelope(b, "m1", "cifrado"))
	if f := ca.read(); f.Type != wire.Ack || string(f.Peer) != b || string(f.MsgID) != "m1" {
		t.Fatalf("Ack ao remetente: %+v", f)
	}
	for _, cb := range []*client{cb1, cb2} {
		if f := cb.read(); f.Type != wire.Envelope || string(f.Peer) != a || string(f.MsgID) != "m1" ||
			string(f.Payload) != "cifrado" {
			t.Fatalf("Envelope ao destinatário: %+v", f)
		}
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE recipient_id = $1`, b); n != 1 {
		t.Fatalf("gravados: %d", n)
	}

	ca.send(envelope(b, "m1", "cifrado")) // reenvio
	if f := ca.read(); f.Type != wire.Ack || string(f.MsgID) != "m1" {
		t.Fatalf("Ack do reenvio: %+v", f)
	}
	cb1.quiet(200 * time.Millisecond)
	if n := e.count(`SELECT count(*) FROM envelopes`); n != 1 {
		t.Fatalf("reenvio duplicou: %d", n)
	}

	cb2.send(wire.AppendAck(nil, []byte(a), []byte("m1")))
	for deadline := time.Now().Add(5 * time.Second); e.count(`SELECT count(*) FROM envelopes`) != 0; {
		if time.Now().After(deadline) {
			t.Fatal("Ack não apagou")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestEnvelopeRules(t *testing.T) {
	e := newEnv(t)
	a, b, stranger := e.user(), e.user(), e.user()
	e.friends(a, b)
	ca := e.online(a)

	ca.send(envelope(stranger, "m1", "x"))
	ca.wantError("forbidden")
	ca.send(wire.AppendEnvelope(nil, []byte(b), []byte("m2"), make([]byte, wire.MaxEnvelopePayload+1)))
	ca.wantError("too_large")
	ca.send([]byte{wire.Envelope, 0, 1})
	ca.wantError("bad_frame")
	ca.send(envelope(b, "m\x00", "x")) // o Postgres não guarda NUL nem UTF-8 inválido em text
	ca.wantError("bad_frame")
	ca.send(envelope(b, "\xff\xfe", "x"))
	ca.wantError("bad_frame")
	// o wire.Decode não sabe de direção
	for _, f := range [][]byte{
		wire.AppendAuth(nil, []byte(a)),
		wire.AppendLocationUpdate(nil, []byte(b), []byte("p")),
		wire.AppendLocationOff(nil, []byte(b)),
		wire.AppendError(nil, "x"),
	} {
		ca.send(f)
		ca.wantError("bad_frame")
	}

	ca.send(envelope(b, "ok", "x"))
	if f := ca.read(); f.Type != wire.Ack {
		t.Fatalf("depois dos erros: %+v", f)
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE sender_id = $1`, a); n != 1 {
		t.Fatalf("gravados: %d", n)
	}

	cs := e.online(stranger)
	cs.send(wire.AppendAck(nil, []byte(a), []byte("ok")))
	cs.quiet(200 * time.Millisecond)
	if n := e.count(`SELECT count(*) FROM envelopes`); n != 1 {
		t.Fatalf("Ack alheio apagou")
	}
}

func TestReloadFriends(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	ca := e.online(a)
	ca.send(envelope(b, "m1", "x"))
	ca.wantError("forbidden")
	e.friends(a, b)
	e.hub.ReloadFriends(a)
	ca.send(envelope(b, "m2", "x"))
	if f := ca.read(); f.Type != wire.Ack {
		t.Fatalf("depois do ReloadFriends: %+v", f)
	}
	e.hub.ReloadFriends(b) // offline aqui
}

func TestLocationOffDirect(t *testing.T) {
	e := newEnv(t)
	a, b, c := e.user(), e.user(), e.user()
	cb := e.online(b)
	e.hub.LocationOff(a, []string{b, c}) // a nunca conectou; c offline
	if f := cb.read(); f.Type != wire.LocationOff || string(f.Peer) != a {
		t.Fatalf("LocationOff: %+v", f)
	}
}

func TestBacklog(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	const n = 3*queueCap + 7
	if _, err := e.pool.Exec(context.Background(), `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		SELECT $1, $2, 'm' || g, 'p' FROM generate_series(1, $3) g`, b, a, n); err != nil {
		t.Fatal(err)
	}
	cb := e.connect(b)
	time.Sleep(300 * time.Millisecond) // cliente lento: a fila enche antes de ele ler
	for i := 1; i <= n; i++ {
		f := cb.read()
		if f.Type != wire.Envelope || string(f.Peer) != a || string(f.MsgID) != fmt.Sprintf("m%d", i) {
			t.Fatalf("envelope %d: %d %s", i, f.Type, f.MsgID)
		}
	}
}

func TestSlowConsumerClosed(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	e.online(b) // nunca lê
	ca := e.online(a)
	big := strings.Repeat("x", 30<<10)
	for i := range 4 * queueCap {
		ca.send(envelope(b, fmt.Sprintf("m%d", i), big))
	}
	// cai pela fila cheia (1013) ou, se o buffer do kernel engolir os MB (loopback do macOS), pelo write travado em
	// writeWait: os dois são o comportamento certo para um cliente que não lê
	e.waitOnlineWithin(b, 0, writeWait+5*time.Second)
	for deadline := time.Now().Add(30 * time.Second); e.count(`SELECT count(*) FROM envelopes`) != 4*queueCap; {
		if time.Now().After(deadline) {
			t.Fatalf("gravados %d", e.count(`SELECT count(*) FROM envelopes`))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLocationHooks(t *testing.T) {
	e := newEnv(t)
	a, b, stranger := e.user(), e.user(), e.user()
	e.friends(a, b)
	ca, cb, cs := e.online(a), e.online(b), e.online(stranger)

	ca.send(wire.AppendLocation(nil, []byte("pos")))
	if got := <-e.locs; got != fmt.Sprintf("%s %d pos", a, wire.Location) {
		t.Fatalf("onLocation: %q", got)
	}
	ca.send(wire.AppendLocationOff(nil, nil))
	if got := <-e.locs; got != fmt.Sprintf("%s %d ", a, wire.LocationOff) {
		t.Fatalf("onLocation off: %q", got)
	}

	e.hub.SendFriends(a, wire.AppendLocationUpdate(Buf(), []byte(a), []byte("pos")))
	if f := cb.read(); f.Type != wire.LocationUpdate || string(f.Peer) != a || string(f.Payload) != "pos" {
		t.Fatalf("SendFriends: %+v", f)
	}
	cs.quiet(200 * time.Millisecond)
	ca.quiet(50 * time.Millisecond)
}

func (e *env) rest(method, path, user, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("X-User", user)
	w := httptest.NewRecorder()
	mux := http.NewServeMux()
	e.hub.Routes(mux, func(h http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h(w, r.WithContext(httpx.WithUser(r.Context(), r.Header.Get("X-User"))))
		})
	})
	mux.ServeHTTP(w, r)
	return w
}

type queued struct {
	Envelopes []struct {
		ID          string `json:"id"`
		From        string `json:"from"`
		ClientMsgID string `json:"client_msg_id"`
		Payload     []byte `json:"payload"`
	} `json:"envelopes"`
}

func TestRESTQueue(t *testing.T) {
	e := newEnv(t)
	a, b, c := e.user(), e.user(), e.user()
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		SELECT $1, $2, 'm' || g, 'cifra' FROM generate_series(1, 60) g`, b, a); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		VALUES ($1, $2, 'de-b', 'x')`, c, b); err != nil {
		t.Fatal(err)
	}

	w := e.rest("GET", "/v1/envelopes", b, "")
	var q queued
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &q) != nil || len(q.Envelopes) != 50 {
		t.Fatalf("página: %d %d", w.Code, len(q.Envelopes))
	}
	if f := q.Envelopes[0]; f.From != a || f.ClientMsgID != "m1" || string(f.Payload) != "cifra" {
		t.Fatalf("primeiro: %+v", f)
	}

	ids := make([]string, 0, len(q.Envelopes))
	for _, x := range q.Envelopes {
		ids = append(ids, x.ID)
	}
	other := e.rest("GET", "/v1/envelopes", c, "")
	var oq queued
	_ = json.Unmarshal(other.Body.Bytes(), &oq)
	ids = append(ids, oq.Envelopes[0].ID, "999999999") // alheio e inexistente
	body, _ := json.Marshal(map[string][]string{"ids": ids})
	if w := e.rest("POST", "/v1/envelopes/ack", b, string(body)); w.Code != 204 {
		t.Fatalf("ack: %d %s", w.Code, w.Body.String())
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE recipient_id = $1`, b); n != 10 {
		t.Fatalf("sobraram %d de b", n)
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE recipient_id = $1`, c); n != 1 {
		t.Fatal("ack apagou envelope alheio")
	}

	many, _ := json.Marshal(map[string][]string{"ids": make([]string, 101)})
	for _, bad := range []string{string(many), `{"ids":["abc"]}`, `{"ids":`} {
		if w := e.rest("POST", "/v1/envelopes/ack", b, bad); w.Code != 400 {
			t.Errorf("%.30s: %d", bad, w.Code)
		}
	}
	if w := e.rest("GET", "/v1/envelopes", c, ""); !strings.Contains(w.Body.String(), `"client_msg_id":"de-b"`) {
		t.Fatalf("fila de c: %s", w.Body.String())
	}
	if w := e.rest("GET", "/v1/envelopes", e.user(), ""); w.Body.String() != "{\"envelopes\":[]}\n" {
		t.Fatalf("fila vazia: %s", w.Body.String())
	}
}

func TestRESTQueueSize(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	if _, err := e.pool.Exec(context.Background(), `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		SELECT $1, $2, 'm' || g, decode(repeat('ab', 20000), 'hex') FROM generate_series(1, 5) g`, b, a); err != nil {
		t.Fatal(err)
	}
	w := e.rest("GET", "/v1/envelopes", b, "")
	var q queued
	if json.Unmarshal(w.Body.Bytes(), &q) != nil || len(q.Envelopes) != 1 || w.Body.Len() > 48<<10 {
		t.Fatalf("%d envelopes, %d bytes", len(q.Envelopes), w.Body.Len())
	}
}

func TestCleanup(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload, created_at)
		VALUES ($1, $2, 'velho', 'x', now() - interval '31 days'), ($1, $2, 'novo', 'x', now())`, b, a); err != nil {
		t.Fatal(err)
	}
	if err := e.hub.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE client_msg_id = 'novo'`); n != 1 ||
		e.count(`SELECT count(*) FROM envelopes`) != 1 {
		t.Fatal("cleanup errado")
	}
}

func TestKick(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	ca1 := e.online(a)
	ca2 := e.connect(a)
	e.waitOnline(a, 2)
	cb := e.online(b)
	e.hub.Kick(a)
	for _, c := range []*client{ca1, ca2} {
		c.wantError("unauthorized")
		c.wantClose(websocket.ClosePolicyViolation)
	}
	e.waitOnline(a, 0)
	cb.quiet(100 * time.Millisecond)
	e.hub.Kick(a) // ninguém online
}

func TestNoReadBufferAliasing(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	ca, cb1, cb2 := e.online(a), e.online(b), e.online(b)
	const n = 200
	go func() {
		for i := range n {
			_ = ca.ws.WriteMessage(websocket.BinaryMessage,
				envelope(b, fmt.Sprintf("m%03d", i), strings.Repeat(string(rune('a'+i%26)), 100+i)))
		}
	}()
	// o mesmo envelope pode chegar ao vivo e pelo backlog (o app deduplica): confere o conteúdo pelo msg id
	for _, cb := range []*client{cb1, cb2} {
		seen := map[string]bool{}
		for len(seen) < n {
			f := cb.read()
			var i int
			if _, err := fmt.Sscanf(string(f.MsgID), "m%03d", &i); err != nil || f.Type != wire.Envelope {
				t.Fatalf("frame inesperado: %d %q", f.Type, f.MsgID)
			}
			if want := strings.Repeat(string(rune('a'+i%26)), 100+i); string(f.Payload) != want || string(f.Peer) != a {
				t.Fatalf("envelope %d corrompido: %q", i, f.Payload)
			}
			seen[string(f.MsgID)] = true
		}
	}
	for range n {
		if f := ca.read(); f.Type != wire.Ack {
			t.Fatalf("Ack: %+v", f)
		}
	}
}

// Amizade desfeita enquanto a 2ª conexão do usuário se autentica: a lista em memória não pode voltar a ser a velha.
func TestFriendsChangeDuringAuth(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	e.online(a)
	cb := e.online(b)
	e.hub.beforeRegister = func() {
		if _, err := e.pool.Exec(context.Background(), `DELETE FROM friendships`); err != nil {
			t.Error(err)
		}
		e.hub.ReloadFriends(a)
		e.hub.ReloadFriends(b)
	}
	ca2 := e.connect(a)
	e.waitOnline(a, 2)
	e.hub.beforeRegister = nil

	e.hub.SendFriends(a, wire.AppendLocationUpdate(Buf(), []byte(a), []byte("pos")))
	cb.quiet(300 * time.Millisecond)
	ca2.send(envelope(b, "m1", "x"))
	ca2.wantError("forbidden")
}

// Fan-out com um destinatário lento: os rápidos liberam o buffer compartilhado antes; se ele voltasse ao pool cedo, o
// lento receberia bytes de outro frame.
func TestFanOutSlowRecipient(t *testing.T) {
	e := newEnv(t)
	a, slow, f1, f2 := e.user(), e.user(), e.user(), e.user()
	for _, f := range []string{slow, f1, f2} {
		e.friends(a, f)
	}
	e.online(a)
	cs, c1, c2 := e.online(slow), e.online(f1), e.online(f2)
	const n = queueCap - 4 // sem descarte
	payload := func(i int) string { return fmt.Sprintf("pos-%02d-%s", i, strings.Repeat("z", i*7)) }
	for i := range n {
		e.hub.SendFriends(a, wire.AppendLocationUpdate(Buf(), []byte(a), []byte(payload(i))))
		// força o reaproveitamento do que os rápidos liberam
		ob := getOut()
		ob.b = append(ob.b, strings.Repeat("!", 300)...)
		ob.release()
	}
	for _, c := range []*client{c1, c2, cs} {
		for i := range n {
			if f := c.read(); f.Type != wire.LocationUpdate || string(f.Payload) != payload(i) {
				t.Fatalf("frame %d: %d %q", i, f.Type, f.Payload)
			}
		}
	}
}

func TestRESTDeadline(t *testing.T) {
	e := newEnv(t)
	e.hub.dbWait = 300 * time.Millisecond
	a, b := e.user(), e.user()
	ctx := context.Background()
	var id string
	if err := e.pool.QueryRow(ctx, `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		VALUES ($1, $2, 'm', 'x') RETURNING id::text`, b, a).Scan(&id); err != nil {
		t.Fatal(err)
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT 1 FROM envelopes FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if w := e.rest("POST", "/v1/envelopes/ack", b, `{"ids":["`+id+`"]}`); w.Code != 503 {
		t.Fatalf("ack com a linha travada: %d", w.Code)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("esperou %v", d)
	}
}

func TestFriendsLimit(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	if _, err := e.pool.Exec(context.Background(), `WITH u AS (
			INSERT INTO users (email, friend_code) SELECT 'lim' || g || '@ex.com', 'Z' || lpad(g::text, 7, '0')
			FROM generate_series(1, $2::int) g RETURNING id)
		INSERT INTO friendships (requester_id, addressee_id, status) SELECT $1, id, 'accepted' FROM u`,
		a, maxFriendsRead+5); err != nil {
		t.Fatal(err)
	}
	ids, _, err := e.hub.store.circle(context.Background(), a)
	if err != nil || len(ids) != maxFriendsRead {
		t.Fatalf("%d amigos, %v", len(ids), err)
	}
}

func TestRevokedTokenVersion(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	if _, err := e.pool.Exec(context.Background(), `UPDATE users SET token_version = 1 WHERE id = $1`, a); err != nil {
		t.Fatal(err)
	}
	c := e.connect(a)
	c.wantError("unauthorized")
	c.wantClose(websocket.ClosePolicyViolation)
	e.waitOnline(a, 0)
}

func TestCleanupBatches(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload, created_at)
		SELECT $1::uuid, $2::uuid, 'velho' || g, 'x'::bytea, now() - interval '31 days' FROM generate_series(1, $3::int) g
		UNION ALL SELECT $1::uuid, $2::uuid, 'novo', 'x', now()`, b, a, 2*cleanupBatch+500); err != nil {
		t.Fatal(err)
	}
	if err := e.hub.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if n := e.count(`SELECT count(*) FROM envelopes`); n != 1 || e.count(`SELECT count(*) FROM envelopes WHERE client_msg_id = 'novo'`) != 1 {
		t.Fatalf("sobraram %d", n)
	}
}

func TestRevokedSeesNothing(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	e.online(b)
	if _, err := e.pool.Exec(context.Background(), `UPDATE users SET token_version = 1 WHERE id = $1`, a); err != nil {
		t.Fatal(err)
	}
	e.hub.afterRegister = func() {
		env := getOut()
		env.b = wire.AppendEnvelope(env.b, []byte(b), []byte("m1"), []byte("cifrado"))
		e.hub.deliver([]byte(a), env, false)
		env.release()
		e.hub.SendFriends(b, wire.AppendLocationUpdate(Buf(), []byte(b), []byte("pos")))
	}
	c := e.connect(a)
	c.wantError("unauthorized")
	c.wantClose(websocket.ClosePolicyViolation)
}

func relayGoroutines() (readers, writers, backlogs int) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "relay.(*Conn).readLoop") {
			readers++
		}
		if strings.Contains(g, "relay.(*Conn).flush") {
			writers++
		}
		if strings.Contains(g, "relay.(*Conn).backlog") {
			backlogs++
		}
	}
	return readers, writers, backlogs
}

// Sem t.Parallel: as conexões de outros testes também têm readLoop.
func TestIdleConnGoroutines(t *testing.T) {
	e := newEnvPool(t, testdb.Pool(t))
	const n = 50
	for range n {
		e.online(e.user()) // a resposta bad_frame sobe e derruba um escritor em cada uma
	}
	var r, w, b int
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if r, w, b = relayGoroutines(); r == n && w == 0 && b == 0 {
			return
		}
	}
	t.Fatalf("%d leitores, %d escritores e %d backlogs para %d conexões ociosas", r, w, b, n)
}

func (e *env) group(members ...string) string {
	e.t.Helper()
	ctx := context.Background()
	var g string
	if err := e.pool.QueryRow(ctx, `INSERT INTO groups (owner_id) VALUES ($1) RETURNING id`, members[0]).Scan(&g); err != nil {
		e.t.Fatal(err)
	}
	for _, m := range members {
		if _, err := e.pool.Exec(ctx, `INSERT INTO group_members (group_id, user_id) VALUES ($1, $2)`, g, m); err != nil {
			e.t.Fatal(err)
		}
	}
	return g
}

func (e *env) block(a, b string) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), `INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2)`, a, b); err != nil {
		e.t.Fatal(err)
	}
}

func groupEnv(g, id, payload string) []byte {
	return wire.AppendGroupEnvelope(nil, []byte(g), []byte(id), []byte(payload), nil)
}

func TestGroupEnvelope(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	var pinged []string
	e.hub.Notify = func(u string) { mu.Lock(); pinged = append(pinged, u); mu.Unlock() }
	a, b, blocked, off, outsider := e.user(), e.user(), e.user(), e.user(), e.user()
	g := e.group(a, b, blocked, off)
	e.block(blocked, a)
	ca, cb, cbl, co := e.online(a), e.online(b), e.online(blocked), e.online(outsider)

	ca.send(groupEnv(g, "m1", "cifrado"))
	if f := ca.read(); f.Type != wire.Ack || string(f.Peer) != g || string(f.MsgID) != "m1" {
		t.Fatalf("Ack ao remetente: %+v", f)
	}
	if f := cb.read(); f.Type != wire.GroupEnvelope || string(f.Group) != g || string(f.MsgID) != "m1" ||
		string(f.Payload) != "cifrado" || string(f.Peer) != a {
		t.Fatalf("GroupEnvelope ao membro: %+v", f)
	}
	cbl.quiet(200 * time.Millisecond) // bloqueio em qualquer sentido: sem cópia
	if n := e.count(`SELECT count(*) FROM envelopes WHERE group_id = $1`, g); n != 2 {
		t.Fatalf("cópias: %d", n)
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE recipient_id = $1 AND sender_id = $2 AND group_id = $3`, off, a, g); n != 1 {
		t.Fatal("sem cópia para o membro offline")
	}
	mu.Lock()
	if fmt.Sprint(pinged) != fmt.Sprint([]string{off}) {
		t.Errorf("push: %v", pinged)
	}
	mu.Unlock()

	ca.send(groupEnv(g, "m1", "cifrado")) // reenvio
	if f := ca.read(); f.Type != wire.Ack || string(f.MsgID) != "m1" {
		t.Fatalf("Ack do reenvio: %+v", f)
	}
	cb.quiet(200 * time.Millisecond)
	if n := e.count(`SELECT count(*) FROM envelopes WHERE group_id = $1`, g); n != 2 {
		t.Fatalf("reenvio duplicou: %d", n)
	}

	co.send(groupEnv(g, "x", "p")) // não-membro: forbidden, sem revelar
	co.wantError("forbidden")
	co.send(groupEnv("00000000-0000-0000-0000-000000000099", "x", "p"))
	co.wantError("forbidden")
	ca.send(wire.AppendGroupEnvelope(nil, []byte(g), []byte("m2"), []byte("p"), []byte(b))) // from é do servidor
	ca.wantError("bad_frame")

	cb.send(wire.AppendAck(nil, []byte(a), []byte("m1"))) // o Ack de sempre apaga a cópia
	for deadline := time.Now().Add(5 * time.Second); e.count(`SELECT count(*) FROM envelopes WHERE recipient_id = $1`, b) != 0; {
		if time.Now().After(deadline) {
			t.Fatal("Ack não apagou a cópia")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cOff := e.online(off)
	if f := cOff.read(); f.Type != wire.GroupEnvelope || string(f.Group) != g || string(f.Peer) != a {
		t.Fatalf("backlog: %+v", f)
	}
	if w := e.rest("GET", "/v1/envelopes", off, ""); !strings.Contains(w.Body.String(), `"group_id":"`+g+`"`) {
		t.Fatalf("REST: %s", w.Body.String())
	}
	if w := e.rest("GET", "/v1/envelopes", off, ""); strings.Count(w.Body.String(), "group_id") != 1 {
		t.Fatalf("group_id em 1:1: %s", w.Body.String())
	}
}

func TestGroupContacts(t *testing.T) {
	e := newEnv(t)
	a, b, c := e.user(), e.user(), e.user()
	g := e.group(a, b, c)
	e.block(a, c)
	ca, cb := e.online(a), e.online(b)
	ca.send(envelope(b, "skdm", "x"))
	if f := ca.read(); f.Type != wire.Ack {
		t.Fatalf("1:1 entre membros: %+v", f)
	}
	if f := cb.read(); f.Type != wire.Envelope || string(f.Peer) != a {
		t.Fatalf("b: %+v", f)
	}
	ca.send(envelope(c, "skdm", "x"))
	ca.wantError("forbidden")

	if _, err := e.pool.Exec(context.Background(), `DELETE FROM group_members WHERE group_id = $1 AND user_id = $2`, g, b); err != nil {
		t.Fatal(err)
	}
	e.hub.GroupChanged(g, []string{a, b})
	if f := ca.read(); f.Type != wire.GroupChanged || string(f.Group) != g {
		t.Fatalf("GroupChanged em a: %+v", f)
	}
	if f := cb.read(); f.Type != wire.GroupChanged || string(f.Group) != g {
		t.Fatalf("GroupChanged em quem saiu: %+v", f)
	}
	ca.send(envelope(b, "depois", "x")) // contatos recarregados pelo GroupChanged
	ca.wantError("forbidden")
}

func TestChatRateLimit(t *testing.T) {
	e := newEnv(t)
	e.hub.chat = ratelimit.New(chatPerMin)
	a, b := e.user(), e.user()
	e.friends(a, b)
	g := e.group(a, b)
	ca := e.online(a)
	for i := range chatPerMin - 1 {
		ca.send(envelope(b, fmt.Sprintf("m%d", i), "x"))
		if f := ca.read(); f.Type != wire.Ack {
			t.Fatalf("mensagem %d: %+v", i, f)
		}
	}
	ca.send(groupEnv(g, "g1", "x")) // a de grupo conta 1
	if f := ca.read(); f.Type != wire.Ack {
		t.Fatalf("grupo: %+v", f)
	}
	ca.send(envelope(b, "demais", "x"))
	ca.wantError("rate_limited")
	ca.send(groupEnv(g, "demais", "x"))
	ca.wantError("rate_limited")
	if n := e.count(`SELECT count(*) FROM envelopes WHERE client_msg_id = 'demais'`); n != 0 {
		t.Fatalf("gravou acima do limite: %d", n)
	}
}

func TestGroupRecipientLimit(t *testing.T) {
	e := newEnv(t)
	e.hub.chat = ratelimit.New(chatPerMin)
	e.hub.groupRcpt = ratelimit.NewWindow(10, time.Hour) // recarga desprezível durante o teste
	a, b, c, d, f := e.user(), e.user(), e.user(), e.user(), e.user()
	e.friends(a, b)
	g := e.group(a, b, c, d, f)
	ca := e.online(a)
	for i := range 3 { // 10 - 3×(1 + 3 de dívida): a 3ª ainda passa, a 4ª não
		ca.send(groupEnv(g, fmt.Sprintf("g%d", i), "x"))
		if fr := ca.read(); fr.Type != wire.Ack {
			t.Fatalf("grupo %d: %+v", i, fr)
		}
	}
	ca.send(groupEnv(g, "demais", "x"))
	ca.wantError("rate_limited")
	ca.send(envelope(b, "m1", "x")) // o 1:1 não usa este limitador
	if fr := ca.read(); fr.Type != wire.Ack {
		t.Fatalf("1:1: %+v", fr)
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE client_msg_id = 'demais'`); n != 0 {
		t.Fatalf("gravou acima do limite: %d", n)
	}
	if groupRcptPerMin < 10*49 {
		t.Fatalf("grupo de 50 não manda 10 por minuto: %d", groupRcptPerMin)
	}
}

func TestGroupLimitCountsFull(t *testing.T) {
	e := newEnv(t)
	e.hub.groupRcpt = ratelimit.NewWindow(8, time.Hour)
	a, x, y, z := e.user(), e.user(), e.user(), e.user()
	g := e.group(a, x, y, z)
	if _, err := e.pool.Exec(context.Background(), `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		SELECT r, $3, 'old' || i, 'x' FROM unnest($1::uuid[]) r, generate_series(1, $2) i`,
		[]string{x, y, z}, maxPendingPerRecipient, a); err != nil {
		t.Fatal(err)
	}
	ca := e.online(a)
	for i := range 3 { // nada gravado, mas cada uma custa os 3 membros: 8 - 3×3
		ca.send(groupEnv(g, fmt.Sprintf("g%d", i), "x"))
		if fr := ca.read(); fr.Type != wire.Ack {
			t.Fatalf("grupo %d: %+v", i, fr)
		}
	}
	ca.send(groupEnv(g, "demais", "x"))
	ca.wantError("rate_limited")
}

func TestRecipientCap(t *testing.T) {
	e := newEnv(t)
	a, full, c := e.user(), e.user(), e.user()
	e.friends(a, full)
	g := e.group(a, full, c)
	if _, err := e.pool.Exec(context.Background(), `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		SELECT $1, $2, 'old' || i, 'x' FROM generate_series(1, $3) i`, full, c, maxPendingPerRecipient-1); err != nil {
		t.Fatal(err)
	}
	ca, cc := e.online(a), e.online(c)

	ca.send(envelope(full, "m0", "x")) // a milésima
	if fr := ca.read(); fr.Type != wire.Ack {
		t.Fatalf("m0: %+v", fr)
	}
	ca.send(envelope(full, "m0", "x")) // reenvio do que já está gravado: Ack, não recipient_full
	if fr := ca.read(); fr.Type != wire.Ack || string(fr.MsgID) != "m0" {
		t.Fatalf("reenvio com a fila cheia: %+v", fr)
	}
	ca.send(envelope(full, "m1", "x"))
	ca.wantError("recipient_full")
	ca.send(groupEnv(g, "g1", "x"))
	if fr := ca.read(); fr.Type != wire.Ack {
		t.Fatalf("grupo: %+v", fr)
	}
	if fr := cc.read(); fr.Type != wire.GroupEnvelope {
		t.Fatalf("membro com espaço: %+v", fr)
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE recipient_id = $1`, full); n != maxPendingPerRecipient {
		t.Fatalf("passou do teto: %d", n)
	}

	if _, err := e.pool.Exec(context.Background(), `DELETE FROM envelopes WHERE recipient_id = $1 AND client_msg_id = 'old1'`, full); err != nil {
		t.Fatal(err)
	}
	ca.send(envelope(full, "m1", "x"))
	if fr := ca.read(); fr.Type != wire.Ack {
		t.Fatalf("depois de abrir vaga: %+v", fr)
	}
}

func TestPushOnlyOffline(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	var pinged []string
	e.hub.Notify = func(u string) { mu.Lock(); pinged = append(pinged, u); mu.Unlock() }
	a, on, off := e.user(), e.user(), e.user()
	e.friends(a, on)
	e.friends(a, off)
	ca, con := e.online(a), e.online(on)
	ca.send(envelope(on, "m1", "x"))
	ca.read()
	con.read()
	ca.send(envelope(off, "m2", "x"))
	ca.read()
	ca.send(envelope(off, "m2", "x")) // reenvio: sem push de novo
	ca.read()
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(pinged) != fmt.Sprint([]string{off}) {
		t.Fatalf("push: %v", pinged)
	}
}

func TestUsernameRequired(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	if _, err := e.pool.Exec(context.Background(), `UPDATE users SET username = NULL WHERE id = $1`, a); err != nil {
		t.Fatal(err)
	}
	c := e.connect(a)
	c.wantError("username_required")
	c.wantClose(websocket.ClosePolicyViolation)
	e.waitOnline(a, 0)
}

// Ack acima do limite cai em silêncio: sem frame de erro, e o envelope segue na fila; outro usuário não sente.
func TestAckRateLimit(t *testing.T) {
	e := newEnv(t)
	e.hub.acks = ratelimit.New(3) // o de verdade é ackPerMin; 3 basta para ver o corte
	a, b, c := e.user(), e.user(), e.user()
	e.friends(a, b)
	e.friends(a, c)
	ca, cb, cc := e.online(a), e.online(b), e.online(c)
	for i := range 4 {
		ca.send(envelope(b, fmt.Sprintf("m%d", i), "x"))
		ca.read()
		cb.read()
	}
	ca.send(envelope(c, "c0", "x"))
	ca.read()
	cc.read()
	for i := range 4 {
		cb.send(wire.AppendAck(nil, []byte(a), []byte(fmt.Sprintf("m%d", i))))
	}
	cc.send(wire.AppendAck(nil, []byte(a), []byte("c0")))
	cb.quiet(300 * time.Millisecond) // nenhum Error
	left := func() int { return e.count(`SELECT count(*) FROM envelopes`) }
	for deadline := time.Now().Add(5 * time.Second); left() != 1; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("envelopes na fila: %d, quero só o m3", left())
		}
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE client_msg_id = 'm3'`); n != 1 {
		t.Fatal("o Ack descartado apagou o envelope")
	}
}
