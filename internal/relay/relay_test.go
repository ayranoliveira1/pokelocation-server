package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/auth"
	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

type env struct {
	t    *testing.T
	hub  *Hub
	srv  *httptest.Server
	pool *pgxpool.Pool
	exp  sync.Map // token → vencimento diferente de 1 h
	locs chan string
}

var codeSeq atomic.Int32

// Token de teste = id do usuário; "ruim" e qualquer coisa que não seja uuid → ErrUnauthorized.
func newEnv(t *testing.T) *env {
	t.Helper()
	t.Parallel()
	e := &env{t: t, pool: testdb.Pool(t), locs: make(chan string, 16)}
	verify := func(_ context.Context, tok string) (string, time.Time, error) {
		if len(tok) != 36 {
			return "", time.Time{}, auth.ErrUnauthorized
		}
		exp := time.Now().Add(time.Hour)
		if v, ok := e.exp.Load(tok); ok {
			exp = v.(time.Time)
		}
		return tok, exp, nil
	}
	e.hub = New(e.pool, verify, func(c *Conn, f wire.Frame) {
		e.locs <- fmt.Sprintf("%s %d %s", c.User(), f.Type, f.Payload)
	})
	e.hub.authWait = 300 * time.Millisecond
	mux := http.NewServeMux()
	e.hub.Routes(mux, func(h http.HandlerFunc) http.Handler { // no lugar do auth.Authed: X-User é o usuário
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
	if err := e.pool.QueryRow(context.Background(), `INSERT INTO users (email, friend_code) VALUES ($1, $2) RETURNING id`,
		fmt.Sprintf("u%d@ex.com", n), code).Scan(&id); err != nil {
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

// connect abre e autentica; não espera nada de volta (o servidor não responde ao Auth).
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

// online: conecta e espera o Auth terminar (registro, amigos, corte do backlog). O servidor não responde ao Auth;
// um frame inválido só é respondido (bad_frame) depois dele.
func (e *env) online(user string) *client {
	e.t.Helper()
	c := e.connect(user)
	c.send([]byte{0xff})
	c.wantError("bad_frame")
	return c
}

func (e *env) waitOnline(user string, n int) {
	e.t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
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

// quiet: nada chega em d.
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

// ---- conexão e Auth ----

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
	e.connect(a) // a 5ª derruba a mais antiga
	first.wantClose(websocket.ClosePolicyViolation)
	e.waitOnline(a, 4)
}

// ---- envelopes ----

func TestEnvelopeLive(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	ca, cb1, cb2 := e.online(a), e.online(b), e.online(b)

	ca.send(envelope(b, "m1", "cifrado"))
	if f := ca.read(); f.Type != wire.Ack || string(f.Peer) != b || string(f.MsgID) != "m1" {
		t.Fatalf("Ack ao remetente: %+v", f)
	}
	for _, cb := range []*client{cb1, cb2} { // todas as conexões do destinatário
		if f := cb.read(); f.Type != wire.Envelope || string(f.Peer) != a || string(f.MsgID) != "m1" ||
			string(f.Payload) != "cifrado" {
			t.Fatalf("Envelope ao destinatário: %+v", f)
		}
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE recipient_id = $1`, b); n != 1 {
		t.Fatalf("gravados: %d", n)
	}

	ca.send(envelope(b, "m1", "cifrado")) // reenvio: Ack de novo, sem duplicar nem reentregar
	if f := ca.read(); f.Type != wire.Ack || string(f.MsgID) != "m1" {
		t.Fatalf("Ack do reenvio: %+v", f)
	}
	cb1.quiet(200 * time.Millisecond)
	if n := e.count(`SELECT count(*) FROM envelopes`); n != 1 {
		t.Fatalf("reenvio duplicou: %d", n)
	}

	cb2.send(wire.AppendAck(nil, []byte(a), []byte("m1"))) // o Ack de qualquer conexão apaga
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
	ca.send(envelope(b, "m\x00", "x")) // o Postgres não guarda NUL em text
	ca.wantError("bad_frame")
	ca.send(envelope(b, "\xff\xfe", "x")) // nem UTF-8 inválido
	ca.wantError("bad_frame")
	// tipos que só o servidor manda (wire.Decode não sabe de direção)
	for _, f := range [][]byte{
		wire.AppendAuth(nil, []byte(a)), // Auth de novo
		wire.AppendLocationUpdate(nil, []byte(b), []byte("p")),
		wire.AppendLocationOff(nil, []byte(b)),
		wire.AppendError(nil, "x"),
	} {
		ca.send(f)
		ca.wantError("bad_frame")
	}

	// a conexão segue viva
	ca.send(envelope(b, "ok", "x"))
	if f := ca.read(); f.Type != wire.Ack {
		t.Fatalf("depois dos erros: %+v", f)
	}
	if n := e.count(`SELECT count(*) FROM envelopes WHERE sender_id = $1`, a); n != 1 {
		t.Fatalf("gravados: %d", n)
	}

	// o Ack de quem não é o destinatário não apaga
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
	e.hub.ReloadFriends(b) // offline aqui: nada
}

// Fila pendente: entregue depois do Auth, mais antigos primeiro, com contrapressão (nunca fecha por backlog).
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

// Fila ao vivo cheia de envelopes: fecha a conexão (os envelopes seguem no Postgres).
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
	e.waitOnline(b, 0)
	for deadline := time.Now().Add(30 * time.Second); e.count(`SELECT count(*) FROM envelopes`) != 4*queueCap; {
		if time.Now().After(deadline) {
			t.Fatalf("gravados %d", e.count(`SELECT count(*) FROM envelopes`))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---- localização (fronteira com o S7) ----

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

// ---- REST ----

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
	ids = append(ids, oq.Envelopes[0].ID, "999999999") // alheio e inexistente: ignorados
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

// Página limitada a ~48 KiB de JSON (resposta ≤ 64 KiB, limite do app).
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

// Revogação (logout, reuso de refresh, vínculo Google): todas as conexões do usuário caem com unauthorized + 1008.
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
	e.hub.Kick(a) // ninguém online: nada
}

// O buffer de leitura é reaproveitado a cada mensagem: nada que vai para outra conexão pode fatiá-lo.
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
	// o mesmo envelope pode chegar ao vivo e pelo backlog (o app deduplica por from+msg id): confere o conteúdo de
	// cada um pelo seu msg id
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

// Amizade desfeita enquanto a 2ª conexão do usuário se autentica (o :overlay reconectando enquanto ele bloqueia pelo
// principal): a lista em memória não pode voltar a ser a velha.
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
		e.hub.ReloadFriends(a) // o OnChange do friends, depois do commit
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

// Fan-out com um destinatário lento: os outros liberam o buffer compartilhado antes; os bytes que o lento recebe
// depois têm de ser os enviados, na ordem (nada de buffer devolvido ao pool cedo e reaproveitado).
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
		// o pool recicla na hora o que os rápidos liberam; frames de outro tamanho reaproveitam esses buffers
		ob := getOut()
		ob.b = append(ob.b, strings.Repeat("!", 300)...)
		ob.release()
	}
	for _, c := range []*client{c1, c2, cs} { // o lento por último
		for i := range n {
			if f := c.read(); f.Type != wire.LocationUpdate || string(f.Payload) != payload(i) {
				t.Fatalf("frame %d: %d %q", i, f.Type, f.Payload)
			}
		}
	}
}
