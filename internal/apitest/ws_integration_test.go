package apitest

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

const quiet = 700 * time.Millisecond // o servidor repassa em ~ms

// wsConn lê numa goroutine e entrega por canal: o gorilla não deixa ler de novo depois de um timeout.
type wsConn struct {
	s      *server
	c      *websocket.Conn
	frames chan wire.Frame
	done   chan struct{} // fechado quando a leitura termina; err diz como
	err    error
	wmu    sync.Mutex
}

func (s *server) ws(token string) *wsConn { return s.wsBuf(token, 1024) }

// wsBuf com buf 0 simula cliente lento: o TCP enche e o servidor sente a contrapressão.
func (s *server) wsBuf(token string, buf int) *wsConn {
	s.t.Helper()
	n := ipSeq.Add(1)
	h := http.Header{"X-Forwarded-For": {fmt.Sprintf("198.19.%d.%d", n>>8&255, n&255)}}
	c, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.base, "http")+"/v1/ws", h)
	if err != nil {
		st := 0
		if resp != nil {
			st = resp.StatusCode
		}
		s.t.Fatalf("dial /v1/ws: %v (HTTP %d)", err, st)
	}
	w := &wsConn{s: s, c: c, frames: make(chan wire.Frame, buf), done: make(chan struct{})}
	go w.read()
	s.t.Cleanup(func() { _ = c.Close() })
	if token != "" {
		w.send(wire.AppendAuth(nil, []byte(token)))
	}
	return w
}

func (w *wsConn) read() {
	defer close(w.done)
	for {
		typ, m, err := w.c.ReadMessage()
		if err != nil {
			w.err = err
			return
		}
		if typ != websocket.BinaryMessage {
			w.err = fmt.Errorf("servidor mandou mensagem tipo %d", typ)
			return
		}
		f, err := wire.Decode(m) // m é novo a cada ReadMessage: os campos fatiados continuam válidos
		if err != nil {
			w.err = fmt.Errorf("servidor mandou frame inválido (%w): %x", err, m)
			return
		}
		w.frames <- f
	}
}

func (w *wsConn) send(b []byte) {
	w.s.t.Helper()
	w.wmu.Lock()
	defer w.wmu.Unlock()
	if err := w.c.WriteMessage(websocket.BinaryMessage, b); err != nil {
		w.s.t.Fatalf("escrever no ws: %v", err)
	}
}

// next: closed = a conexão terminou sem mais frames.
func (w *wsConn) next(within time.Duration) (f wire.Frame, closed, timeout bool) {
	select {
	case f = <-w.frames:
		return f, false, false
	default:
	}
	select {
	case f = <-w.frames:
		return f, false, false
	case <-w.done:
		select { // o leitor pode ter posto frames antes de fechar
		case f = <-w.frames:
			return f, false, false
		default:
			return f, true, false
		}
	case <-time.After(within):
		return f, false, true
	}
}

func (w *wsConn) expect(typ byte, within time.Duration) wire.Frame {
	w.s.t.Helper()
	f, closed, timeout := w.next(within)
	switch {
	case closed:
		w.s.t.Fatalf("conexão caiu esperando %s: %v", frameName(typ), w.err)
	case timeout:
		w.s.t.Fatalf("nenhum %s em %v", frameName(typ), within)
	case f.Type != typ:
		w.s.t.Fatalf("esperado %s, veio %s", frameName(typ), describe(f))
	}
	return f
}

func (w *wsConn) expectErr(code string) {
	w.s.t.Helper()
	if f := w.expect(wire.Error, 3*time.Second); string(f.Code) != code {
		w.s.t.Errorf("esperado Error{%s}, veio Error{%s}", code, f.Code)
	}
}

func (w *wsConn) silent(what string, d time.Duration) {
	w.s.t.Helper()
	if f, closed, _ := w.next(d); !closed && f.Type != 0 {
		w.s.t.Errorf("%s: recebeu %s", what, describe(f))
	}
}

// expectClose ignora os frames que chegarem antes do close.
func (w *wsConn) expectClose(code int, within time.Duration) {
	w.s.t.Helper()
	select {
	case <-w.done:
	case <-time.After(within):
		w.s.t.Fatalf("conexão não fechou em %v (esperado close %d)", within, code)
	}
	var ce *websocket.CloseError
	if !errors.As(w.err, &ce) || ce.Code != code {
		w.s.t.Errorf("esperado close %d, veio %v", code, w.err)
	}
}

// ready prova que o Auth já foi processado: o servidor só responde bad_frame depois dele.
func (w *wsConn) ready() {
	w.s.t.Helper()
	w.send([]byte{0xff})
	w.expectErr("bad_frame")
}

func (w *wsConn) location(p []byte) { w.send(wire.AppendLocation(nil, p)) }

func (w *wsConn) envelope(to user, id string, p []byte) {
	w.send(wire.AppendEnvelope(nil, []byte(to.id), []byte(id), p))
}

func (w *wsConn) ack(from user, id string) { w.send(wire.AppendAck(nil, []byte(from.id), []byte(id))) }

// payload leva um marcador registrado como segredo: o log do servidor nunca pode conter payload.
func (s *server) payload(n int) []byte {
	mark := "PAYLOAD-" + rand.Text()
	s.secret(mark)
	return append([]byte(mark), bytes.Repeat([]byte{'.'}, max(0, n-len(mark)))...)
}

func frameName(t byte) string {
	if n, ok := map[byte]string{wire.Auth: "Auth", wire.Envelope: "Envelope", wire.Ack: "Ack", wire.Location: "Location",
		wire.LocationUpdate: "LocationUpdate", wire.LocationOff: "LocationOff", wire.Error: "Error"}[t]; ok {
		return n
	}
	return fmt.Sprint("tipo ", t)
}

func describe(f wire.Frame) string {
	return fmt.Sprintf("%s{peer=%s msg=%s code=%s payload=%dB}", frameName(f.Type), f.Peer, f.MsgID, f.Code, len(f.Payload))
}

// pending: "from|client_msg_id" → id.
func pending(s *server, u user) map[string]string {
	s.t.Helper()
	r := s.do("GET", "/v1/envelopes", u.access, nil)
	s.want(r, 200, "")
	out := map[string]string{}
	l, _ := r.body["envelopes"].([]any)
	for _, x := range l {
		m, _ := x.(map[string]any)
		out[fmt.Sprint(m["from"], "|", m["client_msg_id"])] = fmt.Sprint(m["id"])
	}
	return out
}

// locations: user_id → payload.
func locations(s *server, u user) map[string][]byte {
	s.t.Helper()
	r := s.do("GET", "/v1/locations", u.access, nil)
	s.want(r, 200, "")
	out := map[string][]byte{}
	l, ok := r.body["locations"].([]any)
	if !ok {
		s.t.Errorf("GET /v1/locations sem a lista: %s", r.raw)
	}
	for _, x := range l {
		m, _ := x.(map[string]any)
		p, _ := base64.StdEncoding.DecodeString(fmt.Sprint(m["payload"]))
		out[fmt.Sprint(m["user_id"])] = p
	}
	return out
}

// eventually: o GET /v1/locations pode estar até 1 s atrás do WS (o Redis é gravado em lote).
func eventually(s *server, what string, cond func() bool) {
	s.t.Helper()
	for deadline := time.Now().Add(3 * time.Second); !cond(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			s.t.Errorf("%s: não aconteceu em 3 s", what)
			return
		}
	}
}

// waitLoc: o servidor aceita 1 Location a cada ≥ 4 s por usuário.
func waitLoc(last time.Time) { time.Sleep(time.Until(last.Add(4200 * time.Millisecond))) }

func TestWSAuth(t *testing.T) {
	s := start(t)
	s.need("GET", "/v1/ws")
	a, b := s.register("a"), s.register("b")
	now := time.Now().Unix()
	tv := claims(t, a.access)["tv"]

	for name, c := range map[string]struct {
		token string
		frame []byte // nil = não manda nada
		code  string // Error esperado antes do close ("" = não exige)
		close int
		wait  time.Duration
	}{
		"primeiro frame não é Auth": {frame: wire.AppendLocation(nil, []byte("x")), close: 1008, wait: 3 * time.Second},
		"token inválido":            {token: "lixo", code: "unauthorized", close: 1008, wait: 3 * time.Second},
		"token vencido": {token: forge("EdDSA", map[string]any{"sub": a.id, "iat": now - 1000, "tv": tv, "exp": now - 60}, s.key),
			code: "unauthorized", close: 1008, wait: 3 * time.Second},
		"token sem tv": {token: forge("EdDSA", map[string]any{"sub": a.id, "iat": now, "exp": now + 300}, s.key),
			code: "unauthorized", close: 1008, wait: 3 * time.Second},
		"sem Auth em 5 s": {close: 1008, wait: 8 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := s.on(t)
			w := s.ws(c.token)
			if c.frame != nil {
				w.send(c.frame)
			}
			if c.code != "" {
				w.expectErr(c.code)
			}
			w.expectClose(c.close, c.wait)
		})
	}

	t.Run("mensagem de texto", func(t *testing.T) {
		s := s.on(t)
		w := s.ws(a.access)
		w.ready()
		if err := w.c.WriteMessage(websocket.TextMessage, []byte("oi")); err != nil {
			t.Fatal(err)
		}
		w.expectClose(1003, 3*time.Second)
	})
	t.Run("mensagem acima de 64 KiB", func(t *testing.T) {
		s := s.on(t)
		w := s.ws(a.access)
		w.ready()
		w.send(wire.AppendLocation(nil, make([]byte, 64<<10)))
		w.expectClose(1009, 3*time.Second)
	})

	// A conexão continua depois de cada bad_frame.
	t.Run("frames inválidos não derrubam", func(t *testing.T) {
		s := s.on(t)
		w := s.ws(a.access)
		w.ready()
		bid, mid := []byte(b.id), []byte("m1")
		for name, f := range map[string][]byte{
			"lixo":                          {0xff, 1, 2},
			"vazio":                         {},
			"2º Auth":                       wire.AppendAuth(nil, []byte(a.access)),
			"LocationUpdate do cliente":     wire.AppendLocationUpdate(nil, bid, []byte("x")),
			"Error do cliente":              wire.AppendError(nil, "bad_frame"),
			"LocationOff com from":          wire.AppendLocationOff(nil, bid),
			"Location vazio":                wire.AppendLocation(nil, nil),
			"Envelope vazio":                wire.AppendEnvelope(nil, bid, mid, nil),
			"Envelope com UUID maiúsculo":   wire.AppendEnvelope(nil, []byte(strings.ToUpper(b.id)), mid, []byte("x")),
			"Envelope sem client_msg_id":    wire.AppendEnvelope(nil, bid, nil, []byte("x")),
			"Ack com client_msg_id de 65 B": wire.AppendAck(nil, bid, bytes.Repeat([]byte{'a'}, 65)),
			"tamanho além do fim":           {wire.Location, 0, 0, 0, 9, 1},
			"bytes sobrando":                append(wire.AppendAck(nil, bid, mid), 0),
		} {
			w.send(f)
			if g := w.expect(wire.Error, 3*time.Second); string(g.Code) != "bad_frame" {
				t.Errorf("%s: Error{%s}, esperado bad_frame", name, g.Code)
			}
		}
		w.send(wire.AppendEnvelope(nil, bid, mid, make([]byte, wire.MaxEnvelopePayload+1)))
		w.expectErr("too_large")
		w.send(wire.AppendLocation(nil, make([]byte, wire.MaxLocationPayload+1)))
		w.expectErr("too_large")
	})
}

func TestWSMatriz(t *testing.T) {
	s := start(t)
	s.need("GET", "/v1/ws")
	a, b, c, d := s.register("a"), s.register("b"), s.register("c"), s.register("d")
	befriend(s, a, b)
	befriend(s, a, d)
	wa, wb, wc, wd := s.ws(a.access), s.ws(b.access), s.ws(c.access), s.ws(d.access)
	for _, w := range []*wsConn{wa, wb, wc, wd} {
		w.ready()
	}

	// A 2ª posição antes de 4 s é descartada em silêncio.
	p1 := s.payload(64)
	lastA := time.Now()
	wa.location(p1)
	wa.location(s.payload(64))
	if f := wb.expect(wire.LocationUpdate, 3*time.Second); string(f.Peer) != a.id || !bytes.Equal(f.Payload, p1) {
		t.Errorf("b recebeu %s, esperado a posição 1 de a", describe(f))
	}
	wb.silent("2ª posição em menos de 4 s", quiet)
	// d também é amigo de a: consome o que é dele, senão frames velhos na fila contaminam as checagens de bloqueio.
	if f := wd.expect(wire.LocationUpdate, 3*time.Second); string(f.Peer) != a.id || !bytes.Equal(f.Payload, p1) {
		t.Errorf("d recebeu %s, esperado a posição 1 de a", describe(f))
	}
	wd.silent("2ª posição em menos de 4 s (d)", quiet)
	wc.silent("não-amigo recebeu a posição de a", quiet)
	wa.silent("a recebeu a própria posição", quiet)
	wc.location(s.payload(64))
	wa.silent("posição de não-amigo chegou em a", quiet)
	wb.silent("posição de não-amigo chegou em b", quiet)
	eventually(s, "GET /v1/locations de b com a posição de a", func() bool { return bytes.Equal(locations(s, b)[a.id], p1) })
	if _, ok := locations(s, c)[a.id]; ok {
		t.Error("não-amigo vê a posição de a no GET /v1/locations")
	}
	if _, ok := locations(s, a)[c.id]; ok {
		t.Error("a vê a posição de não-amigo no GET /v1/locations")
	}

	wa.envelope(c, "x1", s.payload(32))
	wa.expectErr("forbidden")
	wc.silent("não-amigo recebeu envelope", quiet)

	m1 := s.payload(128)
	wa.envelope(b, "m1", m1)
	if f := wb.expect(wire.Envelope, 3*time.Second); string(f.Peer) != a.id || string(f.MsgID) != "m1" || !bytes.Equal(f.Payload, m1) {
		t.Errorf("b recebeu %s", describe(f))
	}
	if f := wa.expect(wire.Ack, 3*time.Second); string(f.Peer) != b.id || string(f.MsgID) != "m1" {
		t.Errorf("a recebeu %s, esperado Ack{to b, m1}", describe(f))
	}
	// Reenvio do mesmo client_msg_id é ignorado, mas recebe o Ack de novo.
	wa.envelope(b, "m1", m1)
	wa.expect(wire.Ack, 3*time.Second)
	wb.silent("reenvio do mesmo client_msg_id entregue de novo", quiet)

	envID, ok := pending(s, b)[a.id+"|m1"]
	if !ok {
		t.Fatalf("GET /v1/envelopes de b sem o m1: %v", pending(s, b))
	}
	wc.ack(a, "m1")
	s.want(s.do("POST", "/v1/envelopes/ack", c.access, map[string]any{"ids": []string{envID}}), 204, "")
	s.want(s.do("POST", "/v1/envelopes/ack", a.access, map[string]any{"ids": []string{envID}}), 204, "")
	time.Sleep(quiet)
	if _, ok := pending(s, b)[a.id+"|m1"]; !ok {
		t.Error("ack de outro usuário apagou o envelope de b")
	}
	if len(pending(s, c)) != 0 || len(pending(s, a)) != 0 {
		t.Error("GET /v1/envelopes mostra fila de outro usuário")
	}
	wb.ack(a, "m1")
	eventually(s, "Ack de b apaga o m1", func() bool { _, ok := pending(s, b)[a.id+"|m1"]; return !ok })

	wa.send(wire.AppendLocationOff(nil, nil))
	if f := wb.expect(wire.LocationOff, 3*time.Second); string(f.Peer) != a.id {
		t.Errorf("b recebeu %s, esperado LocationOff{a}", describe(f))
	}
	if f := wd.expect(wire.LocationOff, 3*time.Second); string(f.Peer) != a.id {
		t.Errorf("d recebeu %s, esperado LocationOff{a}", describe(f))
	}
	wc.silent("não-amigo recebeu LocationOff", quiet)
	eventually(s, "posição de a some depois do LocationOff", func() bool { _, ok := locations(s, b)[a.id]; return !ok })

	s.want(s.do("DELETE", "/v1/friends/"+a.id, b.access, nil), 204, "")
	waitLoc(lastA)
	lastA = time.Now()
	wa.location(s.payload(64))
	wb.silent("ex-amigo (desfeito) recebeu posição", time.Second)
	if f := wd.expect(wire.LocationUpdate, 3*time.Second); string(f.Peer) != a.id {
		t.Errorf("d (ainda amigo) recebeu %s", describe(f))
	}
	wa.envelope(b, "m2", s.payload(32))
	wa.expectErr("forbidden")

	s.want(s.do("POST", "/v1/blocks/"+a.id, d.access, nil), 204, "")
	wd.location(s.payload(64))
	wa.silent("bloqueado recebeu a posição de quem bloqueou", time.Second)
	waitLoc(lastA)
	wa.location(s.payload(64))
	wd.silent("quem bloqueou recebeu a posição do bloqueado", time.Second)
	wa.envelope(d, "m3", s.payload(32))
	wa.expectErr("forbidden")
	wd.envelope(a, "m4", s.payload(32))
	wd.expectErr("forbidden")
}

func TestWSMultiConexao(t *testing.T) {
	s := start(t)
	s.need("GET", "/v1/ws")
	a, b := s.register("a"), s.register("b")
	befriend(s, a, b)
	wa := s.ws(a.access)
	wa.ready()
	var wb []*wsConn
	for range 5 {
		w := s.ws(b.access)
		w.ready() // garante a ordem: o servidor registra esta antes da próxima
		wb = append(wb, w)
	}
	wb[0].expectClose(1008, 3*time.Second)
	live := wb[1:]

	p := s.payload(64)
	wa.location(p)
	m := s.payload(64)
	wa.envelope(b, "m1", m)
	for i, w := range live {
		if f := w.expect(wire.LocationUpdate, 3*time.Second); !bytes.Equal(f.Payload, p) {
			t.Errorf("conexão %d: %s", i+2, describe(f))
		}
		if f := w.expect(wire.Envelope, 3*time.Second); string(f.MsgID) != "m1" {
			t.Errorf("conexão %d: %s", i+2, describe(f))
		}
	}
	wa.expect(wire.Ack, 3*time.Second)
	live[1].ack(a, "m1")
	eventually(s, "1º Ack apaga o envelope", func() bool { return len(pending(s, b)) == 0 })
	live[2].ack(a, "m1")
	live[2].silent("2º Ack", quiet)
}

func TestWSKick(t *testing.T) {
	s := start(t)
	s.need("GET", "/v1/ws")
	t.Run("logout", func(t *testing.T) {
		s := s.on(t)
		a := s.register("a")
		w := s.ws(a.access)
		w.ready()
		s.want(s.do("POST", "/v1/auth/logout", "", map[string]string{"refresh_token": a.refresh}), 204, "")
		w.expectErr("unauthorized")
		w.expectClose(1008, 3*time.Second)
	})
	t.Run("reuso de refresh", func(t *testing.T) {
		s := s.on(t)
		a := s.register("b")
		r := s.do("POST", "/v1/auth/refresh", "", map[string]string{"refresh_token": a.refresh})
		s.want(r, 200, "")
		s.secret(r.str("access_token"), r.str("refresh_token"))
		w := s.ws(r.str("access_token"))
		w.ready()
		s.want(s.do("POST", "/v1/auth/refresh", "", map[string]string{"refresh_token": a.refresh}), 401, "unauthorized")
		w.expectErr("unauthorized")
		w.expectClose(1008, 3*time.Second)
	})
	t.Run("exp do token", func(t *testing.T) {
		s := s.on(t)
		a := s.register("c")
		now := time.Now().Unix()
		c := claims(t, a.access)
		w := s.ws(forge("EdDSA", map[string]any{"sub": a.id, "iat": c["iat"], "tv": c["tv"], "exp": now + 3}, s.key))
		w.ready()
		if f := w.expect(wire.Error, 8*time.Second); string(f.Code) != "unauthorized" {
			t.Errorf("no exp: %s", describe(f))
		}
		w.expectClose(1008, 3*time.Second)
	})
}

// ponytail: manda pelo WS; quando o chat ganhar limite de mensagens por minuto, semear por SQL na tabela envelopes.
func TestWSBacklog(t *testing.T) {
	s := start(t)
	s.need("GET", "/v1/ws")
	a, b := s.register("a"), s.register("b")
	befriend(s, a, b)
	wa := s.ws(a.access)
	wa.ready()
	const total = 150
	p := s.payload(16 << 10) // 150 × 16 KiB ≈ 2,4 MB: mais que os buffers de TCP do loopback
	for i := range total {
		wa.envelope(b, fmt.Sprintf("m%03d", i), p)
	}
	for range total {
		wa.expect(wire.Ack, 10*time.Second) // Ack S→C = gravado
	}

	wb := s.wsBuf(b.access, 0) // lento: o leitor só avança quando o teste consome
	var got []string
	for i := range total {
		f := wb.expect(wire.Envelope, 10*time.Second)
		got = append(got, string(f.MsgID))
		if i < 70 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	want := make([]string, total)
	for i := range want {
		want[i] = fmt.Sprintf("m%03d", i)
	}
	if !slices.Equal(got, want) {
		t.Errorf("backlog fora de ordem ou incompleto: %v", got)
	}
	for _, id := range got {
		wb.ack(a, id)
	}
	eventually(s, "acks do backlog esvaziam a fila", func() bool { return len(pending(s, b)) == 0 })
	select {
	case <-wb.done:
		t.Errorf("conexão do backlog caiu: %v", wb.err)
	default:
	}
}

// Com o Redis fora, o repasse ao vivo continua; só o GET /v1/locations fica vazio.
func TestWSRedisCai(t *testing.T) {
	t.Parallel()
	_, ctl := testdb.StoppableRedis(t)
	s := startWith(t, "redis://"+ctl.Addr()+"/0")
	s.need("GET", "/v1/ws")
	a, b := s.register("a"), s.register("b")
	befriend(s, a, b)
	wa, wb := s.ws(a.access), s.ws(b.access)
	wa.ready()
	wb.ready()

	p1 := s.payload(64)
	last := time.Now()
	wa.location(p1)
	wb.expect(wire.LocationUpdate, 3*time.Second)
	eventually(s, "posição no GET antes da queda", func() bool { return bytes.Equal(locations(s, b)[a.id], p1) })

	ctl.Stop(t)
	eventually(s, "GET /v1/locations vazio com o Redis fora", func() bool { return len(locations(s, b)) == 0 })
	s.want(s.do("GET", "/healthz", "", nil), 200, "")

	waitLoc(last)
	p2 := s.payload(64)
	wa.location(p2)
	if f := wb.expect(wire.LocationUpdate, 3*time.Second); !bytes.Equal(f.Payload, p2) {
		t.Errorf("repasse ao vivo com o Redis fora: %s", describe(f))
	}
	m := s.payload(64)
	wa.envelope(b, "m1", m)
	if f := wb.expect(wire.Envelope, 3*time.Second); !bytes.Equal(f.Payload, m) {
		t.Errorf("chat com o Redis fora: %s", describe(f))
	}
	wa.expect(wire.Ack, 3*time.Second)
	wb.ack(a, "m1")
	eventually(s, "ack com o Redis fora", func() bool { return len(pending(s, b)) == 0 })
}
