package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	"github.com/ayranoliveira1/pokelocation-server/internal/auth"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

// outBuf: frame codificado uma vez e compartilhado, sem cópia, pelas filas que o recebem. Quem cria tem 1 referência,
// cada fila soma 1, e o último release devolve ao pool.
type outBuf struct {
	b    []byte
	refs atomic.Int32
}

var (
	outPool = sync.Pool{New: func() any { return &outBuf{b: make([]byte, 0, 512)} }}
	// emptyPool guarda os wrappers que o Buf esvazia, para o SendFriends não alocar um novo.
	emptyPool = sync.Pool{New: func() any { return new(outBuf) }}
)

func getOut() *outBuf {
	ob := outPool.Get().(*outBuf)
	ob.b = ob.b[:0]
	ob.refs.Store(1)
	return ob
}

func wrapOut(b []byte) *outBuf {
	ob := emptyPool.Get().(*outBuf)
	ob.b = b
	ob.refs.Store(1)
	return ob
}

func (o *outBuf) ref() { o.refs.Add(1) }

func (o *outBuf) release() {
	if o.refs.Add(-1) == 0 {
		outPool.Put(o)
	}
}

// readPool: um buffer por mensagem, não por conexão: conexão parada não segura 64 KiB.
var readPool = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

type item struct {
	ob   *outBuf
	drop bool // posição
}

// queue: o writer da conexão é o único consumidor.
type queue struct {
	mu        sync.Mutex
	items     [queueCap]item
	head, n   int
	closing   bool
	closeCode int    // 0 = só fecha o socket
	closeErr  string // "" = sem Error antes do close
	wake      chan struct{}
	space     chan struct{}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// push: a referência passa à fila. Cheia, tira a posição mais velha; só com envelopes, uma posição nova é descartada
// e um envelope novo devolve false (quem chama fecha a conexão: o envelope segue no Postgres).
func (q *queue) push(ob *outBuf, drop bool) bool {
	q.mu.Lock()
	if q.closing {
		q.mu.Unlock()
		ob.release()
		return true
	}
	if q.n == queueCap && !q.evictDroppable() {
		q.mu.Unlock()
		ob.release()
		return drop
	}
	q.items[(q.head+q.n)%queueCap] = item{ob, drop}
	q.n++
	q.mu.Unlock()
	signal(q.wake)
	return true
}

// pushWait: contrapressão do backlog. false = conexão fechando.
func (q *queue) pushWait(ob *outBuf, done <-chan struct{}) bool {
	for {
		q.mu.Lock()
		if q.closing {
			q.mu.Unlock()
			ob.release()
			return false
		}
		if q.n < backlogMax {
			q.items[(q.head+q.n)%queueCap] = item{ob, false}
			q.n++
			q.mu.Unlock()
			signal(q.wake)
			return true
		}
		q.mu.Unlock()
		select {
		case <-q.space:
		case <-done:
			ob.release()
			return false
		}
	}
}

// evictDroppable: chamar com q.mu. false = só envelopes na fila.
func (q *queue) evictDroppable() bool {
	for i := range q.n {
		if j := (q.head + i) % queueCap; q.items[j].drop {
			q.items[j].ob.release()
			for k := i; k < q.n-1; k++ {
				q.items[(q.head+k)%queueCap] = q.items[(q.head+k+1)%queueCap]
			}
			q.n--
			q.items[(q.head+q.n)%queueCap] = item{}
			return true
		}
	}
	return false
}

func (q *queue) pop() (item, bool) {
	q.mu.Lock()
	if q.closing {
		q.mu.Unlock()
		return item{}, true
	}
	if q.n == 0 {
		q.mu.Unlock()
		return item{}, false
	}
	it := q.items[q.head]
	q.items[q.head] = item{}
	q.head = (q.head + 1) % queueCap
	q.n--
	q.mu.Unlock()
	signal(q.space)
	return it, false
}

// Conn: uma goroutine de leitura (readLoop) e uma de escrita (writeLoop).
type Conn struct {
	hub   *Hub
	ws    *websocket.Conn
	user  string
	userB []byte // para montar frames sem converter a cada envio
	u     *user
	q     queue
	done  chan struct{} // fecha quando o writer termina
	exp   *time.Timer
}

func newConn(h *Hub, ws *websocket.Conn) *Conn {
	return &Conn{hub: h, ws: ws, done: make(chan struct{}),
		q: queue{wake: make(chan struct{}, 1), space: make(chan struct{}, 1)}}
}

func (c *Conn) User() string { return c.user }

func (c *Conn) setUser(id string) {
	c.user = id
	c.userB = []byte(id)
}

func (c *Conn) friend(peer []byte) bool {
	fs := c.u.friends.Load()
	if fs == nil {
		return false
	}
	_, ok := fs.has[string(peer)]
	return ok
}

// kill: o primeiro vale. Não bloqueia: o writer manda o Error e o close e fecha o socket. code 0 = só fecha.
func (c *Conn) kill(code int, errCode string) {
	c.q.mu.Lock()
	if c.q.closing {
		c.q.mu.Unlock()
		return
	}
	c.q.closing, c.q.closeCode, c.q.closeErr = true, code, errCode
	for ; c.q.n > 0; c.q.n-- {
		c.q.items[c.q.head].ob.release()
		c.q.items[c.q.head] = item{}
		c.q.head = (c.q.head + 1) % queueCap
	}
	c.q.mu.Unlock()
	signal(c.q.wake)
	signal(c.q.space)
}

func (c *Conn) sendError(code string) {
	ob := getOut()
	ob.b = wire.AppendError(ob.b, code)
	if !c.q.push(ob, false) {
		c.kill(websocket.CloseTryAgainLater, "")
	}
}

func (c *Conn) writeLoop() {
	defer close(c.done)
	defer c.ws.Close() // acorda o readLoop
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-c.q.wake:
		case <-ping.C:
			if err := c.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				c.kill(0, "")
			}
		}
		for {
			it, closing := c.q.pop()
			if closing {
				c.writeClose()
				return
			}
			if it.ob == nil {
				break
			}
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			err := c.ws.WriteMessage(websocket.BinaryMessage, it.ob.b)
			it.ob.release()
			if err != nil {
				c.kill(0, "")
			}
		}
	}
}

func (c *Conn) writeClose() {
	c.q.mu.Lock()
	code, msg := c.q.closeCode, c.q.closeErr
	c.q.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	if msg != "" {
		_ = c.ws.SetWriteDeadline(deadline)
		_ = c.ws.WriteMessage(websocket.BinaryMessage, wire.AppendError(nil, msg))
	}
	if code != 0 {
		_ = c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""), deadline)
	}
}

var errText = errors.New("relay: mensagem de texto")

func (c *Conn) next(buf *[]byte) ([]byte, error) {
	typ, r, err := c.ws.NextReader()
	if err != nil {
		return nil, err
	}
	if typ != websocket.BinaryMessage {
		return nil, errText
	}
	b := (*buf)[:0]
	for {
		if len(b) == cap(b) {
			b = append(b, 0)[:len(b)] // o SetReadLimit põe o teto
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	*buf = b
	return b, nil
}

func (c *Conn) readLoop() {
	defer c.finish()
	if !c.auth() {
		return
	}
	c.ws.SetPongHandler(func(string) error { return c.ws.SetReadDeadline(time.Now().Add(idleWait)) })
	for {
		_ = c.ws.SetReadDeadline(time.Now().Add(idleWait))
		buf := readPool.Get().(*[]byte)
		msg, err := c.next(buf)
		if err == nil {
			c.handle(msg) // nada de msg sobrevive: o buffer volta ao pool
		}
		readPool.Put(buf)
		if errors.Is(err, errText) {
			c.kill(websocket.CloseUnsupportedData, "")
			return
		}
		if err != nil {
			c.kill(0, "") // o 1009 o gorilla já mandou
			return
		}
	}
}

func (c *Conn) auth() bool {
	h := c.hub
	_ = c.ws.SetReadDeadline(time.Now().Add(h.authWait))
	buf := readPool.Get().(*[]byte)
	defer readPool.Put(buf)
	msg, err := c.next(buf)
	if errors.Is(err, errText) {
		c.kill(websocket.CloseUnsupportedData, "")
		return false
	}
	var f wire.Frame
	if err == nil {
		f, err = wire.Decode(msg)
	}
	if err != nil || f.Type != wire.Auth {
		c.kill(websocket.ClosePolicyViolation, "")
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), dbWait)
	defer cancel()
	token := string(f.Peer)
	id, exp, err := h.verify(ctx, token)
	if errors.Is(err, auth.ErrUnauthorized) {
		c.kill(websocket.ClosePolicyViolation, "unauthorized")
		return false
	}
	if err != nil {
		logDB("relay: auth", err)
		c.kill(websocket.CloseInternalServerErr, "") // banco fora: o app reconecta com espera
		return false
	}
	c.setUser(id)
	// a conexão vence com o token: uma revogação derruba o WS em ≤ 15 min mesmo se o Kick se perder
	c.exp = time.AfterFunc(time.Until(exp), func() { c.kill(websocket.ClosePolicyViolation, "unauthorized") })
	if h.beforeRegister != nil {
		h.beforeRegister()
	}
	h.register(c, nil)
	if h.cl != nil { // cluster: o que outra instância publicar daqui em diante já chega a esta conexão
		h.cl.wait(id, subWait)
	}
	// Corte do backlog logo depois do register: o que for gravado daqui em diante já sai ao vivo; sem o corte, tudo que
	// chegasse durante o Auth sairia em dobro.
	cutoff, err := h.store.maxID(ctx, id)
	if err != nil {
		logDB("relay: auth", err)
		c.kill(websocket.CloseInternalServerErr, "")
		return false
	}
	// Amigos e token relidos depois do register: um ReloadFriends ou Kick que comitou antes dele não acharia esta
	// conexão.
	if h.reload(c.u, id) != nil {
		c.kill(websocket.CloseInternalServerErr, "")
		return false
	}
	if _, _, err := h.verify(ctx, token); errors.Is(err, auth.ErrUnauthorized) {
		c.kill(websocket.ClosePolicyViolation, "unauthorized")
		return false
	} else if err != nil {
		logDB("relay: auth", err)
		c.kill(websocket.CloseInternalServerErr, "")
		return false
	}
	go c.backlog(cutoff)
	return true
}

func (c *Conn) finish() {
	c.kill(0, "")
	<-c.done
	if c.exp != nil {
		c.exp.Stop()
	}
	if c.user != "" {
		c.hub.unregister(c)
	}
}

func (c *Conn) backlog(upTo int64) {
	var after int64
	for {
		ctx, cancel := context.WithTimeout(context.Background(), dbWait)
		page, err := c.hub.store.page(ctx, c.user, after, upTo, pageSize)
		cancel()
		if err != nil {
			logDB("relay: backlog", err) // o resto sai pelo GET /v1/envelopes ou na próxima conexão
			return
		}
		for _, e := range page {
			ob := getOut()
			ob.b = wire.AppendEnvelope(ob.b, []byte(e.From), []byte(e.ClientMsgID), e.Payload)
			if !c.q.pushWait(ob, c.done) {
				return
			}
			after = e.ID
		}
		if len(page) < pageSize {
			return
		}
	}
}

// validMsgID: client_msg_id é text no Postgres, que recusa UTF-8 inválido e NUL.
func validMsgID(id []byte) bool {
	for _, b := range id {
		if b == 0 {
			return false
		}
	}
	return utf8.Valid(id)
}

// handle: f fatia msg; nada dele pode ser guardado nem enfileirado.
func (c *Conn) handle(msg []byte) {
	f, err := wire.Decode(msg)
	switch {
	case errors.Is(err, wire.ErrTooLarge):
		c.sendError("too_large")
		return
	case err != nil:
		c.sendError("bad_frame")
		return
	}
	switch {
	case f.Type == wire.Envelope:
		c.envelope(f)
	case f.Type == wire.Ack:
		c.ack(f)
	case f.Type == wire.Location || f.Type == wire.LocationOff && f.Peer == nil:
		c.hub.onLocation(c, f)
	default: // o wire.Decode não sabe de direção: tipos só do servidor e Auth repetido
		c.sendError("bad_frame")
	}
}

func (c *Conn) envelope(f wire.Frame) {
	if !validMsgID(f.MsgID) {
		c.sendError("bad_frame")
		return
	}
	if !c.friend(f.Peer) {
		c.sendError("forbidden")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), dbWait)
	fresh, err := c.hub.store.insert(ctx, string(f.Peer), c.user, string(f.MsgID), f.Payload)
	cancel()
	if err != nil {
		// sem Ack: o app mantém o envelope na fila dele e reenvia ao reconectar
		logDB("relay: envelope", err)
		c.kill(websocket.CloseInternalServerErr, "")
		return
	}
	c.hub.relayEnvelope(c, f, fresh)
}

func (c *Conn) ack(f wire.Frame) {
	if !validMsgID(f.MsgID) {
		c.sendError("bad_frame")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), dbWait)
	err := c.hub.store.ack(ctx, c.user, string(f.Peer), string(f.MsgID))
	cancel()
	if err != nil {
		logDB("relay: ack", err) // o envelope volta no backlog/REST e o app deduplica
	}
}

func errType(err error) string { return fmt.Sprintf("%T", err) }
