// Package relay: WebSocket (/v1/ws) e fila de envelopes cifrados (docs/api.md §5 e §6). Caminho quente sem camadas: o
// frame é validado e repassado sem virar entidade. Posição vai para o onLocation; o relay não importa o location.
package relay

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

const (
	shardCount = 64
	maxConns   = 4 // por usuário; a 5ª derruba a mais antiga
	queueCap   = 64
	backlogMax = queueCap / 2 // o backlog nunca lota a fila: sobra vaga para o ao vivo
	readLimit  = 64 << 10
	pageSize   = 50
	pageBytes  = 48 << 10 // o app lê no máximo 64 KiB por resposta
	maxAckIDs  = 100
	pingEvery  = 30 * time.Second
	idleWait   = 70 * time.Second // dois pings sem resposta
	writeWait  = 10 * time.Second
	dbWait     = 5 * time.Second
)

type Hub struct {
	store      *Store
	verify     func(ctx context.Context, token string) (string, time.Time, error)
	onLocation func(c *Conn, f wire.Frame)
	up         websocket.Upgrader
	shards     [shardCount]shard
	authWait   time.Duration // campo para o teste encurtar
	// beforeRegister: gancho de teste (nil em produção).
	beforeRegister func()
	cl             *cluster // nil = uma instância só (EnableCluster liga)
}

type shard struct {
	mu    sync.Mutex
	users map[string]*user
}

// user: conexões em ordem de chegada. reload serializa as recargas da lista (ler o banco e gravar sob a trava): a
// última gravada é sempre a lida por último.
type user struct {
	conns   []*Conn
	friends atomic.Pointer[friendSet]
	reload  sync.Mutex
}

// friendSet: imutável; o ReloadFriends troca o conjunto inteiro.
type friendSet struct {
	ids [][]byte
	has map[string]struct{}
}

func newFriendSet(ids []string) *friendSet {
	fs := &friendSet{ids: make([][]byte, len(ids)), has: make(map[string]struct{}, len(ids))}
	for i, id := range ids {
		fs.ids[i] = []byte(id)
		fs.has[id] = struct{}{}
	}
	return fs
}

// New: onLocation recebe os frames Location e LocationOff do cliente.
func New(pool *pgxpool.Pool, verify func(ctx context.Context, token string) (string, time.Time, error),
	onLocation func(c *Conn, f wire.Frame)) *Hub {
	h := &Hub{store: &Store{pool: pool}, verify: verify, onLocation: onLocation, authWait: 5 * time.Second}
	h.up = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		WriteBufferPool: &sync.Pool{},
		// O app não manda Origin e navegador sempre manda: recusar fecha a porta a WS cross-site.
		CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "" },
	}
	for i := range h.shards {
		h.shards[i].users = map[string]*user{}
	}
	return h
}

// Routes: /v1/ws fica fora do authed porque o token vem no primeiro frame, nunca na URL (não cai em log).
func (h *Hub) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	mux.HandleFunc("GET /v1/ws", h.serveWS)
	mux.Handle("GET /v1/envelopes", authed(h.listEnvelopes))
	mux.Handle("POST /v1/envelopes/ack", authed(h.ackEnvelopes))
}

// Cleanup apaga envelopes com mais de 30 dias.
func (h *Hub) Cleanup(ctx context.Context) error { return h.store.cleanup(ctx) }

// fnv: o mesmo id cai no mesmo shard, venha como string ou []byte.
func fnv[T string | []byte](id T) uint32 {
	x := uint32(2166136261)
	for i := 0; i < len(id); i++ {
		x ^= uint32(id[i])
		x *= 16777619
	}
	return x
}

func (h *Hub) shardOf(id uint32) *shard { return &h.shards[id%shardCount] }

// register não grava a lista de amigos: lida antes do registro, ela poderia sobrescrever uma recarga mais nova. Quem
// registra recarrega depois. fs só serve a testes sem banco.
func (h *Hub) register(c *Conn, fs *friendSet) {
	s := h.shardOf(fnv(c.user))
	s.mu.Lock()
	u := s.users[c.user]
	if u == nil {
		u = &user{}
		s.users[c.user] = u
	}
	if fs != nil {
		u.friends.CompareAndSwap(nil, fs)
	}
	c.u = u
	var evicted *Conn
	if len(u.conns) == maxConns {
		evicted = u.conns[0]
		u.conns = slices.Delete(u.conns, 0, 1)
	}
	u.conns = append(u.conns, c)
	s.mu.Unlock()
	if h.cl != nil {
		h.cl.touch(c.user)
	}
	if evicted != nil {
		evicted.kill(websocket.ClosePolicyViolation, "")
	}
}

func (h *Hub) unregister(c *Conn) {
	s := h.shardOf(fnv(c.user))
	s.mu.Lock()
	if u := s.users[c.user]; u != nil {
		if i := slices.Index(u.conns, c); i >= 0 {
			u.conns = slices.Delete(u.conns, i, i+1)
		}
		if len(u.conns) == 0 {
			delete(s.users, c.user)
		}
	}
	s.mu.Unlock()
	if h.cl != nil {
		h.cl.touch(c.user)
	}
}

func (h *Hub) online(id string) int {
	s := h.shardOf(fnv(id))
	s.mu.Lock()
	defer s.mu.Unlock()
	if u := s.users[id]; u != nil {
		return len(u.conns)
	}
	return 0
}

// deliver: ÚNICO ponto de saída de frames para um usuário: as conexões daqui e, com o cluster ligado, as das outras
// instâncias (cluster.go). droppable = posição (fila cheia descarta a mais velha); senão fila cheia de envelopes fecha
// a conexão. O chamador segue dono da própria referência.
func (h *Hub) deliver(user []byte, ob *outBuf, droppable bool) {
	h.deliverLocal(user, ob, droppable)
	if h.cl != nil {
		h.cl.frame(user, ob.b, droppable)
	}
}

// deliverLocal entrega ob a todas as conexões de user nesta instância, sem cópia: cada fila ganha uma referência.
func (h *Hub) deliverLocal(user []byte, ob *outBuf, droppable bool) {
	s := h.shardOf(fnv(user))
	var full []*Conn
	s.mu.Lock()
	if u := s.users[string(user)]; u != nil {
		for _, c := range u.conns {
			ob.ref()
			if !c.q.push(ob, droppable) {
				full = append(full, c) // só aloca quando um cliente travou
			}
		}
	}
	s.mu.Unlock()
	for _, c := range full {
		c.kill(websocket.CloseTryAgainLater, "") // os envelopes seguem no Postgres
	}
}

// relayEnvelope codifica frames novos: f fatia o buffer de leitura, que volta ao pool quando esta chamada retorna.
func (h *Hub) relayEnvelope(from *Conn, f wire.Frame, fresh bool) {
	ack := getOut()
	ack.b = wire.AppendAck(ack.b, f.Peer, f.MsgID)
	h.deliver(from.userB, ack, false)
	ack.release()
	if !fresh {
		return
	}
	env := getOut()
	env.b = wire.AppendEnvelope(env.b, from.userB, f.MsgID, f.Payload)
	h.deliver(f.Peer, env, false)
	env.release()
}

// Buf: buffer do pool do Hub para montar o frame do SendFriends.
func Buf() []byte {
	ob := getOut()
	b := ob.b
	ob.b = nil
	emptyPool.Put(ob)
	return b
}

// SendFriends entrega frame como posição (descartável) aos amigos online de user. A posse de frame passa ao Hub: é
// compartilhado pelas filas e reciclado no fim, então quem chama não o toca mais.
func (h *Hub) SendFriends(user string, frame []byte) {
	ob := wrapOut(frame)
	h.sendFriends(user, ob)
}

func (h *Hub) sendFriends(id string, ob *outBuf) {
	s := h.shardOf(fnv(id))
	var fs *friendSet
	s.mu.Lock()
	if u := s.users[id]; u != nil {
		fs = u.friends.Load()
	}
	s.mu.Unlock()
	if fs != nil {
		for _, f := range fs.ids {
			h.deliver(f, ob, true)
		}
	}
	ob.release()
}

// ponytail: síncrono (o 204 do friends espera a leitura, até dbWait com o Postgres lento); se pesar, chamar em
// goroutine: a serialização por usuário (user.reload) já garante a ordem.
func (h *Hub) ReloadFriends(user string) {
	h.reloadLocal(user)
	if h.cl != nil {
		h.cl.ctl(kindReload, user)
	}
}

// Kick derruba todas as conexões de user com Error{unauthorized} + 1008.
func (h *Hub) Kick(user string) {
	h.kickLocal(user)
	if h.cl != nil {
		h.cl.ctl(kindKick, user)
	}
}

// reloadLocal/kickLocal: só esta instância; com o cluster, as outras recebem por relay:ctl.
func (h *Hub) reloadLocal(id string) {
	s := h.shardOf(fnv(id))
	s.mu.Lock()
	u := s.users[id]
	s.mu.Unlock()
	if u != nil {
		_ = h.reload(u, id)
	}
}

func (h *Hub) reload(u *user, id string) error {
	u.reload.Lock()
	defer u.reload.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), dbWait)
	defer cancel()
	ids, err := h.store.friends(ctx, id)
	if err != nil {
		logDB("relay: amigos", err)
		return err
	}
	u.friends.Store(newFriendSet(ids))
	return nil
}

func (h *Hub) kickLocal(id string) {
	s := h.shardOf(fnv(id))
	s.mu.Lock()
	var conns []*Conn
	if u := s.users[id]; u != nil {
		conns = slices.Clone(u.conns)
	}
	s.mu.Unlock()
	for _, c := range conns {
		c.kill(websocket.ClosePolicyViolation, "unauthorized")
	}
}

func (h *Hub) serveWS(w http.ResponseWriter, r *http.Request) {
	ws, err := h.up.Upgrade(w, r, nil)
	if err != nil {
		return // o Upgrade já respondeu
	}
	ws.SetReadLimit(readLimit)
	c := newConn(h, ws)
	go c.writeLoop()
	c.readLoop()
}

func (h *Hub) listEnvelopes(w http.ResponseWriter, r *http.Request) {
	page, err := h.store.page(r.Context(), httpx.User(r.Context()), 0, math.MaxInt64, pageSize)
	if err != nil {
		httpx.DBError(w, "relay", err)
		return
	}
	size := 16 // {"envelopes":[]}
	for i, e := range page {
		// base64 cresce 4/3; id, from e as chaves cabem em 160 bytes; client_msg_id pode virar \u00XX
		size += (len(e.Payload)+2)/3*4 + len(e.ClientMsgID)*6 + 160
		if size > pageBytes && i > 0 {
			page = page[:i]
			break
		}
	}
	if page == nil {
		page = []queuedEnvelope{}
	}
	httpx.JSON(w, http.StatusOK, map[string][]queuedEnvelope{"envelopes": page})
}

func (h *Hub) ackEnvelopes(w http.ResponseWriter, r *http.Request) {
	var in struct {
		IDs []string `json:"ids"`
	}
	if !httpx.Decode(w, r, 4<<10, &in) {
		return
	}
	if len(in.IDs) > maxAckIDs {
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return
	}
	ids := make([]int64, len(in.IDs))
	for i, s := range in.IDs {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, "bad_request")
			return
		}
		ids[i] = n
	}
	if err := h.store.ackIDs(r.Context(), httpx.User(r.Context()), ids); err != nil {
		httpx.DBError(w, "relay", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// logDB: sem a mensagem, que o Postgres pode ecoar da entrada.
func logDB(msg string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	slog.Warn(msg, "err_type", errType(err))
}
