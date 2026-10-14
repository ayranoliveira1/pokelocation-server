// Package location: última posição cifrada de cada usuário (Redis loc:{id}, TTL 60 s) e repasse aos amigos online
// (docs/api.md §5 e §6). O caminho quente (OnFrame) só toca memória; o Redis é gravado em lote por Run.
package location

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/relay"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

const (
	minGap       = 4 * time.Second  // 1 a cada 5 s com folga de 1 s
	ttl          = 60 * time.Second // loc:{id}
	flushEvery   = time.Second
	reqWait      = 5 * time.Second // abaixo do statement_timeout do pool, que é só a rede de segurança
	friendsLimit = 600             // LIMIT defensivo acima do teto de 500 amigos
	maxJSON      = 256 << 10       // GET /v1/locations: o app lê até 256 KiB; passou → 413
	shards       = 64
)

type pending uint8

const (
	none pending = iota
	set
	del
)

// Protegido pelo mutex do shard.
// ponytail: limite de taxa e última intenção são por instância. Com várias instâncias, um usuário com conexões em
// duas passa do limite e as duas podem gravar loc:{id} fora de ordem; resolver com um dono por usuário.
type user struct {
	id      []byte // from dos frames, alocado uma vez
	key     string
	last    time.Time // última posição aceita (limite de taxa)
	seen    time.Time // última atividade (varredura)
	payload []byte    // última posição aceita, copiada do buffer de leitura
	todo    pending
	off     bool // LocationOff já tratado; outro igual é ignorado até a próxima posição
}

type shard struct {
	mu sync.Mutex
	m  map[string]*user
}

type Service struct {
	rdb     *redis.Client
	pool    *pgxpool.Pool
	send    func(user string, frame []byte) // hub.SendFriends: a posse do frame passa ao hub
	buf     func() []byte                   // relay.Buf
	now     func() time.Time
	shards  [shards]shard
	down    atomic.Bool // Redis falhou no último flush (loga 1 vez por sequência)
	reqWait time.Duration
}

func New(rdb *redis.Client, pool *pgxpool.Pool, hub *relay.Hub) *Service {
	s := newService(rdb, pool, hub.SendFriends)
	s.buf = relay.Buf
	return s
}

// Roda na goroutine de leitura da conexão; f já foi validado pelo wire.
func (s *Service) OnFrame(c *relay.Conn, f wire.Frame) { s.handle(c.User(), f) }

func newService(rdb *redis.Client, pool *pgxpool.Pool, send func(string, []byte)) *Service {
	s := &Service{rdb: rdb, pool: pool, send: send, now: time.Now, buf: func() []byte { return nil }, reqWait: reqWait}
	for i := range s.shards {
		s.shards[i].m = map[string]*user{}
	}
	return s
}

func (s *Service) shard(id string) *shard {
	h := uint32(2166136261) // FNV-1a, sem alocar
	for i := 0; i < len(id); i++ {
		h = (h ^ uint32(id[i])) * 16777619
	}
	return &s.shards[h%shards]
}

// f fatia o buffer de leitura do relay: nada dele é guardado depois de voltar.
func (s *Service) handle(id string, f wire.Frame) {
	sh := s.shard(id)
	now := s.now()
	sh.mu.Lock()
	u := sh.m[id]
	if u == nil {
		u = &user{id: []byte(id), key: "loc:" + id}
		sh.m[id] = u
	}
	u.seen = now
	var frame []byte
	switch f.Type {
	case wire.Location:
		if !u.last.IsZero() && now.Sub(u.last) < minGap {
			sh.mu.Unlock()
			return // acima do limite: descarta em silêncio
		}
		u.last, u.off, u.todo = now, false, set
		u.payload = append(u.payload[:0], f.Payload...)
		frame = wire.AppendLocationUpdate(s.buf(), u.id, f.Payload)
	case wire.LocationOff:
		if u.off {
			sh.mu.Unlock()
			return
		}
		u.off, u.todo = true, del
		u.payload = u.payload[:0]
		frame = wire.AppendLocationOff(s.buf(), u.id)
	default:
		sh.mu.Unlock()
		return
	}
	sh.mu.Unlock()
	s.send(id, frame)
}

func (s *Service) Run(ctx context.Context) {
	t := time.NewTicker(flushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.flush(ctx)
		}
	}
}

type write struct {
	key     string
	payload []byte // nil = DEL
}

// Redis fora: o lote se perde; a próxima posição regrava em ≤ 5 s e um DEL perdido expira em 60 s.
func (s *Service) flush(ctx context.Context) {
	now := s.now()
	var ws []write
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for id, u := range sh.m {
			switch u.todo {
			case set:
				ws = append(ws, write{u.key, append([]byte(nil), u.payload...)})
			case del:
				ws = append(ws, write{u.key, nil})
			default:
				if now.Sub(u.seen) > ttl {
					delete(sh.m, id)
				}
			}
			u.todo = none
		}
		sh.mu.Unlock()
	}
	if len(ws) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pipe := s.rdb.Pipeline()
	for _, w := range ws {
		if w.payload == nil {
			pipe.Del(ctx, w.key)
		} else {
			pipe.Set(ctx, w.key, w.payload, ttl)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		if !s.down.Swap(true) {
			slog.Warn("location: redis indisponível; posições ao vivo seguem, GET /v1/locations fica vazio")
		}
		return
	}
	if s.down.Swap(false) {
		slog.Info("location: redis voltou")
	}
}

// Só para testes.
func (s *Service) users() int {
	n := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		n += len(s.shards[i].m)
		s.shards[i].mu.Unlock()
	}
	return n
}

func (s *Service) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	mux.Handle("GET /v1/locations", authed(httpx.Timed(s.reqWait, s.locations)))
}

// Plano conferido em indexes_integration_test.go.
const (
	qFriends = `SELECT u.id, u.friend_code, u.username FROM friendships f
		JOIN users u ON u.id = CASE WHEN f.requester_id = $1 THEN f.addressee_id ELSE f.requester_id END
		WHERE f.status = 'accepted' AND (f.requester_id = $1 OR f.addressee_id = $1)
		AND NOT EXISTS (SELECT 1 FROM blocks b WHERE (b.blocker_id = $1 AND b.blocked_id = u.id)
			OR (b.blocker_id = u.id AND b.blocked_id = $1))
		LIMIT $2`
)

type entry struct {
	UserID     string  `json:"user_id"`
	FriendCode string  `json:"friend_code"`
	Username   *string `json:"username"` // null em conta antiga, ainda sem @nome
	Payload    []byte  `json:"payload"`
}

// bytes fixos de cada entrada do JSON, sem os valores
var entryOverhead = len(`{"user_id":"","friend_code":"","username":null,"payload":""},`)

// Redis fora → lista vazia, não erro (contrato).
func (s *Service) locations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rows, err := s.pool.Query(ctx, qFriends, httpx.User(ctx), friendsLimit)
	if err != nil {
		httpx.DBError(w, "location", err)
		return
	}
	var ids, codes, keys []string
	var names []*string
	var id, code string
	var name *string
	if _, err := pgx.ForEachRow(rows, []any{&id, &code, &name}, func() error {
		ids, codes, keys, names = append(ids, id), append(codes, code), append(keys, "loc:"+id), append(names, name)
		return nil
	}); err != nil {
		httpx.DBError(w, "location", err)
		return
	}
	out := struct {
		Locations []entry `json:"locations"`
	}{Locations: []entry{}}
	if len(keys) > 0 {
		mctx, cancel := context.WithTimeout(ctx, time.Second)
		vals, err := s.rdb.MGet(mctx, keys...).Result()
		cancel()
		if err != nil {
			slog.Warn("location: mget falhou", "err_type", fmt.Sprintf("%T", err))
			vals = nil
		}
		size := len(`{"locations":[]}` + "\n")
		for i, v := range vals {
			p, ok := v.(string)
			if !ok {
				continue // sem posição
			}
			n := entryOverhead + len(ids[i]) + len(codes[i]) + base64.StdEncoding.EncodedLen(len(p))
			if names[i] != nil {
				n += len(*names[i]) + 2 - len("null") // "nome" no lugar de null; @nome é só [a-z0-9_.]
			}
			if size+n > maxJSON { // nunca corta em silêncio
				httpx.Error(w, http.StatusRequestEntityTooLarge, "too_large")
				return
			}
			size += n
			out.Locations = append(out.Locations, entry{ids[i], codes[i], names[i], []byte(p)})
		}
	}
	httpx.JSON(w, http.StatusOK, out)
}
