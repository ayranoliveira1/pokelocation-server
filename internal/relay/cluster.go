package relay

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// cluster: várias instâncias atrás do mesmo Redis (S8; desligado por padrão, CLUSTER=1 no main).
//
// Sem tabela de presença: cada instância assina u:{id} de cada usuário com conexão aqui, e deliver publica todo frame
// em u:{destinatário}; o Redis só entrega a quem assina (offline = descarte). Kick e ReloadFriends vão a todas por
// relay:ctl. Mensagem: [instância 8 B][tipo 1 B][corpo]; quem publicou ignora o próprio eco.
//
// Frames vão por uma fila assíncrona (pode descartar); Kick e Reload, não: publicados direto, nunca descartados.
// O Auth espera a assinatura de u:{id} ser confirmada (até subWait) antes do corte do backlog: nada fica entre os dois.
//
// ponytail: publica até para quem não está em instância nenhuma (um PUBLISH a mais por destinatário offline); o
// limite de 4 conexões e o de posição valem por instância.
type cluster struct {
	h   *Hub
	rdb *redis.Client
	ps  *redis.PubSub
	id  [8]byte
	out chan pubMsg

	mu      sync.Mutex
	touched map[string]struct{}        // usuários que ganharam ou perderam conexão desde o último sync
	ready   map[string]struct{}        // u:{id} com assinatura confirmada pelo Redis
	waits   map[string][]chan struct{} // Auth esperando a confirmação
	wake    chan struct{}
	subs    map[string]struct{} // assinados agora; só a goroutine sync toca
}

type pubMsg struct {
	ch   string
	data []byte
}

const (
	ctlChannel = "relay:ctl"
	pubQueue   = 4096 // frames esperando o Redis; cheia = descarta (posição some, envelope segue no Postgres)
	pubBatch   = 256
	ctlWait    = 2 * time.Second // Kick/Reload: vêm dos handlers HTTP, fora do caminho quente
	subWait    = time.Second     // Auth esperando a assinatura (Redis fora: segue sem)

	kindLive   byte = 'e' // frame que não se descarta (envelope, ack)
	kindDrop   byte = 'd' // posição
	kindKick   byte = 'k'
	kindReload byte = 'r'
)

// EnableCluster liga o repasse entre instâncias. Chamar antes de servir; as goroutines param com ctx.
// Não espera o Redis: com ele fora, cada instância segue servindo os próprios usuários e o go-redis reassina ao voltar.
func (h *Hub) EnableCluster(ctx context.Context, rdb *redis.Client) {
	h.enableCluster(ctx, rdb, pubQueue, true)
}

// enableCluster: queue e publisher só mudam no teste (fila sempre cheia, para provar que o controle não depende dela).
func (h *Hub) enableCluster(ctx context.Context, rdb *redis.Client, queue int, publisher bool) {
	c := &cluster{h: h, rdb: rdb, out: make(chan pubMsg, queue), touched: map[string]struct{}{},
		ready: map[string]struct{}{}, waits: map[string][]chan struct{}{}, wake: make(chan struct{}, 1),
		subs: map[string]struct{}{}}
	_, _ = rand.Read(c.id[:])
	c.ps = rdb.Subscribe(ctx, ctlChannel)
	h.cl = c
	if publisher {
		go c.publishLoop(ctx)
	}
	go c.receiveLoop(ctx)
	go c.syncLoop(ctx)
	go func() { <-ctx.Done(); _ = c.ps.Close() }()
}

// frame: depois da entrega local, o mesmo frame para as outras instâncias que tenham user. Copia b (o chamador
// recicla o buffer).
func (c *cluster) frame(user, b []byte, droppable bool) {
	kind := kindLive
	if droppable {
		kind = kindDrop
	}
	c.publish("u:"+string(user), kind, b)
}

// ctl: Kick/Reload, direto no Redis e nunca descartado por fila cheia (bloqueio e revogação valem em todas as
// instâncias). Falhou (Redis fora): o exp do token e a próxima recarga cobrem; fica no log.
func (c *cluster) ctl(kind byte, user string) {
	ctx, cancel := context.WithTimeout(context.Background(), ctlWait)
	defer cancel()
	if err := c.rdb.Publish(ctx, ctlChannel, c.msg(kind, []byte(user))).Err(); err != nil {
		slog.Warn("relay: cluster: controle não publicado", "kind", string(kind), "err_type", fmt.Sprintf("%T", err))
	}
}

func (c *cluster) publish(ch string, kind byte, body []byte) {
	select {
	case c.out <- pubMsg{ch, c.msg(kind, body)}:
	default: // Redis lento ou fora: não trava o caminho quente
	}
}

func (c *cluster) msg(kind byte, body []byte) []byte {
	data := make([]byte, 0, len(c.id)+1+len(body))
	return append(append(append(data, c.id[:]...), kind), body...)
}

// publishLoop manda o que estiver na fila num pipeline só. Loga só a transição ok → falha.
func (c *cluster) publishLoop(ctx context.Context) {
	batch := make([]pubMsg, 0, pubBatch)
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-c.out:
			batch = append(batch[:0], m)
		}
	fill:
		for len(batch) < pubBatch {
			select {
			case m := <-c.out:
				batch = append(batch, m)
			default:
				break fill
			}
		}
		pipe := c.rdb.Pipeline()
		for _, m := range batch {
			pipe.Publish(ctx, m.ch, m.data)
		}
		_, err := pipe.Exec(ctx)
		if err != nil && !failing && ctx.Err() == nil {
			slog.Warn("relay: cluster sem Redis, repasse entre instâncias parado")
		}
		failing = err != nil
	}
}

func (c *cluster) receiveLoop(ctx context.Context) {
	ch := c.ps.ChannelWithSubscriptions()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			switch m := m.(type) {
			case *redis.Message:
				c.receive(m.Channel, m.Payload)
			case *redis.Subscription:
				c.confirmed(m)
			}
		}
	}
}

// confirmed: o Redis confirmou (ou desfez) a assinatura de u:{id}; libera quem espera no Auth.
func (c *cluster) confirmed(s *redis.Subscription) {
	user, ok := strings.CutPrefix(s.Channel, "u:")
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.Kind != "subscribe" {
		delete(c.ready, user)
		return
	}
	c.ready[user] = struct{}{}
	for _, w := range c.waits[user] {
		close(w)
	}
	delete(c.waits, user)
}

// wait: espera u:{user} estar assinado de fato (Auth, antes do corte do backlog), até d.
func (c *cluster) wait(user string, d time.Duration) {
	c.mu.Lock()
	if _, ok := c.ready[user]; ok {
		c.mu.Unlock()
		return
	}
	w := make(chan struct{})
	c.waits[user] = append(c.waits[user], w)
	c.mu.Unlock()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-w:
	case <-t.C:
		c.mu.Lock()
		if ws := slices.DeleteFunc(c.waits[user], func(x chan struct{}) bool { return x == w }); len(ws) > 0 {
			c.waits[user] = ws
		} else {
			delete(c.waits, user)
		}
		c.mu.Unlock()
	}
}

func (c *cluster) receive(channel, p string) {
	if len(p) < len(c.id)+1 || p[:len(c.id)] == string(c.id[:]) {
		return // eco ou lixo
	}
	kind, body := p[len(c.id)], p[len(c.id)+1:]
	switch kind {
	case kindLive, kindDrop:
		user, ok := strings.CutPrefix(channel, "u:")
		if !ok {
			return
		}
		ob := getOut()
		ob.b = append(ob.b, body...)
		c.h.deliverLocal([]byte(user), ob, kind == kindDrop)
		ob.release()
	case kindKick:
		c.h.kickLocal(body)
	case kindReload:
		go c.h.reloadLocal(body) // lê o banco: não segura a fila; user.reload serializa e a última leitura vence
	}
}

// touch: user ganhou ou perdeu conexão aqui; o sync acerta a assinatura de u:{user}.
func (c *cluster) touch(user string) {
	c.mu.Lock()
	c.touched[user] = struct{}{}
	c.mu.Unlock()
	signal(c.wake)
}

// syncLoop: uma goroutine só (a ordem de assinar/cancelar não se inverte), decidindo pelo estado de agora.
func (c *cluster) syncLoop(ctx context.Context) {
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		}
		c.mu.Lock()
		t := c.touched
		c.touched = map[string]struct{}{}
		c.mu.Unlock()
		var sub, unsub []string
		for u := range t {
			_, has := c.subs[u]
			switch on := c.h.online(u) > 0; {
			case on && !has:
				c.subs[u] = struct{}{}
				sub = append(sub, "u:"+u)
			case !on && has:
				delete(c.subs, u)
				unsub = append(unsub, "u:"+u)
			}
		}
		// erro de rede: o go-redis guarda os canais e reassina ao reconectar; loga só a transição
		var err error
		if len(sub) > 0 {
			err = c.ps.Subscribe(ctx, sub...)
		}
		if len(unsub) > 0 {
			err = errors.Join(err, c.ps.Unsubscribe(ctx, unsub...))
		}
		if err != nil && !failing && ctx.Err() == nil {
			slog.Warn("relay: cluster sem Redis, instância não recebe das outras até voltar")
		}
		failing = err != nil
	}
}
