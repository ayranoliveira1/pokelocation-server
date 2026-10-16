package pogo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/push"
)

const (
	interval       = 2 * time.Minute
	defaultWorkers = 8 // por instância: limita a pressão sobre o PoGo (POGO_WORKERS muda)
	maxWorkers     = 64
	claimBatch     = 4
	perRule        = 5 // avisos por regra a cada execução
	numResults     = 100
	maxSeen        = 2 * time.Hour
	unknownTTL     = time.Hour
	// todas as regras no timeout; o lease passa disso, então não precisa de heartbeat
	userWait     = maxRules * proxyWait
	lease        = userWait + 30*time.Second
	maxSleep     = time.Second // pega o que outra instância enfileirou
	rebuildEvery = time.Minute
)

// Conta, token FCM (devices tem um aparelho por usuário) e regras ativas numa ida só, com o token_enc uma vez.
const qUserRules = `SELECT a.token_enc, d.fcm_token,
		coalesce((SELECT json_agg(json_build_object('id', r.id, 'name', r.name, 'filters', r.filters, 'scope', r.scope,
			'lat', r.lat, 'lng', r.lng, 'radius_km', r.radius_km) ORDER BY r.created_at)
			FROM pogo_rules r WHERE r.user_id = a.user_id AND r.enabled), '[]')
	FROM pogo_accounts a LEFT JOIN devices d ON d.user_id = a.user_id
	WHERE a.user_id = $1`

// Quem deve estar na fila: reconstrói um Redis que voltou vazio.
const qQueueUsers = `SELECT DISTINCT r.user_id::text FROM pogo_rules r
	WHERE r.enabled AND EXISTS (SELECT 1 FROM pogo_accounts a WHERE a.user_id = r.user_id)
		AND EXISTS (SELECT 1 FROM devices d WHERE d.user_id = r.user_id AND d.fcm_token IS NOT NULL)`

type dueRule struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Filters  json.RawMessage `json:"filters"`
	Scope    string          `json:"scope"`
	Lat      *float64        `json:"lat"`
	Lng      *float64        `json:"lng"`
	RadiusKm *float64        `json:"radius_km"`
}

func (s *Store) userRules(ctx context.Context, user string) (enc []byte, fcm *string, rules []dueRule, err error) {
	var js []byte
	err = s.pool.QueryRow(ctx, qUserRules, user).Scan(&enc, &fcm, &js)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil, errNoAccount
	}
	if err == nil {
		err = json.Unmarshal(js, &rules)
	}
	return enc, fcm, rules, err
}

type worker struct {
	svc     *Service
	q       *queue
	push    *push.Service
	now     func() time.Time
	workers int
	// campos para o teste encurtar
	interval, lease, userWait, callWait time.Duration
}

func newWorker(svc *Service, q *queue, p *push.Service) *worker {
	return &worker{svc: svc, q: q, push: p, now: time.Now, workers: defaultWorkers, interval: interval, lease: lease, userWait: userWait,
		callWait: proxyWait}
}

// Run atende a fila até ctx acabar. Sem POGO_TOKEN_KEY não faz nada.
func (h *Handler) Run(ctx context.Context, p *push.Service) {
	if h.svc == nil {
		return
	}
	w := newWorker(h.svc, h.q, p)
	w.workers = h.workers
	w.run(ctx)
}

// run: um dispatcher reivindica em lote só quando há worker livre (o lease conta a partir daí) e dorme até o score
// mais antigo, até maxSleep, ou até um ZADD local.
func (w *worker) run(ctx context.Context) {
	jobs := make(chan claim, w.workers)
	free := make(chan struct{}, w.workers)
	for range w.workers {
		free <- struct{}{}
		go func() {
			sc := &scratch{}
			for c := range jobs {
				w.user(ctx, c, sc)
				free <- struct{}{}
			}
		}()
	}
	defer close(jobs)
	timer := time.NewTimer(maxSleep)
	defer timer.Stop()
	w.rebuild(ctx) // na subida: cobre enfileiramentos perdidos com o Redis fora
	lastRebuild := w.now()
	var lastLag, lastErr time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-free:
		}
		n := 1
	more:
		for n < claimBatch {
			select {
			case <-free:
				n++
			default:
				break more
			}
		}
		now := w.now()
		cs, next, err := w.q.claim(ctx, now, w.lease, n)
		for range n - len(cs) {
			free <- struct{}{}
		}
		if err != nil && ctx.Err() == nil && now.Sub(lastErr) > time.Minute { // Redis fora: um aviso por minuto
			slog.Warn("pogo: fila", "err_type", fmt.Sprintf("%T", err))
			lastErr = now
		}
		for _, c := range cs {
			if lag := now.Sub(c.due); lag > w.interval && now.Sub(lastLag) > time.Minute {
				slog.Warn("pogo: fila atrasada", "lag_s", int(lag.Seconds()))
				lastLag = now
			}
			jobs <- c
		}
		if len(cs) > 0 {
			continue
		}
		sleep := maxSleep
		if err == nil && next.IsZero() && now.Sub(lastRebuild) > rebuildEvery {
			w.rebuild(ctx)
			lastRebuild = now
			sleep = 0
		} else if !next.IsZero() {
			sleep = min(max(next.Sub(now), 0), maxSleep)
		}
		timer.Reset(sleep)
		select {
		case <-ctx.Done():
			return
		case <-w.q.wake:
			timer.Stop() // Go ≥ 1.23: o Reset seguinte não vê valor velho no canal
		case <-timer.C:
		}
	}
}

// rebuild põe na fila (ZADD NX, vencido) quem tem regra ativa, vínculo e token FCM. Idempotente entre instâncias.
func (w *worker) rebuild(ctx context.Context) {
	rows, err := w.svc.store.pool.Query(ctx, qQueueUsers)
	var users []string
	if err == nil {
		users, err = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	if err == nil {
		now := w.now().UnixMilli()
		for len(users) > 0 && err == nil {
			n := min(1000, len(users))
			_, err = w.q.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
				for _, u := range users[:n] {
					p.ZAddNX(ctx, dueKey, redis.Z{Score: float64(now), Member: u})
				}
				return nil
			})
			users = users[n:]
		}
	}
	if err != nil && ctx.Err() == nil {
		slog.Warn("pogo: reconstrução da fila", "err_type", fmt.Sprintf("%T", err))
	}
}

// scratch: memória de um worker reaproveitada entre usuários.
type scratch struct {
	buf    bytes.Buffer
	mons   []mon
	cands  []mon
	counts []int    // candidatos por regra, em ordem
	groups []string // texto do dedup por regra
	pack   []byte
}

func (w *worker) user(ctx context.Context, c claim, sc *scratch) {
	uctx, cancel := context.WithTimeout(ctx, w.userWait)
	next := w.runUser(uctx, c, sc)
	cancel()
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dbWait)
	defer cancel()
	if _, err := w.q.finish(fctx, c, next); err != nil {
		slog.Warn("pogo: reagendar", "err_type", fmt.Sprintf("%T", err))
	}
}

// runUser devolve quando rodar de novo; zero = sai da fila (sem vínculo, sem regra ativa ou sem token FCM).
func (w *worker) runUser(ctx context.Context, c claim, sc *scratch) time.Time {
	now := w.now()
	next := now.Add(w.interval)
	enc, fcm, rules, err := w.svc.store.userRules(ctx, c.user)
	if errors.Is(err, errNoAccount) {
		return time.Time{}
	}
	if err != nil {
		slog.Warn("pogo: regras", "err_type", fmt.Sprintf("%T", err))
		return next
	}
	if fcm == nil || len(rules) == 0 {
		return time.Time{}
	}
	token, err := w.svc.open(c.user, enc)
	if err != nil {
		_ = w.svc.drop(ctx, c.user, enc)
		return time.Time{}
	}
	sc.cands, sc.counts, sc.groups = sc.cands[:0], sc.counts[:0], sc.groups[:0]
	for i := range rules {
		r := &rules[i]
		path, limit := "/map/lookup", int64(1<<20)
		if r.Scope == "world" {
			path, limit = "/search", 256<<10
		}
		body, err := r.body()
		if err != nil {
			sc.skip()
			continue
		}
		sc.buf.Reset()
		cctx, cancel := context.WithTimeout(ctx, w.callWait)
		_, err = w.svc.callToken(cctx, c.user, enc, token, "POST", path, body, limit, false, &sc.buf)
		stuck := err != nil && cctx.Err() != nil && ctx.Err() == nil // só esta chamada travou: as outras regras ainda rodam
		cancel()
		var ue *UpstreamError
		if errors.As(err, &ue) && ue.Status == 429 {
			if ue.RetryAfter > 0 {
				next = later(next, now.Add(time.Duration(ue.RetryAfter)*time.Second))
			}
			break
		}
		if errors.Is(err, ErrRelogin) {
			return time.Time{}
		}
		if ue != nil && ue.Status == http.StatusForbidden {
			if code := ue.code(); code == "trainer_not_linked" || code == "shiny_account_unavailable" {
				// o treinador saiu ou nunca valeu: a regra repetiria o erro a cada execução
				if err := w.svc.store.disableRule(ctx, c.user, r, reasonTrainer); err != nil {
					slog.Warn("pogo: desligar regra", "err_type", fmt.Sprintf("%T", err))
				}
			}
		}
		if ue != nil || stuck { // filtro que o PoGo recusa (403, 422…) ou chamada travada: só esta regra
			sc.skip()
			continue
		}
		if err != nil { // PoGo fora ou prazo do usuário esgotado
			break
		}
		before := len(sc.cands)
		w.matches(r, sc.buf.Bytes(), now, sc)
		sc.group(before, now)
	}
	if len(sc.cands) > 0 {
		w.notify(ctx, c, rules, *fcm, now, sc)
	}
	return next
}

func (sc *scratch) skip() {
	sc.counts = append(sc.counts, 0)
	sc.groups = append(sc.groups, "")
}

// group fecha a regra corrente: os candidatos de cands[from:] viram um texto "encounter ttl …" para o dedup.
func (sc *scratch) group(from int, now time.Time) {
	sc.pack = sc.pack[:0]
	for i := from; i < len(sc.cands); i++ {
		m := &sc.cands[i]
		sc.pack = append(sc.pack, m.Encounter...)
		sc.pack = append(sc.pack, ' ')
		sc.pack = strconv.AppendInt(sc.pack, seenTTL(m.despawn, now).Milliseconds(), 10)
		sc.pack = append(sc.pack, ' ')
	}
	sc.counts = append(sc.counts, len(sc.cands)-from)
	sc.groups = append(sc.groups, string(sc.pack))
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// notify marca como vistos até perRule itens por regra (uma ida ao Redis, com o fencing) e enfileira os avisos.
// Fila do push cheia: desmarca, para tentar de novo na próxima execução.
func (w *worker) notify(ctx context.Context, c claim, rules []dueRule, fcm string, now time.Time, sc *scratch) {
	picked, err := w.q.markSeen(ctx, c, perRule, now, sc.groups)
	if err != nil || len(picked) == 0 {
		return
	}
	msgs := make([]push.Data, 0, len(picked))
	ruleOf, end := 0, sc.counts[0]
	for _, p := range picked {
		for p >= end {
			ruleOf++
			end += sc.counts[ruleOf]
		}
		msgs = append(msgs, w.data(&rules[ruleOf], &sc.cands[p], now))
	}
	if !w.push.TryPush(c.user, fcm, msgs) {
		ids := make([]any, len(picked))
		for i, p := range picked {
			ids[i] = string(sc.cands[p].Encounter)
		}
		_ = w.q.rdb.ZRem(ctx, seenKey(c.user), ids...).Err()
	}
}

func (r *dueRule) body() ([]byte, error) {
	if r.Scope == "world" {
		var f map[string]json.RawMessage
		if err := json.Unmarshal(r.Filters, &f); err != nil {
			return nil, err
		}
		// os nomes do /search diferem dos do mapa
		for from, to := range map[string]string{"iv_min": "iv_percent_min", "iv_max": "iv_percent_max"} {
			if v, ok := f[from]; ok {
				f[to] = v
				delete(f, from)
			}
		}
		delete(f, "num_results") // só do mapa: no /search aumentaria a resposta
		f["iv_mode"] = json.RawMessage(`"percent"`)
		f["sort"] = json.RawMessage(`"DSP"`)
		return json.Marshal(f)
	}
	dLat := *r.RadiusKm / kmPerDeg
	dLng := *r.RadiusKm / (kmPerDeg * math.Max(math.Cos(*r.Lat*math.Pi/180), 0.01))
	// ponytail: caixa cortada em ±180; um raio que atravessa o antimeridiano perde o outro lado.
	return json.Marshal(struct {
		Mode       string          `json:"mode"`
		N          float64         `json:"n_lat"`
		W          float64         `json:"w_lng"`
		S          float64         `json:"s_lat"`
		E          float64         `json:"e_lng"`
		Filters    json.RawMessage `json:"filters"`
		NumResults int             `json:"num_results"`
	}{"pokemon", math.Min(*r.Lat+dLat, 90), math.Max(*r.Lng-dLng, -180), math.Max(*r.Lat-dLat, -90),
		math.Min(*r.Lng+dLng, 180), r.Filters, numResults})
}

const kmPerDeg = 111.32

// flexID: o encounter_id vem como string no /search; no mapa pode vir número.
type flexID string

func (f *flexID) UnmarshalJSON(b []byte) error {
	if string(b) != "null" {
		*f = flexID(strings.Trim(string(b), `"`))
	}
	return nil
}

type mon struct {
	Encounter flexID   `json:"encounter_id"`
	Reveal    string   `json:"reveal_code"`
	Name      string   `json:"pokemon_name"`
	CP        *float64 `json:"cp"`
	IV        *float64 `json:"iv_percent"` // 0 é um IV válido: ausente precisa ser nil
	DespawnMs int64    `json:"despawn_at_ms"`
	DespawnAt string   `json:"despawn_at"`
	Lat       float64  `json:"apx_lat"` // o /map/lookup sempre manda; o /search não, mas lá não há raio
	Lng       float64  `json:"apx_long"`
	Shiny     bool     `json:"shiny"`
	despawn   time.Time
}

// matches põe em sc.cands os itens da resposta que valem um aviso, na ordem do PoGo.
func (w *worker) matches(r *dueRule, body []byte, now time.Time, sc *scratch) {
	// o Unmarshal reaproveita os elementos sem zerar: um campo ausente herdaria o valor do item anterior
	ms := sc.mons[:cap(sc.mons)]
	clear(ms)
	resp := struct {
		Results []mon `json:"results"`
	}{ms[:0]}
	err := json.Unmarshal(body, &resp)
	sc.mons = resp.Results
	if err != nil {
		return
	}
	for i := range resp.Results {
		m := &resp.Results[i]
		if m.Encounter == "" || m.Reveal == "" || strings.ContainsAny(string(m.Encounter), " \t\n") { // o dedup separa por espaço
			continue
		}
		switch {
		case m.DespawnMs != 0:
			m.despawn = time.UnixMilli(m.DespawnMs)
		case m.DespawnAt != "":
			m.despawn, _ = time.Parse(time.RFC3339, m.DespawnAt)
		}
		if !m.despawn.IsZero() && !m.despawn.After(now) {
			continue
		}
		if r.Scope == "near" && distKm(*r.Lat, *r.Lng, m.Lat, m.Lng) > *r.RadiusKm {
			continue
		}
		sc.cands = append(sc.cands, *m)
	}
}

func distKm(lat1, lng1, lat2, lng2 float64) float64 {
	const r = 6371.0
	p1, p2 := lat1*math.Pi/180, lat2*math.Pi/180
	dp, dl := p2-p1, (lng2-lng1)*math.Pi/180
	a := math.Sin(dp/2)*math.Sin(dp/2) + math.Cos(p1)*math.Cos(p2)*math.Sin(dl/2)*math.Sin(dl/2)
	return 2 * r * math.Asin(math.Sqrt(a))
}

func seenTTL(despawn, now time.Time) time.Duration {
	if despawn.IsZero() {
		return unknownTTL
	}
	return min(despawn.Sub(now)+time.Minute, maxSeen)
}

func (w *worker) data(r *dueRule, m *mon, now time.Time) push.Data {
	f := map[string]string{"t": "p", "n": m.Name, "iv": num(m.IV), "cp": num(m.CP), "d": "", "r": r.Name,
		"rc": m.Reveal, "e": string(m.Encounter)}
	ttl := unknownTTL
	if !m.despawn.IsZero() {
		f["d"] = strconv.FormatInt(m.despawn.UnixMilli(), 10)
		ttl = max(m.despawn.Sub(now), 0)
	}
	if m.Shiny {
		f["s"] = "1"
	}
	return push.Data{Fields: f, Collapse: "p:" + r.ID, TTL: ttl}
}

func num(v *float64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatFloat(*v, 'f', -1, 64)
}
