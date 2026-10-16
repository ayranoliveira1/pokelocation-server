package pogo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/push"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

type pushed struct {
	user, token string
	msgs        []push.Data
}

type fakeFCM struct {
	mu  sync.Mutex
	got []pushed
}

func (f *fakeFCM) Send(context.Context, string, string) error { return nil }

func (f *fakeFCM) SendData(_ context.Context, user, token string, msgs []push.Data) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, pushed{user, token, msgs})
	return nil
}

// take espera a fila assíncrona do push entregar n avisos e os devolve.
func (f *fakeFCM) take(t *testing.T, n int) []pushed {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		f.mu.Lock()
		if len(f.got) >= n {
			g := f.got
			f.got = nil
			f.mu.Unlock()
			return g
		}
		f.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("esperava %d avisos", n)
		}
	}
}

func withDevice(t *testing.T, pool *pgxpool.Pool, user string, fcm *string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO devices (user_id, registration_id, identity_key,
		signed_prekey_id, signed_prekey, signed_prekey_sig, kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig,
		fcm_token) VALUES ($1, 1, '\x05', 1, '\x05', '\x00', 1, '\x08', '\x00', $2)`, user, fcm); err != nil {
		t.Fatal(err)
	}
}

func addRule(t *testing.T, h *Handler, user, body string) string {
	t.Helper()
	w := serve(h, user, "POST", "/v1/pogo/rules", body)
	var r Rule
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &r) != nil {
		t.Fatalf("regra: %d %s", w.Code, w.Body.String())
	}
	return r.ID
}

// clock: o relógio do worker, adiantado à mão para simular as próximas execuções.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// drain atende, em série, todo usuário vencido no relógio do worker.
func drain(t *testing.T, w *worker) {
	t.Helper()
	sc := &scratch{}
	for {
		cs, _, err := w.q.claim(context.Background(), w.now(), w.lease, claimBatch)
		if err != nil {
			t.Fatal(err)
		}
		if len(cs) == 0 {
			return
		}
		for _, c := range cs {
			w.user(context.Background(), c, sc)
		}
	}
}

func score(t *testing.T, rdb *redis.Client, user string) (time.Time, bool) {
	t.Helper()
	s, err := rdb.ZScore(context.Background(), dueKey, user).Result()
	if err == redis.Nil {
		return time.Time{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return time.UnixMilli(int64(s)), true
}

func TestWorker(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	now := time.UnixMilli(time.Now().UnixMilli())
	despawn := now.Add(10 * time.Minute)

	// 8 no raio de 1 km em volta de (0,0), um a 5,5 km, um que já sumiu e um sem reveal_code
	var near []string
	for i := 1; i <= 8; i++ {
		near = append(near, fmt.Sprintf(`{"encounter_id":"n%d","reveal_code":"rc%d","pokemon_name":"Dratini","cp":512,
			"pokemon_id":147,"form":"Alola","costume":12,
			"iv_percent":100,"despawn_at_ms":%d,"apx_lat":%f,"apx_long":0}`, i, i, despawn.UnixMilli(), 0.001*float64(i)))
	}
	near = append(near,
		`{"encounter_id":"longe","reveal_code":"x","despawn_at_ms":`+fmt.Sprint(despawn.UnixMilli())+`,"apx_lat":0.05,"apx_long":0}`,
		`{"encounter_id":"sumiu","reveal_code":"x","despawn_at_ms":`+fmt.Sprint(now.Add(-time.Second).UnixMilli())+`,"apx_lat":0,"apx_long":0}`,
		`{"encounter_id":"sem","apx_lat":0,"apx_long":0}`)
	lookupBody := `{"mode":"pokemon","results":[` + strings.Join(near, ",") + `]}`
	searchBody := `{"results":[{"encounter_id":"n1","reveal_code":"rc1","pokemon_name":"Dratini"},
		{"encounter_id":"w1","reveal_code":"rcw","pokemon_name":"Gible","pokemon_id":443,"form":null,"costume":null,
		"cp":900.5,"iv_percent":97.8,
		"despawn_at":"` + despawn.UTC().Format(time.RFC3339) + `","shiny":true}],"tier":"logged_in","cap":10}`

	var mu sync.Mutex
	calls := map[string]int{}
	var searchReq map[string]any
	up := http.NewServeMux()
	handle := func(body string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			mu.Lock()
			calls[tok]++
			if r.URL.Path == "/search" {
				_ = json.NewDecoder(r.Body).Decode(&searchReq)
			}
			mu.Unlock()
			if tok == "te" && r.URL.Path == "/search" { // filtro shiny sem treinador
				w.WriteHeader(403)
				_, _ = io.WriteString(w, `{"detail":{"error":"trainer_not_linked"}}`)
				return
			}
			switch tok {
			case "tc":
				w.WriteHeader(401)
			case "td":
				w.WriteHeader(429)
				_, _ = io.WriteString(w, `{"detail":{"error":"rate_limited","retry_after":300}}`)
			default:
				_, _ = io.WriteString(w, body)
			}
		}
	}
	up.HandleFunc("POST /map/lookup", handle(lookupBody))
	up.HandleFunc("POST /search", handle(searchBody))
	h := newHandler(t, pool, fakeUpstream(t, up))
	fcm := &fakeFCM{}
	ps := push.New(pool, fcm)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ps.Run(ctx)
	start := now.Add(5 * time.Second) // depois dos enqueues, que usam o relógio real
	clk := &clock{t: start}
	w := newWorker(h.svc, h.q, ps)
	w.now = clk.now
	rdb := h.q.rdb

	tok := func(s string) *string { return &s }
	a, b, c, d, e, f := newUser(t, pool), newUser(t, pool), newUser(t, pool), newUser(t, pool), newUser(t, pool), newUser(t, pool)
	withDevice(t, pool, a, tok("fcm-a"))
	withDevice(t, pool, b, nil) // sem token FCM: sai da fila sem chamar o PoGo
	withDevice(t, pool, c, tok("fcm-c"))
	withDevice(t, pool, d, tok("fcm-d"))
	withDevice(t, pool, e, tok("fcm-e"))
	withDevice(t, pool, f, tok("fcm-f")) // sem vínculo: entrou por um token FCM novo
	for u, tk := range map[string]string{a: "ta", b: "tb", c: "tc", d: "td"} {
		link(t, h, u, tk)
	}
	link(t, h, e, "te", `{"trainer_code":"111","active":true}`) // a regra de shiny exige treinador
	nearRule := `{"name":"Perto","filters":{"pokemon_ids":[147]},"scope":"near","lat":0,"lng":0,"radius_km":1}`
	r1 := addRule(t, h, a, nearRule) // cada regra ligada enfileira o usuário
	r2 := addRule(t, h, a, `{"name":"Mundo","filters":{"iv_min":100,"pokemon_ids":[147],"num_results":500},"scope":"world"}`)
	addRule(t, h, a, `{"name":"Desligada","filters":{},"scope":"world","enabled":false}`)
	for _, u := range []string{b, c, d} {
		addRule(t, h, u, nearRule)
	}
	shinyRule := addRule(t, h, e, `{"name":"Shiny","filters":{"shiny_only":true},"scope":"world"}`)
	addRule(t, h, e, nearRule)
	h.Enqueue(f)

	drain(t, w)
	got := fcm.take(t, 2)
	byUser := map[string]pushed{}
	for _, p := range got {
		byUser[p.user] = p
	}
	pa := byUser[a]
	if len(got) != 2 || pa.token != "fcm-a" || len(pa.msgs) != 6 || len(byUser[e].msgs) != 5 {
		t.Fatalf("avisos: %+v", got)
	}
	first := pa.msgs[0]
	want := map[string]string{"t": "p", "n": "Dratini", "iv": "100", "cp": "512", "d": fmt.Sprint(despawn.UnixMilli()),
		"r": "Perto", "rc": "rc1", "e": "n1", "id": "147", "f": "Alola", "c": "12"}
	if fmt.Sprint(first.Fields) != fmt.Sprint(want) || first.Collapse != "p:"+r1 || first.TTL != despawn.Sub(start) {
		t.Fatalf("aviso: %+v", first)
	}
	gible := pa.msgs[5]
	if _, f := gible.Fields["f"]; f || gible.Fields["id"] != "443" {
		t.Fatalf("forma null virou campo: %v", gible.Fields)
	}
	if gible.Fields["e"] != "w1" || gible.Fields["s"] != "1" || gible.Fields["cp"] != "900.5" ||
		gible.Fields["iv"] != "97.8" || gible.Fields["r"] != "Mundo" || gible.Collapse != "p:"+r2 {
		t.Fatalf("mundo: %+v", gible)
	}
	mu.Lock()
	if searchReq["iv_percent_min"] != 100.0 || searchReq["iv_min"] != nil || searchReq["iv_mode"] != "percent" ||
		searchReq["num_results"] != nil {
		t.Fatalf("filtros do /search: %v", searchReq)
	}
	if calls["ta"] != 2 || calls["tb"] != 0 || calls["tc"] != 1 || calls["td"] != 1 || calls["te"] != 2 {
		t.Fatalf("chamadas: %v", calls)
	}
	mu.Unlock()
	if linked(t, pool, c) || !linked(t, pool, d) {
		t.Fatal("401 não apagou o vínculo, ou o 429 apagou")
	}
	// o PoGo recusou o filtro shiny de e (o treinador saiu): só essa regra desliga
	var list struct{ Rules []Rule }
	_ = json.Unmarshal(serve(h, e, "GET", "/v1/pogo/rules", "").Body.Bytes(), &list)
	for _, r := range list.Rules {
		shiny := r.ID == shinyRule
		if r.Enabled == shiny || shiny != (r.DisabledReason != nil && *r.DisabledReason == "trainer_required") {
			t.Fatalf("regras de e: %+v", list.Rules)
		}
	}
	for u, want := range map[string]time.Duration{a: defaultInterval, e: defaultInterval, d: 300 * time.Second} {
		if s, ok := score(t, rdb, u); !ok || !s.Equal(start.Add(want)) {
			t.Errorf("reagendado em agora+%v, queria agora+%v", s.Sub(start), want)
		}
	}
	for _, u := range []string{b, c, f} {
		if _, ok := score(t, rdb, u); ok {
			t.Errorf("%s ficou na fila", u)
		}
	}
	if n := rdb.Exists(context.Background(), leaseKey(a), leaseKey(d)).Val(); n != 0 {
		t.Fatalf("%d leases sobraram", n)
	}

	// próxima execução: só os 3 que sobraram no raio, nada repetido; d ainda espera o retry_after
	clk.add(defaultInterval + time.Second)
	drain(t, w)
	for _, p := range fcm.take(t, 2) {
		if p.user == a {
			var es []string
			for _, m := range p.msgs {
				es = append(es, m.Fields["e"])
			}
			if strings.Join(es, ",") != "n6,n7,n8" {
				t.Fatalf("2ª execução de a: %v", es)
			}
		}
	}
	mu.Lock()
	if calls["td"] != 1 || calls["tc"] != 1 || calls["te"] != 3 { // a regra desligada de e não roda mais
		t.Fatalf("chamadas na 2ª execução: %v", calls)
	}
	mu.Unlock()

	clk.add(defaultInterval + time.Second) // tudo já avisado
	drain(t, w)
	time.Sleep(50 * time.Millisecond)
	if got := fcm.take(t, 0); len(got) != 0 {
		t.Fatalf("3ª execução: %+v", got)
	}

	// desligar as regras: sai na próxima execução; desvincular: sai na hora
	for _, id := range []string{r1, r2} {
		serve(h, a, "PUT", "/v1/pogo/rules/"+id, `{"name":"x","filters":{},"scope":"world","enabled":false}`)
	}
	clk.add(defaultInterval + time.Second)
	drain(t, w)
	if _, ok := score(t, rdb, a); ok {
		t.Fatal("sem regra ativa e ainda na fila")
	}
	if w := serve(h, e, "DELETE", "/v1/pogo/login", ""); w.Code != 204 {
		t.Fatal(w.Code)
	}
	if _, ok := score(t, rdb, e); ok {
		t.Fatal("desvincular não tirou da fila")
	}
}

func TestQueue(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	q, ctx := newQueue(rdb), context.Background()
	t0 := time.UnixMilli(time.Now().UnixMilli())
	users := []string{"u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8"}
	for _, u := range users {
		if err := q.enqueue(ctx, u, t0); err != nil {
			t.Fatal(err)
		}
	}

	// workers ao mesmo tempo nunca pegam o mesmo usuário
	var mu sync.Mutex
	var claimed []string
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				cs, _, err := q.claim(ctx, t0, time.Minute, 3)
				if err != nil || len(cs) == 0 {
					return
				}
				mu.Lock()
				for _, c := range cs {
					claimed = append(claimed, c.user)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	slices.Sort(claimed)
	if !slices.Equal(claimed, users) {
		t.Fatalf("reivindicados: %v", claimed)
	}
	if cs, next, _ := q.claim(ctx, t0, time.Minute, 4); len(cs) != 0 || !next.Equal(t0.Add(time.Minute)) {
		t.Fatalf("todos em lease: %v %v", cs, next)
	}

	// lease vencido volta; o worker antigo perde o fencing
	cs, _, _ := q.claim(ctx, t0.Add(2*time.Minute), time.Minute, 1)
	if len(cs) != 1 {
		t.Fatalf("lease vencido não voltou: %v", cs)
	}
	stolen := cs[0]
	old := claim{user: stolen.user, token: "velho"}
	if ok, _ := q.finish(ctx, old, t0.Add(time.Hour)); ok {
		t.Fatal("worker com lease vencido reagendou")
	}
	if _, err := q.markSeen(ctx, old, 5, t0, []string{"1 60000"}); err != errFenced {
		t.Fatalf("dedup sem fencing: %v", err)
	}
	if ok, _ := q.finish(ctx, stolen, t0.Add(5*time.Minute)); !ok {
		t.Fatal("dono do lease não reagendou")
	}
	if s, _ := score(t, rdb, stolen.user); !s.Equal(t0.Add(5 * time.Minute)) {
		t.Fatalf("score: %v", s)
	}

	// enqueue durante o processamento derruba o lease: o worker não tira da fila com dados velhos
	cs, _, _ = q.claim(ctx, t0.Add(10*time.Minute), time.Minute, 1)
	if len(cs) != 1 {
		t.Fatal("nada vencido")
	}
	_ = q.enqueue(ctx, cs[0].user, t0.Add(10*time.Minute))
	if ok, _ := q.finish(ctx, cs[0], time.Time{}); ok {
		t.Fatal("removeu depois de um enqueue")
	}
	if s, ok := score(t, rdb, cs[0].user); !ok || !s.Equal(t0.Add(10*time.Minute)) {
		t.Fatalf("enfileirado de novo não voltou vencido: %v", s)
	}

	// enqueue de quem já está agendado no futuro, sem lease, não adianta a execução
	_ = q.enqueue(ctx, "u9", t0)
	cs, _, _ = q.claim(ctx, t0, time.Minute, 1)
	if len(cs) != 1 || cs[0].user != "u9" {
		t.Fatalf("u9: %v", cs)
	}
	_, _ = q.finish(ctx, cs[0], t0.Add(time.Hour))
	_ = q.enqueue(ctx, "u9", t0)
	if s, _ := score(t, rdb, "u9"); !s.Equal(t0.Add(time.Hour)) {
		t.Fatalf("enqueue adiantou: %v", s)
	}

	// dedup: até o limite por regra, sem repetir entre regras
	cs, _, _ = q.claim(ctx, t0.Add(2*time.Hour), time.Minute, 1)
	u := cs[0].user
	picked, err := q.markSeen(ctx, cs[0], 2, t0, []string{"a 60000 b 60000 c 60000 ", "", "a 60000 d 60000"})
	if err != nil || fmt.Sprint(picked) != "[0 1 4]" {
		t.Fatalf("marcados: %v %v", picked, err)
	}
	if s := rdb.ZScore(ctx, seenKey(u), "a").Val(); s != float64(t0.Add(time.Minute).UnixMilli()) {
		t.Fatalf("expira em %v", s)
	}
	if ttl := rdb.PTTL(ctx, seenKey(u)).Val(); ttl <= maxSeen-time.Minute || ttl > maxSeen {
		t.Fatalf("ttl da chave: %v", ttl)
	}
	// passado o despawn, o item sai do ZSET e pode avisar de novo
	picked, _ = q.markSeen(ctx, cs[0], 5, t0.Add(2*time.Minute), []string{"a 60000 x 60000"})
	if fmt.Sprint(picked) != "[0 1]" || rdb.ZCard(ctx, seenKey(u)).Val() != 2 {
		t.Fatalf("depois do despawn: %v, %d no ZSET", picked, rdb.ZCard(ctx, seenKey(u)).Val())
	}
	if err := q.remove(ctx, cs[0].user); err != nil || rdb.Exists(ctx, leaseKey(cs[0].user)).Val() != 0 {
		t.Fatalf("remove: %v", err)
	}
}

func TestRebuild(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	h := newHandler(t, pool, "http://127.0.0.1:1")
	tok := func(s string) *string { return &s }
	full, noFCM, noLink, off := newUser(t, pool), newUser(t, pool), newUser(t, pool), newUser(t, pool)
	for u, f := range map[string]*string{full: tok("a-" + full), noFCM: nil, noLink: tok("b-" + noLink), off: tok("c-" + off)} {
		withDevice(t, pool, u, f)
	}
	for _, u := range []string{full, noFCM, off} {
		link(t, h, u, "t")
	}
	for _, u := range []string{full, noFCM, noLink} {
		addRule(t, h, u, `{"name":"a","filters":{},"scope":"world"}`)
	}
	addRule(t, h, off, `{"name":"a","filters":{},"scope":"world","enabled":false}`)
	rdb := h.q.rdb
	_ = rdb.Del(context.Background(), dueKey).Err() // Redis reiniciado sem persistência
	w := newWorker(h.svc, h.q, nil)
	w.rebuild(context.Background())
	w.rebuild(context.Background()) // idempotente
	if got := rdb.ZRange(context.Background(), dueKey, 0, -1).Val(); !slices.Equal(got, []string{full}) {
		t.Fatalf("fila reconstruída: %v", got)
	}
}

// Fila do push cheia: o aviso não sai e o Pokémon não fica marcado, para a próxima execução tentar de novo.
func TestPushQueueFull(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	up := http.NewServeMux()
	up.HandleFunc("POST /search", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"results":[{"encounter_id":"x1","reveal_code":"r","pokemon_name":"A"}]}`)
	})
	h := newHandler(t, pool, fakeUpstream(t, up))
	ps := push.New(pool, &fakeFCM{}) // sem Run: ninguém esvazia a fila
	for ps.TryPush("u", "t", nil) {
	}
	u := newUser(t, pool)
	fcm := "f-" + u
	withDevice(t, pool, u, &fcm)
	link(t, h, u, "t")
	addRule(t, h, u, `{"name":"a","filters":{},"scope":"world"}`)
	drain(t, newWorker(h.svc, h.q, ps))
	if err := h.q.rdb.ZScore(context.Background(), seenKey(u), "x1").Err(); err != redis.Nil {
		t.Fatal("ficou marcado como visto sem aviso")
	}
	if _, ok := score(t, h.q.rdb, u); !ok {
		t.Fatal("saiu da fila")
	}
}

// O Unmarshal reaproveita a fatia de rascunho sem zerar: o shiny (e o cp) de um item não podem vazar para o próximo.
func TestMatchesReuse(t *testing.T) {
	w, sc := &worker{}, &scratch{}
	r := &dueRule{Scope: "world"}
	now := time.Now()
	w.matches(r, []byte(`{"results":[{"encounter_id":"a","reveal_code":"r","shiny":true,"cp":10,"despawn_at_ms":1,
		"pokemon_id":25,"form":"Alola","costume":3}]}`), time.UnixMilli(0), sc)
	sc.cands = sc.cands[:0]
	w.matches(r, []byte(`{"results":[{"encounter_id":"b","reveal_code":"r"}]}`), now, sc)
	if len(sc.cands) != 1 || sc.cands[0].Shiny || sc.cands[0].CP != nil || sc.cands[0].DespawnMs != 0 {
		t.Fatalf("vazou: %+v", sc.cands)
	}
	d := w.data(r, &sc.cands[0], now)
	for _, k := range []string{"s", "id", "f", "c"} {
		if _, ok := d.Fields[k]; ok {
			t.Fatalf("%s vazou: %v", k, d.Fields)
		}
	}
	if d.Fields["cp"] != "" {
		t.Fatalf("aviso: %v", d.Fields)
	}
}

func TestFlexInt(t *testing.T) {
	var v struct{ A, B, C, D, E, F, G flexInt }
	if err := json.Unmarshal([]byte(`{"A":147,"B":"147","C":147.0,"D":null,"E":"x","F":-3,"G":99999999999999999999999}`),
		&v); err != nil {
		t.Fatal(err)
	}
	if v.A != 147 || v.B != 147 || v.C != 147 || v.D != 0 || v.E != 0 || v.F != 0 || v.G != 0 {
		t.Fatalf("%+v", v)
	}
}

func TestFlexID(t *testing.T) {
	var v struct{ A, B, C, D, E flexID }
	if err := json.Unmarshal([]byte(`{"A":"Alola","B":0,"C":null,"D":"Galar\u00e9 \"x\"","E":"3265749275115993354"}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.A != "Alola" || v.B != "0" || v.C != "" || v.D != `Galaré "x"` || v.E != "3265749275115993354" {
		t.Fatalf("%+v", v)
	}
}

// Um usuário com o PoGo travado não segura os outros: o pool segue com os workers livres.
func TestSlowUserDoesNotBlock(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	var fast atomic.Int32
	up := http.NewServeMux()
	up.HandleFunc("POST /search", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer lento" {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			return
		}
		fast.Add(1)
		_, _ = io.WriteString(w, `{"results":[]}`)
	})
	h := newHandler(t, pool, fakeUpstream(t, up))
	mk := func(tk string) {
		u := newUser(t, pool)
		fcm := "f-" + u
		withDevice(t, pool, u, &fcm)
		link(t, h, u, tk)
		addRule(t, h, u, `{"name":"a","filters":{},"scope":"world"}`)
	}
	mk("lento")
	for range 20 {
		mk("rapido")
	}
	w := newWorker(h.svc, h.q, push.New(pool, &fakeFCM{}))
	w.userWait = 3 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	go w.run(ctx)
	for fast.Load() < 20 {
		if time.Since(start) > 2*time.Second {
			t.Fatalf("só %d rápidos em 2 s com um usuário lento", fast.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("20 usuários rápidos em %v com um travado", time.Since(start))

	// um ZADD local acorda o dispatcher na hora, sem esperar o sono
	before := fast.Load()
	time.Sleep(1500 * time.Millisecond)
	t1 := time.Now()
	mk("rapido")
	for fast.Load() == before {
		if time.Since(t1) > 500*time.Millisecond {
			t.Fatal("novo usuário esperou o sono do dispatcher")
		}
		time.Sleep(time.Millisecond)
	}
}

// Uma chamada travada no PoGo só gasta o prazo dela: as outras regras do usuário rodam logo depois.
func TestCallWait(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	var second atomic.Int32
	up := http.NewServeMux()
	up.HandleFunc("POST /search", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if strings.Contains(string(b), "trava") {
			<-r.Context().Done()
			return
		}
		second.Add(1)
		_, _ = io.WriteString(w, `{"results":[]}`)
	})
	h := newHandler(t, pool, fakeUpstream(t, up))
	u := newUser(t, pool)
	fcm := "f-" + u
	withDevice(t, pool, u, &fcm)
	link(t, h, u, "t")
	addRule(t, h, u, `{"name":"a","filters":{"trava":true},"scope":"world"}`)
	addRule(t, h, u, `{"name":"b","filters":{},"scope":"world"}`)
	w := newWorker(h.svc, h.q, push.New(pool, &fakeFCM{}))
	w.callWait = 300 * time.Millisecond
	start := time.Now()
	drain(t, w)
	if took := time.Since(start); second.Load() != 1 || took > 2*time.Second {
		t.Fatalf("2ª regra: %d chamadas, usuário levou %v", second.Load(), took)
	}
}

func TestRuleBodyNear(t *testing.T) {
	lat, lng, r := 60.0, 179.99, 10.0
	b, err := (&dueRule{Scope: "near", Lat: &lat, Lng: &lng, RadiusKm: &r, Filters: json.RawMessage(`{"a":1}`)}).body()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	n, south, west, east := m["n_lat"].(float64), m["s_lat"].(float64), m["w_lng"].(float64), m["e_lng"].(float64)
	// o círculo de 10 km cabe na caixa: ~0,09° de latitude e ~0,18° de longitude a 60°
	if n < lat+0.089 || south > lat-0.089 || west > lng-0.179 || east != 180 || m["mode"] != "pokemon" ||
		m["num_results"] != 100.0 || fmt.Sprint(m["filters"]) != "map[a:1]" {
		t.Fatalf("caixa: %s", b)
	}
	if d := distKm(0, 0, 0.009, 0); d < 0.99 || d > 1.01 {
		t.Fatalf("distKm: %v", d)
	}
}
