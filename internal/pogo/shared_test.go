package pogo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayranoliveira1/pokelocation-server/internal/push"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestSharedConfig(t *testing.T) {
	s, err := newService(Config{TokenKey: newKey(t), SharedTokens: " a, ,b ,"}, nil)
	if err != nil || len(s.shared) != 2 || s.shared[0].token != "a" || s.shared[1].token != "b" {
		t.Fatalf("%v %v", s, err)
	}
	for _, bad := range []string{"a,b c", "a,\x01", strings.Repeat("x", maxToken+1)} {
		if _, err := newService(Config{TokenKey: newKey(t), SharedTokens: bad}, nil); err == nil {
			t.Errorf("%q aceito", bad)
		}
	}
	if _, err := New(Config{SharedTokens: "a"}, nil, nil); err == nil {
		t.Error("POGO_SHARED_TOKENS sem POGO_TOKEN_KEY aceito")
	}
}

func TestWantsShiny(t *testing.T) {
	for body, want := range map[string]bool{
		``:                           false,
		`{"pokemon_ids":[1]}`:        false,
		`{"filters":{"iv_min":100}}`: false,
		`{"shiny_only":false,"shiny_account":""}`: false,
		`{"shiny_only":true}`:                     true,
		`{"background_only":true}`:                true,
		`{"filters":{"shiny_account":"111"}}`:     true,
		`{"shiny_only":"sim"}`:                    true, // não lê: vai com o token do usuário
	} {
		if got := wantsShiny([]byte(body)); got != want {
			t.Errorf("%s: %v", body, got)
		}
	}
}

func TestShared(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	var mu sync.Mutex
	var seen []string
	limited := false
	up := http.NewServeMux()
	handle := func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		seen = append(seen, tok)
		lim := limited
		mu.Unlock()
		switch {
		case tok == "s2":
			w.WriteHeader(401)
		case lim:
			w.WriteHeader(429)
			_, _ = io.WriteString(w, `{"detail":{"error":"rate_limited","retry_after":60}}`)
		default:
			_, _ = io.WriteString(w, `{"results":[]}`)
		}
	}
	up.HandleFunc("POST /map/lookup", handle)
	up.HandleFunc("POST /search", handle)
	up.HandleFunc("GET /reveal/{code}", handle)
	up.HandleFunc("GET /trainer", handle)
	h, err := New(Config{TokenKey: newKey(t), Upstream: fakeUpstream(t, up), SharedTokens: "s1,s2,s3"}, pool, testdb.Redis(t))
	if err != nil {
		t.Fatal(err)
	}
	take := func() []string {
		mu.Lock()
		defer mu.Unlock()
		s := seen
		seen = nil
		return s
	}

	free, linkedU := newUser(t, pool), newUser(t, pool)
	link(t, h, linkedU, "tu", `{"trainer_code":"111"}`)

	// sem vínculo: rodízio a partir do próximo; o s2 recusado passa para o seguinte e não vira pogo_relogin
	for range 3 {
		if w := serve(h, free, "POST", "/v1/pogo/lookup", `{"filters":{"pokemon_ids":[1]}}`); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if w := serve(h, free, "GET", "/v1/pogo/reveal/abc", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if got := strings.Join(take(), " "); got != "s1 s2 s3 s3 s1" {
		t.Fatalf("ordem: %s", got)
	}

	// shiny e /trainer: só com o token do usuário
	wantError(t, serve(h, free, "POST", "/v1/pogo/search", `{"shiny_only":true}`), 409, "pogo_relogin")
	if w := serve(h, linkedU, "POST", "/v1/pogo/search", `{"shiny_only":true}`); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := serve(h, linkedU, "GET", "/v1/pogo/trainer", ""); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if got := strings.Join(take(), " "); got != "tu tu" {
		t.Fatalf("token do usuário: %s", got)
	}

	// todos no limite: o 429 chega ao app depois de passar por todos (o s2, recusado, esfria e é pulado)
	mu.Lock()
	limited = true
	mu.Unlock()
	w := serve(h, linkedU, "POST", "/v1/pogo/search", `{"pokemon_ids":[1]}`)
	if got := strings.Join(take(), " "); w.Code != 429 || w.Header().Get("Retry-After") != "60" || got != "s1 s3" && got != "s3 s1" {
		t.Fatal(w.Code, w.Body.String())
	}
	if !linked(t, pool, linkedU) {
		t.Fatal("token do servidor recusado apagou o vínculo")
	}
}

// Sem vínculo, a regra sem shiny roda pelos tokens do servidor; desvincular não tira da fila.
func TestSharedWorker(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	now := time.UnixMilli(time.Now().UnixMilli())
	var mu sync.Mutex
	var seen []string
	up := http.NewServeMux()
	up.HandleFunc("POST /map/lookup", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"mode":"pokemon","results":[{"encounter_id":"e1","reveal_code":"rc","pokemon_id":147,
			"despawn_at_ms":%d,"apx_lat":0,"apx_long":0}]}`, now.Add(10*time.Minute).UnixMilli())
	})
	h, err := New(Config{TokenKey: newKey(t), Upstream: fakeUpstream(t, up), SharedTokens: "s1"}, pool, testdb.Redis(t))
	if err != nil {
		t.Fatal(err)
	}
	fcm := &fakeFCM{}
	ps := push.New(pool, fcm)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ps.Run(ctx)
	clk := &clock{t: now.Add(5 * time.Second)}
	w := newWorker(h.svc, h.q, ps)
	w.now = clk.now

	u, v := newUser(t, pool), newUser(t, pool)
	tok := "fcm-u"
	withDevice(t, pool, u, &tok)
	tokV := "fcm-v"
	withDevice(t, pool, v, &tokV)
	link(t, h, v, "tv", `{"trainer_code":"111"}`)
	nearRule := `{"name":"Perto","filters":{"pokemon_ids":[147]},"scope":"near","lat":0,"lng":0,"radius_km":1}`
	addRule(t, h, u, nearRule)
	addRule(t, h, v, `{"name":"Shiny","filters":{"shiny_only":true},"scope":"near","lat":0,"lng":0,"radius_km":1}`)
	if w := serve(h, v, "DELETE", "/v1/pogo/login", ""); w.Code != 204 {
		t.Fatal(w.Code)
	}
	if _, ok := score(t, h.q.rdb, v); !ok {
		t.Fatal("desvincular tirou da fila")
	}

	drain(t, w)
	got := fcm.take(t, 1)
	if len(got) != 1 || got[0].user != u || got[0].msgs[0].Fields["e"] != "e1" {
		t.Fatalf("avisos: %+v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(seen) != "[Bearer s1]" {
		t.Fatalf("chamadas: %v", seen)
	}
	if _, ok := score(t, h.q.rdb, u); !ok {
		t.Fatal("u saiu da fila")
	}
	if _, ok := score(t, h.q.rdb, v); ok {
		t.Fatal("só shiny sem vínculo e ainda na fila")
	}
}

// 401/402/403 de um token do servidor passam para o próximo; todos recusados viram 502 sem vínculo e o token do
// usuário com vínculo. 429 e erro do PoGo não caem no token do usuário; filtro inválido volta na hora.
func TestSharedRefused(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	var mu sync.Mutex
	var seen []string
	reply := map[string]string{} // token → "status corpo"; sem entrada = 200
	up := http.NewServeMux()
	up.HandleFunc("POST /map/lookup", func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		seen = append(seen, tok)
		rep := reply[tok]
		mu.Unlock()
		if code, body, ok := strings.Cut(rep, " "); ok {
			n, _ := strconv.Atoi(code)
			w.WriteHeader(n)
			_, _ = io.WriteString(w, body)
			return
		}
		_, _ = io.WriteString(w, `{"results":[]}`)
	})
	h, err := New(Config{TokenKey: newKey(t), Upstream: fakeUpstream(t, up), SharedTokens: "s1,s2,s3"}, pool, testdb.Redis(t))
	if err != nil {
		t.Fatal(err)
	}
	set := func(s1, s2, s3 string) {
		mu.Lock()
		defer mu.Unlock()
		reply["s1"], reply["s2"], reply["s3"] = s1, s2, s3
		seen = nil
		for i := range h.svc.shared {
			h.svc.shared[i].until.Store(0)
		}
	}
	take := func() string {
		mu.Lock()
		defer mu.Unlock()
		s := strings.Join(seen, " ")
		seen = nil
		return s
	}
	free, linkedU := newUser(t, pool), newUser(t, pool)
	link(t, h, linkedU, "tu")
	lookup := func(user string) *httptest.ResponseRecorder {
		return serve(h, user, "POST", "/v1/pogo/lookup", `{"filters":{"pokemon_ids":[1]}}`)
	}

	set(`401 {}`, `402 {"detail":{"error":"subscription_required"}}`, `403 {"detail":{"error":"banned"}}`)
	wantError(t, lookup(free), 502, "upstream_unavailable")
	if got := take(); len(strings.Fields(got)) != 3 || strings.Contains(got, "tu") {
		t.Fatalf("sem vínculo: %s", got)
	}
	// recusados esfriam: nem o sem vínculo nem o com vínculo voltam a tocá-los
	wantError(t, lookup(free), 502, "upstream_unavailable")
	if w := lookup(linkedU); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if got := take(); got != "tu" {
		t.Fatalf("esfriando: %s", got)
	}

	set(`429 {"detail":{"error":"rate_limited","retry_after":60}}`, `402 {}`, `403 {}`)
	if w := lookup(linkedU); w.Code != 429 || w.Header().Get("Retry-After") != "60" {
		t.Fatal(w.Code, w.Body.String())
	}
	if got := take(); strings.Contains(got, "tu") {
		t.Fatalf("429 caiu no token do usuário: %s", got)
	}
	// no limite: 429 na hora com o tempo que falta, sem ir ao PoGo
	if w := lookup(free); w.Code != 429 || w.Header().Get("Retry-After") != "60" && w.Header().Get("Retry-After") != "59" {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	if got := take(); got != "" {
		t.Fatalf("token no limite chamado: %s", got)
	}

	set(`500 {}`, `500 {}`, `500 {}`)
	wantError(t, lookup(linkedU), 502, "upstream_unavailable")
	if got := take(); got != "s1" && got != "s2" && got != "s3" {
		t.Fatalf("PoGo fora: %s", got)
	}

	set(`422 {"detail":"x"}`, `422 {"detail":"x"}`, `422 {"detail":"x"}`)
	if w := lookup(linkedU); w.Code != 422 {
		t.Fatal(w.Code, w.Body.String())
	}
	if got := take(); strings.Contains(got, " ") {
		t.Fatalf("filtro inválido passou para o próximo: %s", got)
	}
	if !linked(t, pool, linkedU) {
		t.Fatal("token do servidor recusado apagou o vínculo")
	}
}

// Servidor todo recusado no worker: com vínculo vai pelo token do usuário, e o segundo usuário já não toca os tokens
// recusados. O 403 trainer_not_linked vindo do servidor não desliga a regra.
func TestSharedWorkerRefused(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	now := time.UnixMilli(time.Now().UnixMilli())
	var mu sync.Mutex
	var seen []string
	filter := false
	up := http.NewServeMux()
	up.HandleFunc("POST /map/lookup", func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		seen = append(seen, tok)
		f := filter
		mu.Unlock()
		switch {
		case f && tok != "tv":
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"detail":{"error":"trainer_not_linked"}}`)
		case tok == "s1":
			w.WriteHeader(402)
		case tok == "s2":
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"detail":"Forbidden"}`)
		default:
			_, _ = fmt.Fprintf(w, `{"mode":"pokemon","results":[{"encounter_id":"e1","reveal_code":"rc","pokemon_id":147,
				"despawn_at_ms":%d,"apx_lat":0,"apx_long":0}]}`, now.Add(10*time.Minute).UnixMilli())
		}
	})
	h, err := New(Config{TokenKey: newKey(t), Upstream: fakeUpstream(t, up), SharedTokens: "s1,s2"}, pool, testdb.Redis(t))
	if err != nil {
		t.Fatal(err)
	}
	fcm := &fakeFCM{}
	ps := push.New(pool, fcm)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ps.Run(ctx)
	w := newWorker(h.svc, h.q, ps)
	clk := &clock{t: now.Add(5 * time.Second)}
	w.now = clk.now

	u, v := newUser(t, pool), newUser(t, pool)
	tokU, tokV := "fcm-u", "fcm-v"
	withDevice(t, pool, u, &tokU)
	withDevice(t, pool, v, &tokV)
	link(t, h, v, "tv")
	rule := `{"name":"Perto","filters":{"pokemon_ids":[147]},"scope":"near","lat":0,"lng":0,"radius_km":1}`
	addRule(t, h, u, rule)
	addRule(t, h, v, rule)

	drain(t, w)
	got := fcm.take(t, 1)
	if len(got) != 1 || got[0].user != v {
		t.Fatalf("avisos: %+v", got)
	}
	mu.Lock()
	calls := strings.Join(seen, " ")
	mu.Unlock()
	if n := strings.Count(calls, "s1"); n != 1 || strings.Count(calls, "s2") != 1 || strings.Count(calls, "tv") != 1 {
		t.Fatalf("chamadas: %s", calls)
	}
	if _, ok := score(t, h.q.rdb, u); !ok {
		t.Fatal("u saiu da fila")
	}

	mu.Lock()
	filter, seen = true, nil
	mu.Unlock()
	for i := range h.svc.shared {
		h.svc.shared[i].until.Store(0)
	}
	clk.add(w.interval)
	drain(t, w)
	mu.Lock()
	calls = strings.Join(seen, " ")
	mu.Unlock()
	if strings.Count(calls, "s") != 2 || strings.Contains(calls, "tv") {
		t.Fatalf("403 de filtro: %s", calls)
	}
	var off int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pogo_rules WHERE NOT enabled AND user_id IN ($1, $2)`,
		u, v).Scan(&off); err != nil || off != 0 {
		t.Fatalf("regras desligadas: %d %v", off, err)
	}
}
