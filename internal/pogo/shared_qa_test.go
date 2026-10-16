package pogo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayranoliveira1/pokelocation-server/internal/push"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

// qaUpstream: resposta por token ("status corpo", "close" derruba a conexão, "hang" só volta com o cancelamento);
// sem entrada = 200. Guarda "caminho token" de cada chamada.
type qaUpstream struct {
	mu    sync.Mutex
	reply map[string]string
	calls []string
	body  func(path string) string
}

func (u *qaUpstream) set(kv ...string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.reply = map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		u.reply[kv[i]] = kv[i+1]
	}
	u.calls = nil
}

func (u *qaUpstream) take() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := strings.Join(u.calls, ", ")
	u.calls = nil
	return s
}

func (u *qaUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	u.mu.Lock()
	u.calls = append(u.calls, r.URL.Path+" "+tok)
	rep := u.reply[tok]
	u.mu.Unlock()
	switch rep {
	case "close":
		c, _, _ := w.(http.Hijacker).Hijack()
		_ = c.Close()
		return
	case "hang":
		_, _ = io.Copy(io.Discard, r.Body) // sem ler o corpo o servidor não vê a conexão fechar
		<-r.Context().Done()
		return
	}
	if code, body, ok := strings.Cut(rep, " "); ok {
		n, _ := strconv.Atoi(code)
		if ra, rest, ok := strings.Cut(body, "|"); ok { // "Retry-After|corpo"
			w.Header().Set("Retry-After", ra)
			body = rest
		}
		w.WriteHeader(n)
		_, _ = io.WriteString(w, body)
		return
	}
	b := `{"results":[]}`
	if u.body != nil {
		b = u.body(r.URL.Path)
	}
	_, _ = io.WriteString(w, b)
}

func qaService(t *testing.T, tokens string) (*Service, *qaUpstream) {
	t.Helper()
	up := &qaUpstream{reply: map[string]string{}}
	mux := http.NewServeMux()
	mux.Handle("/", up)
	s, err := newService(Config{TokenKey: newKey(t), Upstream: fakeUpstream(t, mux), SharedTokens: tokens}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s, up
}

func cooling(s *Service, k int) (time.Duration, bool) {
	return time.Until(time.UnixMilli(s.shared[k].until.Load())), s.shared[k].limited.Load()
}

func reset(s *Service) {
	for i := range s.shared {
		s.shared[i].until.Store(0)
		s.shared[i].limited.Store(false)
	}
	s.next.Store(0)
}

func lookupShared(s *Service, ctx context.Context) error {
	buf := getBuf()
	defer putBuf(buf)
	_, err := s.callShared(ctx, "POST", "/map/lookup", []byte(`{"filters":{}}`), 1<<20, false, buf)
	return err
}

// 401/402/403 esfriam 5 min; 429 até o retry_after (corpo, senão header, senão 60 s). Os dois passam para o próximo.
func TestSharedCoolingQA(t *testing.T) {
	t.Parallel()
	s, up := qaService(t, "s1,s2")
	for _, c := range []struct {
		rep     string
		min     time.Duration
		max     time.Duration
		limited bool
	}{
		{`401 {"detail":"Invalid token"}`, refusedCool - 2*time.Second, refusedCool, false},
		{`402 {}`, refusedCool - 2*time.Second, refusedCool, false},
		{`403 {"detail":"Forbidden"}`, refusedCool - 2*time.Second, refusedCool, false},
		{`429 {"detail":{"error":"rate_limited","retry_after":120}}`, 118 * time.Second, 120 * time.Second, true},
		{`429 30|{"detail":"x"}`, 28 * time.Second, 30 * time.Second, true},
		{`429 <html>`, limitedCool - 2*time.Second, limitedCool, true},
	} {
		reset(s)
		up.set("s1", c.rep)
		if err := lookupShared(s, context.Background()); err != nil {
			t.Fatalf("%s: %v", c.rep, err)
		}
		if got := up.take(); got != "/map/lookup s1, /map/lookup s2" {
			t.Fatalf("%s: %s", c.rep, got)
		}
		d, lim := cooling(s, 0)
		if d < c.min || d > c.max || lim != c.limited {
			t.Errorf("%s: fora por %v, limited %v", c.rep, d, lim)
		}
		if d, _ := cooling(s, 1); d > 0 {
			t.Errorf("%s: o que serviu esfriou", c.rep)
		}
	}
}

// 400/404/422/5xx, 403 de filtro, conexão caída e timeout voltam na hora: nem passam para o próximo nem esfriam o token.
func TestSharedImmediateQA(t *testing.T) {
	t.Parallel()
	s, up := qaService(t, "s1,s2")
	for _, c := range []struct {
		rep    string
		status int
		err    error
	}{
		{`400 {"detail":"Invalid mode"}`, 400, nil},
		{`404 {}`, 404, nil},
		{`410 {}`, 410, nil},
		{`422 {"detail":[]}`, 422, nil},
		{`403 {"detail":{"error":"vip_required"}}`, 403, nil},
		{`403 {"detail":{"error":"trainer_not_linked"}}`, 403, nil},
		{`403 {"detail":{"error":"shiny_account_unavailable"}}`, 403, nil},
		{`500 {}`, 0, ErrUnavailable},
		{`503 {}`, 0, ErrUnavailable},
		{"close", 0, ErrUnavailable},
		{"hang", 0, ErrTimeout},
	} {
		reset(s)
		up.set("s1", c.rep)
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		err := lookupShared(s, ctx)
		cancel()
		var ue *UpstreamError
		if c.err != nil && !errors.Is(err, c.err) || c.err == nil && (!errors.As(err, &ue) || ue.Status != c.status) {
			t.Errorf("%s: %v", c.rep, err)
		}
		if got := up.take(); got != "/map/lookup s1" {
			t.Errorf("%s: chamadas %s", c.rep, got)
		}
		if d, _ := cooling(s, 0); d > 0 {
			t.Errorf("%s: esfriou %v", c.rep, d)
		}
	}
}

// Nenhum serviu e algum no limite: 429 com o menor tempo restante, sem chamar o PoGo.
func TestSharedRetryAfterQA(t *testing.T) {
	t.Parallel()
	s, up := qaService(t, "s1,s2,s3")
	s.shared[0].cool(50*time.Second, true)
	s.shared[1].cool(10*time.Second, true)
	s.shared[2].cool(refusedCool, false) // recusado não entra na conta
	var ue *UpstreamError
	if err := lookupShared(s, context.Background()); !errors.As(err, &ue) || ue.Status != 429 || ue.RetryAfter != 10 {
		t.Fatalf("todos esfriando: %v %+v", err, ue)
	}
	if got := up.take(); got != "" {
		t.Fatalf("chamou o PoGo: %s", got)
	}

	// um no limite com 10 s e o outro devolvendo 429 de 60 s agora: o app volta em 10 s, não 60
	reset(s)
	s.shared[0].cool(10*time.Second, true)
	up.set("s2", `429 {"detail":{"error":"rate_limited","retry_after":60}}`, "s3", `403 {}`)
	ue = nil
	if err := lookupShared(s, context.Background()); !errors.As(err, &ue) || ue.Status != 429 || ue.RetryAfter > 10 {
		t.Errorf("misto: %v, retry_after %d", err, ue.RetryAfter)
	}
	// 429 sem retry_after: o token fica 60 s fora, o app tem de saber disso
	reset(s)
	up.set("s1", `429 {}`, "s2", `429 {}`, "s3", `429 {}`)
	ue = nil
	if err := lookupShared(s, context.Background()); !errors.As(err, &ue) || ue.Status != 429 || ue.RetryAfter != 60 {
		t.Errorf("429 sem retry_after: %v, retry_after %d", err, ue.RetryAfter)
	}
	// dois 429 na mesma passada: vale o menor, em qualquer ordem
	for _, order := range [][2]string{{"120", "30"}, {"30", "120"}} {
		reset(s)
		up.set("s1", `429 {"detail":{"retry_after":`+order[0]+`}}`, "s2", `429 {"detail":{"retry_after":`+order[1]+`}}`,
			"s3", `403 {}`)
		ue = nil
		if err := lookupShared(s, context.Background()); !errors.As(err, &ue) || ue.RetryAfter != 30 {
			t.Errorf("%v: %v", order, err)
		}
	}
}

// O log da recusa leva só a posição e o status, nunca o token. Sem t.Parallel: troca o slog global.
func TestSharedLogQA(t *testing.T) {
	var out bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, nil)))
	defer slog.SetDefault(old)
	s, up := qaService(t, "tok-servidor-AAA,tok-servidor-BBB")
	up.set("tok-servidor-AAA", `401 {"detail":"Invalid token"}`)
	if err := lookupShared(s, context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "tok-servidor") {
		t.Fatalf("token no log: %s", out.String())
	}
	var rec map[string]any
	for line := range strings.Lines(out.String()) {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "pogo: token do servidor recusado" {
			rec = m
		}
	}
	delete(rec, "time")
	delete(rec, "level")
	if fmt.Sprint(rec) != "map[msg:pogo: token do servidor recusado pos:0 status:401]" {
		t.Fatalf("log: %v", rec)
	}
}

// Rotas que dependem da conta (me, login, trainer*, shiny em todas as formas) nunca usam token do servidor; recusa
// geral sem vínculo dá 502 sem o detail do servidor; com vínculo, o 401 do token do usuário apaga o vínculo.
func TestSharedRoutesQA(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	up := &qaUpstream{reply: map[string]string{}, body: func(path string) string {
		switch path {
		case "/auth/google/callback":
			return `{"token":"tnovo"}`
		case "/users/me":
			return `{"display_name":"N","trainers":[{"trainer_code":"111"}]}`
		case "/trainer/confirm":
			return `{"accounts":[{"trainer_code":"111"}]}`
		}
		return `{"results":[]}`
	}}
	mux := http.NewServeMux()
	mux.Handle("/", up)
	h, err := New(Config{TokenKey: newKey(t), Upstream: fakeUpstream(t, mux), SharedTokens: "s1,s2,s3"}, pool, testdb.Redis(t))
	if err != nil {
		t.Fatal(err)
	}
	free, linkedU := newUser(t, pool), newUser(t, pool)
	link(t, h, linkedU, "tu", `{"trainer_code":"111"}`)

	own := []struct{ method, path, body string }{
		{"GET", "/v1/pogo/me", ""},
		{"GET", "/v1/pogo/trainer", ""},
		{"POST", "/v1/pogo/trainer/lookup", `{"trainer_code":"111111111111"}`},
		{"POST", "/v1/pogo/trainer/confirm", `{"trainer_code":"111111111111"}`},
		{"POST", "/v1/pogo/search", `{"shiny_only":true}`},
		{"POST", "/v1/pogo/search", `{"background_only":true}`},
		{"POST", "/v1/pogo/search", `{"shiny_account":"111"}`},
		{"POST", "/v1/pogo/lookup", `{"mode":"pokemon","filters":{"shiny_only":true}}`},
		{"POST", "/v1/pogo/lookup", `{"mode":"pokemon","filters":{"background_only":true}}`},
		{"POST", "/v1/pogo/lookup", `{"mode":"pokemon","filters":{"shiny_account":"111"}}`},
	}
	for _, c := range own {
		wantError(t, serve(h, free, c.method, c.path, c.body), 409, "pogo_relogin")
		if w := serve(h, linkedU, c.method, c.path, c.body); w.Code != 200 {
			t.Fatalf("%s %s %s: %d %s", c.method, c.path, c.body, w.Code, w.Body.String())
		}
		if got := up.take(); !strings.HasSuffix(got, " tu") || strings.Contains(got, ",") {
			t.Fatalf("%s %s %s: chamadas %s", c.method, c.path, c.body, got)
		}
	}
	if w := serve(h, free, "POST", "/v1/pogo/login", `{"provider":"google","code":"c"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if got := up.take(); got != "/auth/google/callback , /users/me tnovo" {
		t.Fatalf("login: %s", got)
	}
	if _, err := h.svc.store.account(context.Background(), free); err != nil {
		t.Fatal(err)
	}
	if w := serve(h, free, "DELETE", "/v1/pogo/login", ""); w.Code != 204 {
		t.Fatal(w.Code)
	}

	// tudo recusado, sem vínculo: 502 seco, sem o detail da conta do servidor
	up.set("s1", `401 {"detail":"Invalid token"}`, "s2", `402 {"detail":{"error":"segredo-402"}}`,
		"s3", `403 {"detail":{"error":"segredo-403"}}`)
	w := serve(h, free, "POST", "/v1/pogo/search", `{"pokemon_ids":[1]}`)
	if w.Code != 502 || strings.TrimSpace(w.Body.String()) != `{"error":"upstream_unavailable"}` {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	up.take()
	// com vínculo cai no token do usuário; o 401 dele é dele: vínculo apagado e pogo_relogin
	up.set("tu", `401 {"detail":"Invalid token"}`)
	reset(h.svc)
	h.svc.shared[0].cool(refusedCool, false)
	h.svc.shared[1].cool(refusedCool, false)
	h.svc.shared[2].cool(refusedCool, false)
	wantError(t, serve(h, linkedU, "GET", "/v1/pogo/reveal/abc", ""), 409, "pogo_relogin")
	if got := up.take(); got != "/reveal/abc tu" {
		t.Fatalf("fallback: %s", got)
	}
	if linked(t, pool, linkedU) {
		t.Fatal("401 do token do usuário não apagou o vínculo")
	}
	wantError(t, serve(h, linkedU, "GET", "/v1/pogo/reveal/abc", ""), 502, "upstream_unavailable")
}

// Worker: shiny com vínculo vai pelo token do usuário e o resto pelo servidor; 401 do servidor cai no token do
// usuário sem apagar o vínculo; 429 do servidor reagenda pelo retry_after sem tocar o token do usuário.
func TestSharedWorkerQA(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	now := time.UnixMilli(time.Now().UnixMilli())
	up := &qaUpstream{reply: map[string]string{}, body: func(string) string {
		return fmt.Sprintf(`{"results":[{"encounter_id":"e1","reveal_code":"rc","pokemon_id":147,"despawn_at_ms":%d,
			"apx_lat":0,"apx_long":0}]}`, now.Add(10*time.Minute).UnixMilli())
	}}
	mux := http.NewServeMux()
	mux.Handle("/", up)
	h, err := New(Config{TokenKey: newKey(t), Upstream: fakeUpstream(t, mux), SharedTokens: "s1"}, pool, testdb.Redis(t))
	if err != nil {
		t.Fatal(err)
	}
	ps := push.New(pool, &fakeFCM{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ps.Run(ctx)
	clk := &clock{t: now.Add(5 * time.Second)}
	w := newWorker(h.svc, h.q, ps)
	w.now = clk.now

	v := newUser(t, pool)
	tok := "fcm-v"
	withDevice(t, pool, v, &tok)
	link(t, h, v, "tv", `{"trainer_code":"111"}`)
	addRule(t, h, v, `{"name":"Perto","filters":{"pokemon_ids":[147]},"scope":"near","lat":0,"lng":0,"radius_km":1}`)
	addRule(t, h, v, `{"name":"Shiny","filters":{"shiny_only":true},"scope":"world"}`)
	drain(t, w)
	if got := up.take(); got != "/map/lookup s1, /search tv" {
		t.Fatalf("chamadas: %s", got)
	}

	up.set("s1", `401 {"detail":"Invalid token"}`)
	clk.add(w.interval)
	drain(t, w)
	if got := up.take(); got != "/map/lookup s1, /map/lookup tv, /search tv" {
		t.Fatalf("401 do servidor: %s", got)
	}
	if !linked(t, pool, v) {
		t.Fatal("401 do servidor apagou o vínculo")
	}

	reset(h.svc)
	up.set("s1", `429 {"detail":{"error":"rate_limited","retry_after":600}}`)
	clk.add(w.interval)
	drain(t, w)
	if got := up.take(); got != "/map/lookup s1" {
		t.Fatalf("429 do servidor: %s", got)
	}
	if at, ok := score(t, h.q.rdb, v); !ok || at.Before(clk.now().Add(599*time.Second)) {
		t.Fatalf("reagendado para %v (agora %v)", at, clk.now())
	}
	var off int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pogo_rules WHERE user_id = $1 AND NOT enabled`,
		v).Scan(&off); err != nil || off != 0 {
		t.Fatalf("regras desligadas: %d %v", off, err)
	}
}
