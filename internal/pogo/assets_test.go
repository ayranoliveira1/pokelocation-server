package pogo

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/ratelimit"
)

// sem banco: o catálogo e os ícones não tocam no Postgres nem no Redis
func assetsHandler(t *testing.T, up string) *Handler {
	t.Helper()
	h, err := New(Config{TokenKey: newKey(t), Upstream: up}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, s := range h.assets.icons {
		s.base = up + "/" + k + "/"
		h.assets.icons[k] = s
	}
	return h
}

func get(h *Handler, user, path string, hdr ...string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	h.Routes(mux, func(next http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next(w, r.WithContext(httpx.WithUser(r.Context(), user)))
		})
	})
	r := httptest.NewRequest("GET", path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		r.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestCatalog(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	var mu sync.Mutex
	body, status := `[{"pokemon_id":25,"name":"Pikachu"}]`, 200
	up := http.NewServeMux()
	up.HandleFunc("GET /pokemon/catalog", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.RawQuery != "costumes=true" || r.Header.Get("Authorization") != "" {
			t.Errorf("pedido ao PoGo: %q %q", r.URL.RawQuery, r.Header.Get("Authorization"))
		}
		time.Sleep(100 * time.Millisecond) // os pedidos simultâneos chegam todos durante a ida
		mu.Lock()
		defer mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
	h := assetsHandler(t, fakeUpstream(t, up))
	var users atomic.Int32
	user := func() string { return strconv.Itoa(int(users.Add(1))) } // sem esbarrar no limite por usuário

	var wg sync.WaitGroup
	codes := make([]int, 20)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = get(h, user(), "/v1/pogo/catalog").Code
		}()
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("idas ao PoGo: %d", n)
	}
	for _, c := range codes {
		if c != 200 {
			t.Fatalf("respostas: %v", codes)
		}
	}

	w := get(h, user(), "/v1/pogo/catalog", "Accept-Encoding", "gzip")
	zr, err := gzip.NewReader(w.Body)
	if err != nil || w.Header().Get("Content-Encoding") != "gzip" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("gzip: %v %v", err, w.Header())
	}
	if got, _ := io.ReadAll(zr); string(got) != body {
		t.Fatalf("corpo: %s", got)
	}
	plain := get(h, user(), "/v1/pogo/catalog")
	etag := plain.Header().Get("ETag")
	if plain.Body.String() != body || etag == "" || plain.Header().Get("Content-Encoding") != "" ||
		plain.Header().Get("Vary") != "Accept-Encoding" {
		t.Fatalf("sem gzip: %v %s", plain.Header(), plain.Body)
	}
	if w := get(h, user(), "/v1/pogo/catalog", "If-None-Match", etag); w.Code != 304 || w.Body.Len() != 0 {
		t.Fatalf("If-None-Match: %d %s", w.Code, w.Body)
	}
	if calls.Load() != 1 {
		t.Fatalf("o cache não segurou: %d idas", calls.Load())
	}

	// vencido: responde o guardado na hora e atualiza por trás
	mu.Lock()
	body = `[{"pokemon_id":1}]`
	mu.Unlock()
	h.assets.mu.Lock()
	h.assets.cat.at = h.assets.cat.at.Add(-catalogTTL)
	h.assets.mu.Unlock()
	if w := get(h, user(), "/v1/pogo/catalog"); w.Body.String() != `[{"pokemon_id":25,"name":"Pikachu"}]` {
		t.Fatalf("vencido não respondeu o guardado: %s", w.Body)
	}
	for deadline := time.Now().Add(5 * time.Second); get(h, user(), "/v1/pogo/catalog").Body.String() != body; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("não atualizou")
		}
	}

	// PoGo fora com o guardado vencido: segue o guardado
	mu.Lock()
	status = 500
	mu.Unlock()
	h.assets.mu.Lock()
	h.assets.cat.at = h.assets.cat.at.Add(-catalogTTL)
	h.assets.mu.Unlock()
	before := calls.Load()
	for range 3 {
		if w := get(h, user(), "/v1/pogo/catalog"); w.Code != 200 || w.Body.String() != body {
			t.Fatalf("PoGo fora: %d %s", w.Code, w.Body)
		}
		time.Sleep(150 * time.Millisecond)
	}
	if n := calls.Load() - before; n != 1 {
		t.Fatalf("PoGo fora: %d idas, queria 1 (de novo só em 1 min)", n)
	}
}

// Sem nada guardado, qualquer falha do PoGo é 502: o 4xx deles não é do usuário.
func TestCatalogDown(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		status int
		body   string
	}{{200, `não é json`}, {429, `{"detail":{"error":"rate_limited","retry_after":30}}`}, {404, `{"detail":"Not Found"}`}} {
		up := http.NewServeMux()
		up.HandleFunc("GET /pokemon/catalog", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.status)
			_, _ = io.WriteString(w, c.body)
		})
		h := assetsHandler(t, fakeUpstream(t, up))
		if w := get(h, "u", "/v1/pogo/catalog"); w.Code != 502 || !strings.Contains(w.Body.String(), "upstream_unavailable") {
			t.Errorf("%d: %d %s", c.status, w.Code, w.Body)
		}
	}
}

var png = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{1}, 100)...)

func TestIcon(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	calls := map[string]int{}
	up := http.NewServeMux()
	up.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		mu.Unlock()
		switch r.URL.Path {
		case "/pm/pm25.icon.png", "/u/25.png", "/pm/pm19.fALOLA.s.icon.png", "/pm/pm25.cHALLOWEEN_2017.icon.png", "/it/1301.png":
			_, _ = w.Write(png)
		case "/pm/pm1.icon.png":
			_, _ = io.WriteString(w, "<html>")
		case "/pm/pm2.icon.png":
			_, _ = w.Write(append(png, make([]byte, maxIcon)...))
		case "/pm/pm3.icon.png":
			w.WriteHeader(500)
		case "/pm/pm4.icon.png":
			http.Redirect(w, r, "/pm/pm25.icon.png", http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	})
	h := assetsHandler(t, fakeUpstream(t, up))

	for range 2 {
		w := get(h, "u", "/v1/pogo/icon/pm/pm25.icon.png")
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), png) || w.Header().Get("Content-Type") != "image/png" ||
			w.Header().Get("Cache-Control") != "private, max-age=31536000, immutable" || w.Header().Get("ETag") == "" {
			t.Fatalf("ícone: %d %v", w.Code, w.Header())
		}
	}
	etag := get(h, "u", "/v1/pogo/icon/pm/pm25.icon.png").Header().Get("ETag")
	if w := get(h, "u", "/v1/pogo/icon/pm/pm25.icon.png", "If-None-Match", etag); w.Code != 304 {
		t.Fatalf("If-None-Match: %d", w.Code)
	}
	for _, p := range []string{"pm/pm19.fALOLA.s.icon.png", "pm/pm25.cHALLOWEEN_2017.icon.png", "u/25.png", "it/1301.png", "it/1301.png"} {
		if w := get(h, "u", "/v1/pogo/icon/"+p); w.Code != 200 || !bytes.Equal(w.Body.Bytes(), png) {
			t.Fatalf("%s: %d", p, w.Code)
		}
	}
	for range 2 { // 404 também fica guardado
		if w := get(h, "u", "/v1/pogo/icon/pm/pm25.s.icon.png"); w.Code != 404 || !strings.Contains(w.Body.String(), "not_found") {
			t.Fatalf("404: %d %s", w.Code, w.Body)
		}
	}
	for _, p := range []string{"pm1", "pm2", "pm3", "pm4"} { // não é PNG, grande demais, 500, redirect
		for range 2 {
			if w := get(h, "u", "/v1/pogo/icon/pm/"+p+".icon.png"); w.Code != 502 {
				t.Fatalf("%s: %d", p, w.Code)
			}
		}
	}
	mu.Lock()
	for p, want := range map[string]int{"/pm/pm25.icon.png": 1, "/pm/pm25.s.icon.png": 1, "/pm/pm3.icon.png": 2, "/it/1301.png": 1} {
		if calls[p] != want {
			t.Errorf("%s: %d idas, queria %d", p, calls[p], want)
		}
	}
	mu.Unlock()

	for _, p := range []string{"pm/pm25.svg", "pm/x.icon.png", "pm/pm25.cHALLO-WEEN.icon.png", "pm/pm25.icon.png.exe",
		"pm/%2E%2E", "pm/pm25.c..icon.png", "u/pm25.icon.png", "gh/25.png", "u/25.png%3Fx", "pm/pm12345.icon.png",
		"it/1302.png", "it/..%2Fx", "it/pm25.icon.png"} {
		if w := get(h, "u", "/v1/pogo/icon/"+p); w.Code != 400 {
			t.Errorf("%s: %d, queria 400", p, w.Code)
		}
	}
	mu.Lock()
	if len(calls) != 10 {
		t.Errorf("nome inválido chegou ao upstream: %v", calls)
	}
	mu.Unlock()
}

// O limite por usuário conta só o que vai ao GitHub: o seletor pede centenas do cache em rajada.
func TestIconLimitOnlyMisses(t *testing.T) {
	t.Parallel()
	up := http.NewServeMux()
	up.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(png) })
	h := assetsHandler(t, fakeUpstream(t, up))
	for range iconPerMin + 10 {
		if w := get(h, "u", "/v1/pogo/icon/u/25.png"); w.Code != 200 {
			t.Fatalf("hit do cache limitado: %d", w.Code)
		}
	}
	limited := 0
	for i := range iconPerMin + 50 { // o balde recarga 1 a cada 100 ms durante o laço
		if get(h, "v", "/v1/pogo/icon/pm/pm"+strconv.Itoa(i)+".icon.png").Code == 429 {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("miss sem limite")
	}
}

// O teto global protege o IP do servidor no GitHub raw: várias contas juntas não passam dele.
func TestIconLimitGlobal(t *testing.T) {
	t.Parallel()
	up := http.NewServeMux()
	up.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(png) })
	h := assetsHandler(t, fakeUpstream(t, up))
	h.iconAll = ratelimit.NewWindow(3, time.Hour)
	for i := range 3 {
		if w := get(h, strconv.Itoa(i), "/v1/pogo/icon/u/"+strconv.Itoa(i)+".png"); w.Code != 200 {
			t.Fatalf("%d: %d", i, w.Code)
		}
	}
	if w := get(h, "outro", "/v1/pogo/icon/u/9.png"); w.Code != 429 {
		t.Fatalf("passou do teto global: %d", w.Code)
	}
	if w := get(h, "outro", "/v1/pogo/icon/u/0.png"); w.Code != 200 {
		t.Fatalf("hit do cache barrado pelo teto global: %d", w.Code)
	}
}

func TestIconLRU(t *testing.T) {
	c := newLRU(500) // cabem 2 de 200
	now := time.Now()
	item := func(k string) *blob { return &blob{raw: make([]byte, 200-lruOverhead-len(k)), at: now} }
	for _, k := range []string{"a", "b", "c"} {
		c.put(k, item(k))
	}
	if _, ok := c.get("a", now); ok {
		t.Fatal("o mais antigo não saiu")
	}
	c.get("b", now) // b fica mais recente que c
	c.put("d", item("d"))
	if _, ok := c.get("c", now); ok {
		t.Fatal("tirou o usado recentemente")
	}
	if _, ok := c.get("b", now); !ok || c.size != 400 {
		t.Fatalf("b sumiu ou a conta errou: %d", c.size)
	}
	c.put("m", &blob{miss: true, at: now})
	if _, ok := c.get("m", now.Add(iconMiss+time.Second)); ok {
		t.Fatal("404 guardado não venceu")
	}
	c.put("big", &blob{raw: make([]byte, 500), at: now})
	if _, ok := c.get("big", now); ok || c.size > 500 {
		t.Fatal("item maior que o teto ficou")
	}
}
