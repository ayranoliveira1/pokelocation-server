package pogo

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
)

// Catálogo de espécies e ícones dos Pokémon: públicos, sem o token do usuário, guardados aqui para o app não sair do
// nosso servidor.
const (
	catalogPath   = "/pokemon/catalog?costumes=true"
	catalogTTL    = time.Hour
	catalogPerMin = 10
	maxCatalog    = 1 << 20
	maxIcon       = 256 << 10
	iconWait      = 10 * time.Second
	iconMiss      = 6 * time.Hour // um 404 do GitHub pode virar asset novo
	iconPerMin    = 600
	// todas as contas juntas, abaixo do limite por IP do GitHub raw (não publicado; ~5.000/h relatado); a rajada cobre o cache frio
	iconAllPerHour = 4000
	// ponytail: LRU em memória, some na subida; disco se o GitHub raw limitar
	iconCache   = 32 << 20
	lruOverhead = 128 // map, list e blob por item
)

type iconSource struct {
	base string
	name *regexp.Regexp
}

type assets struct {
	icons map[string]iconSource // "pm", "u" e "it": só esses hosts e caminhos
	hc    *http.Client
	sf    singleflight.Group
	mu    sync.Mutex
	cat   *blob
	lru   *lru
}

func newAssets() *assets {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 16
	return &assets{
		icons: map[string]iconSource{
			"pm": {"https://raw.githubusercontent.com/PokeMiners/pogo_assets/master/Images/Pokemon%20-%20256x256/Addressable%20Assets/",
				regexp.MustCompile(`^pm[0-9]{1,4}(\.c[A-Za-z0-9_]{1,64})?(\.f[A-Za-z0-9_]{1,64})?(\.s)?\.icon\.png$`)},
			"u": {"https://raw.githubusercontent.com/nileplumb/PkmnShuffleMap/refs/heads/master/UICONS/pokemon/",
				regexp.MustCompile(`^[0-9]{1,4}\.png$`)},
			"it": {"https://raw.githubusercontent.com/nileplumb/PkmnShuffleMap/refs/heads/master/UICONS/reward/item/",
				regexp.MustCompile(`^1301\.png$`)}, // só a Rare Candy
		},
		hc:  &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		lru: newLRU(iconCache),
	}
}

// blob: uma resposta guardada. miss = 404 guardado.
type blob struct {
	raw, gz []byte
	etag    string
	miss    bool
	at      time.Time
}

func newBlob(raw []byte, at time.Time, compress bool) *blob {
	sum := sha256.Sum256(raw)
	b := &blob{raw: raw, etag: `"` + hex.EncodeToString(sum[:8]) + `"`, at: at}
	if compress {
		var z bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&z, gzip.BestCompression)
		_, _ = zw.Write(raw)
		_ = zw.Close()
		b.gz = z.Bytes()
	}
	return b
}

func (h *Handler) getCatalog(w http.ResponseWriter, r *http.Request) {
	b, err := h.assets.catalog(r.Context(), h.svc.c)
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Cache-Control", "private, no-cache")
	writeBlob(w, r, b, "application/json")
}

// catalog: vencido, responde o guardado na hora e atualiza por trás.
func (a *assets) catalog(ctx context.Context, c *client) (*blob, error) {
	a.mu.Lock()
	b := a.cat
	fresh := b != nil && time.Since(b.at) < catalogTTL
	a.mu.Unlock()
	if fresh {
		return b, nil
	}
	ch := a.sf.DoChan("catalog", func() (any, error) { return a.fetchCatalog(c) })
	if b != nil {
		return b, nil
	}
	return wait(ctx, ch)
}

func (a *assets) fetchCatalog(c *client) (*blob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), proxyWait) // vale para todos os que esperam
	defer cancel()
	buf := getBuf()
	defer putBuf(buf)
	rep, err := c.do(ctx, "GET", catalogPath, "", nil, maxCatalog, false, buf)
	if err != nil {
		err = upErr(err)
	} else if classify(rep.status, rep.retryAfter, buf.Bytes()) != nil || !json.Valid(buf.Bytes()) {
		err = ErrUnavailable // o 4xx deles não é do usuário
	}
	now := time.Now()
	var b *blob
	if err == nil {
		b = newBlob(bytes.Clone(buf.Bytes()), now, true)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		if a.cat != nil { // segue o guardado; tenta de novo em 1 min
			a.cat.at = now.Add(time.Minute - catalogTTL)
		}
		return nil, err
	}
	a.cat = b
	return b, nil
}

func (h *Handler) getIcon(w http.ResponseWriter, r *http.Request) {
	src, ok := h.assets.icons[r.PathValue("src")]
	name := r.PathValue("file")
	if !ok || !src.name.MatchString(name) {
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return
	}
	url := src.base + name
	b, ok := h.assets.lru.get(url, time.Now())
	if !ok {
		if now := time.Now(); !h.icon.Allow(httpx.User(r.Context()), now) || !h.iconAll.Allow("", now) {
			httpx.Error(w, http.StatusTooManyRequests, "rate_limited")
			return
		}
		var err error
		if b, err = wait(r.Context(), h.assets.sf.DoChan(url, func() (any, error) { return h.assets.fetchIcon(url) })); err != nil {
			fail(w, err)
			return
		}
	}
	if b.miss {
		httpx.Error(w, http.StatusNotFound, "not_found")
		return
	}
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	writeBlob(w, r, b, "image/png")
}

var pngSig = []byte("\x89PNG\r\n\x1a\n")

// fetchIcon guarda o 200 com PNG e o 404; o resto (5xx, redirect, outro conteúdo) volta 502 sem guardar.
func (a *assets) fetchIcon(url string) (*blob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), iconWait)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, ErrUnavailable
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := a.hc.Do(req)
	if err != nil {
		return nil, upErr(err)
	}
	defer resp.Body.Close()
	now := time.Now()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		b := &blob{miss: true, at: now}
		a.lru.put(url, b)
		return b, nil
	default:
		return nil, ErrUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxIcon+1))
	if err != nil {
		return nil, upErr(err)
	}
	if len(raw) > maxIcon || !bytes.HasPrefix(raw, pngSig) {
		return nil, ErrUnavailable
	}
	b := newBlob(raw, now, false)
	a.lru.put(url, b)
	return b, nil
}

// wait: o pedido pode desistir; a ida ao upstream segue para os outros que esperam.
func wait(ctx context.Context, ch <-chan singleflight.Result) (*blob, error) {
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*blob), nil
	case <-ctx.Done():
		return nil, upErr(ctx.Err())
	}
}

func writeBlob(w http.ResponseWriter, r *http.Request, b *blob, contentType string) {
	w.Header().Set("ETag", b.etag)
	if b.gz != nil {
		w.Header().Set("Vary", "Accept-Encoding")
	}
	if strings.Contains(r.Header.Get("If-None-Match"), b.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	body := b.raw
	if b.gz != nil && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		body = b.gz
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}

// lru: teto em bytes; o 404 guardado vence em iconMiss.
type lru struct {
	mu        sync.Mutex
	max, size int
	ll        *list.List // frente = usado por último
	m         map[string]*list.Element
}

type lruItem struct {
	key string
	b   *blob
}

func newLRU(max int) *lru { return &lru{max: max, ll: list.New(), m: map[string]*list.Element{}} }

func cost(key string, b *blob) int { return len(key) + len(b.raw) + lruOverhead }

func (c *lru) get(key string, now time.Time) (*blob, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok {
		return nil, false
	}
	it := e.Value.(*lruItem)
	if it.b.miss && now.Sub(it.b.at) > iconMiss {
		c.remove(e)
		return nil, false
	}
	c.ll.MoveToFront(e)
	return it.b, true
}

func (c *lru) put(key string, b *blob) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[key]; ok {
		c.remove(e)
	}
	if cost(key, b) > c.max {
		return
	}
	c.m[key] = c.ll.PushFront(&lruItem{key, b})
	c.size += cost(key, b)
	for c.size > c.max {
		c.remove(c.ll.Back())
	}
}

func (c *lru) remove(e *list.Element) {
	it := c.ll.Remove(e).(*lruItem)
	delete(c.m, it.key)
	c.size -= cost(it.key, it.b)
}
