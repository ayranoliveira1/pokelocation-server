package pogo

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/push"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func lookupJSON(n int, despawn time.Time) []byte {
	var sb strings.Builder
	sb.WriteString(`{"mode":"pokemon","source":"store","num_results":100,"results":[`)
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"encounter_id":"%d","reveal_code":"CrswgcMVabcdefghijklmnopqrstuvwxyz0123456789%06d","pokemon_id":147,`+
			`"pokemon_name":"Dratini","form":0,"costume":0,"gender":1,"size":3,"cp":512,"level":30,"iv_percent":97.8,`+
			`"raw_iv":[15,14,15],"despawn_at_ms":%d,"despawn_verified":true,"apx_lat":%f,"apx_long":%f}`,
			3265749275115993354+int64(i), i, despawn.UnixMilli(), 0.0001*float64(i%50), 0.0001*float64(i%37))
	}
	sb.WriteString(`]}`)
	return []byte(sb.String())
}

func searchJSON(n int, despawn time.Time) []byte {
	var sb strings.Builder
	sb.WriteString(`{"results":[`)
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"encounter_id":"%d","reveal_code":"CrswgcMVabcdefghijklmnopqrstuvwxyz0123456789%06d","pokemon_id":147,`+
			`"pokemon_name":"Dratini","cp":512,"iv_percent":97.8,"level":30,"despawn_at":"%s","despawn_verified":true,`+
			`"shiny":false,"background":false}`, 7265749275115993354-int64(i), i, despawn.UTC().Format(time.RFC3339))
	}
	sb.WriteString(`],"tier":"logged_in","cap":10}`)
	return []byte(sb.String())
}

type fakeUp struct {
	url   string
	conns atomic.Int64
	reqs  atomic.Int64
}

func newFakeUp(tb testing.TB, gz bool, bodies map[string][]byte) *fakeUp {
	f := &fakeUp{}
	zipped := map[string][]byte{}
	for k, v := range bodies {
		var b bytes.Buffer
		zw := gzip.NewWriter(&b)
		_, _ = zw.Write(v)
		_ = zw.Close()
		zipped[k] = b.Bytes()
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.reqs.Add(1)
		key := r.URL.Path
		if strings.HasPrefix(key, "/reveal/") {
			key = "/reveal"
		}
		_, _ = io_copy_discard(r)
		if gz && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Length", fmt.Sprint(len(zipped[key])))
			_, _ = w.Write(zipped[key])
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(bodies[key])))
		_, _ = w.Write(bodies[key])
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			f.conns.Add(1)
		}
	}
	srv.Start()
	tb.Cleanup(srv.Close)
	f.url = srv.URL
	return f
}

func io_copy_discard(r *http.Request) (int64, error) {
	var buf [4096]byte
	var n int64
	for {
		m, err := r.Body.Read(buf[:])
		n += int64(m)
		if err != nil {
			return n, nil
		}
	}
}

type discardRW struct {
	h    http.Header
	code int
	n    int
}

func (d *discardRW) Header() http.Header         { return d.h }
func (d *discardRW) WriteHeader(c int)           { d.code = c }
func (d *discardRW) Write(p []byte) (int, error) { d.n += len(p); return len(p), nil }

func benchKey() string {
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	return base64.StdEncoding.EncodeToString(k)
}

var benchSeq atomic.Int64

func benchUser(tb testing.TB, pool *pgxpool.Pool) string {
	n := benchSeq.Add(1)
	var id string
	if err := pool.QueryRow(context.Background(), `INSERT INTO users (email, friend_code) VALUES ($1, $2) RETURNING id`,
		fmt.Sprintf("b%d@ex.com", n), fmt.Sprintf("B%07d", n)).Scan(&id); err != nil {
		tb.Fatal(err)
	}
	return id
}

func BenchmarkProxy(b *testing.B) {
	pool := testdb.Pool(b)
	despawn := time.Now().Add(time.Hour)
	bodies := map[string][]byte{
		"/map/lookup": lookupJSON(100, despawn),
		"/search":     searchJSON(10, despawn),
		"/reveal":     []byte(`{"coords":"-27.5038039,152.9677155","lat":-27.5038039,"lng":152.9677155}`),
	}
	big := map[string][]byte{"/map/lookup": lookupJSON(500, despawn), "/search": bodies["/search"], "/reveal": bodies["/reveal"]}
	for _, c := range []struct {
		name   string
		gz     bool
		bodies map[string][]byte
		method string
		path   string
		body   string
	}{
		{"lookup100", false, bodies, "POST", "/v1/pogo/lookup", `{"mode":"pokemon","n_lat":1,"s_lat":0,"w_lng":0,"e_lng":1,"filters":{"pokemon_ids":[147]}}`},
		{"lookup100-gz", true, bodies, "POST", "/v1/pogo/lookup", `{"mode":"pokemon","n_lat":1,"s_lat":0,"w_lng":0,"e_lng":1,"filters":{"pokemon_ids":[147]}}`},
		{"lookup500", false, big, "POST", "/v1/pogo/lookup", `{"mode":"pokemon","n_lat":1,"s_lat":0,"w_lng":0,"e_lng":1,"num_results":500}`},
		{"lookup500-gz", true, big, "POST", "/v1/pogo/lookup", `{"mode":"pokemon","n_lat":1,"s_lat":0,"w_lng":0,"e_lng":1,"num_results":500}`},
		{"search", false, bodies, "POST", "/v1/pogo/search", `{"pokemon_ids":[147],"iv_percent_min":90}`},
		{"search-gz", true, bodies, "POST", "/v1/pogo/search", `{"pokemon_ids":[147],"iv_percent_min":90}`},
		{"reveal", false, bodies, "GET", "/v1/pogo/reveal/CrswgcMVabcdef", ""},
	} {
		b.Run(c.name, func(b *testing.B) {
			up := newFakeUp(b, c.gz, c.bodies)
			h, err := New(Config{TokenKey: benchKey(), Upstream: up.url}, pool, testdb.Redis(b))
			if err != nil {
				b.Fatal(err)
			}
			u := benchUser(b, pool)
			if err := h.svc.store.upsert(context.Background(), u, "google", h.svc.seal(u, "eyJ.fake.token-"+strings.Repeat("x", 300)), &Profile{}); err != nil {
				b.Fatal(err)
			}
			// sem rate limit no benchmark
			h.login, h.me, h.lookup, h.search, h.reveal = nil, nil, nil, nil, nil
			mux := http.NewServeMux()
			h.Routes(mux, func(next http.HandlerFunc) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					next(w, r.WithContext(httpx.WithUser(r.Context(), u)))
				})
			})
			rw := &discardRW{h: http.Header{}}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var rd *strings.Reader
				req, _ := http.NewRequestWithContext(ctx, c.method, "http://x"+c.path, nil)
				if c.gz { // o app (HttpURLConnection/OkHttp) sempre aceita gzip
					req.Header.Set("Accept-Encoding", "gzip")
				}
				if c.body != "" {
					rd = strings.NewReader(c.body)
					req.Body = io_nopCloser{rd}
				}
				clear(rw.h)
				rw.n = 0
				mux.ServeHTTP(rw, req)
				if rw.code != 200 {
					b.Fatalf("status %d", rw.code)
				}
			}
			b.ReportMetric(float64(up.conns.Load()), "conns")
			b.ReportMetric(float64(rw.n), "respB")
		})
	}
}

type io_nopCloser struct{ *strings.Reader }

func (io_nopCloser) Close() error { return nil }

type redisCount struct{ cmds, rtts atomic.Int64 }

func (c *redisCount) DialHook(next redis.DialHook) redis.DialHook { return next }
func (c *redisCount) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.cmds.Add(1)
		c.rtts.Add(1)
		return next(ctx, cmd)
	}
}
func (c *redisCount) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		c.cmds.Add(int64(len(cmds)))
		c.rtts.Add(1)
		return next(ctx, cmds)
	}
}

type benchFCM struct{ msgs atomic.Int64 }

func (f *benchFCM) Send(context.Context, string, string) error { return nil }
func (f *benchFCM) SendData(_ context.Context, _, _ string, m []push.Data) error {
	f.msgs.Add(int64(len(m)))
	return nil
}

// Uma passada da fila por todos os usuários (o equivalente à rodada antiga): users usuários, rules regras cada
// (metade near, metade world), atendidos pelo pool real (8 workers).
func BenchmarkWorkerRound(b *testing.B) {
	for _, c := range []struct {
		users, rules int
		fresh        bool // apaga o dedup a cada passada (todo item é novo)
	}{
		{50, 2, true}, {50, 2, false}, {20, 10, true}, {20, 10, false},
	} {
		b.Run(fmt.Sprintf("u%d-r%d-fresh=%v", c.users, c.rules, c.fresh), func(b *testing.B) {
			pool, rdb := testdb.Pool(b), testdb.Redis(b)
			despawn := time.Now().Add(1000 * time.Hour) // o relógio do worker anda um intervalo por passada
			up := newFakeUp(b, false, map[string][]byte{"/map/lookup": lookupJSON(100, despawn), "/search": searchJSON(10, despawn)})
			h, err := New(Config{TokenKey: benchKey(), Upstream: up.url}, pool, rdb)
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			var users []any
			for i := range c.users {
				u := benchUser(b, pool)
				users = append(users, u)
				if _, err := pool.Exec(ctx, `INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id,
					signed_prekey, signed_prekey_sig, kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig, fcm_token)
					VALUES ($1, 1, '\x05', 1, '\x05', '\x00', 1, '\x08', '\x00', $2)`, u, fmt.Sprintf("fcm-%d-%d", i, benchSeq.Load())); err != nil {
					b.Fatal(err)
				}
				if err := h.svc.store.upsert(ctx, u, "google", h.svc.seal(u, "eyJ.fake.token-"+strings.Repeat("x", 300)), &Profile{}); err != nil {
					b.Fatal(err)
				}
				for r := range c.rules {
					scope := "near"
					if r%2 == 1 {
						scope = "world"
					}
					if _, err := pool.Exec(ctx, `INSERT INTO pogo_rules (user_id, name, filters, scope, lat, lng, radius_km)
						VALUES ($1, $2, '{"pokemon_ids":[147,148,149],"iv_min":90}', $3, 0, 0, 2)`, u, fmt.Sprintf("Regra %d", r), scope); err != nil {
						b.Fatal(err)
					}
				}
			}
			// cliente à parte, sem o contador, para o benchmark ver quando a passada acabou
			watch := redis.NewClient(rdb.Options())
			defer watch.Close()
			rc := &redisCount{}
			rdb.AddHook(rc)
			fcm := &benchFCM{}
			ps := push.New(pool, fcm)
			wctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go ps.Run(wctx)
			clk := &clock{t: time.Now()}
			w := newWorker(h.svc, h.q, ps)
			w.now = clk.now
			var peak atomic.Int64
			base := runtime.NumGoroutine()
			go w.run(wctx)
			time.Sleep(50 * time.Millisecond) // o pool sobe antes de medir
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				t := time.NewTicker(200 * time.Microsecond)
				defer t.Stop()
				for {
					select {
					case <-stop:
						return
					case <-t.C:
						if n := int64(runtime.NumGoroutine()); n > peak.Load() {
							peak.Store(n)
						}
					}
				}
			}()
			pass := func() {
				clk.add(defaultInterval + time.Second) // todos vencidos
				now := clk.now()
				_ = watch.ZAdd(ctx, dueKey, func() []redis.Z {
					zs := make([]redis.Z, len(users))
					for i, u := range users {
						zs[i] = redis.Z{Score: float64(now.UnixMilli()), Member: u}
					}
					return zs
				}()...).Err()
				h.q.signal()
				done := strconv.FormatInt(now.Add(defaultInterval).UnixMilli(), 10)
				for watch.ZCount(ctx, dueKey, done, done).Val() < int64(len(users)) {
					time.Sleep(100 * time.Microsecond)
				}
			}
			pass() // aquece as conexões e os rascunhos
			if !c.fresh {
				pass()
			}
			rc.cmds.Store(0)
			rc.rtts.Store(0)
			acq0 := pool.Stat().AcquireCount()
			req0 := up.reqs.Load()
			b.ReportAllocs()
			b.ResetTimer()
			rounds := 0
			for b.Loop() {
				if c.fresh {
					b.StopTimer()
					for _, k := range watch.Keys(ctx, "pogo:seen:*").Val() {
						_ = watch.Del(ctx, k).Err()
					}
					b.StartTimer()
				}
				pass()
				rounds++
			}
			b.StopTimer()
			close(stop)
			wg.Wait()
			div := float64(rounds)
			b.ReportMetric(float64(rc.rtts.Load())/div, "redisRTT/round")
			b.ReportMetric(float64(rc.rtts.Load())/div/float64(c.users), "redisRTT/user")
			b.ReportMetric(float64(rc.cmds.Load())/div, "redisCmds/round")
			b.ReportMetric(float64(pool.Stat().AcquireCount()-acq0)/div, "dbQueries/round")
			b.ReportMetric(float64(up.reqs.Load()-req0)/div, "upstreamReq/round")
			b.ReportMetric(float64(up.conns.Load()), "upstreamConns")
			b.ReportMetric(float64(fcm.msgs.Load())/div, "pushMsgs/round")
			b.ReportMetric(float64(peak.Load()-int64(base)), "peakExtraGoroutines")
		})
	}
}

func BenchmarkAccountRead(b *testing.B) {
	pool := testdb.Pool(b)
	h, err := New(Config{TokenKey: benchKey(), Upstream: "http://127.0.0.1:1"}, pool, nil)
	if err != nil {
		b.Fatal(err)
	}
	u := benchUser(b, pool)
	if err := h.svc.store.upsert(context.Background(), u, "google", h.svc.seal(u, "eyJ.fake.token-"+strings.Repeat("x", 300)), &Profile{}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		a, err := h.svc.store.account(ctx, u)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := h.svc.open(u, a.enc); err != nil {
			b.Fatal(err)
		}
	}
}
