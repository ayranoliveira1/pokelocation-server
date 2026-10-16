package pogo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ayranoliveira1/pokelocation-server/internal/push"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

// Carga da fila: POGO_LOAD=<usuários> (ex.: 10000), uma regra near cada, PoGo falso com POGO_LOAD_MS (200) de
// latência e POGO_WORKERS (8) workers. Uma passada completa: vazão, atraso da fila (p50/p99), tempo por usuário, idas ao Redis e alocações.
//
//	POGO_LOAD=10000 tools/gtest.sh -run TestQueueLoad -v -timeout 30m ./internal/pogo/
func TestQueueLoad(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("POGO_LOAD"))
	if n == 0 {
		t.Skip("POGO_LOAD=<usuários> liga o teste de carga")
	}
	lat := 200 * time.Millisecond
	if ms, _ := strconv.Atoi(os.Getenv("POGO_LOAD_MS")); ms > 0 {
		lat = time.Duration(ms) * time.Millisecond
	}
	pool, rdb := testdb.Pool(t), testdb.Redis(t)
	ctx := context.Background()

	var mu sync.Mutex
	firstCall := make(map[string]time.Time, n)
	despawn := time.Now().Add(time.Hour).UnixMilli()
	body := fmt.Sprintf(`{"mode":"pokemon","results":[`+
		`{"encounter_id":"1","reveal_code":"r1","pokemon_name":"Dratini","cp":512,"iv_percent":100,"despawn_at_ms":%d,"apx_lat":0,"apx_long":0},`+
		`{"encounter_id":"2","reveal_code":"r2","pokemon_name":"Gible","cp":900,"iv_percent":98,"despawn_at_ms":%d,"apx_lat":0.001,"apx_long":0}]}`,
		despawn, despawn)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		now := time.Now()
		mu.Lock()
		if _, ok := firstCall[tok]; !ok {
			firstCall[tok] = now
		}
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		time.Sleep(lat)
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	h, err := New(Config{TokenKey: newKey(t), Upstream: srv.URL}, pool, rdb)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(ctx, `WITH u AS (
			INSERT INTO users (email, friend_code) SELECT 'l' || g || '@ex.com', 'V' || lpad(g::text, 7, '0')
			FROM generate_series(1, $1::int) g RETURNING id)
		INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_sig,
			kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig, fcm_token)
		SELECT id, 1, '\x05', 1, '\x05', '\x00', 1, '\x08', '\x00', 'fcm-' || id FROM u RETURNING user_id::text`, n)
	if err != nil {
		t.Fatal(err)
	}
	users, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	userOf := make(map[string]string, n) // token do PoGo → usuário
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"pogo_accounts"}, []string{"user_id", "provider", "token_enc"},
		pgx.CopyFromSlice(len(users), func(i int) ([]any, error) {
			tok := "tok-" + users[i]
			userOf[tok] = users[i]
			return []any{users[i], "google", h.svc.seal(users[i], tok)}, nil
		})); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO pogo_rules (user_id, name, filters, scope, lat, lng, radius_km)
		SELECT user_id, 'Perto', '{}', 'near', 0, 0, 1 FROM pogo_accounts`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
		t.Fatal(err)
	}

	fcm := &loadFCM{at: make(map[string]time.Time, n)}
	ps := push.New(pool, fcm)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go ps.Run(wctx)
	w := newWorker(h.svc, h.q, ps)
	if n, _ := strconv.Atoi(os.Getenv("POGO_WORKERS")); n > 0 {
		w.workers = n
	}
	w.interval = time.Hour // uma passada só: ninguém volta antes de a fila esvaziar
	rc := &redisCount{}
	rdb.AddHook(rc)
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	t0 := time.Now()
	go w.run(wctx) // fila vazia: a reconstrução põe os n usuários vencidos em t0
	for {
		fcm.mu.Lock()
		done := len(fcm.at)
		fcm.mu.Unlock()
		if done == n {
			break
		}
		if time.Since(t0) > 30*time.Minute {
			t.Fatalf("%d de %d em 30 min", done, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	total := time.Since(t0)
	runtime.ReadMemStats(&m1)
	rtt := rc.rtts.Load()

	var delay, per []time.Duration
	mu.Lock()
	fcm.mu.Lock()
	for tok, at := range firstCall {
		delay = append(delay, at.Sub(t0))
		per = append(per, fcm.at[userOf[tok]].Sub(at))
	}
	fcm.mu.Unlock()
	mu.Unlock()
	slices.Sort(delay)
	slices.Sort(per)
	pct := func(d []time.Duration, p float64) time.Duration { return d[int(float64(len(d)-1)*p)] }
	t.Logf("n=%d latência=%v workers=%d: passada em %v, vazão %.1f usuários/s", n, lat, w.workers, total.Round(time.Millisecond),
		float64(n)/total.Seconds())
	t.Logf("atraso da fila (vencido → 1ª chamada ao PoGo): p50 %v, p99 %v, máx %v", pct(delay, .5).Round(time.Millisecond),
		pct(delay, .99).Round(time.Millisecond), delay[len(delay)-1].Round(time.Millisecond))
	t.Logf("tempo por usuário (1ª chamada → aviso entregue ao FCM): p50 %v, p99 %v", pct(per, .5).Round(time.Millisecond),
		pct(per, .99).Round(time.Millisecond))
	t.Logf("Redis: %.2f idas por usuário; processo inteiro (inclui o PoGo falso): %.0f allocs e %.1f KB por usuário",
		float64(rtt)/float64(n), float64(m1.Mallocs-m0.Mallocs)/float64(n), float64(m1.TotalAlloc-m0.TotalAlloc)/float64(n)/1024)
	t.Logf("capacidade com %d workers: %.0f usuários (1 regra) por intervalo de %v", w.workers,
		float64(w.workers)*defaultInterval.Seconds()/lat.Seconds(), defaultInterval)
}

type loadFCM struct {
	mu sync.Mutex
	at map[string]time.Time
}

func (f *loadFCM) Send(context.Context, string, string) error { return nil }

func (f *loadFCM) SendData(_ context.Context, user, _ string, _ []push.Data) error {
	f.mu.Lock()
	f.at[user] = time.Now()
	f.mu.Unlock()
	return nil
}
