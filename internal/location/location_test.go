package location

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/friends"
	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/relay"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

const userA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

// sent: o que o Service entregou ao hub (SendFriends), decodificado e copiado.
type sent struct {
	user string
	f    wire.Frame
}

type fakeHub struct {
	mu  sync.Mutex
	got []sent
}

func (h *fakeHub) SendFriends(user string, frame []byte) {
	f, err := wire.Decode(frame)
	if err != nil {
		panic("frame inválido: " + err.Error())
	}
	f.Peer, f.Payload = bytes.Clone(f.Peer), bytes.Clone(f.Payload)
	h.mu.Lock()
	h.got = append(h.got, sent{user, f})
	h.mu.Unlock()
}

func (h *fakeHub) take() []sent {
	h.mu.Lock()
	defer h.mu.Unlock()
	g := h.got
	h.got = nil
	return g
}

// clock controlável: o limite de 1/5 s depende só dele.
type clock struct{ t time.Time }

func (c *clock) now() time.Time             { return c.t }
func (c *clock) add(d time.Duration) *clock { c.t = c.t.Add(d); return c }

func newTest(t *testing.T, rdb *redis.Client, pool *pgxpool.Pool) (*Service, *fakeHub, *clock) {
	t.Helper()
	h := &fakeHub{}
	c := &clock{time.Unix(1_000_000, 0)}
	s := newService(rdb, pool, h.SendFriends)
	s.now = c.now
	return s, h, c
}

func loc(payload string) wire.Frame { return wire.Frame{Type: wire.Location, Payload: []byte(payload)} }

var off = wire.Frame{Type: wire.LocationOff}

func stored(t *testing.T, rdb *redis.Client, user string) (string, time.Duration) {
	t.Helper()
	ctx := context.Background()
	v, err := rdb.Get(ctx, "loc:"+user).Result()
	if err == redis.Nil {
		return "", 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return v, rdb.TTL(ctx, "loc:"+user).Val()
}

func TestRelayAndStore(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	s, h, _ := newTest(t, rdb, nil)

	buf := []byte("p1")
	s.handle(userA, wire.Frame{Type: wire.Location, Payload: buf})
	buf[0] = 'X' // o buffer de leitura é reciclado pelo relay depois do OnFrame

	g := h.take()
	if len(g) != 1 || g[0].user != userA || g[0].f.Type != wire.LocationUpdate ||
		string(g[0].f.Peer) != userA || string(g[0].f.Payload) != "p1" {
		t.Fatalf("repasse: %+v", g)
	}
	if v, _ := stored(t, rdb, userA); v != "" {
		t.Fatal("gravou no Redis antes do flush (caminho quente sem I/O)")
	}
	s.flush(context.Background())
	if v, ttl := stored(t, rdb, userA); v != "p1" || ttl < 55*time.Second || ttl > 60*time.Second {
		t.Fatalf("Redis: %q ttl %v, want p1 ~60s (cópia, não fatia do buffer de leitura)", v, ttl)
	}
}

// No máximo 1 a cada 5 s por usuário, com folga: ≥ 4 s passa, < 4 s é descartado em silêncio.
func TestRateLimit(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	s, h, c := newTest(t, rdb, nil)

	s.handle(userA, loc("1"))
	c.add(3999 * time.Millisecond)
	s.handle(userA, loc("2")) // descartado
	c.add(time.Millisecond)
	s.handle(userA, loc("3"))
	c.add(time.Second)
	s.handle(userA, loc("4"))                                  // descartado
	s.handle("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", loc("b")) // outro usuário, balde próprio

	var payloads []string
	for _, g := range h.take() {
		payloads = append(payloads, string(g.f.Payload))
	}
	if strings.Join(payloads, ",") != "1,3,b" {
		t.Fatalf("repassados %v, want 1,3,b", payloads)
	}
	s.flush(context.Background())
	if v, _ := stored(t, rdb, userA); v != "3" {
		t.Fatalf("Redis %q, want a última aceita (3)", v)
	}
}

func TestOff(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	s, h, c := newTest(t, rdb, nil)
	ctx := context.Background()

	s.handle(userA, loc("1"))
	s.flush(ctx)
	h.take()

	s.handle(userA, off)
	if g := h.take(); len(g) != 1 || g[0].f.Type != wire.LocationOff || string(g[0].f.Peer) != userA || g[0].user != userA {
		t.Fatalf("LocationOff repassado: %+v", g)
	}
	s.handle(userA, off) // repetido: nada (sem spam aos amigos)
	if g := h.take(); len(g) != 0 {
		t.Fatalf("LocationOff repetido repassado: %+v", g)
	}
	s.flush(ctx)
	if v, _ := stored(t, rdb, userA); v != "" {
		t.Fatalf("Redis depois do LocationOff: %q", v)
	}

	s.handle(userA, loc("2")) // Off não zera o limite: < 4 s da última posição
	c.add(4 * time.Second)
	s.handle(userA, loc("3"))
	if g := h.take(); len(g) != 1 || string(g[0].f.Payload) != "3" {
		t.Fatalf("depois do Off: %+v", g)
	}
	s.flush(ctx)
	if v, _ := stored(t, rdb, userA); v != "3" {
		t.Fatalf("Redis depois de voltar: %q", v)
	}

	// Off sem posição anterior nesta instância (reinício, outra instância): apaga e avisa mesmo assim
	b := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	rdb.Set(ctx, "loc:"+b, "velha", time.Minute)
	s.handle(b, off)
	if g := h.take(); len(g) != 1 || g[0].f.Type != wire.LocationOff {
		t.Fatalf("Off sem estado: %+v", g)
	}
	s.flush(ctx)
	if v, _ := stored(t, rdb, b); v != "" {
		t.Fatalf("Off sem estado não apagou: %q", v)
	}
}

// Dentro de um mesmo intervalo de flush, vale a última intenção do usuário (SET ou DEL).
func TestLastIntentWins(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	s, _, c := newTest(t, rdb, nil)
	ctx := context.Background()

	s.handle(userA, loc("1"))
	s.handle(userA, off)
	s.flush(ctx)
	if v, _ := stored(t, rdb, userA); v != "" {
		t.Fatalf("posição e depois Off: Redis %q, want vazio", v)
	}
	c.add(5 * time.Second)
	s.handle(userA, off) // já desligado: ignorado
	s.handle(userA, loc("2"))
	s.flush(ctx)
	if v, _ := stored(t, rdb, userA); v != "2" {
		t.Fatalf("Off e depois posição: Redis %q, want 2", v)
	}
}

// Estado parado há mais de 60 s sai da memória (a chave do Redis já expirou).
func TestSweep(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	s, _, c := newTest(t, rdb, nil)
	ctx := context.Background()
	for _, u := range []string{userA, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"} {
		s.handle(u, loc("x"))
	}
	s.flush(ctx)
	if n := s.users(); n != 2 {
		t.Fatalf("%d usuários em memória, want 2", n)
	}
	c.add(61 * time.Second)
	s.handle(userA, loc("y"))
	s.flush(ctx)
	if n := s.users(); n != 1 {
		t.Fatalf("depois da varredura: %d usuários, want 1", n)
	}
}

// Redis fora: o repasse ao vivo continua; o flush falha sem derrubar nada e volta quando o Redis volta.
func TestRedisDown(t *testing.T) {
	t.Parallel()
	rdb, ctl := testdb.StoppableRedis(t)
	s, h, c := newTest(t, rdb, nil)
	ctx := context.Background()

	ctl.Stop(t)
	s.handle(userA, loc("1"))
	if g := h.take(); len(g) != 1 {
		t.Fatalf("repasse com Redis fora: %+v", g)
	}
	s.flush(ctx) // não trava nem entra em pânico
	ctl.Start(t)
	c.add(5 * time.Second)
	s.handle(userA, loc("2"))
	s.flush(ctx)
	if v, _ := stored(t, rdb, userA); v != "2" {
		t.Fatalf("depois do Redis voltar: %q", v)
	}
}

// Caminho quente: 0 alocações com o buffer vindo do pool do hub (aqui, um buffer só, reaproveitado).
func TestAllocs(t *testing.T) {
	s := newService(nil, nil, func(string, []byte) {})
	pool := make([]byte, 0, 512)
	s.buf = func() []byte { return pool[:0] }
	c := &clock{time.Unix(1_000_000, 0)}
	s.now = c.now
	payload := bytes.Repeat([]byte{7}, 125)
	f := wire.Frame{Type: wire.Location, Payload: payload}
	s.handle(userA, f) // aquece o estado do usuário
	for name, step := range map[string]time.Duration{"repassada": 5 * time.Second, "descartada": time.Second} {
		if n := testing.AllocsPerRun(1000, func() { c.add(step); s.handle(userA, f) }); n != 0 {
			t.Fatalf("posição %s: %v allocs/op, want 0", name, n)
		}
	}
	if n := testing.AllocsPerRun(1000, func() { s.handle(userA, off); c.add(5 * time.Second); s.handle(userA, f) }); n != 0 {
		t.Fatalf("LocationOff + posição: %v allocs/op, want 0", n)
	}
}

func BenchmarkLocation(b *testing.B) {
	s := newService(nil, nil, func(string, []byte) {})
	pool := make([]byte, 0, 512)
	s.buf = func() []byte { return pool[:0] }
	c := &clock{time.Unix(1_000_000, 0)}
	s.now = c.now
	f := wire.Frame{Type: wire.Location, Payload: bytes.Repeat([]byte{7}, 125)}
	b.ReportAllocs()
	for b.Loop() {
		c.add(5 * time.Second)
		s.handle(userA, f)
	}
}

// Concorrência: várias conexões (goroutines de leitura) e o flusher ao mesmo tempo (-race).
func TestConcurrent(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	s := newService(rdb, nil, func(string, []byte) {})
	var wg sync.WaitGroup
	for i := range 32 {
		u := strings.Replace(userA, "aaaa", string(rune('a'+i%6))+"aaa", 1)
		wg.Go(func() {
			for range 50 {
				s.handle(u, loc("x"))
				s.handle(u, off)
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			s.flush(context.Background())
		}
	})
	wg.Wait()
}

type env struct {
	t    *testing.T
	pool *pgxpool.Pool
	rdb  *redis.Client
	mux  *http.ServeMux
	s    *Service
}

func newEnv(t *testing.T, rdb *redis.Client) *env {
	t.Helper()
	pool := testdb.Pool(t)
	s := newService(rdb, pool, func(string, []byte) {})
	mux := http.NewServeMux()
	s.Routes(mux, func(h http.HandlerFunc) http.Handler { return h })
	return &env{t, pool, rdb, mux, s}
}

func (e *env) user() (id, code string) {
	e.t.Helper()
	err := e.pool.QueryRow(context.Background(),
		`INSERT INTO users (email, friend_code) VALUES (gen_random_uuid()::text || '@x', $1) RETURNING id, friend_code`,
		friends.NewCode()).Scan(&id, &code)
	if err != nil {
		e.t.Fatal(err)
	}
	return id, code
}

func (e *env) exec(q string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), q, args...); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) get(user string) (int, []entry, string) {
	e.t.Helper()
	req := httptest.NewRequest("GET", "/v1/locations", nil).WithContext(httpx.WithUser(context.Background(), user))
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	var out struct{ Locations []entry }
	if w.Code == 200 {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			e.t.Fatal(err)
		}
		if !strings.Contains(w.Body.String(), `"locations":[`) {
			e.t.Fatalf("locations não é lista: %s", w.Body)
		}
	}
	return w.Code, out.Locations, w.Body.String()
}

func TestGetLocations(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	e := newEnv(t, rdb)
	ctx := context.Background()
	a, _ := e.user()
	b, bCode := e.user()
	c, _ := e.user()
	stranger, _ := e.user()
	pending, _ := e.user()
	blocked, _ := e.user()
	accepted := `INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'accepted')`
	e.exec(accepted, a, b)
	e.exec(accepted, c, a) // amigo sem posição
	e.exec(`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'pending')`, a, pending)
	e.exec(accepted, a, blocked) // bloqueio com amizade ainda lá: defesa do location
	e.exec(`INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2)`, blocked, a)
	for _, u := range []string{b, stranger, pending, blocked, a} {
		rdb.Set(ctx, "loc:"+u, "pos-"+u, time.Minute)
	}

	e.exec(`UPDATE users SET username = 'bob_1' WHERE id = $1`, b)
	status, got, raw := e.get(a)
	if status != 200 || len(got) != 1 || got[0].UserID != b || got[0].FriendCode != bCode || string(got[0].Payload) != "pos-"+b ||
		got[0].Username == nil || *got[0].Username != "bob_1" {
		t.Fatalf("GET /v1/locations: %d %s", status, raw)
	}
	if _, got, _ := e.get(stranger); len(got) != 0 {
		t.Fatalf("estranho vê posições: %+v", got)
	}
}

func TestGetLocationsRedisDown(t *testing.T) {
	t.Parallel()
	rdb, ctl := testdb.StoppableRedis(t)
	e := newEnv(t, rdb)
	a, _ := e.user()
	b, _ := e.user()
	e.exec(`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'accepted')`, a, b)
	ctl.Stop(t)
	if status, got, raw := e.get(a); status != 200 || len(got) != 0 {
		t.Fatalf("Redis fora: %d %s, want 200 []", status, raw)
	}
}

func TestGetLocationsDBDown(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	e := newEnv(t, rdb)
	a, _ := e.user()
	pool, err := pgxpool.New(context.Background(), e.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	e.mux = http.NewServeMux()
	newService(rdb, pool, func(string, []byte) {}).Routes(e.mux, func(h http.HandlerFunc) http.Handler { return h })
	if status, _, raw := e.get(a); status != 503 || !strings.Contains(raw, "unavailable") {
		t.Fatalf("Postgres fora: %d %s", status, raw)
	}
}

// Teto do JSON (o app lê até 256 KiB): passou → 413 too_large, nunca uma lista cortada em silêncio.
func TestGetLocationsCap(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	e := newEnv(t, rdb)
	ctx := context.Background()
	a, _ := e.user()
	e.exec(`WITH x AS (
		INSERT INTO users (email, friend_code)
		SELECT gen_random_uuid()::text || '@x', upper(substr(md5(random()::text), 1, 8)) FROM generate_series(1, 300)
		RETURNING id)
		INSERT INTO friendships (requester_id, addressee_id, status) SELECT id, $1, 'accepted' FROM x`, a)
	rows, err := e.pool.Query(ctx, `SELECT requester_id FROM friendships WHERE addressee_id = $1`, a)
	if err != nil {
		t.Fatal(err)
	}
	pipe := rdb.Pipeline()
	var n int
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		pipe.Set(ctx, "loc:"+id, bytes.Repeat([]byte{1}, 1024), time.Minute) // payload máximo: 1 KiB
		n++
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	// 300 amigos × 1 KiB de payload ≈ 420 KiB de JSON
	if status, _, raw := e.get(a); status != 413 || !strings.Contains(raw, `"too_large"`) {
		t.Fatalf("acima do teto: %d %.200s", status, raw)
	}
	// 150 deles ≈ 210 KiB: cabe inteiro
	e.exec(`DELETE FROM friendships WHERE id IN (SELECT id FROM friendships WHERE addressee_id = $1 LIMIT 150)`, a)
	status, got, raw := e.get(a)
	if status != 200 || len(got) != 150 || len(raw) > maxJSON {
		t.Fatalf("abaixo do teto: %d, %d entradas, %d bytes (limite %d)", status, len(got), len(raw), maxJSON)
	}
}

// New liga o Hub real (SendFriends + relay.Buf) e OnFrame repassa pela conexão. Sem amigos online: nada a entregar,
// mas o estado e a gravação seguem.
func TestNewWithHub(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	pool := testdb.Pool(t)
	hub := relay.New(pool, func(string) (string, time.Time, int, error) { return "", time.Time{}, 0, nil }, nil)
	s := New(rdb, pool, hub)
	s.OnFrame(&relay.Conn{}, loc("p")) // Conn zero: User() == ""
	s.flush(context.Background())
	if v, _ := stored(t, rdb, ""); v != "p" {
		t.Fatalf("OnFrame não chegou ao estado: %q", v)
	}
}

// O mesmo pelo caminho real: OnFrame → relay.Buf → hub.SendFriends (buffer volta ao pool do hub).
func TestAllocsWithHub(t *testing.T) {
	hub := relay.New(nil, func(string) (string, time.Time, int, error) { return "", time.Time{}, 0, nil }, nil)
	s := New(nil, nil, hub)
	c := &clock{time.Unix(1_000_000, 0)}
	s.now = c.now
	conn := &relay.Conn{}
	f := wire.Frame{Type: wire.Location, Payload: bytes.Repeat([]byte{7}, 125)}
	s.OnFrame(conn, f)
	if n := testing.AllocsPerRun(1000, func() { c.add(5 * time.Second); s.OnFrame(conn, f) }); n != 0 {
		t.Fatalf("OnFrame com o hub real: %v allocs/op, want 0", n)
	}
}

func TestGetLocationsDeadline(t *testing.T) {
	t.Parallel()
	e := newEnv(t, testdb.Redis(t))
	a, _ := e.user()
	e.s.reqWait = time.Nanosecond
	e.mux = http.NewServeMux() // o prazo entra no registro das rotas
	e.s.Routes(e.mux, func(h http.HandlerFunc) http.Handler { return h })
	if status, _, raw := e.get(a); status != 503 || !strings.Contains(raw, "unavailable") {
		t.Fatalf("prazo vencido: %d %s", status, raw)
	}
}

func TestGetLocationsLimit(t *testing.T) {
	t.Parallel()
	rdb := testdb.Redis(t)
	e := newEnv(t, rdb)
	ctx := context.Background()
	a, _ := e.user()
	e.exec(`WITH x AS (
		INSERT INTO users (email, friend_code)
		SELECT gen_random_uuid()::text || '@x', upper(substr(md5(random()::text), 1, 8)) FROM generate_series(1, $1)
		RETURNING id)
		INSERT INTO friendships (requester_id, addressee_id, status) SELECT id, $2, 'accepted' FROM x`, friendsLimit+5, a)
	rows, err := e.pool.Query(ctx, `SELECT requester_id FROM friendships WHERE addressee_id = $1`, a)
	if err != nil {
		t.Fatal(err)
	}
	pipe := rdb.Pipeline()
	var id string
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		pipe.Set(ctx, "loc:"+id, "p", time.Minute)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if status, got, _ := e.get(a); status != 200 || len(got) != friendsLimit {
		t.Fatalf("%d, %d posições; want 200 e o LIMIT %d", status, len(got), friendsLimit)
	}
}

func TestMemoryPerUser(t *testing.T) {
	const n = 15000
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%08x-aaaa-4aaa-8aaa-aaaaaaaaaaaa", i)
	}
	f := wire.Frame{Type: wire.Location, Payload: bytes.Repeat([]byte{7}, 125)}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	s := newService(nil, nil, func(string, []byte) {})
	pool := make([]byte, 0, 512)
	s.buf = func() []byte { return pool[:0] }
	for _, id := range ids {
		s.handle(id, f)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := int(after.HeapAlloc-before.HeapAlloc) / n
	t.Logf("%d usuários: %d B por usuário, %.1f MiB no total", n, per, float64(after.HeapAlloc-before.HeapAlloc)/(1<<20))
	if per > 1024 {
		t.Fatalf("%d B por usuário, teto 1024", per)
	}
	runtime.KeepAlive(s)
}
