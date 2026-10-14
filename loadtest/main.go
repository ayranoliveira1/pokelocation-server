// Gerador de carga do protocolo WS real: latência ponta a ponta, perdas, RSS e CPU do servidor contra as metas da
// spec. Roteiro e flags em loadtest/README.md. O -url padrão usa 127.0.0.1 e ::1 porque o macOS tem ~16 mil portas
// efêmeras por destino.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"

	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

var (
	urls     = flag.String("url", "ws://127.0.0.1:8080/v1/ws,ws://[::1]:8080/v1/ws", "endpoints WS, separados por vírgula (conexões em rodízio)")
	dbURL    = flag.String("db", os.Getenv("DATABASE_URL"), "Postgres do servidor (semeia usuários e amizades)")
	keyB64   = flag.String("key", os.Getenv("AUTH_SIGNING_KEY"), "seed Ed25519 em base64 (a mesma do servidor)")
	n        = flag.Int("n", 15000, "conexões (1 usuário cada)")
	nFriends = flag.Int("friends", 10, "amigos por usuário (anel: i é amigo de i±1…i±friends/2)")
	msgEvery = flag.Duration("msg", 60*time.Second, "cada usuário manda 1 envelope a um amigo a cada intervalo (0 = sem chat)")
	locEvery = flag.Duration("loc", 5*time.Second, "intervalo do Location (o servidor descarta < 4 s)")
	warm     = flag.Duration("warm", 20*time.Second, "aquecimento depois de conectar")
	measure  = flag.Duration("measure", 60*time.Second, "janela de medição")
	drain    = flag.Duration("drain", 10*time.Second, "espera, depois da janela, pelos envelopes e Acks em trânsito")
	dials    = flag.Int("dials", 200, "handshakes simultâneos")
	pid      = flag.Int("pid", 0, "pid do servidor para medir o RSS (0 = não mede)")
	maxKB    = flag.Float64("max-kb", 25, "meta: RSS por conexão (KB)")
	maxP99   = flag.Duration("p99", 50*time.Millisecond, "meta: latência p99")
	keep     = flag.Bool("keep", false, "não apaga os usuários semeados no fim")
)

const emailLike = "lt-%@load.test"

type user struct{ id, token string }

type client struct {
	u    user
	ws   *websocket.Conn
	wmu  sync.Mutex // gorilla aceita um escritor por vez: o sender e os Acks do leitor
	wbuf []byte
	peer []user
}

// envs: chave "remetente|client_msg_id".
var (
	measuring, stopping              atomic.Bool
	locSent, locRecv, envSent, drops atomic.Int64
	errCodes                         sync.Map // código → *atomic.Int64
	latMu                            sync.Mutex
	locLat, envLat                   []time.Duration
	envMu                            sync.Mutex
	envs                             = map[string]*envState{}
	msgSeq                           atomic.Int64
)

type envState struct {
	sent     time.Time
	got, dup int
	acked    bool
}

func main() {
	flag.Parse()
	ctx := context.Background()
	seed, err := base64.StdEncoding.DecodeString(*keyB64)
	if err != nil || len(seed) != ed25519.SeedSize {
		fail("-key/AUTH_SIGNING_KEY: seed Ed25519 de 32 bytes em base64")
	}
	key := ed25519.NewKeyFromSeed(seed)
	if *nFriends < 2 || *nFriends >= *n {
		fail("-friends entre 2 e n-1")
	}

	db, err := pgx.Connect(ctx, *dbURL)
	if err != nil {
		fail("banco: %v", err)
	}
	t0 := time.Now()
	users := seedUsers(ctx, db, key)
	fmt.Printf("SEED %d usuários, %d amizades em %.1fs\n", len(users), len(users)**nFriends/2, time.Since(t0).Seconds())
	cleanup := func() {
		if !*keep {
			if _, err := db.Exec(ctx, `DELETE FROM users WHERE email LIKE $1`, emailLike); err != nil {
				fmt.Println("limpeza:", err)
			}
		}
		_ = db.Close(ctx)
	}
	if err := preflight(users[0]); err != nil {
		cleanup()
		fail("%v", err)
	}

	idle := rss()
	clients := connect(users)
	connRSS := rss()
	time.Sleep(*warm)

	fmt.Println("MEASURE_START")
	cpu0, wall0 := cpuTime(), time.Now()
	measuring.Store(true)
	peak := connRSS
	for end := time.Now().Add(*measure); time.Now().Before(end); time.Sleep(2 * time.Second) {
		peak = max(peak, rss())
	}
	measuring.Store(false)
	cores := (cpuTime() - cpu0).Seconds() / time.Since(wall0).Seconds()
	fmt.Println("MEASURE_END")
	stopping.Store(true)
	time.Sleep(*drain)
	for _, c := range clients {
		_ = c.ws.Close()
	}
	code := report(len(clients), idle, connRSS, peak, cores)
	cleanup()
	os.Exit(code)
}

func seedUsers(ctx context.Context, db *pgx.Conn, key ed25519.PrivateKey) []user {
	if _, err := db.Exec(ctx, `DELETE FROM users WHERE email LIKE $1`, emailLike); err != nil {
		fail("limpeza: %v", err)
	}
	users := make([]user, *n)
	rows := make([][]any, *n)
	exp := time.Now().Add(*warm + *measure + *drain + 30*time.Minute).Unix() // o servidor fecha o WS no exp
	for i := range users {
		id := uuid()
		users[i] = user{id: id, token: sign(key, map[string]any{"sub": id, "iat": time.Now().Unix(), "exp": exp, "tv": 0})}
		rows[i] = []any{id, fmt.Sprintf("lt-%d@load.test", i), friendCode()}
	}
	if _, err := db.CopyFrom(ctx, pgx.Identifier{"users"}, []string{"id", "email", "friend_code"},
		pgx.CopyFromRows(rows)); err != nil {
		fail("seed users: %v", err)
	}
	var pairs [][]any
	for i := range users {
		for d := 1; d <= *nFriends/2; d++ {
			pairs = append(pairs, []any{users[i].id, users[(i+d)%*n].id, "accepted"})
		}
	}
	if _, err := db.CopyFrom(ctx, pgx.Identifier{"friendships"}, []string{"requester_id", "addressee_id", "status"},
		pgx.CopyFromRows(pairs)); err != nil {
		fail("seed friendships: %v", err)
	}
	return users
}

// preflight pega chave ou tv errados antes de abrir milhares de conexões.
func preflight(u user) error {
	base := strings.Replace(strings.Split(*urls, ",")[0], "ws", "http", 1)
	base = strings.TrimSuffix(base, "/v1/ws")
	req, _ := http.NewRequest("GET", base+"/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+u.token)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("preflight: %w", err)
	}
	defer r.Body.Close()
	var me struct{ ID string }
	_ = json.NewDecoder(r.Body).Decode(&me)
	if r.StatusCode != 200 || me.ID != u.id {
		return fmt.Errorf("preflight GET /v1/me: %d (id %q): confira -key e o banco", r.StatusCode, me.ID)
	}
	return nil
}

func connect(users []user) []*client {
	byID := make(map[string]int, len(users))
	for i, u := range users {
		byID[u.id] = i
	}
	eps := strings.Split(*urls, ",")
	d := websocket.Dialer{ReadBufferSize: 1024, WriteBufferSize: 1024, HandshakeTimeout: 30 * time.Second}
	out := make([]*client, len(users))
	sem := make(chan struct{}, *dials)
	var wg sync.WaitGroup
	var ok atomic.Int64
	t0 := time.Now()
	for i, u := range users {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			var ws *websocket.Conn
			var err error
			for try := 0; try < 20; try++ {
				if ws, _, err = d.Dial(eps[i%len(eps)], nil); err == nil {
					break
				}
				time.Sleep(time.Duration(100+randN(400)) * time.Millisecond)
			}
			if err != nil {
				fmt.Printf("dial %d: %v\n", i, err)
				return
			}
			c := &client{u: u, ws: ws, wbuf: make([]byte, 0, 512)}
			for dd := 1; dd <= *nFriends/2; dd++ {
				c.peer = append(c.peer, users[(i+dd)%len(users)], users[(i-dd+len(users))%len(users)])
			}
			if c.write(func(b []byte) []byte { return wire.AppendAuth(b, []byte(u.token)) }) != nil {
				return
			}
			out[i] = c
			ok.Add(1)
			go c.read()
		})
	}
	wg.Wait()
	fmt.Printf("CONNECTED %d/%d em %.1fs\n", ok.Load(), len(users), time.Since(t0).Seconds())
	out = slices.DeleteFunc(out, func(c *client) bool { return c == nil })
	for _, c := range out {
		go c.send()
	}
	return out
}

func (c *client) write(frame func([]byte) []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.wbuf = frame(c.wbuf[:0])
	return c.ws.WriteMessage(websocket.BinaryMessage, c.wbuf)
}

// send começa em fase aleatória para as conexões não mandarem em rajada.
func (c *client) send() {
	time.Sleep(time.Duration(randN(int64(*locEvery))))
	loc := time.NewTicker(*locEvery)
	var msg <-chan time.Time
	if *msgEvery > 0 {
		t := time.NewTimer(time.Duration(randN(int64(*msgEvery))))
		msg = t.C
	}
	payload := make([]byte, 200)
	for !stopping.Load() {
		select {
		case <-loc.C:
			binary.BigEndian.PutUint64(payload, uint64(time.Now().UnixNano()))
			if c.write(func(b []byte) []byte { return wire.AppendLocation(b, payload[:100]) }) != nil {
				return
			}
			if measuring.Load() {
				locSent.Add(1)
			}
		case <-msg:
			to := c.peer[randN(int64(len(c.peer)))]
			id := strconv.FormatInt(msgSeq.Add(1), 36)
			now := time.Now()
			binary.BigEndian.PutUint64(payload, uint64(now.UnixNano()))
			if measuring.Load() {
				envMu.Lock()
				envs[c.u.id+"|"+id] = &envState{sent: now}
				envMu.Unlock()
				envSent.Add(1)
			}
			if c.write(func(b []byte) []byte { return wire.AppendEnvelope(b, []byte(to.id), []byte(id), payload) }) != nil {
				return
			}
			msg = time.After(*msgEvery)
		}
	}
}

func (c *client) read() {
	for {
		_, m, err := c.ws.ReadMessage()
		if err != nil {
			if !stopping.Load() {
				drops.Add(1)
			}
			return
		}
		f, err := wire.Decode(m)
		if err != nil {
			count("decode:" + err.Error())
			continue
		}
		switch f.Type {
		case wire.LocationUpdate:
			if measuring.Load() && len(f.Payload) >= 8 {
				locRecv.Add(1)
				if randN(10) == 0 { // amostra: ~30 mil/s com 15 mil conexões
					lat(&locLat, f.Payload)
				}
			}
		case wire.Envelope:
			k := string(f.Peer) + "|" + string(f.MsgID)
			envMu.Lock()
			if s := envs[k]; s != nil {
				if s.got++; s.got > 1 {
					s.dup++
				} else if len(f.Payload) >= 8 {
					lat(&envLat, f.Payload)
				}
			}
			envMu.Unlock()
			from, id := slices.Clone(f.Peer), slices.Clone(f.MsgID) // f fatia m
			if c.write(func(b []byte) []byte { return wire.AppendAck(b, from, id) }) != nil {
				return
			}
		case wire.Ack: // S→C: o servidor gravou o envelope
			envMu.Lock()
			if s := envs[c.u.id+"|"+string(f.MsgID)]; s != nil {
				s.acked = true
			}
			envMu.Unlock()
		case wire.Error:
			count(string(f.Code))
		}
	}
}

func randN(n int64) int64 {
	return mrand.Int64N(n) // #nosec G404 -- gerador de carga, não é segredo
}

func lat(dst *[]time.Duration, payload []byte) {
	d := time.Since(time.Unix(0, int64(binary.BigEndian.Uint64(payload)))) // #nosec G115 -- nanos do próprio gerador
	latMu.Lock()
	*dst = append(*dst, d)
	latMu.Unlock()
}

func count(code string) {
	v, _ := errCodes.LoadOrStore(code, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func report(conns int, idle, connRSS, peak int64, cores float64) int {
	sec := measure.Seconds()
	latMu.Lock()
	pl, pe := pcts(locLat), pcts(envLat)
	latMu.Unlock()
	var lost, dup, noack, delivered int
	envMu.Lock()
	for _, s := range envs {
		if s.got == 0 {
			lost++
		} else {
			delivered++
		}
		dup += s.dup
		if !s.acked {
			noack++
		}
	}
	envMu.Unlock()

	locTx, locRx := float64(locSent.Load())/sec, float64(locRecv.Load())/sec
	envTx, envRx := float64(envSent.Load())/sec, float64(delivered)/sec
	fmt.Printf("RESULT conexões=%d quedas=%d\n", conns, drops.Load())
	fmt.Printf("  location: enviadas/s=%.0f recebidas/s=%.0f p50=%v p99=%v max=%v\n", locTx, locRx, pl[0], pl[1], pl[2])
	fmt.Printf("  envelope: enviados/s=%.1f entregues/s=%.1f enviados=%d perdidos=%d sem_ack=%d duplicados=%d p50=%v p99=%v max=%v\n",
		envTx, envRx, envSent.Load(), lost, noack, dup, pe[0], pe[1], pe[2])
	errCodes.Range(func(k, v any) bool {
		fmt.Printf("  Error{%s}=%d\n", k, v.(*atomic.Int64).Load())
		return true
	})

	ms := func(d time.Duration) string { return fmt.Sprintf("%.2f ms", float64(d)/1e6) }
	yes := func(ok bool) string {
		if ok {
			return "ok"
		}
		return "**FALHOU**"
	}
	okConn, okLoss := conns == *n, lost == 0 && noack == 0
	okP99 := pl[1] < *maxP99 && pe[1] < *maxP99
	rows := [][3]string{
		{"Conexões", fmt.Sprintf("%d de %d (quedas %d)", conns, *n, drops.Load()), yes(okConn)},
	}
	okRAM := true
	if *pid != 0 && conns > 0 {
		perConn := float64(peak-idle) / float64(conns)
		okRAM = perConn <= *maxKB
		rows = append(rows,
			[3]string{"RAM ociosa / conectada / pico", fmt.Sprintf("%.1f / %.1f / %.1f MB",
				float64(idle)/1024, float64(connRSS)/1024, float64(peak)/1024), ""},
			[3]string{"RAM por conexão (pico − ocioso)", fmt.Sprintf("%.2f KB", perConn), yes(okRAM)},
			[3]string{"CPU média do servidor", fmt.Sprintf("%.2f núcleo", cores), ""})
	}
	rows = append(rows,
		[3]string{"Location enviadas/s · recebidas/s", fmt.Sprintf("%.0f · %.0f", locTx, locRx), ""},
		[3]string{"Envelopes enviados/s · entregues/s", fmt.Sprintf("%.1f · %.1f", envTx, envRx), ""},
		[3]string{"Latência posição p50 / p99 / max", ms(pl[0]) + " / " + ms(pl[1]) + " / " + ms(pl[2]), yes(pl[1] < *maxP99)},
		[3]string{"Latência envelope p50 / p99 / max", ms(pe[0]) + " / " + ms(pe[1]) + " / " + ms(pe[2]), yes(pe[1] < *maxP99)},
		[3]string{"Envelopes perdidos · sem Ack · duplicados", fmt.Sprintf("%d · %d · %d", lost, noack, dup), yes(okLoss)})
	fmt.Printf("\n| Métrica (%d conexões, %d amigos, janela de %v) | Valor | Meta |\n|---|---|---|\n", *n, *nFriends, *measure)
	for _, r := range rows {
		fmt.Printf("| %s | %s | %s |\n", r[0], r[1], r[2])
	}
	fmt.Printf("\nMetas: %d conexões, ≤ %.0f KB/conexão, p99 < %v, 0 perdas e 0 sem Ack.\n", *n, *maxKB, *maxP99)

	if okConn && okLoss && okP99 && okRAM {
		fmt.Println("METAS OK")
		return 0
	}
	fmt.Println("METAS FALHARAM")
	return 1
}

// pcts: p50, p99, max.
func pcts(l []time.Duration) [3]time.Duration {
	if len(l) == 0 {
		return [3]time.Duration{}
	}
	slices.Sort(l)
	return [3]time.Duration{l[len(l)/2], l[(len(l)-1)*99/100], l[len(l)-1]}
}

// rss do servidor em KB.
func rss() int64 {
	if *pid == 0 {
		return 0
	}
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(*pid)).Output() // #nosec G204 -- pid numérico
	if err != nil {
		fail("ps do pid %d: %v", *pid, err)
	}
	v, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return v
}

func cpuTime() time.Duration {
	if *pid == 0 {
		return 0
	}
	out, err := exec.Command("ps", "-o", "time=", "-p", strconv.Itoa(*pid)).Output() // #nosec G204 -- pid numérico
	if err != nil {
		fail("ps do pid %d: %v", *pid, err)
	}
	return parseCPU(strings.TrimSpace(string(out)))
}

// parseCPU lê o "time" do ps: m:ss.ss (macOS), [dd-]hh:mm:ss (Linux).
func parseCPU(f string) time.Duration {
	var days float64
	if d, rest, ok := strings.Cut(f, "-"); ok {
		days, _ = strconv.ParseFloat(d, 64)
		f = rest
	}
	var sec float64
	for _, p := range strings.Split(f, ":") {
		v, _ := strconv.ParseFloat(p, 64)
		sec = sec*60 + v
	}
	return time.Duration((days*86400 + sec) * float64(time.Second))
}

func sign(key ed25519.PrivateKey, claims map[string]any) string {
	seg := func(v any) string { j, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(j) }
	in := seg(map[string]string{"alg": "EdDSA", "typ": "JWT"}) + "." + seg(claims)
	return in + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(in)))
}

func uuid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func friendCode() string {
	const abc = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	out := make([]byte, 8)
	for i := range out {
		v, _ := rand.Int(rand.Reader, big.NewInt(32))
		out[i] = abc[v.Int64()]
	}
	return string(out)
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "loadtest: "+format+"\n", a...)
	os.Exit(2)
}
