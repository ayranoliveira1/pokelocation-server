package keys

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/friends"
	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

const (
	nobody     = "00000000-0000-4000-8000-000000000000"
	lastResort = 999999 // key_id da last resort nos bundles de teste: fora das faixas das one-time
)

type env struct {
	t    *testing.T
	pool *pgxpool.Pool
	mux  *http.ServeMux
}

func newEnv(t *testing.T) *env {
	t.Helper()
	t.Parallel()
	pool := testdb.Pool(t)
	mux := http.NewServeMux()
	New(pool).Routes(mux, func(h http.HandlerFunc) http.Handler { return h })
	return &env{t, pool, mux}
}

func (e *env) user() string {
	e.t.Helper()
	var id string
	err := e.pool.QueryRow(context.Background(),
		`INSERT INTO users (email, friend_code) VALUES (gen_random_uuid()::text || '@x', $1) RETURNING id`,
		friends.NewCode()).Scan(&id)
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *env) exec(q string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), q, args...); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) friends(a, b string) {
	e.exec(`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'accepted')`, a, b)
}

func (e *env) do(user, method, path string, body []byte) (int, []byte) {
	e.t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req = req.WithContext(httpx.WithUser(req.Context(), user))
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, req)
	return w.Code, w.Body.Bytes()
}

func (e *env) want(user, method, path string, body []byte, status int, code string) []byte {
	e.t.Helper()
	got, out := e.do(user, method, path, body)
	if got != status {
		e.t.Fatalf("%s %s: %d %s, want %d %s", method, path, got, out, status, code)
	}
	if code != "" {
		var er struct{ Error string }
		if json.Unmarshal(out, &er); er.Error != code {
			e.t.Fatalf("%s %s: %s, want error %q", method, path, out, code)
		}
	}
	return out
}

// ---- chaves falsas no formato da libsignal ----

func rnd(n int, prefix byte) []byte {
	b := make([]byte, n)
	rand.Read(b)
	if prefix != 0 {
		b[0] = prefix
	}
	return b
}

func ec() []byte    { return rnd(33, 0x05) }
func kyber() []byte { return rnd(1569, 0x08) }
func sig() []byte   { return rnd(64, 0) }

type key struct {
	KeyID     any    `json:"key_id,omitempty"`
	PublicKey []byte `json:"public_key,omitempty"`
	Signature []byte `json:"signature,omitempty"`
}

type body struct {
	RegistrationID  any    `json:"registration_id,omitempty"`
	IdentityKey     []byte `json:"identity_key,omitempty"`
	SignedPrekey    *key   `json:"signed_prekey,omitempty"`
	KyberLastResort *key   `json:"kyber_last_resort,omitempty"`
	OneTimePrekeys  []key  `json:"one_time_prekeys"`
	KyberPrekeys    []key  `json:"kyber_prekeys"`
}

// mkBundle: válido com one-time EC de ids ecFrom..ecFrom+nEC-1 e Kyber idem.
func mkBundle(identity []byte, ecFrom, nEC, kyFrom, nKy int) *body {
	b := &body{
		RegistrationID:  1234,
		IdentityKey:     identity,
		SignedPrekey:    &key{KeyID: 1, PublicKey: ec(), Signature: sig()},
		KyberLastResort: &key{KeyID: lastResort, PublicKey: kyber(), Signature: sig()},
		OneTimePrekeys:  []key{},
		KyberPrekeys:    []key{},
	}
	for i := range nEC {
		b.OneTimePrekeys = append(b.OneTimePrekeys, key{KeyID: ecFrom + i, PublicKey: ec()})
	}
	for i := range nKy {
		b.KyberPrekeys = append(b.KyberPrekeys, key{KeyID: kyFrom + i, PublicKey: kyber(), Signature: sig()})
	}
	return b
}

func (b *body) json() []byte {
	j, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}
	return j
}

func (e *env) put(user string, b *body) {
	e.t.Helper()
	e.want(user, "PUT", "/v1/keys", b.json(), 204, "")
}

func (e *env) count(user string) (ec, ky int) {
	e.t.Helper()
	var c struct{ EC, Kyber int }
	if err := json.Unmarshal(e.want(user, "GET", "/v1/keys/count", nil, 200, ""), &c); err != nil {
		e.t.Fatal(err)
	}
	return c.EC, c.Kyber
}

type got struct {
	RegistrationID int    `json:"registration_id"`
	IdentityKey    []byte `json:"identity_key"`
	SignedPrekey   key    `json:"signed_prekey"`
	KyberPrekey    key    `json:"kyber_prekey"`
	OneTimePrekey  *key   `json:"one_time_prekey"`
}

func (e *env) fetch(me, of string) got {
	e.t.Helper()
	var g got
	if err := json.Unmarshal(e.want(me, "GET", "/v1/keys/"+of, nil, 200, ""), &g); err != nil {
		e.t.Fatal(err)
	}
	return g
}

func id(k key) int { return int(k.KeyID.(float64)) }

func TestPutFetch(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	up := mkBundle(ec(), 10, 2, 20, 2)
	e.put(a, up)
	if ec, ky := e.count(a); ec != 2 || ky != 2 {
		t.Fatalf("count = %d/%d, want 2/2", ec, ky)
	}

	seenEC, seenKy := map[int]bool{}, map[int]bool{}
	for i := range 3 {
		g := e.fetch(b, a)
		if g.RegistrationID != 1234 || !bytes.Equal(g.IdentityKey, up.IdentityKey) ||
			id(g.SignedPrekey) != 1 || !bytes.Equal(g.SignedPrekey.PublicKey, up.SignedPrekey.PublicKey) ||
			!bytes.Equal(g.SignedPrekey.Signature, up.SignedPrekey.Signature) {
			t.Fatalf("fixos do bundle errados: %+v", g)
		}
		if i < 2 { // one-time ainda disponíveis
			k := id(g.KyberPrekey)
			want := up.KyberPrekeys[k-20]
			if seenKy[k] || !bytes.Equal(g.KyberPrekey.PublicKey, want.PublicKey) || !bytes.Equal(g.KyberPrekey.Signature, want.Signature) {
				t.Fatalf("kyber one-time %d errada/repetida", k)
			}
			seenKy[k] = true
			if g.OneTimePrekey == nil {
				t.Fatal("sem one-time EC com estoque")
			}
			k = id(*g.OneTimePrekey)
			if seenEC[k] || !bytes.Equal(g.OneTimePrekey.PublicKey, up.OneTimePrekeys[k-10].PublicKey) || g.OneTimePrekey.Signature != nil {
				t.Fatalf("EC one-time %d errada/repetida", k)
			}
			seenEC[k] = true
			continue
		}
		// acabaram: kyber = last resort, sem EC
		if id(g.KyberPrekey) != lastResort || !bytes.Equal(g.KyberPrekey.PublicKey, up.KyberLastResort.PublicKey) ||
			!bytes.Equal(g.KyberPrekey.Signature, up.KyberLastResort.Signature) || g.OneTimePrekey != nil {
			t.Fatalf("sem estoque: %+v", g)
		}
	}
	if ec, ky := e.count(a); ec != 0 || ky != 0 {
		t.Fatalf("count depois de consumir = %d/%d", ec, ky)
	}
	_, raw := e.do(b, "GET", "/v1/keys/"+a, nil)
	if bytes.Contains(raw, []byte(`"one_time_prekey"`)) && !bytes.Contains(raw, []byte(`"one_time_prekey":null`)) {
		t.Fatalf("one_time_prekey presente sem estoque: %s", raw)
	}
}

// Reenvio acrescenta sem duplicar; troca de identity apaga as one-time antigas.
func TestPutAppendAndIdentityChange(t *testing.T) {
	e := newEnv(t)
	a, b := e.user(), e.user()
	e.friends(a, b)
	idA := ec()
	e.put(a, mkBundle(idA, 1, 2, 1, 2))
	e.put(a, mkBundle(idA, 2, 2, 2, 2)) // id 2 repetido
	if ec, ky := e.count(a); ec != 3 || ky != 3 {
		t.Fatalf("depois do append: %d/%d, want 3/3", ec, ky)
	}
	idB := ec()
	newer := mkBundle(idB, 50, 1, 60, 0)
	newer.RegistrationID = 99
	e.put(a, newer)
	if ec, ky := e.count(a); ec != 1 || ky != 0 {
		t.Fatalf("depois da troca de identity: %d/%d, want 1/0", ec, ky)
	}
	g := e.fetch(b, a)
	if !bytes.Equal(g.IdentityKey, idB) || g.RegistrationID != 99 || id(*g.OneTimePrekey) != 50 ||
		!bytes.Equal(g.KyberPrekey.PublicKey, newer.KyberLastResort.PublicKey) {
		t.Fatalf("bundle depois da troca: %+v", g)
	}
}

func TestPutInvalid(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	for name, mut := range map[string]func(b *body){
		"registration_id 0":       func(b *body) { b.RegistrationID = 0 },
		"registration_id 16381":   func(b *body) { b.RegistrationID = 16381 },
		"registration_id texto":   func(b *body) { b.RegistrationID = "1" },
		"sem identity":            func(b *body) { b.IdentityKey = nil },
		"identity 32":             func(b *body) { b.IdentityKey = rnd(32, 0x05) },
		"identity prefixo":        func(b *body) { b.IdentityKey = rnd(33, 0x06) },
		"sem signed":              func(b *body) { b.SignedPrekey = nil },
		"signed sem key_id":       func(b *body) { b.SignedPrekey.KeyID = nil },
		"signed key_id -1":        func(b *body) { b.SignedPrekey.KeyID = -1 },
		"signed key_id 2^24":      func(b *body) { b.SignedPrekey.KeyID = 1 << 24 },
		"signed key_id fração":    func(b *body) { b.SignedPrekey.KeyID = 1.5 },
		"signed sig 63":           func(b *body) { b.SignedPrekey.Signature = rnd(63, 0) },
		"signed sem sig":          func(b *body) { b.SignedPrekey.Signature = nil },
		"signed kyber":            func(b *body) { b.SignedPrekey.PublicKey = kyber() },
		"sem last resort":         func(b *body) { b.KyberLastResort = nil },
		"last resort ec":          func(b *body) { b.KyberLastResort.PublicKey = ec() },
		"last resort prefixo":     func(b *body) { b.KyberLastResort.PublicKey = rnd(1569, 0x05) },
		"last resort sem sig":     func(b *body) { b.KyberLastResort.Signature = nil },
		"ec one-time assinada":    func(b *body) { b.OneTimePrekeys[0].Signature = sig() },
		"ec one-time 34":          func(b *body) { b.OneTimePrekeys[0].PublicKey = rnd(34, 0x05) },
		"ec one-time sem key_id":  func(b *body) { b.OneTimePrekeys[0].KeyID = nil },
		"ec one-time key_id 2^24": func(b *body) { b.OneTimePrekeys[0].KeyID = 1 << 24 },
		"kyber one-time sem sig":  func(b *body) { b.KyberPrekeys[0].Signature = nil },
		"kyber one-time ec":       func(b *body) { b.KyberPrekeys[0].PublicKey = ec() },
		"101 ec":                  func(b *body) { *b = *mkBundle(b.IdentityKey, 0, 101, 0, 0) },
		"101 kyber":               func(b *body) { *b = *mkBundle(b.IdentityKey, 0, 0, 0, 101) },
	} {
		t.Run(name, func(t *testing.T) {
			b := mkBundle(ec(), 1, 1, 1, 1)
			mut(b)
			e.want(a, "PUT", "/v1/keys", b.json(), 400, "bad_request")
		})
	}
	for _, raw := range [][]byte{
		[]byte("x"),
		[]byte(`{"registration_id":1,"identity_key":"***"}`), // base64 errado
		oversize(), // válido, mas > 512 KiB por um campo desconhecido
	} {
		e.want(a, "PUT", "/v1/keys", raw, 400, "bad_request")
	}
	var n int
	if err := e.pool.QueryRow(context.Background(),
		`SELECT (SELECT count(*) FROM devices) + (SELECT count(*) FROM one_time_prekeys)`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("PUT inválido gravou %d linhas", n)
	}
}

// oversize: bundle válido que só passa do limite do corpo (campo desconhecido é ignorado, não o tamanho).
func oversize() []byte {
	j := mkBundle(ec(), 0, 1, 0, 1).json()
	pad := maxBody - len(j) + 1
	return append([]byte(`{"pad":"`+strings.Repeat("a", pad)+`",`), j[1:]...)
}

// No máximo 200 guardadas por tipo; o PUT que passaria disso é recusado inteiro.
func TestStoredCap(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	idA := ec()
	e.put(a, mkBundle(idA, 0, 100, 0, 100))
	e.put(a, mkBundle(idA, 100, 100, 100, 100))
	e.want(a, "PUT", "/v1/keys", mkBundle(idA, 200, 1, 0, 0).json(), 400, "bad_request")
	e.want(a, "PUT", "/v1/keys", mkBundle(idA, 0, 0, 200, 1).json(), 400, "bad_request")
	e.put(a, mkBundle(idA, 0, 1, 0, 1)) // repetidas não contam
	if ec, ky := e.count(a); ec != 200 || ky != 200 {
		t.Fatalf("count = %d/%d, want 200/200", ec, ky)
	}
}

// Só amigo aceito sem bloqueio em nenhum sentido lê (e consome) as chaves.
func TestFetchPermissions(t *testing.T) {
	e := newEnv(t)
	a, friend, stranger, asker, blocker, blocked, nokeys := e.user(), e.user(), e.user(), e.user(), e.user(), e.user(), e.user()
	e.put(a, mkBundle(ec(), 1, 5, 1, 5))
	e.friends(a, friend)
	e.exec(`INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, 'pending')`, asker, a)
	// bloqueio com a amizade ainda lá (o friends apaga; aqui se testa a defesa do keys)
	e.friends(a, blocker)
	e.exec(`INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2)`, a, blocker)
	e.friends(blocked, a)
	e.exec(`INSERT INTO blocks (blocker_id, blocked_id) VALUES ($1, $2)`, blocked, a)
	e.friends(a, nokeys)

	for name, c := range map[string]struct{ me, of string }{
		"estranho":            {stranger, a},
		"pedido pendente":     {asker, a},
		"a bloqueou":          {blocker, a},
		"bloqueou a":          {blocked, a},
		"si mesmo":            {a, a},
		"inexistente":         {a, nobody},
		"id malformado":       {a, "x"},
		"id em maiúsculas ok": {friend, strings.ToUpper(a)},
	} {
		if strings.HasSuffix(name, " ok") {
			e.want(c.me, "GET", "/v1/keys/"+c.of, nil, 200, "")
			continue
		}
		e.want(c.me, "GET", "/v1/keys/"+c.of, nil, 403, "forbidden")
	}
	if ec, ky := e.count(a); ec != 4 || ky != 4 {
		t.Fatalf("leituras recusadas consumiram chaves: %d/%d, want 4/4", ec, ky)
	}
	e.want(a, "GET", "/v1/keys/"+nokeys, nil, 404, "not_found")
}

// Leituras simultâneas nunca entregam a mesma one-time duas vezes.
func TestFetchConcurrent(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	const n = 10
	e.put(a, mkBundle(ec(), 0, n, 0, n))
	readers := make([]string, 2*n)
	for i := range readers {
		readers[i] = e.user()
		e.friends(readers[i], a)
	}
	var (
		mu      sync.Mutex
		ecIDs   = map[int]int{}
		kyIDs   = map[int]int{}
		wg      sync.WaitGroup
		failure string
	)
	for _, r := range readers {
		wg.Go(func() {
			status, raw := e.do(r, "GET", "/v1/keys/"+a, nil)
			var g got
			mu.Lock()
			defer mu.Unlock()
			if status != 200 || json.Unmarshal(raw, &g) != nil {
				failure = fmt.Sprintf("%d %s", status, raw)
				return
			}
			kyIDs[id(g.KyberPrekey)]++
			if g.OneTimePrekey != nil {
				ecIDs[id(*g.OneTimePrekey)]++
			}
		})
	}
	wg.Wait()
	if failure != "" {
		t.Fatal(failure)
	}
	if len(ecIDs) != n {
		t.Fatalf("EC entregues: %v, want %d distintas", ecIDs, n)
	}
	for k, c := range ecIDs {
		if c != 1 {
			t.Fatalf("EC %d entregue %d vezes", k, c)
		}
	}
	if kyIDs[lastResort] != n || len(kyIDs) != n+1 { // n one-time distintas + n vezes a last resort
		t.Fatalf("Kyber entregues: %v", kyIDs)
	}
	for k, c := range kyIDs {
		if k != lastResort && c != 1 {
			t.Fatalf("Kyber %d entregue %d vezes", k, c)
		}
	}
}

func TestUnavailable(t *testing.T) {
	e := newEnv(t)
	a := e.user()
	pool, err := pgxpool.New(context.Background(), e.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	e.mux = http.NewServeMux()
	New(pool).Routes(e.mux, func(h http.HandlerFunc) http.Handler { return h })
	e.want(a, "GET", "/v1/keys/count", nil, 503, "unavailable")
	e.want(a, "PUT", "/v1/keys", mkBundle(ec(), 0, 1, 0, 1).json(), 503, "unavailable")
	e.want(a, "GET", "/v1/keys/"+nobody, nil, 503, "unavailable")
}

// 10 por minuto por par (quem pede, dono): um amigo não esvazia as one-time do outro.
func TestFetchRateLimit(t *testing.T) {
	e := newEnv(t)
	a, b, c := e.user(), e.user(), e.user()
	e.put(a, mkBundle(ec(), 0, 20, 0, 20))
	e.friends(a, b)
	e.friends(a, c)
	for range 10 {
		e.fetch(b, a)
	}
	e.want(b, "GET", "/v1/keys/"+a, nil, 429, "rate_limited")
	e.fetch(c, a) // outro par não é afetado
	if ec, ky := e.count(a); ec != 9 || ky != 9 {
		t.Fatalf("recusa consumiu chaves: %d/%d, want 9/9", ec, ky)
	}
}
