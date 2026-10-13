package apitest

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const nobody = "00000000-0000-0000-0000-000000000001"

// Toda rota 🔒 sem token → 401 (docs/api.md §1). Rotas que ainda não existem são puladas.
func TestRotasExigemToken(t *testing.T) {
	s := start(t)
	for _, r := range []struct{ m, p string }{
		{"GET", "/v1/me"},
		{"GET", "/v1/friends"},
		{"POST", "/v1/friends/requests"},
		{"POST", "/v1/friends/requests/" + nobody + "/accept"},
		{"POST", "/v1/friends/requests/" + nobody + "/decline"},
		{"DELETE", "/v1/friends/" + nobody},
		{"POST", "/v1/blocks/" + nobody},
		{"DELETE", "/v1/blocks/" + nobody},
		{"PUT", "/v1/keys"},
		{"GET", "/v1/keys/count"},
		{"GET", "/v1/keys/" + nobody},
		{"GET", "/v1/envelopes"},
		{"POST", "/v1/envelopes/ack"},
		{"GET", "/v1/locations"},
	} {
		t.Run(r.m+" "+r.p, func(t *testing.T) {
			s := s.on(t)
			s.need(r.m, r.p)
			s.want(s.do(r.m, r.p, "", nil), 401, "unauthorized")
		})
	}
}

// Pedido, aceite, bloqueio e quem pode ler as chaves de quem. docs/api.md §3 e §4.
func TestMatrizAmigos(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/friends/requests")
	a, b, c := s.register("a"), s.register("b"), s.register("c")
	req := func(from user, code string) resp {
		return s.do("POST", "/v1/friends/requests", from.access, map[string]string{"code": code})
	}

	// Pedido a → b, com o código digitado como gente digita (minúsculas, hífen): o servidor normaliza.
	r := req(a, strings.ToLower(b.code[:4])+"-"+b.code[4:])
	s.want(r, 201, "")
	reqID := r.str("id")
	if r.str("status") != "pending" || reqID == "" {
		t.Fatalf("pedido: %s", r.raw)
	}
	if r := req(a, b.code); !s.check("pedido repetido", r, 200, "") || r.str("id") != reqID {
		t.Errorf("pedido repetido não é idempotente: %s", r.raw)
	}
	s.check("pedido para si", req(a, a.code), 400, "self_request")
	s.check("código inválido", req(a, "!!!!!!!!"), 404, "code_not_found")
	s.check("código inexistente", req(a, "ZZZZZZZZ"), 404, "code_not_found")

	// Só o destinatário aceita. Qualquer outro caso é o mesmo 404 (não revela se o pedido existe).
	for name, x := range map[string]struct {
		u    user
		path string
	}{
		"terceiro aceita":   {c, "/accept"},
		"terceiro recusa":   {c, "/decline"},
		"quem pediu aceita": {a, "/accept"},
	} {
		s.check(name, s.do("POST", "/v1/friends/requests/"+reqID+x.path, x.u.access, nil), 404, "not_found")
	}
	s.check("aceitar pedido inexistente", s.do("POST", "/v1/friends/requests/"+nobody+"/accept", b.access, nil), 404, "not_found")
	s.check("aceitar id malformado", s.do("POST", "/v1/friends/requests/xyz/accept", b.access, nil), 404, "not_found")

	if l := friends(s, b); !slices.Contains(l["incoming"], a.id) {
		t.Errorf("b não vê o pedido de a: %v", l)
	}
	if l := friends(s, a); !slices.Contains(l["outgoing"], b.id) {
		t.Errorf("a não vê o próprio pedido: %v", l)
	}
	if l := friends(s, c); len(l["friends"])+len(l["incoming"])+len(l["outgoing"]) != 0 {
		t.Errorf("c vê dados de a/b: %v", l)
	}

	// Pedido pendente não é amizade: sem chaves.
	s.want(s.do("PUT", "/v1/keys", b.access, newBundle(2, 2)), 204, "")
	s.check("chaves com pedido pendente", s.do("GET", "/v1/keys/"+b.id, a.access, nil), 403, "forbidden")

	s.want(s.do("POST", "/v1/friends/requests/"+reqID+"/accept", b.access, nil), 204, "")
	s.check("aceitar de novo", s.do("POST", "/v1/friends/requests/"+reqID+"/accept", b.access, nil), 404, "not_found")
	s.check("pedido a amigo", req(a, b.code), 409, "already_friends")
	s.check("pedido a amigo (outro lado)", req(b, a.code), 409, "already_friends")
	if !slices.Contains(friends(s, a)["friends"], b.id) || !slices.Contains(friends(s, b)["friends"], a.id) {
		t.Error("amizade aceita não aparece nos dois lados")
	}

	// Chaves: só amigo aceito.
	s.want(s.do("PUT", "/v1/keys", a.access, newBundle(2, 2)), 204, "")
	s.check("amigo lê chaves", s.do("GET", "/v1/keys/"+a.id, b.access, nil), 200, "")
	s.check("estranho lê chaves", s.do("GET", "/v1/keys/"+a.id, c.access, nil), 403, "forbidden")
	s.check("chaves de inexistente", s.do("GET", "/v1/keys/"+nobody, c.access, nil), 403, "forbidden")
	s.check("chaves de si mesmo", s.do("GET", "/v1/keys/"+c.id, c.access, nil), 403, "forbidden")
	s.want(req(c, a.code), 201, "")
	s.check("chaves de quem eu pedi", s.do("GET", "/v1/keys/"+a.id, c.access, nil), 403, "forbidden")
	s.check("chaves de quem me pediu", s.do("GET", "/v1/keys/"+c.id, a.access, nil), 403, "forbidden")

	// Bloqueio: desfaz a amizade, vale nos dois sentidos e não é revelado a quem foi bloqueado.
	s.want(s.do("POST", "/v1/blocks/"+b.id, a.access, nil), 204, "")
	s.check("bloquear de novo", s.do("POST", "/v1/blocks/"+b.id, a.access, nil), 204, "")
	s.check("bloquear inexistente", s.do("POST", "/v1/blocks/"+nobody, a.access, nil), 404, "not_found")
	s.check("bloqueado lê chaves", s.do("GET", "/v1/keys/"+a.id, b.access, nil), 403, "forbidden")
	s.check("quem bloqueou lê chaves", s.do("GET", "/v1/keys/"+b.id, a.access, nil), 403, "forbidden")
	s.check("bloqueado pede amizade", req(b, a.code), 404, "code_not_found")
	s.check("quem bloqueou pede amizade", req(a, b.code), 403, "blocked")
	if slices.Contains(friends(s, a)["friends"], b.id) || slices.Contains(friends(s, b)["friends"], a.id) {
		t.Error("bloqueio não desfez a amizade")
	}

	// Desbloquear não refaz a amizade.
	s.want(s.do("DELETE", "/v1/blocks/"+b.id, a.access, nil), 204, "")
	s.check("desbloquear de novo", s.do("DELETE", "/v1/blocks/"+b.id, a.access, nil), 204, "")
	s.check("chaves depois de desbloquear", s.do("GET", "/v1/keys/"+b.id, a.access, nil), 403, "forbidden")

	// DELETE /v1/friends desfaz pedido pendente, em qualquer direção, e é idempotente.
	s.want(s.do("DELETE", "/v1/friends/"+a.id, c.access, nil), 204, "")
	s.check("desfazer de novo", s.do("DELETE", "/v1/friends/"+a.id, c.access, nil), 204, "")
	if slices.Contains(friends(s, a)["incoming"], c.id) {
		t.Error("pedido de c continua depois do DELETE")
	}

	// [proposta] Quem pediu pode cancelar (decline do próprio pedido).
	r = req(a, c.code)
	s.want(r, 201, "")
	s.check("cancelar o próprio pedido", s.do("POST", "/v1/friends/requests/"+r.str("id")+"/decline", a.access, nil), 204, "")
	// [proposta] Pedido cruzado aceita na hora.
	s.want(req(b, a.code), 201, "")
	if r := req(a, b.code); !s.check("pedido cruzado", r, 200, "") || r.str("status") != "accepted" {
		t.Errorf("pedido cruzado não virou amizade: %s", r.raw)
	}
}

// Validação, consumo atômico das one-time e last resort. docs/api.md §4.
func TestMatrizChaves(t *testing.T) {
	s := start(t)
	s.need("PUT", "/v1/keys")
	a, b := s.register("a"), s.register("b")
	befriend(s, a, b)
	put := func(k bundle) resp { return s.do("PUT", "/v1/keys", a.access, k) }

	for name, mut := range map[string]func(*bundle){
		"identity 32 bytes":         func(k *bundle) { k.IdentityKey = k.IdentityKey[:32] },
		"identity sem 0x05":         func(k *bundle) { k.IdentityKey[0] = 6 },
		"identity ausente":          func(k *bundle) { k.IdentityKey = nil },
		"assinatura 63 bytes":       func(k *bundle) { k.SignedPrekey.Signature = k.SignedPrekey.Signature[:63] },
		"signed sem assinatura":     func(k *bundle) { k.SignedPrekey.Signature = nil },
		"last resort 1568 bytes":    func(k *bundle) { k.KyberLastResort.PublicKey = k.KyberLastResort.PublicKey[:1568] },
		"last resort sem 0x08":      func(k *bundle) { k.KyberLastResort.PublicKey[0] = 5 },
		"last resort sem assin.":    func(k *bundle) { k.KyberLastResort.Signature = nil },
		"kyber one-time sem assin.": func(k *bundle) { k.Kyber[0].Signature = nil },
		"one-time EC 34 bytes":      func(k *bundle) { k.OneTime[0].PublicKey = append(k.OneTime[0].PublicKey, 0) },
		"registration_id 0":         func(k *bundle) { k.RegistrationID = 0 },
		"registration_id 16381":     func(k *bundle) { k.RegistrationID = 16381 },
		"key_id negativo":           func(k *bundle) { k.OneTime[0].KeyID = -1 },
		"key_id 2^24":               func(k *bundle) { k.SignedPrekey.KeyID = 1 << 24 },
		"101 one-time EC":           func(k *bundle) { k.OneTime = newBundle(101, 0).OneTime },
		"101 one-time Kyber":        func(k *bundle) { k.Kyber = newBundle(0, 101).Kyber },
	} {
		k := newBundle(1, 1)
		mut(&k)
		s.check(name, put(k), 400, "bad_request")
	}
	s.check("JSON quebrado", s.do("PUT", "/v1/keys", a.access, `{"registration_id":`), 400, "bad_request")
	s.check("amigo sem chaves", s.do("GET", "/v1/keys/"+a.id, b.access, nil), 404, "not_found")

	// Reenvio das mesmas one-time não duplica.
	k1 := newBundle(3, 3)
	s.want(put(k1), 204, "")
	s.want(put(k1), 204, "")
	checkCount(s, a, 3, 3)

	// Identity nova (reinstalação) apaga as one-time antigas.
	k2 := newBundle(1, 1)
	s.want(put(k2), 204, "")
	checkCount(s, a, 1, 1)

	got := fetch(s, b, a)
	if !slices.Equal(got.IdentityKey, k2.IdentityKey) || !slices.Equal(got.SignedPrekey.PublicKey, k2.SignedPrekey.PublicKey) ||
		got.RegistrationID != k2.RegistrationID {
		t.Error("bundle devolvido não é o enviado")
	}
	if got.OneTime == nil || got.OneTime.KeyID != k2.OneTime[0].KeyID || got.KyberPrekey.KeyID != k2.Kyber[0].KeyID {
		t.Errorf("não entregou as one-time: %+v", got)
	}
	checkCount(s, a, 0, 0)
	// Esgotadas: sem one-time EC e a Kyber é a last resort.
	if got := fetch(s, b, a); got.OneTime != nil || got.KyberPrekey.KeyID != k2.KyberLastResort.KeyID {
		t.Errorf("sem one-time: %+v", got)
	}

	// Consumo atômico: 4 amigos × 6 buscas simultâneas (limite de 10/min por par) para 20 one-time → 20 distintas e
	// 4 sem one-time.
	k3 := k2
	k3.OneTime, k3.Kyber = newBundle(20, 20).OneTime, newBundle(20, 20).Kyber
	s.want(put(k3), 204, "")
	fs := []user{b}
	for _, n := range []string{"f1", "f2", "f3"} {
		f := s.register(n)
		befriend(s, a, f)
		fs = append(fs, f)
	}
	burst := time.Now() // baldes dos pares cheios (10) aqui
	var mu sync.Mutex
	var ec, ky []int
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for i := range 24 {
		f := fs[i%len(fs)]
		wg.Go(func() {
			r, err := s.raw("GET", "/v1/keys/"+a.id, "Bearer "+f.access, nil)
			if err == nil && r.status != 200 {
				err = fmt.Errorf("%d %s", r.status, r.raw)
			}
			var got fetched
			if err == nil {
				err = json.Unmarshal(r.raw, &got)
			}
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if got.OneTime != nil {
				ec = append(ec, got.OneTime.KeyID)
			}
			if got.KyberPrekey.KeyID != k3.KyberLastResort.KeyID {
				ky = append(ky, got.KyberPrekey.KeyID)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error("busca simultânea:", err)
	}
	slices.Sort(ec)
	slices.Sort(ky)
	if len(ec) != 20 || len(slices.Compact(ec)) != 20 || len(ky) != 20 || len(slices.Compact(ky)) != 20 {
		t.Errorf("one-time entregues: ec=%v kyber=%v (esperado 20 distintas de cada)", ec, ky)
	}

	// 10 por minuto por par: f1 já gastou 6 (sobram 4 desde o burst). Outro par (f2) segue livre.
	f1, f2 := fs[1], fs[2]
	s.bucket("chaves por par", 4, 6*time.Second, burst, 200, func(int) resp {
		return s.do("GET", "/v1/keys/"+a.id, f1.access, nil)
	})
	s.check("outro par", s.do("GET", "/v1/keys/"+a.id, f2.access, nil), 200, "")

	// Teto de 200 guardadas por tipo.
	k4 := k3
	k4.Kyber = []preKey{}
	for i := range 2 {
		k4.OneTime = newBundle(100, 0).OneTime
		s.check(fmt.Sprint("lote ", i+1, " de 100"), put(k4), 204, "")
	}
	k4.OneTime = newBundle(1, 0).OneTime
	s.check("201ª one-time EC", put(k4), 400, "bad_request")
}

// 10 pedidos de amizade por minuto por usuário, contando os erros (docs/api.md §3: barra a varredura de códigos).
func TestPedidoRateLimit(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/friends/requests")
	x, y, z := s.register("x"), s.register("y"), s.register("z")
	// Só códigos inexistentes: o 429 chegar prova que os erros contam.
	s.bucket("pedidos com erro", 10, 6*time.Second, time.Now(), 404, func(i int) resp {
		return s.do("POST", "/v1/friends/requests", x.access, map[string]string{"code": fmt.Sprintf("ZZZZZZ%02d", i)})
	})
	s.check("outro usuário", s.do("POST", "/v1/friends/requests", z.access, map[string]string{"code": y.code}), 201, "")
}

// Tetos de §3: 500 amigos, 50 pedidos enviados, 50 recebidos pelo alvo → 429. Semeado por SQL: pela API o limite de
// 10 pedidos/min levaria minutos.
func TestTetosAmigos(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/friends/requests")
	a, b, c := s.register("a"), s.register("b"), s.register("c")

	// a com 50 pedidos enviados pendentes.
	seedFriends(s, a.id, 50, "pending", true)
	s.check("51º enviado", s.do("POST", "/v1/friends/requests", a.access, map[string]string{"code": b.code}), 429, "rate_limited")

	// b com 50 recebidos pendentes: c não consegue pedir.
	seedFriends(s, b.id, 50, "pending", false)
	s.check("51º recebido do alvo", s.do("POST", "/v1/friends/requests", c.access, map[string]string{"code": b.code}), 429, "rate_limited")
	if r := s.do("GET", "/v1/friends", b.access, nil); len(r.raw) > 64<<10 {
		t.Errorf("GET /v1/friends com 50 recebidos: %d bytes", len(r.raw))
	}

	// c com 500 amigos: não pede mais ninguém; a lista cabe em 64 KiB (o do() confere).
	seedFriends(s, c.id, 500, "accepted", true)
	s.want(s.do("GET", "/v1/friends", c.access, nil), 200, "")
	// O 501º barra no pedido ou no aceite (o contrato aceita os dois); o que não pode é virar amizade.
	if r := s.do("POST", "/v1/friends/requests", c.access, map[string]string{"code": a.code}); r.status != 429 {
		s.want(r, 201, "")
		s.check("aceite do 501º amigo", s.do("POST", "/v1/friends/requests/"+r.str("id")+"/accept", a.access, nil), 429, "rate_limited")
	}
	if n := len(friends(s, c)["friends"]); n != 500 {
		t.Errorf("c tem %d amigos (teto 500)", n)
	}
}

// ---- auxiliares ----

// seedFriends cria n usuários e n linhas em friendships com u (outgoing: u é quem pediu).
func seedFriends(s *server, u string, n int, status string, outgoing bool) {
	s.t.Helper()
	q := `WITH nu AS (
		INSERT INTO users (email, friend_code)
		SELECT 'seed-' || gen_random_uuid() || '@teste.dev',
		       (SELECT string_agg(substr('0123456789ABCDEFGHJKMNPQRSTVWXYZ', 1 + floor(random() * 32)::int, 1), '')
		          FROM generate_series(1, 8) WHERE g > 0)
		FROM generate_series(1, $2) g
		RETURNING id)
	INSERT INTO friendships (requester_id, addressee_id, status)
	SELECT CASE WHEN $4 THEN $1::uuid ELSE id END, CASE WHEN $4 THEN id ELSE $1::uuid END, $3 FROM nu`
	if _, err := s.pool.Exec(context.Background(), q, u, n, status, outgoing); err != nil {
		s.t.Fatal("seed:", err)
	}
}

func befriend(s *server, a, b user) {
	s.t.Helper()
	r := s.do("POST", "/v1/friends/requests", a.access, map[string]string{"code": b.code})
	s.want(r, 201, "")
	s.want(s.do("POST", "/v1/friends/requests/"+r.str("id")+"/accept", b.access, nil), 204, "")
}

// friends devolve os user_id de cada lista de GET /v1/friends.
func friends(s *server, u user) map[string][]string {
	s.t.Helper()
	r := s.do("GET", "/v1/friends", u.access, nil)
	s.want(r, 200, "")
	out := map[string][]string{}
	for _, k := range []string{"friends", "incoming", "outgoing"} {
		l, ok := r.body[k].([]any)
		if !ok {
			s.t.Errorf("GET /v1/friends sem a lista %q: %s", k, r.raw)
		}
		out[k] = []string{}
		for _, x := range l {
			m, _ := x.(map[string]any)
			id, _ := m["user_id"].(string)
			out[k] = append(out[k], id)
		}
	}
	return out
}

func checkCount(s *server, u user, ec, ky int) {
	s.t.Helper()
	r := s.do("GET", "/v1/keys/count", u.access, nil)
	s.want(r, 200, "")
	if r.body["ec"] != float64(ec) || r.body["kyber"] != float64(ky) {
		s.t.Errorf("count: esperado ec=%d kyber=%d, veio %s", ec, ky, r.raw)
	}
}

func fetch(s *server, from, of user) fetched {
	s.t.Helper()
	r := s.do("GET", "/v1/keys/"+of.id, from.access, nil)
	s.want(r, 200, "")
	var f fetched
	if err := json.Unmarshal(r.raw, &f); err != nil {
		s.t.Fatal(err)
	}
	return f
}

type preKey struct {
	KeyID     int    `json:"key_id"`
	PublicKey []byte `json:"public_key"`
	Signature []byte `json:"signature,omitempty"`
}

// bundle é o corpo de PUT /v1/keys, igual ao Api.keysJson do app. []byte vira base64 padrão.
type bundle struct {
	RegistrationID  int      `json:"registration_id"`
	IdentityKey     []byte   `json:"identity_key"`
	SignedPrekey    preKey   `json:"signed_prekey"`
	KyberLastResort preKey   `json:"kyber_last_resort"`
	OneTime         []preKey `json:"one_time_prekeys"`
	Kyber           []preKey `json:"kyber_prekeys"`
}

type fetched struct {
	RegistrationID int     `json:"registration_id"`
	IdentityKey    []byte  `json:"identity_key"`
	SignedPrekey   preKey  `json:"signed_prekey"`
	KyberPrekey    preKey  `json:"kyber_prekey"`
	OneTime        *preKey `json:"one_time_prekey"`
}

var keySeq struct {
	sync.Mutex
	n int
}

// newBundle gera chaves com os tamanhos da libsignal 0.86 (bytes aleatórios: o servidor não verifica assinatura).
// key_id únicos no binário inteiro, para o teste distinguir lotes.
func newBundle(nEC, nKyber int) bundle {
	k := bundle{
		RegistrationID:  1 + int(randBytes(1)[0]),
		IdentityKey:     pub(33, 5),
		SignedPrekey:    preKey{nextID(), pub(33, 5), randBytes(64)},
		KyberLastResort: preKey{nextID(), pub(1569, 8), randBytes(64)},
		OneTime:         []preKey{},
		Kyber:           []preKey{},
	}
	for range nEC {
		k.OneTime = append(k.OneTime, preKey{KeyID: nextID(), PublicKey: pub(33, 5)})
	}
	for range nKyber {
		k.Kyber = append(k.Kyber, preKey{nextID(), pub(1569, 8), randBytes(64)})
	}
	return k
}

func nextID() int {
	keySeq.Lock()
	defer keySeq.Unlock()
	keySeq.n++
	return keySeq.n
}

func pub(n int, typ byte) []byte {
	b := randBytes(n)
	b[0] = typ
	return b
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
