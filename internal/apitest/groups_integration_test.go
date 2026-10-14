package apitest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

func (w *wsConn) group(gid, id string, p []byte) {
	w.send(wire.AppendGroupEnvelope(nil, []byte(gid), []byte(id), p, nil))
}

func createGroup(s *server, owner user, members ...user) string {
	s.t.Helper()
	ids := []string{}
	for _, m := range members {
		ids = append(ids, m.id)
	}
	r := s.do("POST", "/v1/groups", owner.access, map[string]any{"members": ids})
	s.want(r, 201, "")
	return r.str("id")
}

func group(s *server, u user, gid string) (owner string, members []string, ok bool) {
	s.t.Helper()
	owner, m, ok := groupNames(s, u, gid)
	for id := range m {
		members = append(members, id)
	}
	return owner, members, ok
}

func groupNames(s *server, u user, gid string) (owner string, members map[string]string, ok bool) {
	s.t.Helper()
	r := s.do("GET", "/v1/groups/"+gid, u.access, nil)
	if r.status == 404 {
		return "", nil, false
	}
	s.want(r, 200, "")
	members = map[string]string{}
	l, _ := r.body["members"].([]any)
	for _, x := range l {
		m, _ := x.(map[string]any)
		members[fmt.Sprint(m["user_id"])] = fmt.Sprint(m["username"])
	}
	return r.str("owner_id"), members, true
}

// seedFriendIDs vai direto no banco: pela API, os limites levariam minutos.
func seedFriendIDs(s *server, u string, n int) []string {
	s.t.Helper()
	rows, err := s.pool.Query(context.Background(), `WITH nu AS (
		INSERT INTO users (email, username, friend_code)
		SELECT 'seed-' || gen_random_uuid() || '@teste.dev', 's' || substr(md5(random()::text), 1, 12),
		       (SELECT string_agg(substr('0123456789ABCDEFGHJKMNPQRSTVWXYZ', 1 + floor(random() * 32)::int, 1), '')
		          FROM generate_series(1, 8) WHERE g > 0)
		FROM generate_series(1, $2) g RETURNING id),
	f AS (INSERT INTO friendships (requester_id, addressee_id, status) SELECT $1::uuid, id, 'accepted' FROM nu)
	SELECT id::text FROM nu`, u, n)
	if err != nil {
		s.t.Fatal("seed:", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			s.t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		s.t.Fatal("seed:", err)
	}
	return ids
}

func TestGrupoMembros(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/groups")
	a, b, c, d, e := s.register("a"), s.register("b"), s.register("c"), s.register("d"), s.register("e")
	befriend(s, a, b)
	befriend(s, a, c)
	befriend(s, b, d)

	s.check("criar com quem não é amigo", s.do("POST", "/v1/groups", a.access, map[string]any{"members": []string{e.id}}), 403, "forbidden")
	s.check("grupo só com o dono", s.do("POST", "/v1/groups", e.access, map[string]any{"members": []string{}}), 201, "")
	gid := createGroup(s, a, b, c)

	if owner, m, ok := group(s, b, gid); !ok || owner != a.id || !slices.Contains(m, a.id) || len(m) != 3 {
		t.Errorf("GET do grupo por membro: dono %s, membros %v", owner, m)
	}
	if _, _, ok := group(s, e, gid); ok {
		t.Error("não-membro lê o grupo")
	}
	s.check("GET de grupo inexistente", s.do("GET", "/v1/groups/"+nobody, a.access, nil), 404, "not_found")
	l := s.do("GET", "/v1/groups", c.access, nil)
	s.want(l, 200, "")
	if !bytes.Contains(l.raw, []byte(gid)) {
		t.Errorf("GET /v1/groups de membro sem o grupo: %s", l.raw)
	}
	if r := s.do("GET", "/v1/groups", e.access, nil); bytes.Contains(r.raw, []byte(gid)) {
		t.Error("GET /v1/groups de não-membro mostra o grupo")
	}

	add := func(by user, who string) resp {
		return s.do("POST", "/v1/groups/"+gid+"/members", by.access, map[string]string{"user_id": who})
	}
	s.check("membro adiciona amigo dele", add(b, d.id), 204, "")
	s.check("adicionar de novo", add(b, d.id), 204, "")
	s.check("membro adiciona quem não é amigo dele", add(c, e.id), 403, "forbidden")
	s.check("não-membro adiciona", add(e, a.id), 404, "not_found")

	del := func(by user, who string) resp {
		return s.do("DELETE", "/v1/groups/"+gid+"/members/"+who, by.access, nil)
	}
	s.check("membro remove outro", del(c, b.id), 403, "forbidden")
	s.check("não-membro remove", del(e, b.id), 404, "not_found")
	s.check("dono remove", del(a, c.id), 204, "")
	if _, _, ok := group(s, c, gid); ok {
		t.Error("removido ainda lê o grupo")
	}
	s.check("sair", del(d, d.id), 204, "")

	s.check("dono sai", del(a, a.id), 204, "")
	if owner, m, ok := group(s, b, gid); !ok || owner != b.id || len(m) != 1 {
		t.Errorf("depois do dono sair: dono %s, membros %v (esperado só b, dono)", owner, m)
	}
	s.check("último sai", del(b, b.id), 204, "")
	// Quem saiu recebe 404 de qualquer jeito: só o banco diz se o grupo sumiu.
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM groups WHERE id = $1`, gid).Scan(&n); err != nil || n != 0 {
		t.Errorf("grupo sem membros continua no banco (%d, %v)", n, err)
	}
}

func TestGrupoTetos(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/groups")
	a, x := s.register("a"), s.register("x")
	befriend(s, a, x)
	ids := seedFriendIDs(s, a.id, 50)

	s.check("50 membros além do dono", s.do("POST", "/v1/groups", a.access, map[string]any{"members": ids}), 409, "too_many_members")
	r := s.do("POST", "/v1/groups", a.access, map[string]any{"members": ids[:49]})
	s.want(r, 201, "")
	s.check("adicionar o 51º", s.do("POST", "/v1/groups/"+r.str("id")+"/members", a.access,
		map[string]string{"user_id": ids[49]}), 409, "too_many_members")

	for i := range 25 {
		if r := s.do("POST", "/v1/groups", x.access, map[string]any{"members": []string{}}); r.status != 201 {
			t.Fatalf("grupo %d de x: %d %s", i+1, r.status, r.raw)
		}
	}
	s.check("26º grupo", s.do("POST", "/v1/groups", x.access, map[string]any{"members": []string{}}), 409, "too_many_groups")
	s.check("criar com membro já em 25", s.do("POST", "/v1/groups", a.access, map[string]any{"members": []string{x.id}}),
		409, "too_many_groups")
	g := createGroup(s, a)
	s.check("adicionar membro já em 25", s.do("POST", "/v1/groups/"+g+"/members", a.access,
		map[string]string{"user_id": x.id}), 409, "too_many_groups")
}

func TestGrupoMensagens(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/groups")
	a, b, c, d, f := s.register("a"), s.register("b"), s.register("c"), s.register("d"), s.register("f")
	for _, u := range []user{b, c, d, f} {
		befriend(s, a, u)
	}
	gid := createGroup(s, a, b, c, f)
	wa, wb, wc, wd, wf := s.ws(a.access), s.ws(b.access), s.ws(c.access), s.ws(d.access), s.ws(f.access)
	for _, w := range []*wsConn{wa, wb, wc, wd, wf} {
		w.ready()
	}

	m1 := s.payload(64)
	wa.group(gid, "m1", m1)
	for name, w := range map[string]*wsConn{"b": wb, "c": wc, "f": wf} {
		g := w.expect(wire.GroupEnvelope, 3*time.Second)
		if string(g.Group) != gid || string(g.MsgID) != "m1" || string(g.Peer) != a.id || !bytes.Equal(g.Payload, m1) {
			t.Errorf("%s recebeu %s group=%s", name, describe(g), g.Group)
		}
	}
	if g := wa.expect(wire.Ack, 3*time.Second); string(g.Peer) != gid || string(g.MsgID) != "m1" {
		t.Errorf("a recebeu %s, esperado Ack{to grupo, m1}", describe(g))
	}
	wa.silent("remetente recebeu a própria mensagem de grupo", quiet)
	wd.silent("não-membro recebeu mensagem do grupo", quiet)
	wa.group(gid, "m1", m1)
	wa.expect(wire.Ack, 3*time.Second)
	wb.silent("reenvio do mesmo client_msg_id entregue de novo", quiet)

	wd.group(gid, "x1", s.payload(32))
	wd.expectErr("forbidden")
	wd.group(nobody, "x2", s.payload(32))
	wd.expectErr("forbidden")

	r := s.do("GET", "/v1/envelopes", b.access, nil)
	s.want(r, 200, "")
	if !bytes.Contains(r.raw, []byte(`"group_id":"`+gid+`"`)) {
		t.Errorf("cópia de grupo sem group_id no GET /v1/envelopes: %s", r.raw)
	}
	wd.ack(a, "m1")
	wc.ack(a, "m1")
	eventually(s, "ack de c apaga a cópia de c", func() bool { _, ok := pending(s, c)[a.id+"|m1"]; return !ok })
	if _, ok := pending(s, b)[a.id+"|m1"]; !ok {
		t.Error("ack de outro membro apagou a cópia de b")
	}

	s.want(s.do("POST", "/v1/blocks/"+a.id, f.access, nil), 204, "")
	wa.group(gid, "m2", s.payload(32))
	wa.expect(wire.Ack, 3*time.Second)
	wb.expect(wire.GroupEnvelope, 3*time.Second)
	wc.expect(wire.GroupEnvelope, 3*time.Second)
	wf.silent("membro recebeu de quem ele bloqueou", time.Second)
	wf.group(gid, "m3", s.payload(32))
	wf.expect(wire.Ack, 3*time.Second)
	wb.expect(wire.GroupEnvelope, 3*time.Second)
	wc.expect(wire.GroupEnvelope, 3*time.Second)
	wa.silent("bloqueado recebeu de quem o bloqueou", time.Second)

	s.want(s.do("DELETE", "/v1/groups/"+gid+"/members/"+c.id, a.access, nil), 204, "")
	for name, w := range map[string]*wsConn{"a": wa, "b": wb, "c (removido)": wc} {
		if g := w.expect(wire.GroupChanged, 3*time.Second); string(g.Group) != gid {
			t.Errorf("%s: GroupChanged de outro grupo %s", name, g.Group)
		}
	}
	wf.expect(wire.GroupChanged, 3*time.Second)
	wa.group(gid, "m4", s.payload(32))
	wa.expect(wire.Ack, 3*time.Second)
	wb.expect(wire.GroupEnvelope, 3*time.Second)
	wc.silent("removido recebeu mensagem", time.Second)

	s.want(s.do("DELETE", "/v1/groups/"+gid+"/members/"+b.id, b.access, nil), 204, "")
	for _, w := range []*wsConn{wa, wb, wf} {
		w.expect(wire.GroupChanged, 3*time.Second)
	}
	wa.group(gid, "m5", s.payload(32))
	wa.expect(wire.Ack, 3*time.Second)
	wb.silent("quem saiu recebeu mensagem", time.Second)
	wc.silent("removido recebeu GroupChanged de saída alheia", quiet)
}

func TestGrupoChavesEntreMembros(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/groups")
	a, b, c, e := s.register("a"), s.register("b"), s.register("c"), s.register("e")
	befriend(s, a, b)
	befriend(s, a, c)
	s.want(s.do("PUT", "/v1/keys", c.access, newBundle(2, 2)), 204, "")
	s.check("estranho, antes do grupo", s.do("GET", "/v1/keys/"+c.id, b.access, nil), 403, "forbidden")

	gid := createGroup(s, a, b, c)
	s.check("membros sem amizade", s.do("GET", "/v1/keys/"+c.id, b.access, nil), 200, "")
	s.check("estranho", s.do("GET", "/v1/keys/"+c.id, e.access, nil), 403, "forbidden")

	wb, wc := s.ws(b.access), s.ws(c.access)
	wb.ready()
	wc.ready()
	skdm := s.payload(64)
	wb.envelope(c, "skdm1", skdm)
	if g := wc.expect(wire.Envelope, 3*time.Second); string(g.Peer) != b.id || !bytes.Equal(g.Payload, skdm) {
		t.Errorf("SKDM 1:1 entre membros: %s", describe(g))
	}
	wb.expect(wire.Ack, 3*time.Second)

	s.want(s.do("DELETE", "/v1/groups/"+gid+"/members/"+c.id, a.access, nil), 204, "")
	wb.expect(wire.GroupChanged, 3*time.Second)
	wc.expect(wire.GroupChanged, 3*time.Second)
	s.check("ex-membro", s.do("GET", "/v1/keys/"+c.id, b.access, nil), 403, "forbidden")
	wb.envelope(c, "skdm2", s.payload(32))
	wb.expectErr("forbidden")
}

func TestGrupoLimiteDeChat(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/groups")
	a := s.register("a")
	ids := seedFriendIDs(s, a.id, 20)
	r := s.do("POST", "/v1/groups", a.access, map[string]any{"members": ids})
	s.want(r, 201, "")
	gid := r.str("id")
	b := s.register("b")
	befriend(s, a, b)
	wa := s.ws(a.access)
	wa.ready()

	since := time.Now()
	wa.group(gid, "g0", s.payload(32)) // 20 cópias: se contasse 20, o 1:1 estouraria por volta da 40ª
	wa.expect(wire.Ack, 3*time.Second)
	passed := 1
	for i := 0; i < 120; i++ {
		wa.envelope(b, fmt.Sprintf("m%d", i), s.payload(16))
		f, closed, timeout := wa.next(3 * time.Second)
		if closed || timeout {
			t.Fatalf("mensagem %d: sem Ack nem Error (caiu=%v)", i+1, closed)
		}
		if f.Type == wire.Error {
			if string(f.Code) != "rate_limited" {
				t.Fatalf("erro inesperado: %s", f.Code)
			}
			if most := 60 + int(time.Since(since)/time.Second); passed < 60 || passed > most {
				t.Errorf("passaram %d antes do rate_limited (esperado 60 a %d)", passed, most)
			}
			return
		}
		passed++
	}
	t.Errorf("nenhum rate_limited em %d mensagens", passed)
}

func (s *server) pings() map[string]int {
	out := map[string]int{}
	for _, l := range strings.Split(s.logs.String(), "\n") {
		var e struct{ Msg, User string }
		if json.Unmarshal([]byte(l), &e) == nil && e.Msg == "fcm ping" {
			out[e.User]++
		}
	}
	return out
}

func TestFCMSoOffline(t *testing.T) {
	s := start(t)
	s.need("PUT", "/v1/devices/fcm")
	a, on, off, semToken := s.register("a"), s.register("on"), s.register("off"), s.register("semtoken")
	for _, u := range []user{on, off, semToken} {
		befriend(s, a, u)
	}
	s.check("FCM sem aparelho", s.do("PUT", "/v1/devices/fcm", on.access, map[string]string{"token": "fcm-x"}), 404, "not_found")
	for _, u := range []user{on, off} {
		// O token FCM fica no aparelho: sem PUT /v1/keys antes, o PUT do token dá 404.
		s.want(s.do("PUT", "/v1/keys", u.access, newBundle(1, 1)), 204, "")
		tok := "fcm-" + rand.Text() + rand.Text()
		s.secret(tok)
		s.want(s.do("PUT", "/v1/devices/fcm", u.access, map[string]string{"token": tok}), 204, "")
	}
	wa, won := s.ws(a.access), s.ws(on.access)
	wa.ready()
	won.ready()

	for i, u := range []user{on, off, semToken} {
		wa.envelope(u, fmt.Sprintf("m%d", i), s.payload(32))
		wa.expect(wire.Ack, 3*time.Second)
	}
	won.expect(wire.Envelope, 3*time.Second)
	eventually(s, "ping para quem está offline", func() bool { return s.pings()[off.id] > 0 })
	gid := createGroup(s, a, on, off, semToken)
	wa.expect(wire.GroupChanged, 3*time.Second) // criar é a entrada dos membros
	won.expect(wire.GroupChanged, 3*time.Second)
	wa.group(gid, "g1", s.payload(32))
	wa.expect(wire.Ack, 3*time.Second)
	won.expect(wire.GroupEnvelope, 3*time.Second)
	time.Sleep(time.Second) // o ping é assíncrono: dá tempo de um ping indevido aparecer
	p := s.pings()
	if p[on.id] != 0 {
		t.Errorf("ping para quem está online (%d)", p[on.id])
	}
	if p[semToken.id] != 0 {
		t.Errorf("ping para quem não tem fcm_token (%d)", p[semToken.id])
	}

	s.want(s.do("PUT", "/v1/devices/fcm", off.access, map[string]any{"token": nil}), 204, "")
	before := s.pings()[off.id]
	wa.envelope(off, "m9", s.payload(32))
	wa.expect(wire.Ack, 3*time.Second)
	time.Sleep(time.Second)
	if s.pings()[off.id] != before {
		t.Error("ping depois de apagar o fcm_token")
	}
	s.check("token acima de 4 KiB", s.do("PUT", "/v1/devices/fcm", off.access,
		map[string]string{"token": strings.Repeat("a", 4097)}), 400, "bad_request")
}
