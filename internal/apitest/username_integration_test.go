package apitest

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ayranoliveira1/pokelocation-server/internal/username"
)

// noUsername imita o 1º login Google direto no banco: o apitest não passa pelo OAuth.
func noUsername(s *server) user {
	s.t.Helper()
	u := user{}
	err := s.pool.QueryRow(context.Background(), `INSERT INTO users (google_sub, friend_code)
		VALUES ('g-' || gen_random_uuid(), (SELECT string_agg(substr('0123456789ABCDEFGHJKMNPQRSTVWXYZ',
			1 + floor(random() * 32)::int, 1), '') FROM generate_series(1, 8)))
		RETURNING id::text, friend_code`).Scan(&u.id, &u.code)
	if err != nil {
		s.t.Fatal("seed:", err)
	}
	now := time.Now().Unix()
	u.access = forge("EdDSA", map[string]any{"sub": u.id, "iat": now, "tv": 0, "exp": now + 600}, s.key)
	s.secret(u.access, u.code)
	return u
}

func setUsername(s *server, u user, name string) resp {
	return s.do("PUT", "/v1/me/username", u.access, map[string]string{"username": name})
}

func TestUsernameCadastro(t *testing.T) {
	s := start(t)
	s.need("PUT", "/v1/me/username")
	a := s.register("a")
	me := s.do("GET", "/v1/me", a.access, nil)
	s.want(me, 200, "")
	if me.str("username") != a.username {
		t.Errorf("GET /v1/me: username %q, esperado %q", me.str("username"), a.username)
	}

	reg := func(name any) resp {
		body := map[string]any{"email": newUsername("r") + "@teste.dev", "password": "senha-longa-1"}
		if name != nil {
			body["username"] = name
		}
		return s.do("POST", "/v1/auth/register", "", body)
	}
	for name, c := range map[string]struct {
		username any
		status   int
		code     string
	}{
		"sem username":         {nil, 400, "invalid_username"},
		"curto":                {"ab", 400, "invalid_username"},
		"começa com dígito":    {"9ana", 400, "invalid_username"},
		"acento":               {"joão_x", 400, "invalid_username"},
		"reservado":            {"Admin", 400, "username_reserved"},
		"repetido (normaliza)": {"  " + strings.ToUpper(a.username) + " ", 409, "username_taken"},
	} {
		s.check(name, reg(c.username), c.status, c.code)
	}
}

func TestUsernameObrigatorio(t *testing.T) {
	s := start(t)
	s.need("PUT", "/v1/me/username")
	u := noUsername(s)

	me := s.do("GET", "/v1/me", u.access, nil)
	s.want(me, 200, "")
	if v, ok := me.body["username"]; !ok || v != nil {
		t.Errorf("GET /v1/me sem @nome: username %v (esperado null)", v)
	}
	for _, r := range []struct{ m, p string }{
		{"GET", "/v1/friends"},
		{"POST", "/v1/friends/requests"},
		{"POST", "/v1/blocks/" + nobody},
		{"PUT", "/v1/keys"},
		{"GET", "/v1/keys/count"},
		{"GET", "/v1/keys/" + nobody},
		{"GET", "/v1/groups"},
		{"POST", "/v1/groups"},
		{"GET", "/v1/envelopes"},
		{"POST", "/v1/envelopes/ack"},
		{"GET", "/v1/locations"},
		{"PUT", "/v1/devices/fcm"},
	} {
		t.Run(r.m+" "+r.p, func(t *testing.T) {
			s := s.on(t)
			s.need(r.m, r.p)
			s.want(s.do(r.m, r.p, u.access, nil), 403, "username_required")
		})
	}
	w := s.ws(u.access)
	w.expectErr("username_required")
	w.expectClose(1008, 3*time.Second)

	name := newUsername("g")
	s.want(setUsername(s, u, name), 204, "")
	s.check("amigos depois de ter @nome", s.do("GET", "/v1/friends", u.access, nil), 200, "")
	w = s.ws(u.access)
	w.ready()
}

func TestUsernameTroca(t *testing.T) {
	s := start(t)
	s.need("PUT", "/v1/me/username")
	a, b := s.register("a"), s.register("b")

	s.check("nome de outro", setUsername(s, b, a.username), 409, "username_taken")
	s.check("inválido", setUsername(s, b, "a..b"), 400, "invalid_username")
	s.check("reservado", setUsername(s, b, "Support"), 400, "username_reserved")

	old := a.username
	names := []string{newUsername("t1"), newUsername("t2"), newUsername("t3")}
	for _, n := range names {
		s.want(setUsername(s, a, n), 204, "")
	}
	s.check("nome antigo livre na hora", setUsername(s, b, old), 204, "")
	s.check("repetir o atual não conta", setUsername(s, a, names[2]), 204, "")
	s.check("repetir com maiúsculas e espaço", setUsername(s, a, " "+strings.ToUpper(names[2])), 204, "")
	s.want(setUsername(s, a, newUsername("t4")), 204, "")
	s.want(setUsername(s, a, newUsername("t5")), 204, "")
	s.check("6ª troca no dia", setUsername(s, a, newUsername("t6")), 429, "rate_limited")
}

func TestUsernamePedido(t *testing.T) {
	s := start(t)
	s.need("PUT", "/v1/me/username")
	a, b, c, z := s.register("a"), s.register("b"), s.register("c"), s.register("z")
	req := func(u user, body map[string]string) resp { return s.do("POST", "/v1/friends/requests", u.access, body) }

	r := req(a, map[string]string{"username": " " + strings.ToUpper(b.username) + " "})
	s.want(r, 201, "")
	if again := req(a, map[string]string{"code": b.code}); !s.check("mesmo pedido por código", again, 200, "") ||
		again.str("id") != r.str("id") {
		t.Errorf("pedido por código não reconheceu o feito por @nome: %s", again.raw)
	}
	s.check("os dois campos", req(a, map[string]string{"username": c.username, "code": c.code}), 400, "bad_request")
	s.check("nenhum campo", req(a, map[string]string{}), 400, "bad_request")

	s.want(s.do("POST", "/v1/blocks/"+a.id, c.access, nil), 204, "")
	missing := req(a, map[string]string{"username": newUsername("ninguem")})
	blocked := req(a, map[string]string{"username": c.username})
	s.check("@nome inexistente", missing, 404, "code_not_found")
	s.check("@nome de quem me bloqueou", blocked, 404, "code_not_found")
	if !bytes.Equal(missing.raw, blocked.raw) {
		t.Errorf("respostas distinguíveis: %q × %q", missing.raw, blocked.raw)
	}

	// Alterna @nome e código: o limite soma os dois.
	s.bucket("pedidos por @nome e código", 10, 6*time.Second, time.Now(), 404, func(i int) resp {
		if i%2 == 0 {
			return req(z, map[string]string{"username": newUsername("x")})
		}
		return req(z, map[string]string{"code": fmt.Sprintf("ZZZZZZ%02d", i)})
	})
}

func TestUsernameNasListas(t *testing.T) {
	s := start(t)
	s.need("PUT", "/v1/me/username")
	a, b, c := s.register("a"), s.register("b"), s.register("c")
	befriend(s, a, b)
	s.want(s.do("POST", "/v1/friends/requests", a.access, map[string]string{"code": c.code}), 201, "")

	l := s.do("GET", "/v1/friends", a.access, nil)
	s.want(l, 200, "")
	for _, want := range []string{b.username, c.username} {
		if !bytes.Contains(l.raw, []byte(`"username":"`+want+`"`)) {
			t.Errorf("GET /v1/friends de a sem %s: %s", want, l.raw)
		}
	}
	if l := s.do("GET", "/v1/friends", c.access, nil); !bytes.Contains(l.raw, []byte(`"username":"`+a.username+`"`)) {
		t.Errorf("incoming de c sem o @nome de a: %s", l.raw)
	}

	wb := s.ws(b.access)
	wb.ready()
	wb.location(s.payload(32))
	eventually(s, "GET /v1/locations com o @nome", func() bool {
		r := s.do("GET", "/v1/locations", a.access, nil)
		return bytes.Contains(r.raw, []byte(`"username":"`+b.username+`"`))
	})

	r := s.do("POST", "/v1/groups", a.access, map[string]any{"members": []string{b.id}})
	s.want(r, 201, "")
	if _, m, ok := groupNames(s, b, r.str("id")); !ok || m[a.id] != a.username || m[b.id] != b.username {
		t.Errorf("membros do grupo sem @nome: %v", m)
	}
}

// Se newUsername sair da regra oficial, todo register do apitest cai com 400.
func TestNewUsernameValido(t *testing.T) {
	for _, name := range []string{"a", "fuzz", "semtoken", "f1", "nomecompridodemais"} {
		n := newUsername(name)
		if got, err := username.Check(n); err != nil || got != n {
			t.Errorf("newUsername(%q) = %q: %q, %v", name, n, got, err)
		}
	}
}
