package apitest

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestAccessToken(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/auth/register")
	a, b := s.register("a"), s.register("b")
	now := time.Now().Unix()
	// tv igual ao do token real: o middleware recusa tv ≠ users.token_version.
	real := claims(t, a.access)
	iat, tv := real["iat"], real["tv"]
	tvN, ok := tv.(float64)
	if !ok {
		t.Fatalf("access sem claim tv numérica: %v", real)
	}
	valid := map[string]any{"sub": a.id, "iat": iat, "tv": tv, "exp": now + 300}

	// Controle: se o token forjado com a chave do servidor não vale, os casos abaixo não provam nada.
	if r := s.do("GET", "/v1/me", forge("EdDSA", valid, s.key), nil); r.status != 200 {
		t.Fatalf("controle: token EdDSA com a chave do servidor deu %d %s — conferir %s e claims com o S1", r.status, r.raw, envJWTKey)
	}

	_, otherKey, _ := ed25519.GenerateKey(rand.Reader)
	parts := strings.Split(a.access, ".")
	if len(parts) != 3 {
		t.Fatalf("access não é JWT: %d partes", len(parts))
	}
	for name, tok := range map[string]string{
		"vazio":                "",
		"lixo":                 "lixo",
		"assinatura trocada":   parts[0] + "." + parts[1] + "." + flip(parts[2]),
		"sem assinatura":       parts[0] + "." + parts[1] + ".",
		"sub de outro usuário": parts[0] + "." + withClaim(t, parts[1], "sub", b.id) + "." + parts[2],
		"exp estendido":        parts[0] + "." + withClaim(t, parts[1], "exp", now+86400) + "." + parts[2],
		"alg none":             forge("none", valid, nil),
		"HS256 com a pública":  forgeHS256(valid, s.key.Public().(ed25519.PublicKey)),
		"outra chave Ed25519":  forge("EdDSA", valid, otherKey),
		"vencido":              forge("EdDSA", map[string]any{"sub": a.id, "iat": now - 1000, "tv": tv, "exp": now - 60}, s.key),
		"sem exp":              forge("EdDSA", map[string]any{"sub": a.id, "iat": iat, "tv": tv}, s.key),
		"sem sub":              forge("EdDSA", map[string]any{"iat": iat, "tv": tv, "exp": now + 300}, s.key),
		"sem tv":               forge("EdDSA", map[string]any{"sub": a.id, "iat": iat, "exp": now + 300}, s.key),
		"tv antigo":            forge("EdDSA", map[string]any{"sub": a.id, "iat": iat, "tv": tvN - 1, "exp": now + 300}, s.key),
		"tv futuro":            forge("EdDSA", map[string]any{"sub": a.id, "iat": iat, "tv": tvN + 1, "exp": now + 300}, s.key),
		"tv texto":             forge("EdDSA", map[string]any{"sub": a.id, "iat": iat, "tv": fmt.Sprint(tvN), "exp": now + 300}, s.key),
	} {
		t.Run(name, func(t *testing.T) {
			s := s.on(t)
			s.want(s.do("GET", "/v1/me", tok, nil), 401, "unauthorized")
		})
	}
	t.Run("sub inexistente", func(t *testing.T) {
		s := s.on(t)
		tok := forge("EdDSA", map[string]any{"sub": "00000000-0000-0000-0000-000000000000", "iat": iat, "tv": tv,
			"exp": now + 300}, s.key)
		s.want(s.do("GET", "/v1/me", tok, nil), 410, "account_deleted")
	})
	t.Run("esquema Basic", func(t *testing.T) {
		s := s.on(t)
		r := s.doHeader("GET", "/v1/me", "Basic "+a.access)
		s.want(r, 401, "unauthorized")
	})
}

func TestRefresh(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/auth/refresh")
	a := s.register("a")
	refresh := func(tok string) resp {
		return s.do("POST", "/v1/auth/refresh", "", map[string]string{"refresh_token": tok})
	}

	r := refresh(a.refresh)
	s.want(r, 200, "")
	r1, acc1 := r.str("refresh_token"), r.str("access_token")
	s.secret(r1, acc1)
	if r1 == "" || r1 == a.refresh {
		t.Fatal("refresh não foi rotacionado")
	}
	s.want(s.do("GET", "/v1/me", acc1, nil), 200, "")

	// Outro login é outra família: o refresh dela sobrevive à revogação da primeira.
	l := s.do("POST", "/v1/auth/login", "", map[string]string{"email": a.email, "password": a.password})
	s.want(l, 200, "")
	other, accOther := l.str("refresh_token"), l.str("access_token")
	s.secret(other, accOther)

	// Reuso mata a família inteira e todo access já emitido, na hora, mesmo no mesmo segundo.
	s.want(refresh(a.refresh), 401, "unauthorized")
	s.check("access da família reusada", s.do("GET", "/v1/me", acc1, nil), 401, "unauthorized")
	s.check("access da outra sessão", s.do("GET", "/v1/me", accOther, nil), 401, "unauthorized")
	s.check("refresh da família reusada", refresh(r1), 401, "unauthorized")

	r = refresh(other)
	s.want(r, 200, "")
	other, accOther = r.str("refresh_token"), r.str("access_token")
	s.secret(other, accOther)
	s.check("access novo depois do reuso", s.do("GET", "/v1/me", accOther, nil), 200, "")

	for name, tok := range map[string]string{"lixo": "lixo", "vazio": "", "access no lugar": acc1} {
		t.Run("refresh "+name, func(t *testing.T) {
			s := s.on(t)
			s.want(refresh(tok), 401, "unauthorized")
		})
	}

	t.Run("logout", func(t *testing.T) {
		s := s.on(t)
		s.want(s.do("POST", "/v1/auth/logout", "", map[string]string{"refresh_token": other}), 204, "")
		s.check("refresh depois do logout", refresh(other), 401, "unauthorized")
		s.check("access depois do logout", s.do("GET", "/v1/me", accOther, nil), 401, "unauthorized")
		// O app chama sem Authorization; a resposta não revela se o token existia.
		s.check("logout de novo", s.do("POST", "/v1/auth/logout", "", map[string]string{"refresh_token": other}), 204, "")
		s.check("logout com lixo", s.do("POST", "/v1/auth/logout", "", map[string]string{"refresh_token": "lixo"}), 204, "")
		l := s.do("POST", "/v1/auth/login", "", map[string]string{"email": a.email, "password": a.password})
		s.want(l, 200, "")
		s.secret(l.str("access_token"), l.str("refresh_token"))
		s.check("access de login novo", s.do("GET", "/v1/me", l.str("access_token"), nil), 200, "")
	})
}

// Email inexistente e senha errada dão o mesmo erro: sem enumeração de contas.
func TestLogin(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/auth/login")
	a := s.register("a")
	s.want(s.do("POST", "/v1/auth/login", "", map[string]string{"email": a.email, "password": "errada-123"}), 401, "invalid_credentials")
	s.want(s.do("POST", "/v1/auth/login", "", map[string]string{"email": "ninguem@teste.dev", "password": "qualquer-123"}), 401, "invalid_credentials")
	// O servidor normaliza o email (trim + minúsculas).
	l := s.do("POST", "/v1/auth/login", "", map[string]string{"email": " " + strings.ToUpper(a.email) + " ", "password": a.password})
	s.want(l, 200, "")
	s.secret(l.str("access_token"), l.str("refresh_token"))
	s.want(s.do("POST", "/v1/auth/register", "", map[string]string{"email": strings.ToUpper(a.email), "password": "outra-senha-1",
		"username": newUsername("dup")}), 409, "email_taken")
}

func TestRegisterValida(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/auth/register")
	for name, c := range map[string]struct{ email, password, code string }{
		"sem @":         {"semarroba.dev", "senha-longa-1", "invalid_email"},
		"@ sem local":   {"@teste.dev", "senha-longa-1", "invalid_email"},
		"senha curta":   {"x@teste.dev", "1234567", "weak_password"},
		"senha enorme":  {"y@teste.dev", strings.Repeat("a", 129), "weak_password"},
		"JSON inválido": {"", "", "bad_request"},
	} {
		t.Run(name, func(t *testing.T) {
			s := s.on(t)
			// @nome válido: cada caso testa só o próprio campo.
			var body any = map[string]string{"email": c.email, "password": c.password, "username": newUsername("v")}
			if c.code == "bad_request" {
				body = `{"email":`
			}
			s.want(s.do("POST", "/v1/auth/register", "", body), 400, c.code)
		})
	}
}

// Servidor só deste teste: o limite é em memória, por processo.
func TestLoginRateLimit(t *testing.T) {
	s := start(t)
	s.need("POST", "/v1/auth/login")
	login := func(s *server, email string) resp {
		return s.do("POST", "/v1/auth/login", "", map[string]string{"email": email, "password": "errada-123"})
	}
	t.Run("por email, IP girando", func(t *testing.T) {
		s := s.on(t) // IP novo a cada requisição: só o limite por email segura
		s.bucket("login por email", 5, 12*time.Second, time.Now(), 401, func(int) resp { return login(s, "alvo@teste.dev") })
	})
	t.Run("por IP, email girando", func(t *testing.T) {
		s := s.on(t)
		s.ip = "203.0.113.9"
		s.bucket("login por IP", 5, 12*time.Second, time.Now(), 401, func(i int) resp {
			return login(s, fmt.Sprintf("x%d@teste.dev", i))
		})
	})
}

// forge: key nil = sem assinatura.
func forge(alg string, claims map[string]any, key ed25519.PrivateKey) string {
	in := seg(map[string]any{"alg": alg, "typ": "JWT"}) + "." + seg(claims)
	if key == nil {
		return in + "."
	}
	return in + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(in)))
}

// Confusão de algoritmo: HMAC usando a chave pública como segredo.
func forgeHS256(claims map[string]any, pub ed25519.PublicKey) string {
	in := seg(map[string]any{"alg": "HS256", "typ": "JWT"}) + "." + seg(claims)
	m := hmac.New(sha256.New, pub)
	m.Write([]byte(in))
	return in + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func seg(v any) string {
	j, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(j)
}

func claims(t *testing.T, jwt string) map[string]any {
	t.Helper()
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("não é JWT: %d partes", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func withClaim(t *testing.T, payload, k string, v any) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m[k] = v
	return seg(m)
}

// flip troca um caractere do meio (o último pode só mexer em bits de padding do base64url).
func flip(s string) string {
	b := []byte(s)
	i := len(b) / 2
	if b[i] == 'A' {
		b[i] = 'B'
	} else {
		b[i] = 'A'
	}
	return string(b)
}
