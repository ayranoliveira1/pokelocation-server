package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
)

func testKey() string {
	seed := make([]byte, ed25519.SeedSize)
	_, _ = rand.Read(seed)
	return base64.StdEncoding.EncodeToString(seed)
}

// Handler só com a chave (sem Postgres/Redis): suficiente para o middleware.
func keyOnlyHandler(t *testing.T) *Handler {
	t.Helper()
	h, err := New(Config{SigningKey: testKey()}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestNewRejectsBadKey(t *testing.T) {
	for _, k := range []string{"", "!!", base64.StdEncoding.EncodeToString([]byte("curta"))} {
		if _, err := New(Config{SigningKey: k}, nil, nil); err == nil {
			t.Errorf("chave %q aceita", k)
		}
	}
}

func TestAuthed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	h := e.h
	uid := e.me(e.register("authed@ex.com").Access).ID
	good, err := h.svc.accessToken(uid, 0)
	if err != nil {
		t.Fatal(err)
	}

	old := h.svc.now
	h.svc.now = func() time.Time { return old().Add(-16 * time.Minute) }
	expired, _ := h.svc.accessToken(uid, 0)
	h.svc.now = old

	other := keyOnlyHandler(t)
	foreign, _ := other.svc.accessToken(uid, 0)
	oldTV, _ := h.svc.accessToken(uid, 7)

	future := jwt.NewNumericDate(time.Now().Add(time.Hour))
	zero := 0
	none, _ := jwt.NewWithClaims(jwt.SigningMethodNone, claims{jwt.RegisteredClaims{Subject: uid, ExpiresAt: future},
		&zero}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	sign := func(sub string, exp *jwt.NumericDate, tv *int) string {
		s, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA,
			claims{jwt.RegisteredClaims{Subject: sub, ExpiresAt: exp}, tv}).SignedString(h.svc.key)
		return s
	}
	noExp := sign(uid, nil, &zero)
	noSub := sign("", future, &zero)
	noTV := sign(uid, future, nil)
	ghost := sign("0b9f8c56-0d7e-4c1f-9a39-3c0d1e8e7a11", future, &zero)
	notUUID := sign("x", future, &zero)
	if sign(uid, future, &zero) == "" {
		t.Fatal("assinatura")
	}

	tampered := good[:len(good)-2] + "AA"
	if tampered == good {
		tampered = good[:len(good)-2] + "BB"
	}

	var seen string
	next := h.Authed(func(w http.ResponseWriter, r *http.Request) {
		seen = httpx.User(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})

	for _, c := range []struct {
		name, header string
		status       int
	}{
		{"válido", "Bearer " + good, 204},
		{"sem header", "", 401},
		{"sem Bearer", good, 401},
		{"bearer minúsculo", "bearer " + good, 204},
		{"vencido", "Bearer " + expired, 401},
		{"outra chave", "Bearer " + foreign, 401},
		{"alg none", "Bearer " + none, 401},
		{"sem exp", "Bearer " + noExp, 401},
		{"sem sub", "Bearer " + noSub, 401},
		{"sem tv", "Bearer " + noTV, 401},
		{"tv velho", "Bearer " + oldTV, 401},
		{"montado à mão com tv certo", "Bearer " + sign(uid, future, &zero), 204},
		{"usuário apagado", "Bearer " + ghost, 401},
		{"sub não é uuid", "Bearer " + notUUID, 401},
		{"adulterado", "Bearer " + tampered, 401},
		{"lixo", "Bearer x.y.z", 401},
	} {
		t.Run(c.name, func(t *testing.T) {
			seen = ""
			req := httptest.NewRequest("GET", "/v1/me", nil)
			if c.header != "" {
				req.Header.Set("Authorization", c.header)
			}
			rec := httptest.NewRecorder()
			next.ServeHTTP(rec, req)
			if rec.Code != c.status {
				t.Fatalf("status %d, quero %d", rec.Code, c.status)
			}
			if c.status == 401 {
				if body := rec.Body.String(); body != "{\"error\":\"unauthorized\"}\n" {
					t.Errorf("corpo %q", body)
				}
				if seen != "" {
					t.Error("handler chamado sem auth")
				}
			} else if seen != uid {
				t.Errorf("usuário %q", seen)
			}
		})
	}
}
