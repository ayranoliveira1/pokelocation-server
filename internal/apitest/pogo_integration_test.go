package apitest

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestPogo(t *testing.T) {
	t.Parallel()
	pogoTok, code := "pogo-"+rand.Text(), "code-"+rand.Text()
	up := http.NewServeMux()
	up.HandleFunc("POST /auth/google/callback", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"token":"`+pogoTok+`"}`)
	})
	authed := func(body string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+pogoTok {
				w.WriteHeader(401)
				return
			}
			_, _ = io.WriteString(w, body)
		}
	}
	up.HandleFunc("GET /users/me", authed(`{"display_name":"Fulano","google_email":"x@y.com","trainers":[]}`))
	up.HandleFunc("POST /map/lookup", authed(`{"mode":"pokemon","results":[]}`))
	fake := httptest.NewServer(up)
	defer fake.Close()

	key := make([]byte, 32)
	_, _ = rand.Read(key)
	rdb := testdb.Redis(t)
	s := startWith(t, fmt.Sprintf("redis://%s/%d", rdb.Options().Addr, rdb.Options().DB),
		"POGO_TOKEN_KEY="+base64.StdEncoding.EncodeToString(key), "POGO_UPSTREAM_URL="+fake.URL)
	s.secret(pogoTok, code)
	a := s.register("pogo")

	s.want(s.do("POST", "/v1/pogo/lookup", a.access, map[string]any{"mode": "pokemon"}), 409, "pogo_relogin")
	r := s.do("POST", "/v1/pogo/login", a.access, map[string]string{"provider": "google", "code": code})
	s.want(r, 200, "")
	if r.str("display_name") != "Fulano" || r.body["google_email"] != nil {
		t.Fatalf("perfil: %s", r.raw)
	}
	if r := s.do("POST", "/v1/pogo/lookup", a.access, map[string]any{"mode": "pokemon"}); r.status != 200 ||
		string(r.raw) != `{"mode":"pokemon","results":[]}` {
		t.Fatalf("lookup: %d %s", r.status, r.raw)
	}

	s.want(s.do("POST", "/v1/pogo/rules", a.access, map[string]any{"name": "Dratini", "filters": map[string]any{},
		"scope": "world"}), 201, "")

	s.want(s.do("DELETE", "/v1/me", a.access, map[string]string{"password": a.password}), 204, "")
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM pogo_accounts WHERE user_id = $1) +
		(SELECT count(*) FROM pogo_rules WHERE user_id = $1)`, a.id).Scan(&n); err != nil || n != 0 {
		t.Fatalf("vínculo ou regras depois de apagar a conta: %d %v", n, err)
	}
}
