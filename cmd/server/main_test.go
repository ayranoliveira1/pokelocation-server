package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRequestLog(t *testing.T) {
	var buf syncBuf
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(old)

	srv := httptest.NewServer(withRequestLog(true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hijack" {
			c, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = c.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n\r\n"))
			c.Close()
			return
		}
		w.WriteHeader(http.StatusTeapot)
	})))
	req, _ := http.NewRequest("GET", srv.URL+"/v1/auth/google/callback?code=SEGREDO", nil)
	req.Header.Set("Authorization", "Bearer TOKEN")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
	if resp, err := http.Get(srv.URL + "/hijack"); err == nil {
		resp.Body.Close()
	}
	srv.Close()
	// o Close não espera conexão sequestrada
	out := buf.String()
	for deadline := time.Now().Add(2 * time.Second); !strings.Contains(out, `"status":101`) && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
		out = buf.String()
	}
	for _, want := range []string{`"path":"/v1/auth/google/callback"`, `"status":418`, `"path":"/hijack"`, `"status":101`} {
		if !strings.Contains(out, want) {
			t.Errorf("falta %s em %s", want, out)
		}
	}
	for _, secret := range []string{"SEGREDO", "TOKEN"} {
		if strings.Contains(out, secret) {
			t.Errorf("log vazou %s", secret)
		}
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
