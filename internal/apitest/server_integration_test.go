// Package apitest: testes caixa-preta do contrato (docs/api.md) contra o binário real, um processo por teste. Rota
// que ainda não existe pula o teste em vez de falhar.
package apitest

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

// O teste gera e guarda a chave para forjar tokens (vencido, sem exp…).
const envJWTKey = "AUTH_SIGNING_KEY"

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
	buildOut  []byte
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "apitest")
	if err != nil {
		panic(err)
	}
	binPath = filepath.Join(dir, "server")
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type server struct {
	t       testing.TB
	base    string
	key     ed25519.PrivateKey
	pool    *pgxpool.Pool // para semear o que os limites da API não deixam criar rápido (tetos)
	logs    *syncBuf
	hc      *http.Client
	secrets *[]string
	ip      string // X-Forwarded-For fixo; "" = IP novo por requisição, para não esbarrar no limite por IP
}

var ipSeq atomic.Int32

// start também marca o teste como paralelo e, no fim, confere que o log do servidor não vazou nenhum s.secret.
func start(t testing.TB) *server {
	t.Helper()
	if tt, ok := t.(*testing.T); ok {
		tt.Parallel()
	}
	rdb := testdb.Redis(t)
	return startWith(t, fmt.Sprintf("redis://%s/%d", rdb.Options().Addr, rdb.Options().DB))
}

// startWith: como start, com outro Redis e sem marcar paralelo.
func startWith(t testing.TB, redisURL string) *server {
	t.Helper()
	pool := testdb.Pool(t)
	buildOnce.Do(func() {
		// APITEST_SERVER_BIN: testa um binário já compilado (ex.: o HEAD, quando a árvore está no meio de uma mudança).
		if b := os.Getenv("APITEST_SERVER_BIN"); b != "" {
			binPath = b
			return
		}
		args := []string{"build", "-o", binPath}
		if raceOn {
			args = append(args, "-race")
		}
		buildOut, buildErr = exec.Command("go", append(args, "../../cmd/server")...).CombinedOutput()
	})
	if buildErr != nil {
		t.Fatalf("go build: %v\n%s", buildErr, buildOut)
	}
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	addr := freeAddr(t)
	s := &server{t: t, pool: pool, base: "http://" + addr, key: key, logs: &syncBuf{}, secrets: new([]string), hc: &http.Client{
		Timeout:       10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}

	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(),
		"ADDR="+addr,
		"DATABASE_URL="+pool.Config().ConnString(),
		"REDIS_URL="+redisURL,
		envJWTKey+"="+base64.StdEncoding.EncodeToString(key.Seed()),
	)
	cmd.Stdout, cmd.Stderr = s.logs, s.logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
			t.Error("servidor não parou em 10 s com SIGTERM")
		}
		logs := s.logs.String()
		for _, v := range *s.secrets {
			if strings.Contains(logs, v) {
				t.Errorf("log do servidor vazou um segredo (%d bytes, começa com %.4q)", len(v), v)
			}
		}
		if t.Failed() {
			t.Logf("log do servidor:\n%s", logs)
		}
	})

	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		select {
		case <-exited:
			t.Fatalf("servidor saiu ao subir:\n%s", s.logs.String())
		default:
		}
		if r, err := s.hc.Get(s.base + "/healthz"); err == nil {
			_ = r.Body.Close()
			if r.StatusCode == 200 {
				return s
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("servidor não respondeu /healthz em 15 s:\n%s", s.logs.String())
		}
	}
}

// on liga o servidor a um subteste: Fatal e Skip têm de vir da goroutine do próprio t.
func (s *server) on(t *testing.T) *server {
	c := *s
	c.t = t
	return &c
}

// need: 404 em texto é do ServeMux (rota ausente), não o not_found em JSON do contrato.
func (s *server) need(method, path string) {
	s.t.Helper()
	req, _ := http.NewRequest(method, s.base+path, nil)
	r, err := s.hc.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if r.StatusCode == 404 && strings.HasPrefix(string(b), "404 page not found") ||
		r.StatusCode == 405 && strings.HasPrefix(string(b), "Method Not Allowed") {
		s.t.Skipf("rota %s %s ainda não existe", method, path)
	}
}

// secret: valor que nunca pode aparecer no log do servidor.
func (s *server) secret(vals ...string) {
	for _, v := range vals {
		if v != "" {
			*s.secrets = append(*s.secrets, v)
		}
	}
}

type resp struct {
	status int
	body   map[string]any
	raw    []byte
	header http.Header
}

func (r resp) str(k string) string { v, _ := r.body[k].(string); return v }
func (r resp) errCode() string     { return r.str("error") }

// do: body nil = sem corpo, string = corpo cru, outro = JSON.
func (s *server) do(method, path, token string, body any) resp {
	s.t.Helper()
	if token != "" {
		token = "Bearer " + token
	}
	return s.send(method, path, token, body)
}

func (s *server) doHeader(method, path, auth string) resp {
	s.t.Helper()
	return s.send(method, path, auth, nil)
}

func (s *server) send(method, path, auth string, body any) resp {
	s.t.Helper()
	out, err := s.raw(method, path, auth, body)
	if err != nil {
		s.t.Fatal(err)
	}
	if len(out.raw) > 64<<10 {
		s.t.Errorf("%s %s: resposta de %d bytes passa do limite de 64 KiB do app", method, path, len(out.raw))
	}
	return out
}

// raw só devolve erro: pode rodar fora da goroutine do teste.
func (s *server) raw(method, path, auth string, body any) (resp, error) {
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		j, err := json.Marshal(b)
		if err != nil {
			return resp{}, err
		}
		rd = bytes.NewReader(j)
	}
	req, err := http.NewRequest(method, s.base+path, rd)
	if err != nil {
		return resp{}, err
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	// Vindo de loopback, o auth usa o último X-Forwarded-For como IP do cliente (como atrás do Caddy).
	ip := s.ip
	if ip == "" {
		n := ipSeq.Add(1)
		ip = fmt.Sprintf("198.18.%d.%d", n>>8&255, n&255) // 198.18/15: não é privado
	}
	req.Header.Set("X-Forwarded-For", ip)
	r, err := s.hc.Do(req)
	if err != nil {
		return resp{}, err
	}
	defer r.Body.Close()
	out := resp{status: r.StatusCode, header: r.Header}
	if out.raw, err = io.ReadAll(r.Body); err != nil {
		return resp{}, err
	}
	_ = json.Unmarshal(out.raw, &out.body)
	return out, nil
}

func (s *server) want(r resp, status int, code string) {
	s.t.Helper()
	if !s.match(r, status, code) {
		s.t.Fatalf("esperado %d %q, veio %d %s", status, code, r.status, r.raw)
	}
}

// check, ao contrário de want, não para o teste: cada célula da matriz é reportada.
func (s *server) check(what string, r resp, status int, code string) bool {
	s.t.Helper()
	if !s.match(r, status, code) {
		s.t.Errorf("%s: esperado %d %q, veio %d %s", what, status, code, r.status, r.raw)
		return false
	}
	return true
}

func (s *server) match(r resp, status int, code string) bool {
	return r.status == status && (code == "" || r.errCode() == code)
}

type user struct{ id, email, password, code, access, refresh string }

// register conta no limite de 5/min por email.
func (s *server) register(name string) user {
	s.t.Helper()
	u := user{email: name + "." + strings.ToLower(rand.Text()[:10]) + "@teste.dev", password: "senha-" + rand.Text()}
	r := s.do("POST", "/v1/auth/register", "", map[string]string{"email": u.email, "password": u.password})
	s.want(r, 201, "")
	u.access, u.refresh = r.str("access_token"), r.str("refresh_token")
	me := s.do("GET", "/v1/me", u.access, nil)
	s.want(me, 200, "")
	u.id, u.code = me.str("id"), me.str("friend_code")
	s.secret(u.email, u.password, u.access, u.refresh, u.code)
	return u
}

// bucket confere um limite token bucket sem supor janela fixa: sob carga as chamadas levam mais que every e o
// servidor repõe créditos, então antes do 429 passam de budget a budget + créditos repostos desde since.
func (s *server) bucket(what string, budget int, every time.Duration, since time.Time, ok int, call func(i int) resp) {
	s.t.Helper()
	for i := 0; i < budget+30; i++ {
		r := call(i)
		if r.status == 429 {
			s.check(what+": 429", r, 429, "rate_limited")
			if max := budget + int(time.Since(since)/every); i < budget || i > max {
				s.t.Errorf("%s: passaram %d antes do 429, esperado entre %d e %d", what, i, budget, max)
			}
			return
		}
		if r.status != ok {
			s.t.Fatalf("%s: chamada %d: esperado %d, veio %d %s", what, i+1, ok, r.status, r.raw)
		}
	}
	s.t.Errorf("%s: nenhum 429 em %d chamadas", what, budget+30)
}

func freeAddr(t testing.TB) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) { b.mu.Lock(); defer b.mu.Unlock(); return b.b.Write(p) }
func (b *syncBuf) String() string              { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }
