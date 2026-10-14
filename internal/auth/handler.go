// Package auth: contas (email+senha, Google), tokens e o middleware das rotas autenticadas. Contrato: docs/api.md §2.
package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/groups"
	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/ratelimit"
)

type Config struct {
	SigningKey         string // seed Ed25519 de 32 bytes em base64 padrão
	GoogleClientID     string // vazio = login Google desligado
	GoogleClientSecret string
	PublicURL          string // https://<host> como o navegador vê: base do redirect_uri do Google
}

const (
	maxBody    = 4 << 10
	reqWait    = 5 * time.Second
	googleWait = 15 * time.Second // o callback espera o endpoint de token do Google (até 10 s) e o banco
)

type Handler struct {
	// Chamados depois do commit, para o WebSocket derrubar o revogado e recarregar as listas de amigos.
	OnRevoke        func(user string)
	OnFriendsChange func(a, b string)
	OnGroupsChange  func(groupID string, users []string)

	svc       *Service
	ip, email *ratelimit.Limiter
	reqWait   time.Duration // campo para o teste encurtar
}

func New(cfg Config, pool *pgxpool.Pool, rdb *redis.Client) (*Handler, error) {
	seed, err := base64.StdEncoding.DecodeString(cfg.SigningKey)
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, errors.New("auth: AUTH_SIGNING_KEY deve ser seed Ed25519 de 32 bytes em base64")
	}
	s := &Service{
		key:   ed25519.NewKeyFromSeed(seed),
		now:   time.Now,
		store: &Store{pool: pool},
		rdb:   rdb,
		cfg:   cfg,
		argon: make(chan struct{}, 2*runtime.NumCPU()),
		names: ratelimit.NewWindow(5, 24*time.Hour),
	}
	s.google = s.googleIdentity
	h := &Handler{svc: s, ip: ratelimit.New(5), email: ratelimit.New(5), reqWait: reqWait}
	s.notify = h.notify
	if s.dummy, err = s.hashPassword(context.Background(), rand.Text()); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *Handler) notify(user string, exFriends []string, groupChanges []groups.Change) {
	if h.OnRevoke != nil {
		h.OnRevoke(user)
	}
	if h.OnFriendsChange != nil {
		for _, f := range exFriends {
			h.OnFriendsChange(user, f)
		}
	}
	if h.OnGroupsChange != nil {
		for _, c := range groupChanges {
			h.OnGroupsChange(c.Group, c.Users)
		}
	}
}

func (h *Handler) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	d := h.reqWait
	mux.HandleFunc("POST /v1/auth/register", httpx.Timed(d, h.register))
	mux.HandleFunc("POST /v1/auth/login", httpx.Timed(d, h.login))
	mux.HandleFunc("POST /v1/auth/refresh", httpx.Timed(d, h.refresh))
	mux.HandleFunc("POST /v1/auth/logout", httpx.Timed(d, h.logout))
	mux.HandleFunc("GET /v1/auth/google/start", httpx.Timed(d, h.googleStart))
	mux.HandleFunc("GET /v1/auth/google/callback", httpx.Timed(googleWait, h.googleCallback))
	mux.HandleFunc("POST /v1/auth/google/exchange", httpx.Timed(d, h.googleExchange))
	mux.Handle("GET /v1/me", authed(httpx.Timed(d, h.me)))
	mux.Handle("PUT /v1/me/username", authed(httpx.Timed(d, h.putUsername)))
}

// Cleanup apaga os refresh tokens vencidos.
func (h *Handler) Cleanup(ctx context.Context) error { return h.svc.store.cleanup(ctx, h.svc.now()) }

// ParseAccess valida o access token sem ir ao banco (ErrUnauthorized se inválido ou vencido). Quem chama tem de
// comparar version com users.token_version: só assim a revogação vale.
func (h *Handler) ParseAccess(token string) (userID string, exp time.Time, version int, err error) {
	return h.svc.parse(token)
}

// Authed exige um Bearer válido e põe o id do usuário no contexto (httpx.User).
func (h *Handler) Authed(next http.HandlerFunc) http.Handler { return h.authed(false, next) }

// AuthedNamed: o Authed mais o @nome (403 username_required sem ele), na mesma leitura por PK. Para amigos, chaves,
// grupos, fila, posições e FCM; as rotas do próprio auth ficam com o Authed.
func (h *Handler) AuthedNamed(next http.HandlerFunc) http.Handler { return h.authed(true, next) }

func (h *Handler) authed(needName bool, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
		if !strings.EqualFold(scheme, "Bearer") {
			httpx.Error(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), h.reqWait)
		uid, named, err := h.svc.verify(ctx, token)
		cancel()
		if err == nil && needName && !named {
			err = errUsernameRequired
		}
		if err != nil {
			h.fail(w, err)
			return
		}
		next(w, r.WithContext(httpx.WithUser(r.Context(), uid)))
	})
}

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Username string `json:"username"` // só no register
}

type refreshBody struct {
	Token string `json:"refresh_token"`
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !httpx.Decode(w, r, maxBody, &in) || !h.allow(w, r, in.Email) {
		return
	}
	t, err := h.svc.register(r.Context(), in.Email, in.Password, in.Username)
	h.reply(w, http.StatusCreated, t, err)
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !httpx.Decode(w, r, maxBody, &in) || !h.allow(w, r, in.Email) {
		return
	}
	t, err := h.svc.login(r.Context(), in.Email, in.Password)
	h.reply(w, http.StatusOK, t, err)
}

func (h *Handler) refresh(w http.ResponseWriter, r *http.Request) {
	var in refreshBody
	if !httpx.Decode(w, r, maxBody, &in) {
		return
	}
	t, err := h.svc.refresh(r.Context(), in.Token)
	h.reply(w, http.StatusOK, t, err)
}

func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	var in refreshBody
	if !httpx.Decode(w, r, maxBody, &in) {
		return
	}
	if err := h.svc.logout(r.Context(), in.Token); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) me(w http.ResponseWriter, r *http.Request) {
	id := httpx.User(r.Context())
	email, code, name, err := h.svc.store.me(r.Context(), id)
	if errors.Is(err, errNotFound) {
		err = errUnauthorized // conta apagada com access ainda dentro do prazo
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, struct {
		ID         string  `json:"id"`
		Email      *string `json:"email"`
		FriendCode string  `json:"friend_code"`
		Username   *string `json:"username"`
	}{id, email, code, name})
}

func (h *Handler) putUsername(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Username string `json:"username"`
	}
	if !httpx.Decode(w, r, maxBody, &in) {
		return
	}
	if err := h.svc.setUsername(r.Context(), httpx.User(r.Context()), in.Username); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// googleStart/googleCallback respondem a um navegador: erro sem como voltar ao app é texto puro.
func (h *Handler) googleStart(w http.ResponseWriter, r *http.Request) {
	if h.svc.cfg.GoogleClientID == "" {
		http.Error(w, "google desligado", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	to, err := h.svc.googleStart(r.Context(), one(q, "code_challenge"), one(q, "code_challenge_method"), one(q, "state"))
	if errors.Is(err, errBadRequest) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err != nil {
		logErr("google_start", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	redirect(w, to)
}

func (h *Handler) googleCallback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	challenge, appState, err := h.svc.takeState(r.Context(), one(q, "state"))
	if errors.Is(err, errBadState) {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}
	if err != nil {
		logErr("google_callback", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	back := url.Values{"state": {appState}}
	switch code := one(q, "code"); {
	case q.Get("error") == "access_denied":
		back.Set("error", "access_denied")
	case q.Get("error") != "" || code == "":
		back.Set("error", "google_failed")
	default:
		c, err := h.svc.googleLogin(r.Context(), challenge, code)
		if err != nil {
			logErr("google_login", err)
			back.Set("error", "google_failed")
		} else {
			back.Set("code", c)
		}
	}
	redirect(w, "pokelocationauth://callback?"+back.Encode())
}

func (h *Handler) googleExchange(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Code     string `json:"code"`
		Verifier string `json:"code_verifier"`
	}
	if !httpx.Decode(w, r, maxBody, &in) || !h.allow(w, r, "") {
		return
	}
	t, err := h.svc.googleExchange(r.Context(), in.Code, in.Verifier)
	h.reply(w, http.StatusOK, t, err)
}

// one: parâmetro repetido conta como ausente (sem ambiguidade sobre qual valor vale).
func one(q url.Values, k string) string {
	if v := q[k]; len(v) == 1 {
		return v[0]
	}
	return ""
}

// redirect: a URL leva state e código de uso único, então nada de cache nem Referer.
func redirect(w http.ResponseWriter, to string) {
	w.Header().Set("Location", to)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.WriteHeader(http.StatusFound)
}

func (h *Handler) reply(w http.ResponseWriter, status int, t tokens, err error) {
	if err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, status, t)
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	var ae *apiError
	switch {
	case errors.As(err, &ae):
		httpx.Error(w, ae.status, ae.code)
	case errors.Is(err, errArgonBusy):
		slog.Warn("auth: argon2 sem vaga")
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable")
	case errors.Is(err, context.Canceled):
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable")
	default:
		httpx.DBError(w, "auth", err)
	}
}

// logErr: erro do lado Google (texto nosso ou da lib) vai inteiro; erro de banco/Redis só com SQLSTATE ou tipo, porque
// a mensagem do Postgres pode ecoar entrada do usuário.
func logErr(op string, err error) {
	var g googleError
	var pg *pgconn.PgError
	switch {
	case errors.As(err, &g):
		slog.Error("auth: "+op, "err", err)
	case errors.As(err, &pg):
		slog.Error("auth: "+op, "sqlstate", pg.Code)
	default:
		slog.Warn("auth: "+op, "err_type", fmt.Sprintf("%T", err))
	}
}

// allow: 5/min por IP e por email. Recusado → já respondeu 429.
func (h *Handler) allow(w http.ResponseWriter, r *http.Request, email string) bool {
	e, _ := normEmail(email)
	now := h.svc.now()
	if h.ip.Allow(clientIP(r), now) && (e == "" || h.email.Allow(e, now)) {
		return true
	}
	httpx.Error(w, http.StatusTooManyRequests, "rate_limited")
	return false
}

// clientIP: o X-Forwarded-For é confiável porque o binário só é alcançável pelo Caddy, que descarta o de fora (o
// RemoteAddr é o próprio Caddy). IPv6 conta por /64: um cliente costuma ter o /64 inteiro.
func clientIP(r *http.Request) string {
	var a netip.Addr
	if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
		last := xff[len(xff)-1]
		if i := strings.LastIndexByte(last, ','); i >= 0 {
			last = last[i+1:]
		}
		a, _ = netip.ParseAddr(strings.TrimSpace(last))
	}
	if !a.IsValid() {
		ap, err := netip.ParseAddrPort(r.RemoteAddr)
		if err != nil {
			return r.RemoteAddr
		}
		a = ap.Addr()
	}
	if a = a.Unmap(); a.Is6() {
		p, _ := a.Prefix(64)
		return p.String()
	}
	return a.String()
}
