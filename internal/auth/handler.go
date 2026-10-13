// Package auth: cadastro e login (email+senha, Google), tokens e o middleware que autentica as rotas 🔒.
// Contrato em docs/api.md §2.
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

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/ratelimit"
)

// Config vem das variáveis de ambiente (main).
type Config struct {
	SigningKey         string // AUTH_SIGNING_KEY: seed Ed25519 de 32 bytes em base64 padrão
	GoogleClientID     string // GOOGLE_CLIENT_ID (vazio = login Google desligado)
	GoogleClientSecret string // GOOGLE_CLIENT_SECRET
	PublicURL          string // PUBLIC_URL: https://<host> visto pelo navegador (redirect_uri do Google)
}

const maxBody = 4 << 10

// Handler: rotas HTTP do auth.
type Handler struct {
	// OnRevoke: chamado depois do commit do logout, do reuso de refresh e do vínculo Google (o main liga em hub.Kick).
	OnRevoke func(user string)
	// OnFriendsChange: amizade desfeita pelo vínculo Google (o main liga em hub.ReloadFriends dos dois).
	OnFriendsChange func(a, b string)

	svc       *Service
	ip, email *ratelimit.Limiter
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
	}
	s.google = s.googleIdentity
	h := &Handler{svc: s, ip: ratelimit.New(5), email: ratelimit.New(5)}
	s.notify = h.notify
	if s.dummy, err = s.hashPassword(context.Background(), rand.Text()); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *Handler) notify(user string, exFriends []string) {
	if h.OnRevoke != nil {
		h.OnRevoke(user)
	}
	if h.OnFriendsChange != nil {
		for _, f := range exFriends {
			h.OnFriendsChange(user, f)
		}
	}
}

// Routes registra as rotas; authed protege as 🔒 (no main, o próprio h.Authed).
func (h *Handler) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	mux.HandleFunc("POST /v1/auth/register", h.register)
	mux.HandleFunc("POST /v1/auth/login", h.login)
	mux.HandleFunc("POST /v1/auth/refresh", h.refresh)
	mux.HandleFunc("POST /v1/auth/logout", h.logout)
	mux.HandleFunc("GET /v1/auth/google/start", h.googleStart)
	mux.HandleFunc("GET /v1/auth/google/callback", h.googleCallback)
	mux.HandleFunc("POST /v1/auth/google/exchange", h.googleExchange)
	mux.Handle("GET /v1/me", authed(h.me))
}

// Cleanup apaga refresh tokens vencidos. Para a limpeza diária (main).
func (h *Handler) Cleanup(ctx context.Context) error { return h.svc.store.cleanup(ctx, h.svc.now()) }

// Verify valida um access token (para o Auth do WebSocket, S6): devolve o dono e o vencimento. Token inválido,
// vencido ou revogado (logout, reuso de refresh, vínculo Google) → ErrUnauthorized; outro erro = banco fora.
func (h *Handler) Verify(ctx context.Context, token string) (userID string, exp time.Time, err error) {
	return h.svc.verify(ctx, token)
}

// Authed exige "Authorization: Bearer <access>" válido e põe o id do usuário no contexto (httpx.User).
func (h *Handler) Authed(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
		if !strings.EqualFold(scheme, "Bearer") {
			httpx.Error(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		uid, _, err := h.svc.verify(r.Context(), token)
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
}

type refreshBody struct {
	Token string `json:"refresh_token"`
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !httpx.Decode(w, r, maxBody, &in) || !h.allow(w, r, in.Email) {
		return
	}
	t, err := h.svc.register(r.Context(), in.Email, in.Password)
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
	email, code, err := h.svc.store.me(r.Context(), id)
	if errors.Is(err, errNotFound) {
		err = errUnauthorized // conta apagada com access ainda válido
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, struct {
		ID         string  `json:"id"`
		Email      *string `json:"email"`
		FriendCode string  `json:"friend_code"`
	}{id, email, code})
}

// googleStart/callback são navegação do navegador: erro sem volta ao app é texto puro (api.md §2).
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

// one: o valor do parâmetro só se vier exatamente uma vez.
func one(q url.Values, k string) string {
	if v := q[k]; len(v) == 1 {
		return v[0]
	}
	return ""
}

// redirect 302 sem corpo; a URL leva state/código: sem cache nem Referer.
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

// fail: erro esperado → código do api.md; argon2 lotado → 503 com o log certo; cliente que desistiu → 503 sem log; o
// resto vai para httpx.DBError (500 se o Postgres respondeu, senão 503).
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

// logErr para o fluxo Google (que responde por redirect): erro do Google é nosso texto ou da lib (sem dado do
// usuário) e vai inteiro; erro de banco/Redis só com SQLSTATE ou tipo, como o httpx.DBError.
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

// allow: 5/min por IP e por email (email "" = só IP). Recusado → responde 429 e devolve false.
func (h *Handler) allow(w http.ResponseWriter, r *http.Request, email string) bool {
	e, _ := normEmail(email)
	now := h.svc.now()
	if h.ip.Allow(clientIP(r), now) && (e == "" || h.email.Allow(e, now)) {
		return true
	}
	httpx.Error(w, http.StatusTooManyRequests, "rate_limited")
	return false
}

// clientIP: o X-Forwarded-For que o Caddy põe (api.md §1: ele descarta o de fora; o RemoteAddr é o Caddy). Último
// valor, por garantia. Sem XFF válido (só sem Caddy: testes) cai no RemoteAddr. IPv6 vira o /64 (um cliente costuma
// ter o /64 inteiro).
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
