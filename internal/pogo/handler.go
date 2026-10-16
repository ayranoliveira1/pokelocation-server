package pogo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/ratelimit"
)

const (
	maxIn       = 64 << 10
	maxCode     = 2 << 10
	maxReveal   = 512
	reqWait     = 5 * time.Second
	proxyWait   = 15 * time.Second
	trainerWait = 60 * time.Second // o trainer lookup deles leva até 1 min
)

type Handler struct {
	svc     *Service // nil = sem POGO_TOKEN_KEY: tudo responde 503 pogo_disabled
	q       *queue
	workers int

	login, me, lookup, search, reveal, trainer, trainerLookup, trainerConfirm, rules *ratelimit.Limiter
}

func New(cfg Config, pool *pgxpool.Pool, rdb *redis.Client) (*Handler, error) {
	h := &Handler{
		login:          ratelimit.New(5),
		me:             ratelimit.New(10),
		lookup:         ratelimit.New(30),
		search:         ratelimit.New(10),
		reveal:         ratelimit.New(30),
		trainer:        ratelimit.New(10),
		trainerLookup:  ratelimit.NewWindow(25, time.Hour),
		trainerConfirm: ratelimit.NewWindow(25, time.Hour),
		rules:          ratelimit.New(30),
	}
	h.workers = defaultWorkers
	if cfg.Workers != "" {
		n, err := strconv.Atoi(cfg.Workers)
		if err != nil || n < 1 || n > maxWorkers {
			return nil, fmt.Errorf("pogo: POGO_WORKERS deve ser de 1 a %d", maxWorkers)
		}
		h.workers = n
	}
	if cfg.TokenKey == "" {
		return h, nil
	}
	svc, err := newService(cfg, pool)
	h.svc, h.q = svc, newQueue(rdb)
	return h, err
}

// Workers: POGO_WORKERS validado (padrão 8).
func (h *Handler) Workers() int { return h.workers }

// Enqueue põe o usuário na fila dos alertas (regra ligada, vínculo, token FCM novo). Quem não deve estar lá sai na
// primeira execução. Sem POGO_TOKEN_KEY não faz nada.
func (h *Handler) Enqueue(user string) {
	if h.svc == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), dbWait)
	defer cancel()
	if err := h.q.enqueue(ctx, user, time.Now()); err != nil {
		slog.Warn("pogo: enfileirar", "err_type", fmt.Sprintf("%T", err)) // a reconstrução cobre depois
	}
}

func (h *Handler) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	route := func(pattern string, lim *ratelimit.Limiter, wait time.Duration, f http.HandlerFunc) {
		mux.Handle(pattern, authed(h.guard(lim, httpx.Timed(wait, f))))
	}
	route("POST /v1/pogo/login", h.login, proxyWait, h.postLogin)
	route("DELETE /v1/pogo/login", nil, reqWait, h.deleteLogin)
	route("GET /v1/pogo/me", h.me, proxyWait, h.getMe)
	route("POST /v1/pogo/lookup", h.lookup, proxyWait, h.proxy("/map/lookup", 1<<20))
	route("POST /v1/pogo/search", h.search, proxyWait, h.proxy("/search", 256<<10))
	route("GET /v1/pogo/reveal/{code}", h.reveal, proxyWait, h.getReveal)
	route("GET /v1/pogo/trainer", h.trainer, proxyWait, h.proxy("/trainer", 64<<10))
	route("POST /v1/pogo/trainer/lookup", h.trainerLookup, trainerWait, h.proxy("/trainer/lookup", 64<<10))
	route("POST /v1/pogo/trainer/confirm", h.trainerConfirm, trainerWait, h.confirmTrainer)
	route("GET /v1/pogo/rules", h.rules, reqWait, h.listRules)
	route("POST /v1/pogo/rules", h.rules, reqWait, h.createRule)
	route("PUT /v1/pogo/rules/{id}", h.rules, reqWait, h.updateRule)
	route("DELETE /v1/pogo/rules/{id}", h.rules, reqWait, h.deleteRule)
}

func (h *Handler) guard(lim *ratelimit.Limiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.svc == nil {
			httpx.Error(w, http.StatusServiceUnavailable, "pogo_disabled")
			return
		}
		if lim != nil && !lim.Allow(httpx.User(r.Context()), time.Now()) {
			httpx.Error(w, http.StatusTooManyRequests, "rate_limited")
			return
		}
		next(w, r)
	}
}

func (h *Handler) postLogin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Provider string `json:"provider"`
		Code     string `json:"code"`
	}
	if !httpx.Decode(w, r, maxIn, &in) {
		return
	}
	if in.Provider != "google" && in.Provider != "discord" || in.Code == "" || len(in.Code) > maxCode {
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return
	}
	p, err := h.svc.Login(r.Context(), httpx.User(r.Context()), in.Provider, in.Code)
	if err != nil {
		fail(w, err)
		return
	}
	h.Enqueue(httpx.User(r.Context()))
	httpx.JSON(w, http.StatusOK, p)
}

func (h *Handler) deleteLogin(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Unlink(r.Context(), httpx.User(r.Context())); err != nil {
		fail(w, err)
		return
	}
	if err := h.q.remove(r.Context(), httpx.User(r.Context())); err != nil { // sobrando, sai na próxima execução
		slog.Warn("pogo: tirar da fila", "err_type", fmt.Sprintf("%T", err))
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) getMe(w http.ResponseWriter, r *http.Request) {
	p, err := h.svc.Me(r.Context(), httpx.User(r.Context()))
	if err != nil {
		fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, p)
}

func (h *Handler) getReveal(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	// o mux já decodificou: "%2E%2E" chega como ".." e o PoGo normalizaria o caminho
	if code == "" || len(code) > maxReveal || code == "." || code == ".." || strings.Contains(code, "/") {
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return
	}
	for i := range len(code) {
		if code[i] <= ' ' || code[i] >= 0x7f {
			httpx.Error(w, http.StatusBadRequest, "bad_request")
			return
		}
	}
	h.forward(w, r, "GET", "/reveal/"+url.PathEscape(code), nil, 16<<10)
}

// proxy: GET sem corpo; POST com um objeto JSON repassado como veio.
func (h *Handler) proxy(path string, max int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			h.forward(w, r, "GET", path, nil, max)
			return
		}
		if body, ok := decodeObject(w, r); ok {
			h.forward(w, r, "POST", path, body, max)
		}
	}
}

func decodeObject(w http.ResponseWriter, r *http.Request) (json.RawMessage, bool) {
	var body json.RawMessage
	if !httpx.Decode(w, r, maxIn, &body) {
		return nil, false
	}
	if body[0] != '{' { // Decode já tirou o espaço da frente
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return nil, false
	}
	return body, true
}

func (h *Handler) forward(w http.ResponseWriter, r *http.Request, method, path string, body []byte, max int64) {
	buf := getBuf()
	defer putBuf(buf)
	gz := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
	_, gzipped, err := h.svc.call(r.Context(), httpx.User(r.Context()), method, path, body, max, gz, buf)
	if err != nil {
		fail(w, err)
		return
	}
	writeRaw(w, buf.Bytes(), gzipped)
}

// confirmTrainer: depois do vínculo no PoGo, grava os treinadores para o alerta de shiny valer na hora. A resposta
// vem aberta (é pequena) para ser lida aqui.
func (h *Handler) confirmTrainer(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeObject(w, r)
	if !ok {
		return
	}
	buf := getBuf()
	defer putBuf(buf)
	user := httpx.User(r.Context())
	a, _, err := h.svc.call(r.Context(), user, "POST", "/trainer/confirm", body, 64<<10, false, buf)
	if err != nil {
		fail(w, err)
		return
	}
	var out struct {
		Accounts []json.RawMessage `json:"accounts"`
	}
	if json.Unmarshal(buf.Bytes(), &out) == nil && out.Accounts != nil {
		p := &Profile{Trainers: activeTrainers(out.Accounts)}
		if err := h.svc.store.updateTrainers(r.Context(), user, a.enc, p); err != nil {
			slog.Warn("pogo: treinadores", "err_type", fmt.Sprintf("%T", err)) // o vínculo no PoGo já valeu; o /me acerta
		}
	}
	writeRaw(w, buf.Bytes(), false)
}

func writeRaw(w http.ResponseWriter, body []byte, gzipped bool) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Vary", "Accept-Encoding")
	if gzipped {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func fail(w http.ResponseWriter, err error) {
	var ue *UpstreamError
	switch {
	case errors.As(err, &ue):
		out := struct {
			Error      string          `json:"error"`
			Status     int             `json:"status"`
			Detail     json.RawMessage `json:"detail"`
			RetryAfter int             `json:"retry_after,omitempty"`
		}{"upstream", ue.Status, ue.Detail, ue.RetryAfter}
		if ue.Status == http.StatusTooManyRequests && ue.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(ue.RetryAfter))
		}
		httpx.JSON(w, ue.Status, out)
	case errors.Is(err, ErrRelogin):
		httpx.Error(w, http.StatusConflict, "pogo_relogin")
	case errors.Is(err, ErrInvalidGrant):
		httpx.Error(w, http.StatusBadRequest, "invalid_grant")
	case errors.Is(err, ErrUnavailable):
		httpx.Error(w, http.StatusBadGateway, "upstream_unavailable")
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, http.StatusNotFound, "not_found")
	case errors.Is(err, ErrTooManyRules):
		httpx.Error(w, http.StatusConflict, "too_many_rules")
	case errors.Is(err, ErrTrainerRequired):
		httpx.Error(w, http.StatusUnprocessableEntity, "trainer_required")
	case errors.Is(err, ErrShinyAccount):
		httpx.Error(w, http.StatusUnprocessableEntity, "shiny_account_unavailable")
	case errors.Is(err, errBadRule):
		httpx.Error(w, http.StatusBadRequest, "bad_request")
	default:
		httpx.DBError(w, "pogo", err)
	}
}
