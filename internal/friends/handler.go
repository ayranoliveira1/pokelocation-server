package friends

import (
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/ratelimit"
)

type Handler struct {
	svc      *Service
	requests *ratelimit.Limiter // POST /v1/friends/requests por usuário: o código é a única barreira (32⁸)
}

// New monta handler, service e store sobre o pool.
func New(pool *pgxpool.Pool) *Handler {
	return &Handler{NewService(NewStore(pool)), ratelimit.New(10)}
}

// Routes registra as rotas de docs/api.md §3, todas autenticadas.
func (h *Handler) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	mux.Handle("GET /v1/friends", authed(h.list))
	mux.Handle("POST /v1/friends/requests", authed(h.request))
	mux.Handle("POST /v1/friends/requests/{id}/accept", authed(h.accept))
	mux.Handle("POST /v1/friends/requests/{id}/decline", authed(h.decline))
	mux.Handle("DELETE /v1/friends/{userId}", authed(h.remove))
	mux.Handle("POST /v1/blocks/{userId}", authed(h.block))
	mux.Handle("DELETE /v1/blocks/{userId}", authed(h.unblock))
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	l, err := h.svc.List(r.Context(), httpx.User(r.Context()))
	if err != nil {
		fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, l)
}

func (h *Handler) request(w http.ResponseWriter, r *http.Request) {
	if !h.requests.Allow(httpx.User(r.Context()), time.Now()) { // antes de tudo: erros também contam
		httpx.Error(w, http.StatusTooManyRequests, "rate_limited")
		return
	}
	var in struct {
		Code string `json:"code"`
	}
	if !httpx.Decode(w, r, 4<<10, &in) {
		return
	}
	id, status, created, err := h.svc.Request(r.Context(), httpx.User(r.Context()), in.Code)
	if err != nil {
		fail(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	httpx.JSON(w, code, map[string]string{"id": id, "status": status})
}

func (h *Handler) accept(w http.ResponseWriter, r *http.Request) {
	done(w, h.svc.Accept(r.Context(), httpx.User(r.Context()), r.PathValue("id")))
}

func (h *Handler) decline(w http.ResponseWriter, r *http.Request) {
	done(w, h.svc.Decline(r.Context(), httpx.User(r.Context()), r.PathValue("id")))
}

func (h *Handler) remove(w http.ResponseWriter, r *http.Request) {
	done(w, h.svc.Remove(r.Context(), httpx.User(r.Context()), r.PathValue("userId")))
}

func (h *Handler) block(w http.ResponseWriter, r *http.Request) {
	done(w, h.svc.Block(r.Context(), httpx.User(r.Context()), r.PathValue("userId")))
}

func (h *Handler) unblock(w http.ResponseWriter, r *http.Request) {
	done(w, h.svc.Unblock(r.Context(), httpx.User(r.Context()), r.PathValue("userId")))
}

// done: 204 ou o erro.
func done(w http.ResponseWriter, err error) {
	if err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// fail traduz o erro para o código do contrato; o resto (banco) vai para httpx.DBError.
func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		httpx.Error(w, http.StatusNotFound, "not_found")
	case errors.Is(err, ErrCodeNotFound):
		httpx.Error(w, http.StatusNotFound, "code_not_found")
	case errors.Is(err, errBlockSelf):
		httpx.Error(w, http.StatusBadRequest, "bad_request")
	case errors.Is(err, ErrSelf):
		httpx.Error(w, http.StatusBadRequest, "self_request")
	case errors.Is(err, ErrAlreadyFriends):
		httpx.Error(w, http.StatusConflict, "already_friends")
	case errors.Is(err, ErrBlocked):
		httpx.Error(w, http.StatusForbidden, "blocked")
	case errors.Is(err, ErrLimit):
		httpx.Error(w, http.StatusTooManyRequests, "rate_limited")
	default:
		httpx.DBError(w, "friends", err)
	}
}
