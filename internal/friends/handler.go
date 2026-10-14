package friends

import (
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/ratelimit"
)

const reqWait = 5 * time.Second

type Handler struct {
	*Service                    // embutido: expõe h.OnChange ao main
	requests *ratelimit.Limiter // POST /v1/friends/requests por usuário: o código é a única barreira (32⁸)
	reqWait  time.Duration
}

func New(pool *pgxpool.Pool) *Handler {
	return &Handler{NewService(NewStore(pool)), ratelimit.New(10), reqWait}
}

func (h *Handler) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	mux.Handle("GET /v1/friends", authed(httpx.Timed(h.reqWait, h.list)))
	mux.Handle("POST /v1/friends/requests", authed(httpx.Timed(h.reqWait, h.request)))
	mux.Handle("POST /v1/friends/requests/{id}/accept", authed(httpx.Timed(h.reqWait, h.accept)))
	mux.Handle("POST /v1/friends/requests/{id}/decline", authed(httpx.Timed(h.reqWait, h.decline)))
	mux.Handle("DELETE /v1/friends/{userId}", authed(httpx.Timed(h.reqWait, h.remove)))
	mux.Handle("POST /v1/blocks/{userId}", authed(httpx.Timed(h.reqWait, h.block)))
	mux.Handle("DELETE /v1/blocks/{userId}", authed(httpx.Timed(h.reqWait, h.unblock)))
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	l, err := h.Service.List(r.Context(), httpx.User(r.Context()))
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
		Code     *string `json:"code"`
		Username *string `json:"username"`
	}
	if !httpx.Decode(w, r, 4<<10, &in) {
		return
	}
	if (in.Code == nil) == (in.Username == nil) { // um ou outro (§8)
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return
	}
	var (
		id, status string
		created    bool
		err        error
	)
	if in.Code != nil {
		id, status, created, err = h.Service.Request(r.Context(), httpx.User(r.Context()), *in.Code)
	} else {
		id, status, created, err = h.Service.RequestByUsername(r.Context(), httpx.User(r.Context()), *in.Username)
	}
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
	done(w, h.Service.Accept(r.Context(), httpx.User(r.Context()), r.PathValue("id")))
}

func (h *Handler) decline(w http.ResponseWriter, r *http.Request) {
	done(w, h.Service.Decline(r.Context(), httpx.User(r.Context()), r.PathValue("id")))
}

func (h *Handler) remove(w http.ResponseWriter, r *http.Request) {
	done(w, h.Service.Remove(r.Context(), httpx.User(r.Context()), r.PathValue("userId")))
}

func (h *Handler) block(w http.ResponseWriter, r *http.Request) {
	done(w, h.Service.Block(r.Context(), httpx.User(r.Context()), r.PathValue("userId")))
}

func (h *Handler) unblock(w http.ResponseWriter, r *http.Request) {
	done(w, h.Service.Unblock(r.Context(), httpx.User(r.Context()), r.PathValue("userId")))
}

func done(w http.ResponseWriter, err error) {
	if err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

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
