package groups

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
)

const reqWait = 5 * time.Second

type Handler struct {
	*Service // embutido: expõe h.OnChange ao main
	reqWait  time.Duration
}

func New(pool *pgxpool.Pool) *Handler { return &Handler{NewService(NewStore(pool)), reqWait} }

func (h *Handler) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	mux.Handle("POST /v1/groups", authed(httpx.Timed(h.reqWait, h.create)))
	mux.Handle("GET /v1/groups", authed(httpx.Timed(h.reqWait, h.list)))
	mux.Handle("GET /v1/groups/{id}", authed(httpx.Timed(h.reqWait, h.get)))
	mux.Handle("POST /v1/groups/{id}/members", authed(httpx.Timed(h.reqWait, h.add)))
	mux.Handle("DELETE /v1/groups/{id}/members/{userId}", authed(httpx.Timed(h.reqWait, h.remove)))
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Members []string `json:"members"`
	}
	if !httpx.Decode(w, r, 4<<10, &in) {
		return
	}
	id, err := h.Create(r.Context(), httpx.User(r.Context()), in.Members)
	if err != nil {
		fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	gs, err := h.List(r.Context(), httpx.User(r.Context()))
	if err != nil {
		fail(w, err)
		return
	}
	if gs == nil {
		gs = []Group{}
	}
	body, err := json.Marshal(map[string][]Group{"groups": gs})
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "internal")
		return
	}
	if len(body) > maxJSON { // nunca corta em silêncio
		httpx.Error(w, http.StatusRequestEntityTooLarge, "too_large")
		return
	}
	httpx.JSON(w, http.StatusOK, json.RawMessage(body))
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	g, err := h.Get(r.Context(), httpx.User(r.Context()), strings.ToLower(r.PathValue("id")))
	if err != nil {
		fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, g)
}

func (h *Handler) add(w http.ResponseWriter, r *http.Request) {
	var in struct {
		UserID string `json:"user_id"`
	}
	if !httpx.Decode(w, r, 4<<10, &in) {
		return
	}
	done(w, h.Add(r.Context(), httpx.User(r.Context()), strings.ToLower(r.PathValue("id")), in.UserID))
}

func (h *Handler) remove(w http.ResponseWriter, r *http.Request) {
	done(w, h.Remove(r.Context(), httpx.User(r.Context()), strings.ToLower(r.PathValue("id")), r.PathValue("userId")))
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
	case errors.Is(err, ErrForbidden):
		httpx.Error(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, ErrTooManyMembers):
		httpx.Error(w, http.StatusConflict, "too_many_members")
	case errors.Is(err, ErrTooManyGroups):
		httpx.Error(w, http.StatusConflict, "too_many_groups")
	case errors.Is(err, errBadRequest):
		httpx.Error(w, http.StatusBadRequest, "bad_request")
	default:
		httpx.DBError(w, "groups", err)
	}
}
