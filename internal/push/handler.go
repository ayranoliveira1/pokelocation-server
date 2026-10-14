// Package push: aviso "nova mensagem" pelo FCM, sem conteúdo, para quem não está conectado (docs/api.md §7, S10).
package push

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
)

const (
	maxBody  = 4 << 10
	reqWait  = 5 * time.Second
	maxToken = 4 << 10
)

type Handler struct {
	store *Store
}

func NewHandler(pool *pgxpool.Pool) *Handler { return &Handler{store: &Store{pool: pool}} }

func (h *Handler) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	mux.Handle("PUT /v1/devices/fcm", authed(httpx.Timed(reqWait, h.putToken)))
}

func (h *Handler) putToken(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Token json.RawMessage `json:"token"` // ausente ≠ null (null apaga)
	}
	if !httpx.Decode(w, r, maxBody, &in) {
		return
	}
	var tok *string
	if len(in.Token) == 0 || json.Unmarshal(in.Token, &tok) != nil || tok != nil && (*tok == "" || len(*tok) > maxToken) {
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return
	}
	ok, err := h.store.setToken(r.Context(), httpx.User(r.Context()), tok)
	if err != nil {
		httpx.DBError(w, "push", err)
		return
	}
	if !ok {
		httpx.Error(w, http.StatusNotFound, "not_found") // sem aparelho: o PUT /v1/keys vem antes
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
