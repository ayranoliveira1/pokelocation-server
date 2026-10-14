package keys

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/ratelimit"
)

// Formato serializado da libsignal 0.86 (docs/api.md §4).
const (
	ecLen, ecPrefix       = 33, 0x05
	kyberLen, kyberPrefix = 1569, 0x08 // Kyber1024
	sigLen                = 64
	maxKeyID              = 1<<24 - 1
	maxPerList            = 100
	maxBody               = 512 << 10
	reqWait               = 5 * time.Second
)

// KeyID é ponteiro para distinguir "faltando" de 0 (válido).
type preKey struct {
	KeyID     *int64 `json:"key_id"`
	PublicKey []byte `json:"public_key"`
	Signature []byte `json:"signature"`
}

func (k *preKey) valid(size int, prefix byte, signed bool) bool {
	return k != nil && k.KeyID != nil && *k.KeyID >= 0 && *k.KeyID <= maxKeyID &&
		len(k.PublicKey) == size && k.PublicKey[0] == prefix &&
		(signed && len(k.Signature) == sigLen || !signed && k.Signature == nil)
}

type bundle struct {
	RegistrationID  int64    `json:"registration_id"`
	IdentityKey     []byte   `json:"identity_key"`
	SignedPrekey    *preKey  `json:"signed_prekey"`
	KyberLastResort *preKey  `json:"kyber_last_resort"`
	OneTimePrekeys  []preKey `json:"one_time_prekeys"`
	KyberPrekeys    []preKey `json:"kyber_prekeys"`
}

func (b *bundle) valid() bool {
	if b.RegistrationID < 1 || b.RegistrationID > 16380 || len(b.IdentityKey) != ecLen || b.IdentityKey[0] != ecPrefix ||
		!b.SignedPrekey.valid(ecLen, ecPrefix, true) || !b.KyberLastResort.valid(kyberLen, kyberPrefix, true) ||
		len(b.OneTimePrekeys) > maxPerList || len(b.KyberPrekeys) > maxPerList {
		return false
	}
	for i := range b.OneTimePrekeys {
		if !b.OneTimePrekeys[i].valid(ecLen, ecPrefix, false) {
			return false
		}
	}
	for i := range b.KyberPrekeys {
		if !b.KyberPrekeys[i].valid(kyberLen, kyberPrefix, true) {
			return false
		}
	}
	return true
}

type keyOut struct {
	KeyID     int64  `json:"key_id"`
	PublicKey []byte `json:"public_key"`
	Signature []byte `json:"signature,omitempty"`
}

type fetched struct {
	RegistrationID int64   `json:"registration_id"`
	IdentityKey    []byte  `json:"identity_key"`
	SignedPrekey   *keyOut `json:"signed_prekey"`
	KyberPrekey    *keyOut `json:"kyber_prekey"`
	OneTimePrekey  *keyOut `json:"one_time_prekey,omitempty"`
}

type Handler struct {
	store   *Store
	fetches *ratelimit.Limiter // GET /v1/keys/{userId} por par: um amigo não esvazia as one-time do outro
	reqWait time.Duration
}

func New(pool *pgxpool.Pool) *Handler { return &Handler{NewStore(pool), ratelimit.New(10), reqWait} }

func (h *Handler) Routes(mux *http.ServeMux, authed func(http.HandlerFunc) http.Handler) {
	mux.Handle("PUT /v1/keys", authed(httpx.Timed(h.reqWait, h.put)))
	mux.Handle("GET /v1/keys/count", authed(httpx.Timed(h.reqWait, h.count)))
	mux.Handle("GET /v1/keys/{userId}", authed(httpx.Timed(h.reqWait, h.fetch)))
}

func (h *Handler) put(w http.ResponseWriter, r *http.Request) {
	var b bundle
	if !httpx.Decode(w, r, maxBody, &b) {
		return
	}
	if !b.valid() {
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return
	}
	if err := h.store.put(r.Context(), httpx.User(r.Context()), &b); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) count(w http.ResponseWriter, r *http.Request) {
	ec, ky, err := h.store.count(r.Context(), httpx.User(r.Context()))
	if err != nil {
		fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]int{"ec": ec, "kyber": ky})
}

func (h *Handler) fetch(w http.ResponseWriter, r *http.Request) {
	other := r.PathValue("userId")
	if len(other) != 36 || uuid.Validate(other) != nil { // malformado nunca é amigo
		httpx.Error(w, http.StatusForbidden, "forbidden")
		return
	}
	me := httpx.User(r.Context())
	if !h.fetches.Allow(me+" "+strings.ToLower(other), time.Now()) {
		httpx.Error(w, http.StatusTooManyRequests, "rate_limited")
		return
	}
	f, err := h.store.fetch(r.Context(), me, other)
	if err != nil {
		fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, f)
}

func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errForbidden):
		httpx.Error(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, errNotFound):
		httpx.Error(w, http.StatusNotFound, "not_found")
	case errors.Is(err, errTooMany):
		httpx.Error(w, http.StatusBadRequest, "bad_request")
	default:
		httpx.DBError(w, "keys", err)
	}
}
