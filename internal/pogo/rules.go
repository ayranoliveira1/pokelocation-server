package pogo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
)

const (
	maxRules   = 10
	maxName    = 60
	maxFilters = 8 << 10
	maxRadius  = 50.0
)

var (
	ErrNotFound        = errors.New("not_found")
	ErrTooManyRules    = errors.New("too_many_rules")
	ErrTrainerRequired = errors.New("trainer_required")
	ErrShinyAccount    = errors.New("shiny_account_unavailable")
	errBadRule         = errors.New("bad_request")
)

const (
	lockRules   = `SELECT pg_advisory_xact_lock(hashtextextended('pogo_rules:' || $1::text, 0))`
	qCountRules = `SELECT count(*) FROM pogo_rules WHERE user_id = $1`
	qListRules  = `SELECT ` + ruleCols + ` FROM pogo_rules WHERE user_id = $1 ORDER BY created_at, pogo_rules.id LIMIT $2`
	qInsertRule = `INSERT INTO pogo_rules (user_id, name, filters, scope, lat, lng, radius_km, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING ` + ruleCols
	qUpdateRule = `UPDATE pogo_rules SET name = $3, filters = $4, scope = $5, lat = $6, lng = $7, radius_km = $8,
		enabled = $9, disabled_reason = NULL WHERE id = $1 AND user_id = $2 RETURNING ` + ruleCols
	// Só a regra como o worker a leu: uma edição do usuário no meio não é desligada com o erro velho.
	qDisableRule = `UPDATE pogo_rules SET enabled = false, disabled_reason = $3
		WHERE id = $1 AND user_id = $2 AND enabled AND filters = $4::jsonb`
	qTrainers   = `SELECT trainers FROM pogo_accounts WHERE user_id = $1`
	qDeleteRule = `DELETE FROM pogo_rules WHERE id = $1 AND user_id = $2`
)

const ruleCols = `id::text, name, filters, scope, lat, lng, radius_km, enabled, disabled_reason, created_at`

const reasonTrainer = "trainer_required"

type Rule struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Filters  json.RawMessage `json:"filters"`
	Scope    string          `json:"scope"`
	Lat      *float64        `json:"lat"`
	Lng      *float64        `json:"lng"`
	RadiusKm *float64        `json:"radius_km"`
	Enabled  bool            `json:"enabled"`
	// DisabledReason: null, ou "trainer_required" quando o worker desligou a regra
	DisabledReason *string `json:"disabled_reason"`
	CreatedAt      int64   `json:"created_at"` // epoch ms
}

func scanRule(row pgx.Row) (Rule, error) {
	var r Rule
	var at time.Time
	err := row.Scan(&r.ID, &r.Name, &r.Filters, &r.Scope, &r.Lat, &r.Lng, &r.RadiusKm, &r.Enabled, &r.DisabledReason, &at)
	r.CreatedAt = at.UnixMilli()
	return r, err
}

type ruleIn struct {
	Name     string          `json:"name"`
	Filters  json.RawMessage `json:"filters"`
	Scope    string          `json:"scope"`
	Lat      *float64        `json:"lat"`
	Lng      *float64        `json:"lng"`
	RadiusKm *float64        `json:"radius_km"`
	Enabled  *bool           `json:"enabled"` // ausente = true
}

func (in *ruleIn) normalize() error {
	in.Name = strings.TrimSpace(in.Name)
	if n := utf8.RuneCountInString(in.Name); n == 0 || n > maxName || !utf8.ValidString(in.Name) {
		return errBadRule
	}
	if len(in.Filters) == 0 || in.Filters[0] != '{' || len(in.Filters) > maxFilters {
		return errBadRule
	}
	switch in.Scope {
	case "world":
		in.Lat, in.Lng, in.RadiusKm = nil, nil, nil
	case "near":
		if in.Lat == nil || in.Lng == nil || in.RadiusKm == nil || *in.Lat < -90 || *in.Lat > 90 ||
			*in.Lng < -180 || *in.Lng > 180 || *in.RadiusKm <= 0 || *in.RadiusKm > maxRadius {
			return errBadRule
		}
	default:
		return errBadRule
	}
	if in.Enabled == nil {
		t := true
		in.Enabled = &t
	}
	return nil
}

type trainerRef struct {
	Code   string `json:"trainer_code"`
	Active *bool  `json:"active"`
}

// checkTrainer: filtro shiny (shiny_only, background_only, shiny_account) só com treinador ativo no vínculo, e o
// shiny_account tem de ser um deles. Sem filtro shiny, não lê o banco.
func (s *Store) checkTrainer(ctx context.Context, user string, filters json.RawMessage) error {
	var f shinyFilters
	if json.Unmarshal(filters, &f) != nil {
		return errBadRule
	}
	if !f.any() {
		return nil
	}
	var raw []byte
	err := s.pool.QueryRow(ctx, qTrainers, user).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrTrainerRequired
	}
	if err != nil {
		return err
	}
	var ts []trainerRef
	_ = json.Unmarshal(raw, &ts)
	ts = slices.DeleteFunc(ts, func(t trainerRef) bool { return t.Active != nil && !*t.Active })
	if len(ts) == 0 {
		return ErrTrainerRequired
	}
	if f.ShinyAccount != "" && !slices.ContainsFunc(ts, func(t trainerRef) bool { return t.Code == f.ShinyAccount }) {
		return ErrShinyAccount
	}
	return nil
}

func (s *Store) disableRule(ctx context.Context, user string, r *dueRule, reason string) error {
	_, err := s.pool.Exec(ctx, qDisableRule, r.ID, user, reason, r.Filters)
	return err
}

func (s *Store) rules(ctx context.Context, user string) ([]Rule, error) {
	rows, err := s.pool.Query(ctx, qListRules, user, maxRules)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Rule, error) { return scanRule(row) })
}

// O teto é conferido sob o advisory lock do usuário: duas criações juntas não passam de 10.
func (s *Store) createRule(ctx context.Context, user string, in *ruleIn) (Rule, error) {
	var r Rule
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, lockRules, user); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRow(ctx, qCountRules, user).Scan(&n); err != nil {
			return err
		}
		if n >= maxRules {
			return ErrTooManyRules
		}
		var err error
		r, err = scanRule(tx.QueryRow(ctx, qInsertRule, user, in.Name, in.Filters, in.Scope, in.Lat, in.Lng,
			in.RadiusKm, *in.Enabled))
		return err
	})
	return r, err
}

func (s *Store) updateRule(ctx context.Context, user, id string, in *ruleIn) (Rule, error) {
	r, err := scanRule(s.pool.QueryRow(ctx, qUpdateRule, id, user, in.Name, in.Filters, in.Scope, in.Lat, in.Lng,
		in.RadiusKm, *in.Enabled))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) deleteRule(ctx context.Context, user, id string) error {
	tag, err := s.pool.Exec(ctx, qDeleteRule, id, user)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (h *Handler) listRules(w http.ResponseWriter, r *http.Request) {
	rs, err := h.svc.store.rules(r.Context(), httpx.User(r.Context()))
	if err != nil {
		fail(w, err)
		return
	}
	if rs == nil {
		rs = []Rule{}
	}
	httpx.JSON(w, http.StatusOK, map[string][]Rule{"rules": rs})
}

func (h *Handler) createRule(w http.ResponseWriter, r *http.Request) {
	in, ok := h.decodeRule(w, r)
	if !ok {
		return
	}
	rule, err := h.svc.store.createRule(r.Context(), httpx.User(r.Context()), in)
	if err != nil {
		fail(w, err)
		return
	}
	if rule.Enabled {
		h.Enqueue(httpx.User(r.Context()))
	}
	httpx.JSON(w, http.StatusCreated, rule)
}

func (h *Handler) updateRule(w http.ResponseWriter, r *http.Request) {
	id, ok := ruleID(w, r)
	if !ok {
		return
	}
	in, ok := h.decodeRule(w, r)
	if !ok {
		return
	}
	rule, err := h.svc.store.updateRule(r.Context(), httpx.User(r.Context()), id, in)
	if err != nil {
		fail(w, err)
		return
	}
	if rule.Enabled {
		h.Enqueue(httpx.User(r.Context()))
	}
	httpx.JSON(w, http.StatusOK, rule)
}

func (h *Handler) deleteRule(w http.ResponseWriter, r *http.Request) {
	id, ok := ruleID(w, r)
	if !ok {
		return
	}
	if err := h.svc.store.deleteRule(r.Context(), httpx.User(r.Context()), id); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) decodeRule(w http.ResponseWriter, r *http.Request) (*ruleIn, bool) {
	var in ruleIn
	if !httpx.Decode(w, r, maxIn, &in) {
		return nil, false
	}
	if in.normalize() != nil {
		httpx.Error(w, http.StatusBadRequest, "bad_request")
		return nil, false
	}
	// desligada não roda: quem perdeu o treinador ainda pode desligar ou renomear a regra; religar confere
	if *in.Enabled {
		if err := h.svc.store.checkTrainer(r.Context(), httpx.User(r.Context()), in.Filters); err != nil {
			fail(w, err)
			return nil, false
		}
	}
	return &in, true
}

// Só a forma canônica: id malformado é 404, como id de outro usuário.
func ruleID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := strings.ToLower(r.PathValue("id"))
	if len(id) != 36 || uuid.Validate(id) != nil {
		httpx.Error(w, http.StatusNotFound, "not_found")
		return "", false
	}
	return id, true
}
