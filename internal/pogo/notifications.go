package pogo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ayranoliveira1/pokelocation-server/internal/httpx"
	"github.com/ayranoliveira1/pokelocation-server/internal/push"
)

const (
	maxNotifs    = 200 // por usuário
	notifKeep    = 7 * 24 * time.Hour
	cleanupEvery = time.Hour
	pageDefault  = 20
	pageMax      = 50
)

const (
	qInsertNotif = `INSERT INTO pogo_notifications (user_id, data) VALUES ($1, $2) RETURNING id`
	qTrimNotifs  = `DELETE FROM pogo_notifications WHERE user_id = $1 AND id <= (SELECT id FROM pogo_notifications
		WHERE user_id = $1 ORDER BY id DESC OFFSET $2 LIMIT 1)`
	qListNotifs = `SELECT id, created_at, data FROM pogo_notifications WHERE user_id = $1 AND id < $2
		ORDER BY id DESC LIMIT $3`
	qDeleteNotif   = `DELETE FROM pogo_notifications WHERE id = $1 AND user_id = $2`
	qDeleteNotifs  = `DELETE FROM pogo_notifications WHERE user_id = $1`
	qForgetNotifs  = `DELETE FROM pogo_notifications WHERE id = ANY($1) AND user_id = $2`
	qExpiredNotifs = `DELETE FROM pogo_notifications WHERE created_at < $1`
)

type Notification struct {
	ID        string          `json:"id"`
	CreatedAt int64           `json:"created_at"` // epoch ms
	Data      json.RawMessage `json:"data"`
}

// record grava os avisos e põe o id de cada um no campo "h" do push, para o app apagar sem recarregar a lista.
func (s *Store) record(ctx context.Context, user string, msgs []push.Data) error {
	b := &pgx.Batch{}
	for _, m := range msgs {
		data, err := json.Marshal(m.Fields)
		if err != nil {
			return err
		}
		b.Queue(qInsertNotif, user, data)
	}
	b.Queue(qTrimNotifs, user, maxNotifs)
	br := s.pool.SendBatch(ctx, b)
	defer br.Close()
	for _, m := range msgs {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		m.Fields["h"] = strconv.FormatInt(id, 10)
	}
	_, err := br.Exec()
	return err
}

func (s *Store) forget(ctx context.Context, user string, msgs []push.Data) error {
	ids := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		if id, err := strconv.ParseInt(m.Fields["h"], 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, qForgetNotifs, ids, user)
	return err
}

func (s *Store) notifications(ctx context.Context, user string, before int64, limit int) ([]Notification, error) {
	rows, err := s.pool.Query(ctx, qListNotifs, user, before, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Notification, error) {
		var n Notification
		var id int64
		var at time.Time
		err := row.Scan(&id, &at, &n.Data)
		n.ID, n.CreatedAt = strconv.FormatInt(id, 10), at.UnixMilli()
		return n, err
	})
}

// Unsent apaga do histórico os avisos que o FCM não aceitou: o histórico fica só com o que foi enviado.
func (h *Handler) Unsent(user string, msgs []push.Data) {
	if h.svc == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), dbWait)
	defer cancel()
	if err := h.svc.store.forget(ctx, user, msgs); err != nil {
		slog.Warn("pogo: histórico", "err_type", fmt.Sprintf("%T", err))
	}
}

// cleanup: cada instância roda a sua; repetir não faz mal.
func (w *worker) cleanup(ctx context.Context) {
	t := time.NewTicker(cleanupEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		dctx, cancel := context.WithTimeout(ctx, time.Minute)
		_, err := w.svc.store.pool.Exec(dctx, qExpiredNotifs, w.now().Add(-notifKeep))
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("pogo: limpeza do histórico", "err_type", fmt.Sprintf("%T", err))
		}
	}
}

func (h *Handler) listNotifications(w http.ResponseWriter, r *http.Request) {
	limit, before := pageDefault, int64(math.MaxInt64)
	q := r.URL.Query()
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > pageMax {
			httpx.Error(w, http.StatusBadRequest, "bad_request")
			return
		}
		limit = n
	}
	if s := q.Get("before"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 1 {
			httpx.Error(w, http.StatusBadRequest, "bad_request")
			return
		}
		before = n
	}
	ns, err := h.svc.store.notifications(r.Context(), httpx.User(r.Context()), before, limit+1)
	if err != nil {
		fail(w, err)
		return
	}
	out := struct {
		Items []Notification `json:"items"`
		Next  *string        `json:"next"`
	}{Items: ns}
	if len(ns) > limit {
		out.Items = ns[:limit]
		out.Next = &ns[limit-1].ID
	}
	if out.Items == nil {
		out.Items = []Notification{}
	}
	httpx.JSON(w, http.StatusOK, out)
}

// Id malformado, de outro usuário ou já apagado: 204 também, o app só quer que suma.
func (h *Handler) deleteNotification(w http.ResponseWriter, r *http.Request) {
	if id, err := strconv.ParseInt(r.PathValue("id"), 10, 64); err == nil {
		if _, err := h.svc.store.pool.Exec(r.Context(), qDeleteNotif, id, httpx.User(r.Context())); err != nil {
			fail(w, err)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) deleteNotifications(w http.ResponseWriter, r *http.Request) {
	if _, err := h.svc.store.pool.Exec(r.Context(), qDeleteNotifs, httpx.User(r.Context())); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
