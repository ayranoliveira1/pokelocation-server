// Package httpx: o que todo handler REST repete (contrato em docs/api.md).
package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type userKey struct{}

// WithUser: só o middleware do auth chama.
func WithUser(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, userKey{}, id)
}

// User: "" fora de rota autenticada.
func User(ctx context.Context) string {
	id, _ := ctx.Value(userKey{}).(string)
	return id
}

// JSON: []byte vira base64 padrão com padding, como o app espera.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Error: code é um dos códigos de docs/api.md, nunca texto com dado do usuário.
func Error(w http.ResponseWriter, status int, code string) {
	JSON(w, status, map[string]string{"error": code})
}

// Decode responde 400 bad_request e devolve false se o corpo for inválido, grande, lento ou tiver sobra. O prazo é
// por corpo porque um ReadTimeout global no http.Server derrubaria o WebSocket.
func Decode(w http.ResponseWriter, r *http.Request, max int64, v any) bool {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(10 * time.Second))
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, max))
	if err := dec.Decode(v); err != nil || dec.Decode(&struct{}{}) != io.EOF {
		Error(w, http.StatusBadRequest, "bad_request")
		return false
	}
	return true
}

// DBError: resposta do Postgres é bug nosso (500); o resto é banco indisponível (503). Loga só o SQLSTATE ou o
// tipo do erro, porque a mensagem do Postgres pode ecoar entrada do usuário.
func DBError(w http.ResponseWriter, pkg string, err error) {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		slog.Error(pkg+": sql", "sqlstate", pg.Code)
		Error(w, http.StatusInternalServerError, "internal")
		return
	}
	slog.Warn(pkg+": banco indisponível", "err_type", fmt.Sprintf("%T", err))
	Error(w, http.StatusServiceUnavailable, "unavailable")
}

func Timed(d time.Duration, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		h(w, r.WithContext(ctx))
	}
}
