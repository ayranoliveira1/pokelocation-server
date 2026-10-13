// Package httpx: o pouco que todo handler REST repete — JSON de resposta, erro {"error": code}, corpo com limite e o
// usuário autenticado no contexto (posto pelo middleware do auth). Contrato em docs/api.md.
package httpx

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type userKey struct{}

// WithUser guarda o id do usuário autenticado. Só o middleware do auth chama.
func WithUser(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, userKey{}, id)
}

// User devolve o id do usuário autenticado; "" fora de rota autenticada.
func User(ctx context.Context) string {
	id, _ := ctx.Value(userKey{}).(string)
	return id
}

// JSON responde v com status. []byte vira base64 padrão com padding, como o app espera.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Error responde {"error": code}. code é um dos códigos de docs/api.md, nunca texto com dado do usuário.
func Error(w http.ResponseWriter, status int, code string) {
	JSON(w, status, map[string]string{"error": code})
}

// Decode lê um objeto JSON de no máximo max bytes, em até 10 s, em v. Inválido, grande demais, lento ou com sobra:
// responde 400 bad_request e devolve false (o handler só retorna). O prazo é por corpo porque um ReadTimeout global
// derrubaria o WebSocket.
func Decode(w http.ResponseWriter, r *http.Request, max int64, v any) bool {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(10 * time.Second))
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, max))
	if err := dec.Decode(v); err != nil || dec.Decode(&struct{}{}) != io.EOF {
		Error(w, http.StatusBadRequest, "bad_request")
		return false
	}
	return true
}
