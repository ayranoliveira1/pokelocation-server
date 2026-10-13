// Package friends: código de amigo, pedidos, aceite, remoção e bloqueio (docs/api.md §3).
package friends

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"unicode"

	"github.com/google/uuid"
)

// Tetos por usuário (docs/api.md §3): mantêm GET /v1/friends abaixo de 64 KiB.
const (
	maxFriends = 500
	maxPending = 50
)

var (
	ErrNotFound       = errors.New("not_found")
	ErrCodeNotFound   = errors.New("code_not_found")
	ErrSelf           = errors.New("self_request")
	ErrAlreadyFriends = errors.New("already_friends")
	ErrBlocked        = errors.New("blocked")
	ErrLimit          = errors.New("limit")
	errBlockSelf      = errors.New("block self")
)

type Friend struct {
	UserID     string `json:"user_id"`
	FriendCode string `json:"friend_code"`
}

type Request struct {
	ID         string `json:"id"`
	UserID     string `json:"user_id"`
	FriendCode string `json:"friend_code"`
	CreatedAt  int64  `json:"created_at"` // epoch ms
}

type List struct {
	Friends  []Friend  `json:"friends"`
	Incoming []Request `json:"incoming"`
	Outgoing []Request `json:"outgoing"`
}

// Service: regras que não são do banco — normalização do código, pedido para si, ids malformados.
type Service struct{ store *Store }

func NewService(store *Store) *Service { return &Service{store} }

// Request pede amizade ao dono de rawCode. created: pedido novo (201); senão o existente ou o aceite cruzado (200).
func (s *Service) Request(ctx context.Context, me, rawCode string) (id, status string, created bool, err error) {
	code := Normalize(rawCode)
	if code == "" {
		return "", "", false, ErrCodeNotFound
	}
	to, err := s.store.userByCode(ctx, code)
	if err != nil {
		return "", "", false, err
	}
	if to == me {
		return "", "", false, ErrSelf
	}
	return s.store.request(ctx, me, to)
}

func (s *Service) Accept(ctx context.Context, me, id string) error {
	if !isUUID(id) {
		return ErrNotFound
	}
	return s.store.accept(ctx, me, id)
}

func (s *Service) Decline(ctx context.Context, me, id string) error {
	if !isUUID(id) {
		return ErrNotFound
	}
	return s.store.decline(ctx, me, id)
}

// Remove é idempotente: id malformado é só "nada a desfazer".
func (s *Service) Remove(ctx context.Context, me, other string) error {
	if !isUUID(other) {
		return nil
	}
	return s.store.remove(ctx, me, other)
}

func (s *Service) Block(ctx context.Context, me, other string) error {
	if !isUUID(other) {
		return ErrNotFound
	}
	if strings.EqualFold(other, me) { // o path pode vir em maiúsculas
		return errBlockSelf
	}
	return s.store.block(ctx, me, other)
}

func (s *Service) Unblock(ctx context.Context, me, other string) error {
	if !isUUID(other) {
		return nil
	}
	return s.store.unblock(ctx, me, other)
}

func (s *Service) List(ctx context.Context, me string) (List, error) { return s.store.list(ctx, me) }

// isUUID: só a forma canônica (36 chars), a que o servidor devolve; evita erro de sintaxe do Postgres.
func isUUID(s string) bool { return len(s) == 36 && uuid.Validate(s) == nil }

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewCode sorteia um friend_code: 8 símbolos Crockford base32 com crypto/rand. Único só na prática: quem grava
// (cadastro) repete se o índice único de users.friend_code acusar colisão.
func NewCode() string {
	var b [8]byte
	rand.Read(b[:])
	for i := range b {
		b[i] = crockford[b[i]&31] // 256 é múltiplo de 32: sem viés
	}
	return string(b[:])
}

// Normalize faz o mesmo que Api.normalizeCode do app: maiúsculas, I/L→1, O→0, sem espaços e '-'. "" se, depois
// disso, não sobrarem exatamente 8 símbolos Crockford.
func Normalize(raw string) string {
	var b [8]byte
	n := 0
	for _, r := range raw {
		r = unicode.ToUpper(r)
		switch {
		case r == '-' || unicode.IsSpace(r):
			continue
		case r == 'I' || r == 'L':
			r = '1'
		case r == 'O':
			r = '0'
		}
		if n == len(b) || !isCrockford(r) {
			return ""
		}
		b[n] = byte(r)
		n++
	}
	if n != len(b) {
		return ""
	}
	return string(b[:])
}

func isCrockford(r rune) bool {
	return r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' && r != 'I' && r != 'L' && r != 'O' && r != 'U'
}
