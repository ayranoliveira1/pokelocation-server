// Package groups: grupos e membresia (docs/api.md §7).
package groups

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"
)

const (
	maxMembers = 50 // contando o dono
	maxGroups  = 25
	listLimit  = 2 * maxGroups // LIMIT defensivo acima do teto
	maxJSON    = 192 << 10     // GET /v1/groups: o app lê até 192 KiB; os tetos dão ~108 KiB no pior caso
)

var (
	ErrNotFound       = errors.New("not_found")
	ErrForbidden      = errors.New("forbidden")
	ErrTooManyMembers = errors.New("too_many_members")
	ErrTooManyGroups  = errors.New("too_many_groups")
	errBadRequest     = errors.New("bad_request")
)

type Group struct {
	ID        string   `json:"id"`
	OwnerID   string   `json:"owner_id"`
	Members   []Member `json:"members"`
	CreatedAt int64    `json:"created_at"` // epoch ms
}

type Member struct {
	UserID   string  `json:"user_id"`
	Username *string `json:"username"` // null em conta antiga, ainda sem @nome
}

type Service struct {
	store *Store
	// OnChange, se não for nil, roda depois do commit de toda mudança de membresia, com os membros atuais e quem
	// saiu. Definido antes de servir; nunca trocado depois.
	OnChange func(groupID string, users []string)
}

func NewService(store *Store) *Service { return &Service{store: store} }

func (s *Service) changed(group string, users []string) {
	if s.OnChange != nil && users != nil {
		s.OnChange(group, users)
	}
}

func (s *Service) Create(ctx context.Context, owner string, members []string) (string, error) {
	var ms []string
	for _, m := range members {
		if !isUUID(m) {
			return "", errBadRequest
		}
		m = strings.ToLower(m) // a forma do banco: o relay indexa por ela
		if m != owner && !slices.Contains(ms, m) {
			ms = append(ms, m)
		}
	}
	if len(ms) > maxMembers-1 {
		return "", ErrTooManyMembers
	}
	id, users, err := s.store.create(ctx, owner, ms)
	if err == nil {
		s.changed(id, users)
	}
	return id, err
}

func (s *Service) List(ctx context.Context, me string) ([]Group, error) { return s.store.list(ctx, me) }

func (s *Service) Get(ctx context.Context, me, group string) (Group, error) {
	if !isUUID(group) {
		return Group{}, ErrNotFound
	}
	return s.store.get(ctx, me, group)
}

func (s *Service) Add(ctx context.Context, me, group, user string) error {
	if !isUUID(group) {
		return ErrNotFound
	}
	if !isUUID(user) {
		return errBadRequest
	}
	users, err := s.store.add(ctx, me, group, strings.ToLower(user))
	if err == nil {
		s.changed(group, users)
	}
	return err
}

func (s *Service) Remove(ctx context.Context, me, group, target string) error {
	if !isUUID(group) {
		return ErrNotFound
	}
	if !isUUID(target) {
		return errBadRequest
	}
	users, err := s.store.remove(ctx, me, group, strings.ToLower(target))
	if err == nil {
		s.changed(group, users)
	}
	return err
}

// Só a forma canônica (36 chars): evita erro de sintaxe no Postgres.
func isUUID(s string) bool { return len(s) == 36 && uuid.Validate(s) == nil }
