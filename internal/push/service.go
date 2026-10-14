package push

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sendWait = 10 * time.Second
	dbWait   = 5 * time.Second
	// ponytail: fila em memória com teto; acima dele o aviso se perde (a mensagem não: segue na fila do Postgres e
	// chega quando o app abrir). Vira fila durável se avisos perdidos incomodarem.
	maxPending = 100_000
	batch      = 500 // tokens por consulta
	workers    = 16  // envios ao FCM em paralelo: cada um é uma ida HTTP de dezenas de ms
)

var ErrInvalidToken = errors.New("push: token inválido")

type Sender interface {
	Send(ctx context.Context, user, token string) error
}

type Service struct {
	store   *Store
	sender  Sender
	mu      sync.Mutex
	pending map[string]struct{}
	wake    chan struct{}
}

func New(pool *pgxpool.Pool, sender Sender) *Service {
	return &Service{store: &Store{pool: pool}, sender: sender, pending: map[string]struct{}{}, wake: make(chan struct{}, 1)}
}

func (s *Service) Notify(user string) {
	s.mu.Lock()
	if len(s.pending) < maxPending {
		s.pending[user] = struct{}{}
	}
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Service) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}
		s.mu.Lock()
		users := make([]string, 0, len(s.pending))
		for u := range s.pending {
			users = append(users, u)
		}
		clear(s.pending)
		s.mu.Unlock()
		for len(users) > 0 {
			n := min(batch, len(users))
			s.pushBatch(ctx, users[:n])
			users = users[n:]
		}
	}
}

func (s *Service) pushBatch(ctx context.Context, users []string) {
	dctx, cancel := context.WithTimeout(ctx, dbWait)
	targets, err := s.store.tokens(dctx, users)
	cancel()
	if err != nil {
		slog.Warn("push: tokens", "err_type", fmt.Sprintf("%T", err))
		return
	}
	jobs := make(chan target)
	var wg sync.WaitGroup
	for range min(workers, len(targets)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobs {
				s.send(ctx, t)
			}
		}()
	}
	for _, t := range targets {
		jobs <- t
	}
	close(jobs)
	wg.Wait()
}

func (s *Service) send(ctx context.Context, t target) {
	sctx, cancel := context.WithTimeout(ctx, sendWait)
	err := s.sender.Send(sctx, t.User, t.Token)
	cancel()
	switch {
	case errors.Is(err, ErrInvalidToken):
		dctx, cancel := context.WithTimeout(ctx, dbWait)
		_ = s.store.clearToken(dctx, t.User, t.Token)
		cancel()
	case err != nil:
		slog.Warn("push: fcm", "err_type", fmt.Sprintf("%T", err)) // a mensagem da lib pode trazer o token
	}
}

// logSender loga só o usuário, nunca o token.
type logSender struct{}

func (logSender) Send(_ context.Context, user, _ string) error {
	slog.Info("fcm ping", "user", user)
	return nil
}
