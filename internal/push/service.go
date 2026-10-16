package push

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	// avisos com dados (alertas do PoGo) esperando envio, contados por mensagem (~1 KB cada); cheia, o TryPush recusa
	// e quem chamou tenta depois
	maxData = 10_000
)

var ErrInvalidToken = errors.New("push: token inválido")

type Sender interface {
	Send(ctx context.Context, user, token string) error
	SendData(ctx context.Context, user, token string, msgs []Data) error
}

// Data: aviso com conteúdo, que não é segredo (o do chat não leva nada).
type Data struct {
	Fields map[string]string
	TTL    time.Duration // depois disso o FCM descarta em vez de entregar atrasado
}

type Service struct {
	// DataWorkers: envios com dados em paralelo (padrão 4). Definido antes do Run; acompanha quem produz (POGO_WORKERS).
	DataWorkers int
	store       *Store
	sender      Sender
	mu          sync.Mutex
	pending     map[string]struct{}
	wake        chan struct{}
	data        chan dataJob
	queued      atomic.Int64 // mensagens em data
}

type dataJob struct {
	t    target
	msgs []Data
}

func New(pool *pgxpool.Pool, sender Sender) *Service {
	return &Service{store: &Store{pool: pool}, sender: sender, pending: map[string]struct{}{}, wake: make(chan struct{}, 1),
		data: make(chan dataJob, maxData)}
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
	for range max(s.DataWorkers, 4) {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-s.data:
					s.deliver(ctx, j.t, func(ctx context.Context) error {
						return s.sender.SendData(ctx, j.t.User, j.t.Token, j.msgs)
					})
					s.queued.Add(-int64(len(j.msgs)))
				}
			}
		}()
	}
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
	s.deliver(ctx, t, func(ctx context.Context) error { return s.sender.Send(ctx, t.User, t.Token) })
}

// TryPush enfileira sem bloquear, para o token que quem chama já leu; false = fila cheia, nada foi enfileirado.
func (s *Service) TryPush(user, token string, msgs []Data) bool {
	n := int64(len(msgs))
	if s.queued.Add(n) > maxData {
		s.queued.Add(-n)
		return false
	}
	select {
	case s.data <- dataJob{target{user, token}, msgs}:
		return true
	default:
		s.queued.Add(-n)
		return false
	}
}

func (s *Service) deliver(ctx context.Context, t target, send func(context.Context) error) {
	sctx, cancel := context.WithTimeout(ctx, sendWait)
	err := send(sctx)
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

func (logSender) SendData(_ context.Context, user, _ string, msgs []Data) error {
	slog.Info("fcm data", "user", user, "n", len(msgs))
	return nil
}
