// Servidor do PokeLocation: API REST + gateway WebSocket num binário só. Configuração só por variáveis de ambiente
// (README). Escuta em HTTP puro na rede interna; o TLS fica no Caddy.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/auth"
	"github.com/ayranoliveira1/pokelocation-server/internal/db"
	"github.com/ayranoliveira1/pokelocation-server/internal/friends"
	"github.com/ayranoliveira1/pokelocation-server/internal/keys"
	"github.com/ayranoliveira1/pokelocation-server/internal/location"
	"github.com/ayranoliveira1/pokelocation-server/internal/relay"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

type config struct {
	addr, databaseURL, redisURL string
	auth                        auth.Config
}

func loadConfig() (config, error) {
	c := config{
		addr:        os.Getenv("ADDR"),
		databaseURL: os.Getenv("DATABASE_URL"),
		redisURL:    os.Getenv("REDIS_URL"),
		auth: auth.Config{
			SigningKey:         os.Getenv("AUTH_SIGNING_KEY"),
			GoogleClientID:     os.Getenv("GOOGLE_CLIENT_ID"),
			GoogleClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"),
			PublicURL:          os.Getenv("PUBLIC_URL"),
		},
	}
	if c.addr == "" {
		c.addr = ":8080"
	}
	for name, v := range map[string]string{"DATABASE_URL": c.databaseURL, "REDIS_URL": c.redisURL, "AUTH_SIGNING_KEY": c.auth.SigningKey} {
		if v == "" {
			return c, errors.New(name + " obrigatória")
		}
	}
	return c, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	ropt, err := redis.ParseURL(cfg.redisURL)
	if err != nil {
		return errors.New("REDIS_URL inválida") // sem o err: a URL pode ter senha
	}
	rdb := redis.NewClient(ropt)
	defer rdb.Close()

	authH, err := auth.New(cfg.auth, pool, rdb)
	if err != nil {
		return err
	}
	var loc *location.Service // relay → location só por callback (location importa relay)
	hub := relay.New(pool, authH.Verify, func(c *relay.Conn, f wire.Frame) { loc.OnFrame(c, f) })
	loc = location.New(rdb, pool, hub)

	// amizade mudou → listas em memória dos dois lados; revogação → derruba os WS na hora
	reload := func(a, b string) { hub.ReloadFriends(a); hub.ReloadFriends(b) }
	authH.OnFriendsChange = reload
	authH.OnRevoke = hub.Kick
	fh := friends.New(pool)
	fh.OnChange = reload

	go daily(ctx, "auth: limpeza dos refresh", authH.Cleanup)
	go daily(ctx, "relay: envelopes > 30 dias", hub.Cleanup)
	go loc.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz(pool))
	authH.Routes(mux, authH.Authed)
	fh.Routes(mux, authH.Authed)
	keys.New(pool).Routes(mux, authH.Authed)
	hub.Routes(mux, authH.Authed)
	loc.Routes(mux, authH.Authed)

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("ouvindo", "addr", cfg.addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}

// healthz: 200 se o Postgres responde. Redis fora não derruba (o chat segue sem localização, spec §7).
func healthz(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "db", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}
}

// daily roda f na subida e depois a cada 24 h, até ctx acabar. Erro só vai para o log.
func daily(ctx context.Context, name string, f func(context.Context) error) {
	t := time.NewTicker(24 * time.Hour)
	defer t.Stop()
	for {
		if err := f(ctx); err != nil && ctx.Err() == nil {
			slog.Warn(name, "err_type", fmt.Sprintf("%T", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
