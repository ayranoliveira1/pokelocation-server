// Servidor do PokeLocation: API REST e WebSocket num binário só, configurado por variáveis de ambiente (README).
// HTTP puro na rede interna: o TLS fica no Caddy.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
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
	pprofAddr                   string
	cluster                     bool
	auth                        auth.Config
}

func loadConfig() (config, error) {
	c := config{
		addr:        os.Getenv("ADDR"),
		databaseURL: os.Getenv("DATABASE_URL"),
		redisURL:    os.Getenv("REDIS_URL"),
		pprofAddr:   os.Getenv("PPROF_ADDR"),
		cluster:     os.Getenv("CLUSTER") == "1",
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
	if cfg.cluster {
		hub.EnableCluster(ctx, rdb)
	}

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
	if cfg.pprofAddr != "" {
		go servePprof(cfg.pprofAddr)
	}
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

// healthz ignora o Redis: sem ele o chat continua, só a localização para.
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

// daily roda f na subida e depois a cada 24 h, até ctx acabar.
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

// servePprof: mux próprio, nunca o da API. PPROF_ADDR tem de ser interno (ex.: 127.0.0.1:6060).
func servePprof(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	slog.Info("pprof", "addr", addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Warn("pprof parado", "err_type", fmt.Sprintf("%T", err))
	}
}
