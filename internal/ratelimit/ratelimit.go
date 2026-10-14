// Package ratelimit: N por janela por chave, em memória (golang.org/x/time/rate).
// ponytail: vale por instância; passa para o Redis quando houver mais de uma instância.
package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Limiter struct {
	mu     sync.Mutex
	n      int
	window time.Duration
	m      map[string]*bucket
	swept  time.Time
}

type bucket struct {
	l    *rate.Limiter
	seen time.Time
}

// New: balde de perMin, recarga de 1 a cada 60/perMin s.
func New(perMin int) *Limiter { return NewWindow(perMin, time.Minute) }

// NewWindow: balde de n, recarga de 1 a cada window/n.
func NewWindow(n int, window time.Duration) *Limiter {
	return &Limiter{n: n, window: window, m: map[string]*bucket{}}
}

func (l *Limiter) Allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.swept) > l.window { // parado uma janela inteira = balde cheio = igual a um novo: pode sair do mapa
		for k, b := range l.m {
			if now.Sub(b.seen) > l.window {
				delete(l.m, k)
			}
		}
		l.swept = now
	}
	b := l.m[key]
	if b == nil {
		b = &bucket{l: rate.NewLimiter(rate.Every(l.window/time.Duration(l.n)), l.n)}
		l.m[key] = b
	}
	b.seen = now
	return b.l.AllowN(now, 1)
}
