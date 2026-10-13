// Package ratelimit: N por minuto por chave, em memória (golang.org/x/time/rate).
// ponytail: vale por instância; passa para o Redis quando houver mais de uma instância.
package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Limiter struct {
	mu     sync.Mutex
	perMin int
	m      map[string]*bucket
	swept  time.Time
}

type bucket struct {
	l    *rate.Limiter
	seen time.Time
}

// New: balde de perMin, recarga de 1 a cada 60/perMin s.
func New(perMin int) *Limiter { return &Limiter{perMin: perMin, m: map[string]*bucket{}} }

func (l *Limiter) Allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.swept) > time.Minute { // parado há 1 min = balde cheio = igual a um novo: pode sair do mapa
		for k, b := range l.m {
			if now.Sub(b.seen) > time.Minute {
				delete(l.m, k)
			}
		}
		l.swept = now
	}
	b := l.m[key]
	if b == nil {
		b = &bucket{l: rate.NewLimiter(rate.Every(time.Minute/time.Duration(l.perMin)), l.perMin)}
		l.m[key] = b
	}
	b.seen = now
	return b.l.AllowN(now, 1)
}
