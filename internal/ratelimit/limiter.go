
package ratelimit

import (
	"sync"
	"time"
)

type Limiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func NewLimiter(ratePerSecond float64, burst int) *Limiter {
	return &Limiter{
		tokens:     float64(burst),
		maxTokens:  float64(burst),
		refillRate: ratePerSecond,
		lastRefill: time.Now(),
	}
}

func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.lastRefill = now

	l.tokens += elapsed * l.refillRate
	if l.tokens > l.maxTokens {
		l.tokens = l.maxTokens
	}

	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

