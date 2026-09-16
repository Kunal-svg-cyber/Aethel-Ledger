// Package ratelimit implements a token-bucket rate limiter using only
// the standard library — no external dependency, unlike most Go rate
// limiting libraries (e.g. golang.org/x/time/rate).
package ratelimit

import (
	"sync"
	"time"
)

// Limiter is a thread-safe token bucket: tokens refill continuously at
// ratePerSecond up to a maximum of burst, and each allowed call
// consumes one token. A burst of traffic can consume up to burst
// requests instantly; sustained traffic is capped at ratePerSecond.
type Limiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

// NewLimiter constructs a Limiter allowing ratePerSecond sustained
// requests with bursts up to burst.
func NewLimiter(ratePerSecond float64, burst int) *Limiter {
	return &Limiter{
		tokens:     float64(burst),
		maxTokens:  float64(burst),
		refillRate: ratePerSecond,
		lastRefill: time.Now(),
	}
}

// Allow reports whether a request may proceed right now, consuming one
// token if so. Safe for concurrent use.
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
