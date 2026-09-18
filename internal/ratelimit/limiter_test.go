package ratelimit

import (
	"sync"
	"testing"
	"time"
)

func TestLimiter_AllowsBurstUpToCapacity(t *testing.T) {
	l := NewLimiter(1, 5)

	for i := 0; i < 5; i++ {
		if !l.Allow() {
			t.Fatalf("call %d: expected burst capacity to allow this request", i)
		}
	}
	if l.Allow() {
		t.Fatal("6th immediate call should be rejected once burst capacity is exhausted")
	}
}

func TestLimiter_RefillsOverTime(t *testing.T) {
	l := NewLimiter(100, 1)

	if !l.Allow() {
		t.Fatal("first call should be allowed (burst capacity 1)")
	}
	if l.Allow() {
		t.Fatal("immediate second call should be rejected — no time has passed to refill")
	}

	time.Sleep(20 * time.Millisecond)
	if !l.Allow() {
		t.Fatal("after waiting, a refilled token should allow the next call")
	}
}

func TestLimiter_NeverExceedsMaxTokens(t *testing.T) {
	l := NewLimiter(1000, 3)
	time.Sleep(50 * time.Millisecond)

	allowed := 0
	for i := 0; i < 10; i++ {
		if l.Allow() {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d calls after a long idle period, want exactly burst=3 (tokens must be capped, not accumulate unbounded)", allowed)
	}
}

func TestLimiter_ConcurrentAccessNeverAllowsMoreThanBurst(t *testing.T) {
	const burst = 10
	l := NewLimiter(0, burst)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowedCount := 0

	const numCallers = 100
	wg.Add(numCallers)
	for i := 0; i < numCallers; i++ {
		go func() {
			defer wg.Done()
			if l.Allow() {
				mu.Lock()
				allowedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowedCount != burst {
		t.Fatalf("allowed %d of %d concurrent callers, want exactly burst=%d", allowedCount, numCallers, burst)
	}
}

