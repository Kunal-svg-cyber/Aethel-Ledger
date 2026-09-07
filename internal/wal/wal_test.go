package wal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// countingPublisher records every event it's asked to publish.
type countingPublisher struct {
	mu     sync.Mutex
	events []ledger.Event
}

func (p *countingPublisher) Publish(_ context.Context, ev ledger.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	return nil
}

func (p *countingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func TestWAL_FlushNowForcesImmediateSynchronousFlush(t *testing.T) {
	store := NewInMemoryStore()
	// Batch size and interval both large enough that neither would ever
	// trigger naturally within this test — only FlushNow should move
	// this event into the store.
	w := New(store, nil, Config{BatchSize: 1000, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.Events() <- ledger.Event{Seq: 1, Type: ledger.EventDeposit, Account: "alice", Amount: 10}

	// Give the event a moment to actually reach the Run goroutine's
	// select loop before we call FlushNow, so we're testing that
	// FlushNow flushes what's buffered, not racing the send itself.
	time.Sleep(20 * time.Millisecond)

	if err := w.FlushNow(context.Background()); err != nil {
		t.Fatalf("FlushNow returned error: %v", err)
	}

	if got := len(store.All()); got != 1 {
		t.Fatalf("events in store immediately after FlushNow = %d, want 1", got)
	}
}

// countingSlowStore counts how many times FlushBatch was called and
// adds an artificial delay, simulating a real network round trip —
// this is what makes the group-commit coalescing test meaningful: if
// FlushNow triggered one round trip per caller, N concurrent callers
// would take roughly N*delay; if they're correctly coalesced into one
// flush, they complete in roughly one delay's worth of time.
type countingSlowStore struct {
	mu     sync.Mutex
	events []ledger.Event
	calls  int
	delay  time.Duration
}

func (s *countingSlowStore) FlushBatch(_ context.Context, batch []ledger.Event) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.events = append(s.events, batch...)
	return nil
}

func (s *countingSlowStore) LoadAll(_ context.Context) ([]ledger.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ledger.Event, len(s.events))
	copy(out, s.events)
	return out, nil
}

func (s *countingSlowStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *countingSlowStore) eventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// TestWAL_FlushNowCoalescesConcurrentCallsIntoOneFlush is the direct
// regression test for the bug this fix addresses: many concurrent
// FlushNow callers must share a small number of underlying flushes, not
// one each — otherwise concurrent load against a real network-latency
// store serializes into a queueing pileup (this exact bug produced
// 18-24 second p50 latency under 50-way concurrency against Supabase).
func TestWAL_FlushNowCoalescesConcurrentCallsIntoOneFlush(t *testing.T) {
	store := &countingSlowStore{delay: 100 * time.Millisecond}
	w := New(store, nil, Config{BatchSize: 1000, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	const numCallers = 20
	var wg sync.WaitGroup
	wg.Add(numCallers)
	start := time.Now()
	for i := 0; i < numCallers; i++ {
		go func(seq int64) {
			defer wg.Done()
			w.Events() <- ledger.Event{Seq: seq, Type: ledger.EventDeposit, Account: "alice", Amount: 1}
			if err := w.FlushNow(context.Background()); err != nil {
				t.Errorf("FlushNow error: %v", err)
			}
		}(int64(i))
	}
	wg.Wait()
	elapsed := time.Since(start)

	if got := store.eventCount(); got != numCallers {
		t.Fatalf("events persisted = %d, want %d", got, numCallers)
	}

	// The real assertion: if each caller triggered its own serialized
	// round trip, 20 callers at 100ms each would take at least ~2s. With
	// working group commit, this should complete in a small handful of
	// flushes' worth of time.
	if calls := store.callCount(); calls > 5 {
		t.Fatalf("FlushBatch was called %d times for %d concurrent callers — expected coalescing into a small number of flushes", calls, numCallers)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("20 concurrent FlushNow callers took %s — expected well under 1s with working group commit (each simulated round trip is 100ms)", elapsed)
	}
}

// TestWAL_FlushNowIncludesCallersOwnEvent proves the specific race this
// fix closes: a flush triggered by FlushNow must include the event the
// same caller sent moments earlier, not just whatever was already
// batched before that send.
func TestWAL_FlushNowIncludesCallersOwnEvent(t *testing.T) {
	store := NewInMemoryStore()
	w := New(store, nil, Config{BatchSize: 1000, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.Events() <- ledger.Event{Seq: 42, Type: ledger.EventDeposit, Account: "alice", Amount: 500}
	if err := w.FlushNow(context.Background()); err != nil {
		t.Fatalf("FlushNow error: %v", err)
	}

	events := store.All()
	if len(events) != 1 || events[0].Seq != 42 {
		t.Fatalf("expected the just-sent event (seq=42) to be in the store immediately after FlushNow, got: %+v", events)
	}
}

func TestWAL_FlushNowRespectsContextCancellation(t *testing.T) {
	store := NewInMemoryStore()
	w := New(store, nil, DefaultConfig())
	// Deliberately never call w.Run — so nothing will ever drain
	// flushReq, and FlushNow must return via ctx cancellation instead of
	// blocking forever.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := w.FlushNow(ctx)
	if err == nil {
		t.Fatal("expected FlushNow to return an error when Run is never started")
	}
}

func TestWAL_FlushesOnBatchSize(t *testing.T) {
	store := NewInMemoryStore()
	w := New(store, nil, Config{BatchSize: 5, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	for i := 0; i < 5; i++ {
		w.Events() <- ledger.Event{Seq: int64(i), Type: ledger.EventDeposit, Account: "alice", Amount: 10}
	}

	deadline := time.After(2 * time.Second)
	for {
		if len(store.All()) == 5 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for size-triggered flush; got %d events", len(store.All()))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWAL_FlushesOnInterval(t *testing.T) {
	store := NewInMemoryStore()
	w := New(store, nil, Config{BatchSize: 1000, FlushInterval: 50 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.Events() <- ledger.Event{Seq: 1, Type: ledger.EventDeposit, Account: "alice", Amount: 10}
	w.Events() <- ledger.Event{Seq: 2, Type: ledger.EventDeposit, Account: "bob", Amount: 20}

	deadline := time.After(2 * time.Second)
	for {
		if len(store.All()) == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for interval-triggered flush; got %d events", len(store.All()))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWAL_PublishesEveryEventIndependentlyOfBatching(t *testing.T) {
	store := NewInMemoryStore()
	pub := &countingPublisher{}
	w := New(store, pub, Config{BatchSize: 1000, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	const n = 10
	for i := 0; i < n; i++ {
		w.Events() <- ledger.Event{Seq: int64(i), Type: ledger.EventDeposit, Account: "alice", Amount: 1}
	}

	deadline := time.After(2 * time.Second)
	for {
		if pub.count() == n {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for publishes; got %d, want %d", pub.count(), n)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if len(store.All()) != 0 {
		t.Fatalf("store should still be empty (no flush triggered yet), got %d", len(store.All()))
	}
}

func TestWAL_FlushesRemainingBatchOnShutdown(t *testing.T) {
	store := NewInMemoryStore()
	w := New(store, nil, Config{BatchSize: 1000, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	w.Events() <- ledger.Event{Seq: 1, Type: ledger.EventDeposit, Account: "alice", Amount: 10}
	w.Events() <- ledger.Event{Seq: 2, Type: ledger.EventDeposit, Account: "bob", Amount: 20}

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-w.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("WAL did not signal Done() after context cancellation")
	}

	if got := len(store.All()); got != 2 {
		t.Fatalf("events flushed on shutdown = %d, want 2", got)
	}
}
