package wal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

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
