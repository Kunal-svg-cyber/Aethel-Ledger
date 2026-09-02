package wal

import (
	"context"
	"sync"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// InMemoryStore is a process-local Store with no durability guarantees —
// data is lost on restart. Used when no DATABASE_URL is configured, and
// in tests that only care about batching/flush behavior, not real
// persistence.
type InMemoryStore struct {
	mu     sync.Mutex
	events []ledger.Event
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{}
}

func (s *InMemoryStore) FlushBatch(_ context.Context, batch []ledger.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, batch...)
	return nil
}

// All returns a snapshot of every event flushed so far, in flush order.
func (s *InMemoryStore) All() []ledger.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ledger.Event, len(s.events))
	copy(out, s.events)
	return out
}
