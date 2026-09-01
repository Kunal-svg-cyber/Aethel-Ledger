package wal

import (
	"context"
	"sync"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

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

func (s *InMemoryStore) All() []ledger.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ledger.Event, len(s.events))
	copy(out, s.events)
	return out
}
