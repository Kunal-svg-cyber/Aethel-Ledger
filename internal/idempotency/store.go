// Package idempotency guards the ledger against duplicate mutations
// caused by client retries. The Store interface is backend-agnostic;
// this file provides an in-memory implementation used by default, with
// the production path swapping in a Redis-backed implementation behind
// the same interface.
package idempotency

import (
	"sync"
)

// Store records the outcome of a request keyed by a client-supplied
// idempotency key.
type Store interface {
	// CheckAndReserve returns (nil, false, nil) for a new key, reserving
	// it, or (result, true, nil) if key was already committed.
	CheckAndReserve(key string) (result []byte, alreadyCommitted bool, err error)

	// Commit stores the result for a previously reserved key.
	Commit(key string, result []byte) error
}

// InMemoryStore is a thread-safe, process-local Store with no
// cross-instance coordination.
type InMemoryStore struct {
	mu    sync.Mutex
	state map[string]*entry
}

type entry struct {
	committed bool
	result    []byte
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{state: make(map[string]*entry)}
}

func (s *InMemoryStore) CheckAndReserve(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.state[key]
	if !ok {
		s.state[key] = &entry{}
		return nil, false, nil
	}
	if e.committed {
		return e.result, true, nil
	}
	return nil, true, nil
}

func (s *InMemoryStore) Commit(key string, result []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[key] = &entry{committed: true, result: result}
	return nil
}
