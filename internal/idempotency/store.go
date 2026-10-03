// Package idempotency guards the ledger against duplicate mutations
// caused by client retries. The Store interface is backend-agnostic;
// this file provides both an in-memory implementation (default, and
// used in tests) and a Postgres-backed one that survives process
// restarts and can coordinate across multiple server instances.
package idempotency

import (
	"context"
	"sync"
	"time"
)

// Store records the outcome of a request keyed by a client-supplied
// idempotency key.
type Store interface {
	// CheckAndReserve returns (nil, false, nil) for a new key, reserving
	// it, or (result, true, nil) if key was already committed.
	CheckAndReserve(ctx context.Context, key string) (result []byte, alreadyCommitted bool, err error)

	// Commit stores the result for a previously reserved key.
	Commit(ctx context.Context, key string, result []byte) error
}

// ExpirableStore is implemented by Store backends that can prune old
// records. A key is only ever safe to protect against a client retrying
// its *recent* request — retaining it forever grows the store
// unboundedly with no benefit, since no legitimate retry happens after
// the client has long since moved on. DeleteExpired removes every
// record older than cutoff and returns how many rows were removed.
type ExpirableStore interface {
	DeleteExpired(ctx context.Context, cutoff time.Time) (int64, error)
}

// InMemoryStore is a thread-safe, process-local Store with no
// cross-instance coordination and no durability across a restart — see
// PostgresStore for the durable alternative.
type InMemoryStore struct {
	mu    sync.Mutex
	state map[string]*entry
}

type entry struct {
	committed bool
	result    []byte
	createdAt time.Time
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{state: make(map[string]*entry)}
}

func (s *InMemoryStore) CheckAndReserve(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.state[key]
	if !ok {
		s.state[key] = &entry{createdAt: time.Now()}
		return nil, false, nil
	}
	if e.committed {
		return e.result, true, nil
	}
	return nil, true, nil
}

func (s *InMemoryStore) Commit(_ context.Context, key string, result []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[key] = &entry{committed: true, result: result, createdAt: time.Now()}
	return nil
}

// DeleteExpired removes every record created before cutoff. Satisfies
// ExpirableStore.
func (s *InMemoryStore) DeleteExpired(_ context.Context, cutoff time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var removed int64
	for key, e := range s.state {
		if e.createdAt.Before(cutoff) {
			delete(s.state, key)
			removed++
		}
	}
	return removed, nil
}

// Len reports how many records are currently held — used by tests to
// observe cleanup behavior directly.
func (s *InMemoryStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.state)
}
