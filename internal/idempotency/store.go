
package idempotency

import (
	"context"
	"sync"
)

type Store interface {

	CheckAndReserve(ctx context.Context, key string) (result []byte, alreadyCommitted bool, err error)

	Commit(ctx context.Context, key string, result []byte) error
}

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

func (s *InMemoryStore) CheckAndReserve(_ context.Context, key string) ([]byte, bool, error) {
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

func (s *InMemoryStore) Commit(_ context.Context, key string, result []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[key] = &entry{committed: true, result: result}
	return nil
}

