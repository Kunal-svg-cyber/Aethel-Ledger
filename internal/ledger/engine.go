// Package ledger implements Aethel Ledger's in-memory, thread-safe
// balance engine using deterministic sharded locking: accounts are
// partitioned across shards, each account has its own mutex, and
// multi-account operations always lock in ID order to make deadlock
// structurally impossible (see Transfer).
package ledger

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// numShards controls account-map partition count; higher reduces
// map-lock contention on creation/lookup.
const numShards = 32

var (
	ErrInvalidAmount     = errors.New("ledger: amount must be positive")
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	ErrSameAccount       = errors.New("ledger: cannot transfer to the same account")
)

// account holds mutable state for a single ledger account, guarded by
// its own mutex.
type account struct {
	mu      sync.Mutex
	id      string
	balance int64 // integer minor units — never float64 for money
}

// shard is one partition of the account map, protected by its own RWMutex.
type shard struct {
	mu       sync.RWMutex
	accounts map[string]*account
}

// Engine is the in-memory, thread-safe ledger core.
type Engine struct {
	shards [numShards]*shard
	events chan<- Event // WAL sink; nil is valid for tests/benchmarks
	seq    int64
}

// NewEngine constructs an Engine. Pass nil for events if none are needed.
func NewEngine(events chan<- Event) *Engine {
	e := &Engine{events: events}
	for i := range e.shards {
		e.shards[i] = &shard{accounts: make(map[string]*account)}
	}
	return e
}

func (e *Engine) shardFor(id string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return e.shards[h.Sum32()%numShards]
}

// getOrCreate returns the account for id, creating it on first touch.
func (e *Engine) getOrCreate(id string) *account {
	s := e.shardFor(id)

	s.mu.RLock()
	a, ok := s.accounts[id]
	s.mu.RUnlock()
	if ok {
		return a
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok = s.accounts[id]; ok {
		return a
	}
	a = &account{id: id}
	s.accounts[id] = a
	return a
}

// Deposit credits amount into id and returns the resulting balance.
func (e *Engine) Deposit(_ context.Context, id string, amount int64) (int64, error) {
	if amount <= 0 {
		return 0, ErrInvalidAmount
	}
	a := e.getOrCreate(id)

	a.mu.Lock()
	a.balance += amount
	bal := a.balance
	a.mu.Unlock()

	e.emit(Event{Type: EventDeposit, Account: id, Amount: amount, Seq: e.nextSeq()})
	return bal, nil
}

// Transfer atomically moves amount from `from` to `to`.
//
// Deadlock-freedom proof: a deadlock requires a circular wait — G1 holds
// lock A and waits for B while G2 holds B and waits for A — which can
// only happen if the two goroutines acquire A and B in opposite orders.
// This function never locks in caller-supplied order; it always locks
// the two accounts in a fixed order derived from comparing their IDs.
// Every goroutine in the system therefore acquires the same pair of
// locks in the same order regardless of transfer direction, making a
// circular wait structurally impossible.
//
// See TestTransfer_NoDeadlockUnderReversedConcurrentPairs in
// engine_test.go, which would hang under a hard timeout if this
// property were violated.
func (e *Engine) Transfer(_ context.Context, from, to string, amount int64) error {
	if amount <= 0 {
		return ErrInvalidAmount
	}
	if from == to {
		return ErrSameAccount
	}

	af := e.getOrCreate(from)
	at := e.getOrCreate(to)

	first, second := af, at
	if second.id < first.id {
		first, second = second, first
	}

	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()

	if af.balance < amount {
		return ErrInsufficientFunds
	}
	af.balance -= amount
	at.balance += amount

	e.emit(Event{
		Type: EventTransfer, Account: from, CounterAccount: to,
		Amount: amount, Seq: e.nextSeq(),
	})
	return nil
}

// Account reports whether id has ever been touched, without creating it.
func (e *Engine) Account(id string) (balance int64, exists bool) {
	s := e.shardFor(id)
	s.mu.RLock()
	a, ok := s.accounts[id]
	s.mu.RUnlock()
	if !ok {
		return 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.balance, true
}

// Balance returns the current balance for id (0 if untouched).
func (e *Engine) Balance(id string) int64 {
	a := e.getOrCreate(id)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.balance
}

func (e *Engine) nextSeq() int64 {
	return atomic.AddInt64(&e.seq, 1)
}

// emit is non-blocking so a slow or absent consumer never stalls the
// balance-mutation hot path; a full channel drops the event.
func (e *Engine) emit(ev Event) {
	if e.events == nil {
		return
	}
	select {
	case e.events <- ev:
	default:
	}
}
