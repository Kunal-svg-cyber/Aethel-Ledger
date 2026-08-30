// Package ledger implements Aethel Ledger's in-memory balance engine.
//
// The central engineering problem this package solves: many goroutines
// (one per inbound gRPC transfer request) mutate a shared set of account
// balances concurrently, and must do so with (a) no data races, (b)
// no deadlocks, and (c) high throughput — a single global mutex would be
// safe but would serialize every transfer in the system, which defeats
// the point of a "high-throughput" ledger.
//
// The design used here is deterministic sharded locking:
//   - Accounts are partitioned across N shards by a hash of their ID, so
//     unrelated accounts almost never contend on the same lock.
//   - Each account additionally has its own mutex, so two transfers that
//     happen to land on the same shard but touch different accounts still
//     don't block each other.
//   - Multi-account operations (transfers) always acquire the two account
//     locks in a deterministic, ID-derived order — never in
//     caller-supplied (from, to) order. This is what makes deadlock
//     structurally impossible: see the comment on Transfer for the proof
//     sketch.
package ledger

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// numShards controls the account-map partition count. Higher reduces
// map-lock contention on account creation/lookup; it does not affect
// balance-mutation contention, which is governed by per-account mutexes.
const numShards = 32

var (
	ErrInvalidAmount     = errors.New("ledger: amount must be positive")
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	ErrSameAccount       = errors.New("ledger: cannot transfer to the same account")
)

// account holds the mutable state for a single ledger account, guarded by
// its own mutex so balance mutations on different accounts never block
// each other.
type account struct {
	mu      sync.Mutex
	id      string
	balance int64 // integer minor units — never float64 for money
}

// shard is one partition of the account map, protected by its own
// RWMutex. Reads (the hot path: looking up an existing account) take the
// read lock and can proceed concurrently; only first-touch account
// creation takes the write lock.
type shard struct {
	mu       sync.RWMutex
	accounts map[string]*account
}

// Engine is the in-memory, thread-safe ledger core. Every balance
// mutation goes through here and is emitted as an Event before the
// call returns success to the caller.
type Engine struct {
	shards [numShards]*shard
	events chan<- Event // WAL sink; nil is valid (events dropped) for tests/benchmarks
	seq    int64        // monotonic event sequence number, mutated only via atomic ops
}

// NewEngine constructs an Engine. Pass the send side of a channel that a
// WAL writer goroutine is draining; pass nil if you don't need events
// (e.g. in unit tests that only care about balance correctness).
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
// The common case (account already exists) only ever takes a read lock,
// so concurrent lookups of existing accounts don't serialize on the
// shard's map. Creation is safe against the classic check-then-act race
// via the double-checked lock below.
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
	if a, ok = s.accounts[id]; ok { // re-check: lost the race to create it
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
// Deadlock-freedom proof sketch: a deadlock between two goroutines
// requires a circular wait — goroutine G1 holds lock A and waits for
// lock B, while G2 holds lock B and waits for lock A. That can only
// happen if G1 and G2 acquire A and B in opposite orders. This function
// never locks in the order the caller supplied (from, to); it always
// locks the two accounts in a fixed order derived from comparing their
// IDs. So for any pair of accounts {X, Y}, every single goroutine in the
// system — regardless of whether it's transferring X→Y or Y→X — acquires
// X's lock before Y's lock (or vice versa, but consistently). A circular
// wait is therefore structurally impossible: this is the standard global
// lock-ordering strategy for deadlock avoidance.
//
// See engine_test.go's TestTransfer_NoDeadlockUnderReversedConcurrentPairs
// for a test that would hang (and get killed by the test timeout) if this
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

// Balance returns the current balance for id (0 if it has never been touched).
func (e *Engine) Balance(id string) int64 {
	a := e.getOrCreate(id)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.balance
}

func (e *Engine) nextSeq() int64 {
	return atomic.AddInt64(&e.seq, 1)
}

// emit is non-blocking: a slow or absent event consumer must never stall
// the balance-mutation hot path. Week 5-6 replaces this with a bounded,
// backpressure-aware WAL writer; for now a full channel simply drops the
// event rather than blocking a financial transaction on I/O.
func (e *Engine) emit(ev Event) {
	if e.events == nil {
		return
	}
	select {
	case e.events <- ev:
	default:
	}
}
