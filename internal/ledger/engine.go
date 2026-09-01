
package ledger

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

const numShards = 32

var (
	ErrInvalidAmount     = errors.New("ledger: amount must be positive")
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	ErrSameAccount       = errors.New("ledger: cannot transfer to the same account")
)


type account struct {
	mu      sync.Mutex
	id      string
	balance int64 // integer minor units — never float64 for money
}


type shard struct {
	mu       sync.RWMutex
	accounts map[string]*account
}


type Engine struct {
	shards [numShards]*shard
	events chan<- Event // WAL sink; nil is valid (events dropped) for tests/benchmarks
	seq    int64        // monotonic event sequence number, mutated only via atomic ops
}

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

func (e *Engine) emit(ev Event) {
	if e.events == nil {
		return
	}
	select {
	case e.events <- ev:
	default:
	}
}
