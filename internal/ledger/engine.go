
package ledger

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
)

const numShards = 32

var (
	ErrInvalidAmount     = errors.New("ledger: amount must be positive")
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	ErrSameAccount       = errors.New("ledger: cannot transfer to the same account")
	ErrBalanceOverflow   = errors.New("ledger: operation would overflow the account balance")
)

type account struct {
	mu      sync.Mutex
	id      string
	balance int64
}

type shard struct {
	mu       sync.RWMutex
	accounts map[string]*account
}

type Engine struct {
	shards [numShards]*shard
	events chan<- Event
	seq    int64
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
	if a, ok = s.accounts[id]; ok {
		return a
	}
	a = &account{id: id}
	s.accounts[id] = a
	return a
}

func (e *Engine) Deposit(_ context.Context, id string, amount int64) (int64, error) {
	if amount <= 0 {
		return 0, ErrInvalidAmount
	}
	a := e.getOrCreate(id)

	a.mu.Lock()
	if wouldOverflowAdd(a.balance, amount) {
		a.mu.Unlock()
		return 0, ErrBalanceOverflow
	}
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
	if wouldOverflowAdd(at.balance, amount) {
		return ErrBalanceOverflow
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

func (e *Engine) Balance(id string) int64 {
	a := e.getOrCreate(id)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.balance
}

func (e *Engine) Restore(events []Event) {
	var maxSeq int64
	for _, ev := range events {
		switch ev.Type {
		case EventDeposit:
			a := e.getOrCreate(ev.Account)
			a.mu.Lock()
			a.balance += ev.Amount
			a.mu.Unlock()

		case EventTransfer:
			from := e.getOrCreate(ev.Account)
			to := e.getOrCreate(ev.CounterAccount)

			first, second := from, to
			if second.id < first.id {
				first, second = second, first
			}
			first.mu.Lock()
			second.mu.Lock()
			from.balance -= ev.Amount
			to.balance += ev.Amount
			second.mu.Unlock()
			first.mu.Unlock()
		}
		if ev.Seq > maxSeq {
			maxSeq = ev.Seq
		}
	}
	atomic.StoreInt64(&e.seq, maxSeq)
}

func (e *Engine) CurrentSeq() int64 {
	return atomic.LoadInt64(&e.seq)
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

func wouldOverflowAdd(current, amount int64) bool {
	if amount > 0 && current > math.MaxInt64-amount {
		return true
	}
	if amount < 0 && current < math.MinInt64-amount {
		return true
	}
	return false
}

