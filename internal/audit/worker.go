// Package audit implements real-time mathematical auditing: a consumer
// that independently verifies the ledger's core invariant by replaying
// the event log rather than reading the engine's live balances.
package audit

import (
	"sync"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// Worker derives account balances from a replayed event stream and
// exposes the invariant: sum(all derived balances) must equal total
// deposits, since Transfer can only move value between accounts.
type Worker struct {
	mu              sync.Mutex
	balances        map[string]int64
	totalDeposited  int64
	eventsProcessed int64
}

func NewWorker() *Worker {
	return &Worker{balances: make(map[string]int64)}
}

// Apply replays one event into the worker's derived state.
func (w *Worker) Apply(ev ledger.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch ev.Type {
	case ledger.EventDeposit:
		w.balances[ev.Account] += ev.Amount
		w.totalDeposited += ev.Amount
	case ledger.EventTransfer:
		w.balances[ev.Account] -= ev.Amount
		w.balances[ev.CounterAccount] += ev.Amount
	}
	w.eventsProcessed++
}

// CheckInvariant returns the current drift (should always be 0) and the
// number of events processed so far.
func (w *Worker) CheckInvariant() (drift int64, eventsProcessed int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var sum int64
	for _, b := range w.balances {
		sum += b
	}
	return sum - w.totalDeposited, w.eventsProcessed
}

// Balance returns the worker's derived balance for id.
func (w *Worker) Balance(id string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.balances[id]
}
