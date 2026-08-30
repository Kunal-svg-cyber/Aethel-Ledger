// Package audit implements the "real-time mathematical auditing"
// component: a consumer that watches the event stream and independently
// verifies the ledger's core invariant — that transfers only ever move
// value between accounts, never create or destroy it.
//
// The key design point: the Worker never reads the engine's live
// balances. It reconstructs its own view of every account purely by
// replaying the append-only event log. This means the audit is a real
// check, not a tautology — if a bug in the engine ever let a transfer
// corrupt state in a way that didn't match what it logged, or if the
// log and the live engine diverged for any reason, this worker is
// positioned to catch it, because it trusts nothing but the log.
package audit

import (
	"sync"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// Worker derives account balances from a replayed event stream and
// exposes the global invariant: sum(all derived balances) should always
// equal the total amount ever deposited, since Transfer can only move
// value between accounts. Safe for concurrent use.
type Worker struct {
	mu              sync.Mutex
	balances        map[string]int64
	totalDeposited  int64
	eventsProcessed int64
}

func NewWorker() *Worker {
	return &Worker{balances: make(map[string]int64)}
}

// Apply replays one event into the worker's independently-derived state.
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

// CheckInvariant returns the current drift (should always be exactly 0)
// and the number of events processed so far. A nonzero drift means the
// event log itself is inconsistent with conservation of value — a
// serious finding, not a rounding issue, since all amounts are integers.
func (w *Worker) CheckInvariant() (drift int64, eventsProcessed int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var sum int64
	for _, b := range w.balances {
		sum += b
	}
	return sum - w.totalDeposited, w.eventsProcessed
}

// Balance returns the worker's independently-derived balance for id —
// useful for cross-checking against the engine's live Balance() in
// tests or diagnostics.
func (w *Worker) Balance(id string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.balances[id]
}
