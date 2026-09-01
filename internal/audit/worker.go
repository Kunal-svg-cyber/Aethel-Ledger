
package audit

import (
	"sync"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

type Worker struct {
	mu              sync.Mutex
	balances        map[string]int64
	totalDeposited  int64
	eventsProcessed int64
}

func NewWorker() *Worker {
	return &Worker{balances: make(map[string]int64)}
}

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

func (w *Worker) CheckInvariant() (drift int64, eventsProcessed int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var sum int64
	for _, b := range w.balances {
		sum += b
	}
	return sum - w.totalDeposited, w.eventsProcessed
}

func (w *Worker) Balance(id string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.balances[id]
}
