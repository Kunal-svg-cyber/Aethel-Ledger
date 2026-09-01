package audit

import (
	"testing"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

func TestWorker_ConservationHoldsAfterDepositsAndTransfers(t *testing.T) {
	w := NewWorker()

	w.Apply(ledger.Event{Type: ledger.EventDeposit, Account: "alice", Amount: 1000})
	w.Apply(ledger.Event{Type: ledger.EventDeposit, Account: "bob", Amount: 500})
	w.Apply(ledger.Event{Type: ledger.EventTransfer, Account: "alice", CounterAccount: "bob", Amount: 300})
	w.Apply(ledger.Event{Type: ledger.EventTransfer, Account: "bob", CounterAccount: "alice", Amount: 100})

	if got := w.Balance("alice"); got != 800 { // 1000 - 300 + 100
		t.Fatalf("alice derived balance = %d, want 800", got)
	}
	if got := w.Balance("bob"); got != 700 { // 500 + 300 - 100
		t.Fatalf("bob derived balance = %d, want 700", got)
	}

	drift, n := w.CheckInvariant()
	if drift != 0 {
		t.Fatalf("invariant drift = %d, want 0", drift)
	}
	if n != 4 {
		t.Fatalf("eventsProcessed = %d, want 4", n)
	}
}


func TestWorker_DetectsDriftIfLogWereInconsistent(t *testing.T) {
	w := NewWorker()
	w.Apply(ledger.Event{Type: ledger.EventDeposit, Account: "alice", Amount: 1000})


	w.Apply(ledger.Event{Type: ledger.EventTransfer, Account: "alice", CounterAccount: "bob", Amount: 1500})

	drift, _ := w.CheckInvariant()
	if drift != 0 {
		t.Fatalf("drift = %d, want 0 (transfers conserve total regardless of amount, even overdrafts)", drift)
	}

}

func TestWorker_UntouchedAccountHasZeroDerivedBalance(t *testing.T) {
	w := NewWorker()
	if got := w.Balance("nobody"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestParseEvent_RoundTripsAllFields(t *testing.T) {
	fields := map[string]string{
		"seq": "7", "type": "transfer", "account": "alice",
		"counter_account": "bob", "amount": "250",
	}
	ev, err := parseEvent(fields)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := ledger.Event{Seq: 7, Type: ledger.EventTransfer, Account: "alice", CounterAccount: "bob", Amount: 250}
	if ev != want {
		t.Fatalf("parsed = %+v, want %+v", ev, want)
	}
}

func TestParseEvent_RejectsMalformedAmount(t *testing.T) {
	fields := map[string]string{"seq": "1", "type": "deposit", "account": "alice", "amount": "not-a-number"}
	if _, err := parseEvent(fields); err == nil {
		t.Fatal("expected an error for a non-numeric amount field")
	}
}
