package ledger

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestRestore_RebuildsBalancesFromEventLog(t *testing.T) {
	e := NewEngine(nil)
	history := []Event{
		{Seq: 1, Type: EventDeposit, Account: "alice", Amount: 1000},
		{Seq: 2, Type: EventDeposit, Account: "bob", Amount: 500},
		{Seq: 3, Type: EventTransfer, Account: "alice", CounterAccount: "bob", Amount: 300},
		{Seq: 4, Type: EventTransfer, Account: "bob", CounterAccount: "alice", Amount: 100},
	}
	e.Restore(history)

	if got := e.Balance("alice"); got != 800 {
		t.Fatalf("alice balance after restore = %d, want 800", got)
	}
	if got := e.Balance("bob"); got != 700 {
		t.Fatalf("bob balance after restore = %d, want 700", got)
	}
}

func TestRestore_ContinuesSequenceWithoutCollision(t *testing.T) {
	e := NewEngine(nil)
	history := []Event{
		{Seq: 1, Type: EventDeposit, Account: "alice", Amount: 1000},
		{Seq: 5, Type: EventDeposit, Account: "bob", Amount: 500},
	}
	e.Restore(history)

	if got := e.CurrentSeq(); got != 5 {
		t.Fatalf("CurrentSeq after restore = %d, want 5", got)
	}

	if _, err := e.Deposit(context.Background(), "carol", 200); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := e.CurrentSeq(); got != 6 {
		t.Fatalf("CurrentSeq after post-restore deposit = %d, want 6", got)
	}
}

func TestRestore_OnEmptyHistoryLeavesEngineAtZero(t *testing.T) {
	e := NewEngine(nil)
	e.Restore(nil)
	if got := e.CurrentSeq(); got != 0 {
		t.Fatalf("CurrentSeq after empty restore = %d, want 0", got)
	}
	if got := e.Balance("alice"); got != 0 {
		t.Fatalf("balance after empty restore = %d, want 0", got)
	}
}

func TestDeposit_RejectsAmountThatWouldOverflowBalance(t *testing.T) {
	e := NewEngine(nil)
	ctx := context.Background()

	if _, err := e.Deposit(ctx, "alice", math.MaxInt64-10); err != nil {
		t.Fatalf("setup deposit failed: %v", err)
	}
	if _, err := e.Deposit(ctx, "alice", 100); err != ErrBalanceOverflow {
		t.Fatalf("got %v, want ErrBalanceOverflow", err)
	}

	if got := e.Balance("alice"); got != math.MaxInt64-10 {
		t.Fatalf("balance = %d, want unchanged at MaxInt64-10", got)
	}
}

func TestTransfer_RejectsAmountThatWouldOverflowRecipientBalance(t *testing.T) {
	e := NewEngine(nil)
	ctx := context.Background()

	if _, err := e.Deposit(ctx, "alice", 1000); err != nil {
		t.Fatalf("setup deposit failed: %v", err)
	}
	if _, err := e.Deposit(ctx, "bob", math.MaxInt64-10); err != nil {
		t.Fatalf("setup deposit failed: %v", err)
	}

	err := e.Transfer(ctx, "alice", "bob", 100)
	if err != ErrBalanceOverflow {
		t.Fatalf("got %v, want ErrBalanceOverflow", err)
	}

	if got := e.Balance("alice"); got != 1000 {
		t.Fatalf("alice balance = %d, want unchanged at 1000", got)
	}
	if got := e.Balance("bob"); got != math.MaxInt64-10 {
		t.Fatalf("bob balance = %d, want unchanged at MaxInt64-10", got)
	}
}

func TestDeposit_OrdinaryAmountsAreUnaffectedByOverflowCheck(t *testing.T) {

	e := NewEngine(nil)
	ctx := context.Background()
	for _, amount := range []int64{1, 100, 1000, 1_000_000, 1_000_000_000} {
		if _, err := e.Deposit(ctx, "alice", amount); err != nil {
			t.Fatalf("ordinary deposit of %d unexpectedly failed: %v", amount, err)
		}
	}
}

func TestDeposit(t *testing.T) {
	e := NewEngine(nil)
	bal, err := e.Deposit(context.Background(), "alice", 500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bal != 500 {
		t.Fatalf("balance = %d, want 500", bal)
	}
}

func TestDeposit_RejectsNonPositive(t *testing.T) {
	e := NewEngine(nil)
	if _, err := e.Deposit(context.Background(), "alice", 0); err != ErrInvalidAmount {
		t.Fatalf("got %v, want ErrInvalidAmount", err)
	}
	if _, err := e.Deposit(context.Background(), "alice", -10); err != ErrInvalidAmount {
		t.Fatalf("got %v, want ErrInvalidAmount", err)
	}
}

func TestTransfer_Basic(t *testing.T) {
	e := NewEngine(nil)
	ctx := context.Background()
	_, _ = e.Deposit(ctx, "alice", 1000)

	if err := e.Transfer(ctx, "alice", "bob", 300); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := e.Balance("alice"); got != 700 {
		t.Fatalf("alice balance = %d, want 700", got)
	}
	if got := e.Balance("bob"); got != 300 {
		t.Fatalf("bob balance = %d, want 300", got)
	}
}

func TestTransfer_InsufficientFunds(t *testing.T) {
	e := NewEngine(nil)
	ctx := context.Background()
	_, _ = e.Deposit(ctx, "alice", 100)

	err := e.Transfer(ctx, "alice", "bob", 500)
	if err != ErrInsufficientFunds {
		t.Fatalf("got %v, want ErrInsufficientFunds", err)
	}
	if got := e.Balance("alice"); got != 100 {
		t.Fatalf("alice balance = %d, want 100 (unchanged)", got)
	}
	if got := e.Balance("bob"); got != 0 {
		t.Fatalf("bob balance = %d, want 0 (unchanged)", got)
	}
}

func TestTransfer_RejectsSameAccount(t *testing.T) {
	e := NewEngine(nil)
	if err := e.Transfer(context.Background(), "alice", "alice", 10); err != ErrSameAccount {
		t.Fatalf("got %v, want ErrSameAccount", err)
	}
}

func TestConcurrentTransfers_ConservesTotalBalance(t *testing.T) {
	const (
		numAccounts     = 12
		numGoroutines   = 200
		transfersPerG   = 200
		startingBalance = 10_000
	)

	e := NewEngine(nil)
	ctx := context.Background()

	accounts := make([]string, numAccounts)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("acct-%02d", i)
		if _, err := e.Deposit(ctx, accounts[i], startingBalance); err != nil {
			t.Fatalf("setup deposit failed: %v", err)
		}
	}
	wantTotal := int64(numAccounts * startingBalance)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < transfersPerG; i++ {
				from := accounts[r.Intn(numAccounts)]
				to := accounts[r.Intn(numAccounts)]
				if from == to {
					continue
				}
				amount := int64(r.Intn(50) + 1)
				_ = e.Transfer(ctx, from, to, amount)
			}
		}(int64(g))
	}
	wg.Wait()

	var gotTotal int64
	for _, id := range accounts {
		gotTotal += e.Balance(id)
	}
	if gotTotal != wantTotal {
		t.Fatalf("invariant violated: total balance = %d, want %d", gotTotal, wantTotal)
	}
}

func TestTransfer_NoDeadlockUnderReversedConcurrentPairs(t *testing.T) {
	e := NewEngine(nil)
	ctx := context.Background()
	_, _ = e.Deposit(ctx, "A", 1_000_000)
	_, _ = e.Deposit(ctx, "B", 1_000_000)

	const iterations = 5000
	done := make(chan struct{})

	go func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = e.Transfer(ctx, "A", "B", 1)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = e.Transfer(ctx, "B", "A", 1)
			}
		}()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock detected: reversed concurrent transfers did not complete in time")
	}

	total := e.Balance("A") + e.Balance("B")
	if total != 2_000_000 {
		t.Fatalf("total after reversed-pair stress = %d, want 2000000", total)
	}
}

func BenchmarkTransfer_Parallel(b *testing.B) {
	const numAccounts = 64
	e := NewEngine(nil)
	ctx := context.Background()

	accounts := make([]string, numAccounts)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("acct-%02d", i)
		_, _ = e.Deposit(ctx, accounts[i], 1_000_000_000)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			from := accounts[r.Intn(numAccounts)]
			to := accounts[r.Intn(numAccounts)]
			if from == to {
				continue
			}
			_ = e.Transfer(ctx, from, to, 1)
		}
	})
}

