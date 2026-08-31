package ledger

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

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
	// Balances must be untouched on a rejected transfer.
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
		// success: no deadlock
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
