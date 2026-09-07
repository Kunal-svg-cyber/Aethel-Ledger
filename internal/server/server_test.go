package server

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/Kunal-svg-cyber/aethel-ledger/internal/genproto/ledger/v1"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/idempotency"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

func newTestServer() *LedgerServer {
	return New(ledger.NewEngine(nil), idempotency.NewInMemoryStore(), nil)
}

// countingFlusher records how many times FlushNow was called, letting
// tests confirm the server actually waits for durability.
type countingFlusher struct {
	calls int
	err   error
}

func (f *countingFlusher) FlushNow(_ context.Context) error {
	f.calls++
	return f.err
}

func TestDeposit_CallsFlushNowBeforeReturning(t *testing.T) {
	flusher := &countingFlusher{}
	s := New(ledger.NewEngine(nil), idempotency.NewInMemoryStore(), flusher)

	if _, err := s.Deposit(context.Background(), &ledgerv1.DepositRequest{AccountId: "alice", Amount: 100}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if flusher.calls != 1 {
		t.Fatalf("FlushNow calls = %d, want 1", flusher.calls)
	}
}

func TestTransfer_CallsFlushNowOnlyOnANewMutationNotOnReplay(t *testing.T) {
	flusher := &countingFlusher{}
	s := New(ledger.NewEngine(nil), idempotency.NewInMemoryStore(), flusher)
	ctx := context.Background()
	_, _ = s.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "alice", Amount: 1000})

	req := &ledgerv1.TransferRequest{FromAccountId: "alice", ToAccountId: "bob", Amount: 100, IdempotencyKey: "k1"}
	if _, err := s.Transfer(ctx, req); err != nil {
		t.Fatalf("first transfer failed: %v", err)
	}
	callsAfterFirst := flusher.calls

	// A replayed request (same idempotency key) returns the cached
	// result without touching the engine again, so it must not trigger
	// another flush.
	if _, err := s.Transfer(ctx, req); err != nil {
		t.Fatalf("replayed transfer failed: %v", err)
	}
	if flusher.calls != callsAfterFirst {
		t.Fatalf("FlushNow calls after replay = %d, want unchanged from %d", flusher.calls, callsAfterFirst)
	}
}

func TestTransfer_SucceedsEvenIfFlushNowFails(t *testing.T) {
	flusher := &countingFlusher{err: errors.New("postgres unreachable")}
	s := New(ledger.NewEngine(nil), idempotency.NewInMemoryStore(), flusher)
	ctx := context.Background()
	_, _ = s.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "alice", Amount: 1000})

	// The mutation already succeeded in-memory; a transient durability
	// failure must not turn a successful transfer into an RPC error,
	// since that could cause a client to retry a Deposit that has no
	// idempotency protection.
	resp, err := s.Transfer(ctx, &ledgerv1.TransferRequest{
		FromAccountId: "alice", ToAccountId: "bob", Amount: 100, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("Transfer should succeed despite flush failure, got: %v", err)
	}
	if resp.GetToBalance() != 100 {
		t.Fatalf("bob balance = %d, want 100", resp.GetToBalance())
	}
}

func TestDeposit_Basic(t *testing.T) {
	s := newTestServer()
	resp, err := s.Deposit(context.Background(), &ledgerv1.DepositRequest{
		AccountId: "alice", Amount: 1000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetBalance() != 1000 {
		t.Fatalf("balance = %d, want 1000", resp.GetBalance())
	}
}

func TestDeposit_RejectsMissingAccountID(t *testing.T) {
	s := newTestServer()
	_, err := s.Deposit(context.Background(), &ledgerv1.DepositRequest{Amount: 1000})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got code %v, want InvalidArgument", status.Code(err))
	}
}

func TestTransfer_RequiresIdempotencyKey(t *testing.T) {
	s := newTestServer()
	_, err := s.Transfer(context.Background(), &ledgerv1.TransferRequest{
		FromAccountId: "alice", ToAccountId: "bob", Amount: 100,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got code %v, want InvalidArgument for missing idempotency_key", status.Code(err))
	}
}

func TestTransfer_InsufficientFundsMapsToFailedPrecondition(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()
	_, _ = s.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "alice", Amount: 10})

	_, err := s.Transfer(ctx, &ledgerv1.TransferRequest{
		FromAccountId: "alice", ToAccountId: "bob", Amount: 500, IdempotencyKey: "key-1",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got code %v, want FailedPrecondition", status.Code(err))
	}
}

// TestTransfer_DuplicateKeyReplaysInsteadOfDoubleSpending asserts the
// same idempotency_key submitted twice moves funds exactly once.
func TestTransfer_DuplicateKeyReplaysInsteadOfDoubleSpending(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()
	_, _ = s.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "alice", Amount: 1000})

	req := &ledgerv1.TransferRequest{
		FromAccountId: "alice", ToAccountId: "bob", Amount: 300, IdempotencyKey: "retry-key",
	}

	first, err := s.Transfer(ctx, req)
	if err != nil {
		t.Fatalf("first transfer failed: %v", err)
	}
	if first.GetReplayed() {
		t.Fatal("first attempt should not be marked as replayed")
	}

	second, err := s.Transfer(ctx, req)
	if err != nil {
		t.Fatalf("second (retried) transfer failed: %v", err)
	}
	if !second.GetReplayed() {
		t.Fatal("second attempt with same idempotency_key should be marked as replayed")
	}
	if second.GetFromBalance() != first.GetFromBalance() || second.GetToBalance() != first.GetToBalance() {
		t.Fatalf("replayed result mismatch: first=%+v second=%+v", first, second)
	}

	balResp, err := s.GetBalance(ctx, &ledgerv1.GetBalanceRequest{AccountId: "alice"})
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if balResp.GetBalance() != 700 {
		t.Fatalf("alice balance = %d, want 700 (300 moved exactly once from 1000)", balResp.GetBalance())
	}
}

func TestGetBalance_UntouchedAccountReturnsZero(t *testing.T) {
	s := newTestServer()
	resp, err := s.GetBalance(context.Background(), &ledgerv1.GetBalanceRequest{AccountId: "nobody"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetBalance() != 0 {
		t.Fatalf("balance = %d, want 0", resp.GetBalance())
	}
}
