package idempotency

import (
	"context"
	"testing"
)

func TestInMemoryStore_NewKeyIsReservedNotCommitted(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()

	result, committed, err := s.CheckAndReserve(ctx, "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if committed {
		t.Fatal("a brand-new key should not be reported as already committed")
	}
	if result != nil {
		t.Fatalf("result = %v, want nil for a new key", result)
	}
}

func TestInMemoryStore_CommittedKeyReturnsStoredResult(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()

	if _, _, err := s.CheckAndReserve(ctx, "key-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Commit(ctx, "key-1", []byte("the-result")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, committed, err := s.CheckAndReserve(ctx, "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !committed {
		t.Fatal("a committed key should be reported as committed")
	}
	if string(result) != "the-result" {
		t.Fatalf("result = %q, want %q", result, "the-result")
	}
}

func TestInMemoryStore_ReservedButUncommittedKeyIsFlaggedWithoutResult(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()

	// Simulates a concurrent in-flight request: the key has been
	// reserved but Commit hasn't been called yet.
	if _, _, err := s.CheckAndReserve(ctx, "key-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, committed, err := s.CheckAndReserve(ctx, "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !committed {
		t.Fatal("a reserved-but-uncommitted key should still report alreadyCommitted=true, signaling a concurrent duplicate")
	}
	if result != nil {
		t.Fatalf("result = %v, want nil for a reserved-but-uncommitted key", result)
	}
}

func TestInMemoryStore_DoesNotSurviveAcrossInstances(t *testing.T) {
	// This is the documented limitation, made explicit as a test: a new
	// InMemoryStore instance (simulating a process restart) has no
	// knowledge of keys committed to a previous instance. PostgresStore
	// exists specifically to close this gap.
	first := NewInMemoryStore()
	ctx := context.Background()
	_, _, _ = first.CheckAndReserve(ctx, "key-1")
	_ = first.Commit(ctx, "key-1", []byte("result"))

	second := NewInMemoryStore()
	_, committed, err := second.CheckAndReserve(ctx, "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if committed {
		t.Fatal("a fresh InMemoryStore should have no knowledge of a previous instance's committed keys")
	}
}
