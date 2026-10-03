package idempotency

import (
	"context"
	"testing"
	"time"
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

// --- New: TTL cleanup behavior ---

func TestInMemoryStore_DeleteExpiredRemovesOnlyOldKeys(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()

	// Create an "old" key, then wait, then create a "recent" one, so a
	// cutoff between the two waits removes only the first.
	if _, _, err := s.CheckAndReserve(ctx, "old-key"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	cutoff := time.Now()
	time.Sleep(20 * time.Millisecond)
	if _, _, err := s.CheckAndReserve(ctx, "recent-key"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	removed, err := s.DeleteExpired(ctx, cutoff)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1 (only old-key)", removed)
	}
	if s.Len() != 1 {
		t.Fatalf("remaining keys = %d, want 1 (recent-key)", s.Len())
	}

	// The expired key must now behave like a brand-new key if reused —
	// this is intentional: a key old enough to be pruned is, by
	// definition, old enough that no legitimate retry is still using it.
	_, committed, err := s.CheckAndReserve(ctx, "old-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if committed {
		t.Fatal("an expired-and-pruned key should be treated as new, not as already committed")
	}
}

func TestInMemoryStore_DeleteExpiredWithFutureCutoffRemovesEverything(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	_, _, _ = s.CheckAndReserve(ctx, "a")
	_, _, _ = s.CheckAndReserve(ctx, "b")

	removed, err := s.DeleteExpired(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if s.Len() != 0 {
		t.Fatalf("remaining = %d, want 0", s.Len())
	}
}

func TestInMemoryStore_DeleteExpiredWithPastCutoffRemovesNothing(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	_, _, _ = s.CheckAndReserve(ctx, "a")

	removed, err := s.DeleteExpired(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0 (cutoff is in the past, nothing should qualify as expired)", removed)
	}
	if s.Len() != 1 {
		t.Fatalf("remaining = %d, want 1", s.Len())
	}
}

// Compile-time check that both stores actually satisfy ExpirableStore —
// if a future change breaks this, it fails at build time, not silently.
var (
	_ ExpirableStore = (*InMemoryStore)(nil)
	_ ExpirableStore = (*PostgresStore)(nil)
	_ Store          = (*InMemoryStore)(nil)
	_ Store          = (*PostgresStore)(nil)
)
