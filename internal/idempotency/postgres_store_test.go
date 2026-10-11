package idempotency

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestPostgresStore_SurvivesAcrossInstances is an integration test
// requiring a real Postgres connection; skipped unless DATABASE_URL is
// set. It's the direct regression test for the bug this store fixes:
// an idempotency key committed by one store instance must still be
// recognized by a brand-new instance pointed at the same database —
// simulating exactly what a server restart does. Run with:
//
//	DATABASE_URL="postgres://user:pass@host/db?sslmode=require" go test ./internal/idempotency/ -run TestPostgresStore -v
func TestPostgresStore_SurvivesAcrossInstances(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()

	first, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := first.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	defer first.Close()

	testKey := "test-key-postgres-restart-sim"
	defer func() {
		_, _ = first.db.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE key = $1", testKey)
	}()

	if _, committed, err := first.CheckAndReserve(ctx, testKey); err != nil {
		t.Fatalf("reserve: %v", err)
	} else if committed {
		t.Fatal("brand-new key should not already be committed")
	}
	if err := first.Commit(ctx, testKey, []byte("original-result")); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// A brand-new store instance, simulating a server restart, must
	// still recognize this key as committed and return the original
	// result — not treat the resubmitted request as new.
	second, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("connect (second instance): %v", err)
	}
	defer second.Close()

	result, committed, err := second.CheckAndReserve(ctx, testKey)
	if err != nil {
		t.Fatalf("reserve on second instance: %v", err)
	}
	if !committed {
		t.Fatal("second store instance did not recognize a key committed by the first — idempotency would not survive a restart")
	}
	if string(result) != "original-result" {
		t.Fatalf("result = %q, want %q", result, "original-result")
	}
}

// TestPostgresStore_DeleteExpiredRemovesOldKeysOnly is an integration
// test for TTL cleanup against a real database; skipped unless
// DATABASE_URL is set.
func TestPostgresStore_DeleteExpiredRemovesOldKeysOnly(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()

	store, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	defer store.Close()

	const oldKey, recentKey = "test-ttl-old", "test-ttl-recent"
	cleanup := func() {
		_, _ = store.db.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE key IN ($1, $2)", oldKey, recentKey)
	}
	cleanup()
	defer cleanup()

	if _, err := store.db.ExecContext(ctx,
		"INSERT INTO idempotency_keys (key, created_at) VALUES ($1, now() - interval '48 hours')", oldKey); err != nil {
		t.Fatalf("insert old key: %v", err)
	}
	if _, _, err := store.CheckAndReserve(ctx, recentKey); err != nil {
		t.Fatalf("reserve recent key: %v", err)
	}

	if _, err := store.DeleteExpired(ctx, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("delete expired: %v", err)
	}

	var n int
	if err := store.db.QueryRowContext(ctx,
		"SELECT count(*) FROM idempotency_keys WHERE key = $1", oldKey).Scan(&n); err != nil || n != 0 {
		t.Fatalf("old key should be removed (count=%d, err=%v)", n, err)
	}
	if err := store.db.QueryRowContext(ctx,
		"SELECT count(*) FROM idempotency_keys WHERE key = $1", recentKey).Scan(&n); err != nil || n != 1 {
		t.Fatalf("recent key must survive (count=%d, err=%v)", n, err)
	}
}

func TestPostgresStore_ReleaseFreesUncommittedKeyOnly(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	const open, done = "test-release-open", "test-release-done"
	defer func() {
		_, _ = s.db.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE key IN ($1, $2)", open, done)
	}()

	_, _, _ = s.CheckAndReserve(ctx, open)
	if err := s.Release(ctx, open); err != nil {
		t.Fatal(err)
	}
	if _, already, _ := s.CheckAndReserve(ctx, open); already {
		t.Fatal("released key should be reservable again")
	}

	_, _, _ = s.CheckAndReserve(ctx, done)
	_ = s.Commit(ctx, done, []byte("r"))
	_ = s.Release(ctx, done)
	if _, already, _ := s.CheckAndReserve(ctx, done); !already {
		t.Fatal("release must not remove a committed key")
	}
}
