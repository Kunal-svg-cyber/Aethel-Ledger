package wal

import (
	"context"
	"os"
	"testing"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// TestPostgresStore_FlushAndDedup is an integration test, not a unit
// test — it needs a real reachable Postgres (e.g. your Neon connection
// string). It's skipped by default so `go test ./...` works with zero
// external services. Run it explicitly against your own database with:
//
//	DATABASE_URL="postgres://user:pass@host/db?sslmode=require" go test ./internal/wal/ -run TestPostgresStore -v
func TestPostgresStore_FlushAndDedup(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping Postgres integration test (see comment above to run it)")
	}

	store, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	batch := []ledger.Event{
		{Seq: 999001, Type: ledger.EventDeposit, Account: "test-alice", Amount: 100},
		{Seq: 999002, Type: ledger.EventTransfer, Account: "test-alice", CounterAccount: "test-bob", Amount: 30},
	}

	if err := store.FlushBatch(ctx, batch); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	// Flushing the exact same batch again must not error and must not
	// duplicate rows, thanks to ON CONFLICT (seq) DO NOTHING — this is
	// what makes a WAL retry after a transient failure safe.
	if err := store.FlushBatch(ctx, batch); err != nil {
		t.Fatalf("duplicate flush should not error: %v", err)
	}

	var count int
	row := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ledger_events WHERE seq IN ($1, $2)", 999001, 999002)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Fatalf("row count = %d, want 2 (dedup on retry should prevent duplicates)", count)
	}

	// cleanup
	_, _ = store.db.ExecContext(ctx, "DELETE FROM ledger_events WHERE seq IN ($1, $2)", 999001, 999002)
}
