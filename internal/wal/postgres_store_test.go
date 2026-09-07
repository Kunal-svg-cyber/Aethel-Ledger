package wal

import (
	"context"
	"os"
	"testing"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// TestPostgresStore_FlushAndDedup is an integration test requiring a
// real Postgres connection; skipped unless DATABASE_URL is set. Run with:
//
//	DATABASE_URL="postgres://user:pass@host/db?sslmode=require" go test ./internal/wal/ -run TestPostgresStore -v
func TestPostgresStore_FlushAndDedup(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
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
	if err := store.FlushBatch(ctx, batch); err != nil {
		t.Fatalf("duplicate flush should not error: %v", err)
	}

	var count int
	row := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ledger_events WHERE seq IN ($1, $2)", 999001, 999002)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Fatalf("row count = %d, want 2", count)
	}

	loaded, err := store.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	var found1, found2 bool
	for _, ev := range loaded {
		if ev.Seq == 999001 && ev.Account == "test-alice" && ev.Amount == 100 {
			found1 = true
		}
		if ev.Seq == 999002 && ev.CounterAccount == "test-bob" && ev.Amount == 30 {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Fatalf("LoadAll did not return both seeded events: %+v", loaded)
	}

	_, _ = store.db.ExecContext(ctx, "DELETE FROM ledger_events WHERE seq IN ($1, $2)", 999001, 999002)
}
