package idempotency

import (
	"context"
	"os"
	"testing"
)

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

