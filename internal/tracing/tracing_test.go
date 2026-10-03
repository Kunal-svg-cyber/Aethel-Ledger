package tracing

import (
	"context"
	"testing"
)

func TestNewID_GeneratesNonEmptyUniqueIDs(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := NewID()
		if id == "" {
			t.Fatal("NewID returned an empty string")
		}
		if len(id) != 16 { // 8 bytes, hex-encoded = 16 chars
			t.Fatalf("id length = %d, want 16 (got %q)", len(id), id)
		}
		if seen[id] {
			t.Fatalf("NewID produced a duplicate: %q", id)
		}
		seen[id] = true
	}
}

func TestWithID_AndFromContext_RoundTrip(t *testing.T) {
	ctx := context.Background()
	ctx = WithID(ctx, "abc123")

	got := FromContext(ctx)
	if got != "abc123" {
		t.Fatalf("FromContext = %q, want %q", got, "abc123")
	}
}

func TestFromContext_ReturnsEmptyStringWhenNoneSet(t *testing.T) {
	ctx := context.Background()
	got := FromContext(ctx)
	if got != "" {
		t.Fatalf("FromContext on a bare context = %q, want empty string", got)
	}
}

func TestWithID_DoesNotMutateParentContext(t *testing.T) {
	parent := context.Background()
	child := WithID(parent, "abc123")

	if FromContext(parent) != "" {
		t.Fatal("WithID must not mutate the parent context")
	}
	if FromContext(child) != "abc123" {
		t.Fatal("the child context should carry the new trace ID")
	}
}
