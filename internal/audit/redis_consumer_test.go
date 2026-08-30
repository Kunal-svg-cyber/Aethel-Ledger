package audit

import (
	"context"
	"testing"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/streaming"
)

// fakeReader implements StreamReader, letting tests drive RedisConsumer
// deterministically without a real HTTP round trip.
type fakeReader struct {
	// calls[i] is what ReadRange returns on its i-th invocation.
	calls [][]streaming.StreamEntry
	seen  []string // fromIDExclusive value passed on each call, for assertions
}

func (f *fakeReader) ReadRange(_ context.Context, fromIDExclusive string) ([]streaming.StreamEntry, error) {
	f.seen = append(f.seen, fromIDExclusive)
	if len(f.calls) == 0 {
		return nil, nil
	}
	next := f.calls[0]
	f.calls = f.calls[1:]
	return next, nil
}

func TestRedisConsumer_AppliesEntriesAndAdvancesLastID(t *testing.T) {
	reader := &fakeReader{
		calls: [][]streaming.StreamEntry{
			{
				{ID: "1-0", Fields: map[string]string{"seq": "1", "type": "deposit", "account": "alice", "counter_account": "", "amount": "1000"}},
				{ID: "2-0", Fields: map[string]string{"seq": "2", "type": "transfer", "account": "alice", "counter_account": "bob", "amount": "300"}},
			},
			{
				{ID: "3-0", Fields: map[string]string{"seq": "3", "type": "deposit", "account": "carol", "counter_account": "", "amount": "50"}},
			},
		},
	}
	worker := NewWorker()
	consumer := NewRedisConsumer(reader, worker)

	consumer.poll(context.Background())
	if got := worker.Balance("alice"); got != 700 {
		t.Fatalf("after first poll, alice = %d, want 700", got)
	}
	if got := worker.Balance("bob"); got != 300 {
		t.Fatalf("after first poll, bob = %d, want 300", got)
	}

	consumer.poll(context.Background())
	if got := worker.Balance("carol"); got != 50 {
		t.Fatalf("after second poll, carol = %d, want 50", got)
	}

	// The consumer must have advanced its cursor: second call should
	// have used "2-0" (the last ID from the first batch) as the
	// exclusive lower bound, proving it doesn't re-read old entries.
	if len(reader.seen) != 2 || reader.seen[1] != "2-0" {
		t.Fatalf("expected second ReadRange call to use fromIDExclusive=2-0, got calls: %v", reader.seen)
	}

	drift, n := worker.CheckInvariant()
	if drift != 0 {
		t.Fatalf("drift = %d, want 0", drift)
	}
	if n != 3 {
		t.Fatalf("eventsProcessed = %d, want 3", n)
	}
}

func TestRedisConsumer_SkipsMalformedEntriesWithoutStoppingTheLoop(t *testing.T) {
	reader := &fakeReader{
		calls: [][]streaming.StreamEntry{
			{
				{ID: "1-0", Fields: map[string]string{"seq": "1", "type": "deposit", "account": "alice", "amount": "not-a-number"}},
				{ID: "2-0", Fields: map[string]string{"seq": "2", "type": "deposit", "account": "alice", "amount": "500"}},
			},
		},
	}
	worker := NewWorker()
	consumer := NewRedisConsumer(reader, worker)

	consumer.poll(context.Background())

	// The malformed entry (id 1-0) should be skipped, but the valid one
	// (id 2-0) still applied.
	if got := worker.Balance("alice"); got != 500 {
		t.Fatalf("alice = %d, want 500 (malformed entry should be skipped, not fatal)", got)
	}
}
