package audit

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/streaming"
)

// StreamReader is the subset of streaming.RedisStreamsBus that
// RedisConsumer needs, letting tests supply a fake reader.
type StreamReader interface {
	ReadRange(ctx context.Context, fromIDExclusive string) ([]streaming.StreamEntry, error)
}

// RedisConsumer polls a Redis Stream on an interval, parses each new
// entry back into a ledger.Event, and applies it to a Worker.
type RedisConsumer struct {
	reader StreamReader
	worker *Worker
	lastID string
}

func NewRedisConsumer(reader StreamReader, worker *Worker) *RedisConsumer {
	return &RedisConsumer{reader: reader, worker: worker}
}

// Run polls every interval until ctx is cancelled.
func (c *RedisConsumer) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.poll(ctx)
		}
	}
}

func (c *RedisConsumer) poll(ctx context.Context) {
	entries, err := c.reader.ReadRange(ctx, c.lastID)
	if err != nil {
		log.Printf("audit: redis poll failed: %v", err)
		return
	}
	for _, e := range entries {
		ev, err := parseEvent(e.Fields)
		if err != nil {
			log.Printf("audit: skipping malformed event id=%s: %v", e.ID, err)
			continue
		}
		c.worker.Apply(ev)
		c.lastID = e.ID
	}
}

func parseEvent(fields map[string]string) (ledger.Event, error) {
	seq, err := strconv.ParseInt(fields["seq"], 10, 64)
	if err != nil {
		return ledger.Event{}, err
	}
	amount, err := strconv.ParseInt(fields["amount"], 10, 64)
	if err != nil {
		return ledger.Event{}, err
	}
	return ledger.Event{
		Seq:            seq,
		Type:           ledger.EventType(fields["type"]),
		Account:        fields["account"],
		CounterAccount: fields["counter_account"],
		Amount:         amount,
	}, nil
}
