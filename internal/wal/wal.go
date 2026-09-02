// Package wal implements the write-ahead log that sits between the
// in-memory ledger engine and durable storage. The engine never blocks
// on disk or database I/O: it emits events onto a channel, and this
// package drains that channel on a separate goroutine, batching events
// and flushing them asynchronously. This is what "async write-ahead
// logging" means in the architecture doc — the hot path (a transfer
// request) only ever touches memory; persistence happens after the
// fact, in the background, in batches.
package wal

import (
	"context"
	"log"
	"time"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// Store durably persists a batch of events. The only real implementation
// is PostgresStore; InMemoryStore exists for local dev and tests where
// no database is configured.
type Store interface {
	FlushBatch(ctx context.Context, batch []ledger.Event) error
}

// Publisher optionally re-broadcasts each event as it arrives, e.g. to
// Redis Streams for the audit worker. A nil Publisher is valid — WAL
// simply skips publishing.
type Publisher interface {
	Publish(ctx context.Context, ev ledger.Event) error
}

// Config controls batching behavior: a flush happens when either
// BatchSize events have buffered, or FlushInterval has elapsed since the
// last flush — whichever comes first. This bounds both worst-case
// latency (an event is never stuck unflushed longer than FlushInterval)
// and worst-case batch size (memory use is bounded even under a burst).
type Config struct {
	BatchSize     int
	FlushInterval time.Duration
}

func DefaultConfig() Config {
	return Config{BatchSize: 100, FlushInterval: 250 * time.Millisecond}
}

// WAL drains an internal event channel, batches events, and flushes them
// to Store on the configured cadence.
type WAL struct {
	events chan ledger.Event
	store  Store
	pub    Publisher
	cfg    Config
	done   chan struct{}
}

// New constructs a WAL. Pass the channel it returns from Events() to
// ledger.NewEngine as the engine's event sink.
func New(store Store, pub Publisher, cfg Config) *WAL {
	return &WAL{
		events: make(chan ledger.Event, 1024),
		store:  store,
		pub:    pub,
		cfg:    cfg,
		done:   make(chan struct{}),
	}
}

// Events returns the send side of the internal channel, for wiring into
// ledger.NewEngine.
func (w *WAL) Events() chan<- ledger.Event { return w.events }

// Run drains and flushes events until ctx is cancelled. Intended to run
// in its own goroutine for the lifetime of the process.
func (w *WAL) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]ledger.Event, 0, w.cfg.BatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.store.FlushBatch(ctx, batch); err != nil {
			// Week 5-6 scope: log and drop the batch. A production WAL
			// would retry with backoff and/or spill to a local file so a
			// transient Postgres outage can't lose committed events —
			// that hardening is a natural next iteration, not done here
			// so the core batching/flush mechanics stay easy to read.
			log.Printf("wal: flush failed for %d events: %v", len(batch), err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case ev := <-w.events:
			batch = append(batch, ev)
			if w.pub != nil {
				if err := w.pub.Publish(ctx, ev); err != nil {
					log.Printf("wal: publish failed for event seq=%d: %v", ev.Seq, err)
				}
			}
			if len(batch) >= w.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			close(w.done)
			return
		}
	}
}

// Done is closed once Run has performed its final flush after ctx is cancelled.
func (w *WAL) Done() <-chan struct{} { return w.done }
