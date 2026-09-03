// Package wal implements the async write-ahead log between the
// in-memory ledger engine and durable storage: the engine emits events
// onto a channel, and this package drains it on a separate goroutine,
// batching and flushing asynchronously so the transfer hot path never
// blocks on I/O.
package wal

import (
	"context"
	"log"
	"time"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// Store durably persists a batch of events.
type Store interface {
	FlushBatch(ctx context.Context, batch []ledger.Event) error
}

// Publisher optionally re-broadcasts each event as it arrives. A nil
// Publisher is valid — WAL simply skips publishing.
type Publisher interface {
	Publish(ctx context.Context, ev ledger.Event) error
}

// Config controls batching: a flush happens when BatchSize events have
// buffered, or FlushInterval has elapsed, whichever comes first.
type Config struct {
	BatchSize     int
	FlushInterval time.Duration
}

func DefaultConfig() Config {
	return Config{BatchSize: 100, FlushInterval: 250 * time.Millisecond}
}

// WAL drains an internal event channel, batches events, and flushes
// them to Store on the configured cadence.
type WAL struct {
	events chan ledger.Event
	store  Store
	pub    Publisher
	cfg    Config
	done   chan struct{}
}

// New constructs a WAL. Pass the channel returned by Events() to
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

// Events returns the send side of the internal channel.
func (w *WAL) Events() chan<- ledger.Event { return w.events }

// Run drains and flushes events until ctx is cancelled.
func (w *WAL) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]ledger.Event, 0, w.cfg.BatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.store.FlushBatch(ctx, batch); err != nil {
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

// Done is closed once Run performs its final flush after ctx is cancelled.
func (w *WAL) Done() <-chan struct{} { return w.done }
