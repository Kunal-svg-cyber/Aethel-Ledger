// Package wal implements the async write-ahead log between the
// in-memory ledger engine and durable storage: the engine emits events
// onto a channel, and this package drains it on a separate goroutine,
// batching and flushing asynchronously so the transfer hot path never
// blocks on I/O. A synchronous FlushNow is also provided for callers
// that need a durable-ack guarantee before responding to a client,
// using a group-commit pattern: many concurrent FlushNow callers within
// the same brief window are satisfied by a single underlying flush,
// rather than one network round trip per caller.
package wal

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// Store durably persists a batch of events and can load the full
// history back, used at startup to rebuild engine state after a
// restart.
type Store interface {
	FlushBatch(ctx context.Context, batch []ledger.Event) error
	LoadAll(ctx context.Context) ([]ledger.Event, error)
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
// them to Store on the configured cadence, or immediately (via group
// commit) on FlushNow.
type WAL struct {
	events   chan ledger.Event
	flushReq chan struct{} // buffered 1: coalesces many triggers into one pending signal

	store Store
	pub   Publisher
	cfg   Config
	done  chan struct{}

	waitersMu sync.Mutex
	waiters   []chan error
}

// New constructs a WAL. Pass the channel returned by Events() to
// ledger.NewEngine as the engine's event sink.
func New(store Store, pub Publisher, cfg Config) *WAL {
	return &WAL{
		events:   make(chan ledger.Event, 1024),
		flushReq: make(chan struct{}, 1),
		store:    store,
		pub:      pub,
		cfg:      cfg,
		done:     make(chan struct{}),
	}
}

// Events returns the send side of the internal channel.
func (w *WAL) Events() chan<- ledger.Event { return w.events }

// FlushNow blocks until everything currently buffered — including any
// event this caller sent to Events() moments earlier — has been flushed
// to Store, giving a durable-ack guarantee before the caller
// acknowledges success to its own client. Concurrent FlushNow callers
// register as waiters on a shared upcoming flush rather than each
// triggering their own; Run services one flush per waiters batch and
// notifies everyone waiting on it, so N concurrent callers cost one
// round trip to Store, not N.
func (w *WAL) FlushNow(ctx context.Context) error {
	done := make(chan error, 1)

	w.waitersMu.Lock()
	w.waiters = append(w.waiters, done)
	w.waitersMu.Unlock()

	// Signal Run to flush soon. Non-blocking: if a flush is already
	// pending/imminent, this is a no-op and our registration above will
	// be picked up by that same upcoming flush.
	select {
	case w.flushReq <- struct{}{}:
	default:
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run drains and flushes events until ctx is cancelled.
func (w *WAL) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]ledger.Event, 0, w.cfg.BatchSize)

	appendEvent := func(ev ledger.Event) {
		batch = append(batch, ev)
		if w.pub != nil {
			if err := w.pub.Publish(ctx, ev); err != nil {
				log.Printf("wal: publish failed for event seq=%d: %v", ev.Seq, err)
			}
		}
	}

	// drainPending pulls in anything already queued in events, without
	// blocking, so a triggered flush always includes events sent just
	// before it — closing the race where a caller's own event might
	// otherwise still be in flight when their flush runs.
	drainPending := func() {
		for {
			select {
			case ev := <-w.events:
				appendEvent(ev)
			default:
				return
			}
		}
	}

	flushAndNotifyWaiters := func() {
		// Snapshot exactly which waiters this round commits to service
		// BEFORE draining/flushing. Each waiter's event-send happens
		// before its own waiter registration (per-goroutine, in
		// FlushNow's caller), so anyone captured in this snapshot is
		// guaranteed to already have their event sitting in w.events —
		// meaning the drainPending() call right after this snapshot is
		// guaranteed to pick it up. Snapshotting after the flush instead
		// would let a waiter that registers WHILE this round's
		// FlushBatch is in flight get bundled into "waiters" without
		// their own event being part of the batch just flushed — a
		// false durable-ack for data that isn't actually durable yet.
		w.waitersMu.Lock()
		waiters := w.waiters
		w.waiters = nil
		w.waitersMu.Unlock()

		drainPending()

		var err error
		if len(batch) > 0 {
			err = w.store.FlushBatch(ctx, batch)
			if err != nil {
				log.Printf("wal: flush failed: %v", err)
			}
			batch = batch[:0]
		}

		for _, ch := range waiters {
			ch <- err
		}
	}

	for {
		select {
		case ev := <-w.events:
			appendEvent(ev)
			if len(batch) >= w.cfg.BatchSize {
				flushAndNotifyWaiters()
			}

		case <-w.flushReq:
			flushAndNotifyWaiters()

		case <-ticker.C:
			flushAndNotifyWaiters()

		case <-ctx.Done():
			flushAndNotifyWaiters()
			close(w.done)
			return
		}
	}
}

// Done is closed once Run performs its final flush after ctx is cancelled.
func (w *WAL) Done() <-chan struct{} { return w.done }
