
package wal

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

type Store interface {
	FlushBatch(ctx context.Context, batch []ledger.Event) error
	LoadAll(ctx context.Context) ([]ledger.Event, error)
}

type Publisher interface {
	Publish(ctx context.Context, ev ledger.Event) error
}

type Config struct {
	BatchSize     int
	FlushInterval time.Duration
}

func DefaultConfig() Config {
	return Config{BatchSize: 100, FlushInterval: 250 * time.Millisecond}
}

type WAL struct {
	events   chan ledger.Event
	flushReq chan struct{}

	store Store
	pub   Publisher
	cfg   Config
	done  chan struct{}

	waitersMu sync.Mutex
	waiters   []chan error
}

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

func (w *WAL) Events() chan<- ledger.Event { return w.events }

func (w *WAL) FlushNow(ctx context.Context) error {
	done := make(chan error, 1)

	w.waitersMu.Lock()
	w.waiters = append(w.waiters, done)
	w.waitersMu.Unlock()

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

func (w *WAL) Done() <-chan struct{} { return w.done }

