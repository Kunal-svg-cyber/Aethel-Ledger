
package wal

import (
	"context"
	"log"
	"time"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

type Store interface {
	FlushBatch(ctx context.Context, batch []ledger.Event) error
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
	events chan ledger.Event
	store  Store
	pub    Publisher
	cfg    Config
	done   chan struct{}
}

func New(store Store, pub Publisher, cfg Config) *WAL {
	return &WAL{
		events: make(chan ledger.Event, 1024),
		store:  store,
		pub:    pub,
		cfg:    cfg,
		done:   make(chan struct{}),
	}
}

func (w *WAL) Events() chan<- ledger.Event { return w.events }

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

func (w *WAL) Done() <-chan struct{} { return w.done }
