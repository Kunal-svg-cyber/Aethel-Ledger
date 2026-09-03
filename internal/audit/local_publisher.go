package audit

import (
	"context"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// LocalPublisher applies events directly to an in-process Worker,
// bypassing Redis. It structurally satisfies wal.Publisher without this
// package importing wal, avoiding an import cycle.
type LocalPublisher struct {
	Worker *Worker
}

func (p *LocalPublisher) Publish(_ context.Context, ev ledger.Event) error {
	p.Worker.Apply(ev)
	return nil
}
