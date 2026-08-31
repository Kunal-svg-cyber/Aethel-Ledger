package audit

import (
	"context"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

type LocalPublisher struct {
	Worker *Worker
}

func (p *LocalPublisher) Publish(_ context.Context, ev ledger.Event) error {
	p.Worker.Apply(ev)
	return nil
}
