package audit

import (
	"context"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// LocalPublisher applies events directly to an in-process Worker,
// bypassing Redis entirely. It structurally satisfies wal.Publisher
// (same Publish method signature) without this package importing wal,
// avoiding an import cycle.
//
// This exists so the audit worker demonstrably works with zero external
// services configured — useful for local development, grading, or a
// recorded demo where you don't want to depend on your own Upstash
// account being reachable. When UPSTASH_REDIS_REST_URL and
// UPSTASH_REDIS_REST_TOKEN are set, main.go uses a RedisConsumer instead,
// matching the architecture diagram's separate-service audit worker.
type LocalPublisher struct {
	Worker *Worker
}

func (p *LocalPublisher) Publish(_ context.Context, ev ledger.Event) error {
	p.Worker.Apply(ev)
	return nil
}
