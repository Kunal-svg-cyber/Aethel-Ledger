// Package tracing provides lightweight request correlation: a random
// ID generated per incoming gRPC call, threaded through context, and
// attached to every structured log line emitted while handling that
// call. This stops at the gRPC boundary in the current implementation —
// it does not yet propagate into WAL events or Redis Stream messages,
// which would need a trace_id column added to the event schema. Wiring
// a real distributed tracing backend (e.g. OpenTelemetry exporting to
// Grafana Tempo or Honeycomb) is a natural extension of this same
// context-propagation mechanism, documented as a next step rather than
// implemented here.
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type contextKey struct{}

var traceIDKey = contextKey{}

// NewID generates a new random trace ID, e.g. "a1b2c3d4e5f6a7b8".
func NewID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// WithID returns a new context carrying id as the active trace ID.
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey, id)
}

// FromContext returns the trace ID stored in ctx, or "" if none is set.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(traceIDKey).(string)
	return id
}
