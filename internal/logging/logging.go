// Package logging configures structured logging via the standard
// library's log/slog, replacing the plain log.Printf calls previously
// used throughout this project. Text format is the default — closer to
// the original plain-log output, and easiest to read during local
// development — with JSON available via LOG_FORMAT=json for log
// aggregation pipelines that expect machine-parseable output.
package logging

import (
	"io"
	"log/slog"
	"os"
)

// NewHandler builds a slog.Handler for the given format ("json" for
// structured JSON output; anything else, including "", for
// human-readable text), writing to w. Split out from Init so tests can
// verify behavior deterministically against a buffer instead of global
// state.
func NewHandler(format string, w io.Writer) slog.Handler {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if format == "json" {
		return slog.NewJSONHandler(w, opts)
	}
	return slog.NewTextHandler(w, opts)
}

// Init configures the global slog default logger from the LOG_FORMAT
// environment variable. Call once at process startup, before any
// logging happens.
func Init() {
	slog.SetDefault(slog.New(NewHandler(os.Getenv("LOG_FORMAT"), os.Stdout)))
}
