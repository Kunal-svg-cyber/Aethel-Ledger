# Aethel Ledger

[![CI](https://github.com/Kunal-svg-cyber/aethel-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/Kunal-svg-cyber/aethel-ledger/actions/workflows/ci.yml)
[![Go 1.22](https://img.shields.io/badge/Go-1.22-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A distributed, event-sourced financial ledger engine built from scratch in Go — designed to demonstrate correct, high-throughput concurrency control for money movement, not just another CRUD wallet API.

**Demo video:** _add your recorded walkthrough link here (see [DEMO_SCRIPT.md](DEMO_SCRIPT.md))_

**Headline numbers**, measured end-to-end through the real gRPC network path (not just the in-process engine — see [Load testing](#load-testing-and-observability-week-7)):

| | |
|---|---|
| Throughput | **46,070 req/sec** sustained, 50 concurrent clients |
| p50 / p99 latency | **1.10ms / 2.99ms** |
| Correctness under load | **691,051 / 691,051** transfers succeeded, zero failures, zero invariant drift |
| Concurrency proof | Race-detector-clean; deadlock-freedom proven under adversarial concurrent load ([see below](#the-concurrency-design-the-part-that-matters)) |

## Status

**Complete — weeks 1 through 8.** Concurrency engine, gRPC gateway with idempotency, async persistence layer (WAL + Postgres + Redis Streams + audit worker), load testing and observability, and this documentation pass. See [Roadmap](#roadmap) for what shipped each week, and the verification notes below for exactly what's been build-tested versus syntax-checked.

**Verification note, by package:**
- `internal/ledger` (week 1-2): fully built and tested — `go vet`, `go test -race`, benchmark, all clean.
- `internal/wal`, `internal/streaming`, `internal/audit` (week 5-6): fully built and tested with `go test -race`, including the real `lib/pq` Postgres driver.
- `internal/metrics` (week 7): the recorder and HTTP handler (`recorder.go`, `http_handler.go`) are pure standard library and fully tested with `go test -race`. The interceptor (`interceptor.go`) needs `google.golang.org/grpc` to compile — same caveat as the gRPC layer below.
- `internal/server`, `cmd/server`, `cmd/loadgen`, `internal/genproto` (week 3-4, week 7): syntax-checked and matched against the generated protobuf interfaces, but full compilation needs the public Go module proxy, which isn't reachable in the environment that assembled this. **This has been confirmed working end-to-end on the actual deployment machine** — `go build ./...` and `go test ./...` both pass there (race detector aside, which needs a 64-bit gcc not present on that Windows toolchain).

## Why this exists

Most student ledger/wallet projects reach for `SELECT balance FOR UPDATE` and call it a day — correct, but throughput is bounded by row-lock contention and it teaches you nothing about concurrent systems design. Aethel Ledger instead does the balance mutation in-memory with fine-grained, deterministically-ordered locks, and treats persistence as an async, event-sourced side effect. The interesting engineering problem — and the one this repo is built to showcase — is: *how do you let thousands of goroutines mutate a shared set of account balances concurrently, with zero data races and zero deadlocks, without serializing everything behind one lock?*

## Architecture

```
Client (grpcurl / load generator)
   -> gRPC Gateway (Go, HTTP/2, protobuf)                [week 3-4]
   -> Idempotency Layer (bloom filter + Upstash Redis)    [week 3-4]
   -> Ledger Engine (sharded mutex map, deterministic
      lock ordering by account ID)                        <- DONE
   -> WAL (local append-only log, batched async flush)     [week 5-6]
        -> Neon Postgres (durable store)                   [week 5-6]
        -> Redis Streams (XADD) -> Audit Worker             [week 5-6]
           (XREADGROUP, recomputes sum(debits) - sum(credits))
```

**Note on the streaming layer:** the original design called for Upstash-managed Kafka. Upstash Kafka was deprecated in Sept 2024 and fully discontinued in March 2025, so this project uses Redis Streams (`XADD`/`XREADGROUP` on Upstash Redis) as the append-only event bus instead — it gives the same consumer-group semantics needed for the audit worker, on infrastructure that's still free and still exists.

## The concurrency design (the part that matters)

Implemented in [`internal/ledger/engine.go`](internal/ledger/engine.go).

- **Sharded account map.** Accounts are partitioned across 32 shards by an FNV hash of their ID. Each shard has its own `RWMutex`, so account lookups on unrelated accounts never contend on the same map lock. Lookups of existing accounts only take a read lock; only first-touch creation takes the write lock (double-checked to avoid a create race).
- **Per-account mutex.** Balance mutation is guarded by a mutex on the individual `account`, not the shard — so two transfers landing on the same shard but touching different accounts still don't block each other.
- **Deterministic lock ordering for transfers.** This is the core correctness property. `Transfer(from, to, amount)` never locks in caller-supplied order. It always locks the two accounts in a fixed order derived from comparing their IDs, regardless of which one is the sender. This makes a circular wait — and therefore a deadlock — structurally impossible: deadlock requires two goroutines to acquire the same two locks in opposite orders, and this code never allows that to happen. See the comment above `Transfer` in `engine.go` for the full proof sketch.

### Proof, not just assertion

- `TestConcurrentTransfers_ConservesTotalBalance` — 200 goroutines x 200 random transfers across a 12-account pool, run under `-race`, asserting the global invariant (total balance conserved) holds afterward.
- `TestTransfer_NoDeadlockUnderReversedConcurrentPairs` — hammers `A->B` and `B->A` concurrently (the exact pattern that deadlocks a naive "lock `from` then `to`" implementation) with a hard test timeout, so a regression fails loudly instead of hanging.

```
go test -race -v ./...
```

### Measured throughput

```
go test -bench=. -benchmem -run=^$ ./internal/ledger/
```

```
BenchmarkTransfer_Parallel   11,026,401 iters   107.1 ns/op   0 B/op   0 allocs/op
```

~9.3M transfers/sec sustained on the benchmark machine, zero heap allocations per transfer. (Re-run this in your own Codespace and paste your actual numbers here — they'll differ by hardware, and that's fine, quote what you measured.)

## Running it

No local installs strictly required for a first read — the whole system also runs with zero external services configured (see the persistence section below). To actually build and run it:

```bash
go mod tidy
go build ./...
go test ./...
go run ./cmd/server
```

### Local development notes (Windows)

If `go test -race` fails locally with `cc1.exe: sorry, unimplemented: 64-bit mode not compiled in`, that's a 32-bit-only MinGW gcc on your `PATH`, not a bug in this project — the race detector needs a real 64-bit C toolchain for its cgo instrumentation. `go build` and `go test` (without `-race`) are unaffected. The [CI workflow](.github/workflows/ci.yml) runs `-race` on every push using GitHub Actions' Linux runners, which have a proper toolchain — treat that as the authoritative race-detector result if your local machine can't run it.

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Language | Go | `sync` primitives, goroutines, native concurrency tooling (`-race`, `pprof`) |
| Transport | gRPC + Protocol Buffers | Binary framing over HTTP/2, avoids JSON serialization overhead |
| Cache / idempotency | Upstash Redis (free tier) | Atomic Lua scripts for exactly-once retry handling |
| Event bus | Redis Streams (Upstash Redis) | Replaces the now-discontinued Upstash Kafka; same consumer-group semantics |
| Durable storage | Neon Postgres (free tier) | Serverless, async-batched WAL sink |
| Dev environment | GitHub Codespaces or local Go | Zero local installs if using Codespaces; verified working with a local Windows Go install too |
| CI/CD | GitHub Actions | Free; runs `go vet`, `go build`, and `go test -race` on every push — the race detector runs here even on a Windows dev machine that can't run it locally |

## gRPC API

Defined in [`proto/ledger/v1/ledger.proto`](proto/ledger/v1/ledger.proto), implemented in [`internal/server/server.go`](internal/server/server.go).

- `Deposit(account_id, amount)` — credits an account, creating it on first touch.
- `Transfer(from_account_id, to_account_id, amount, idempotency_key)` — moves funds via the engine's deadlock-free `Transfer`. Requires a client-generated `idempotency_key`; a retried request with the same key returns the original result (`replayed = true`) instead of moving funds again.
- `GetBalance(account_id)` — reads current balance (0 for an untouched account, not an error).

The idempotency layer ([`internal/idempotency/store.go`](internal/idempotency/store.go)) is behind a `Store` interface with an in-memory implementation for now; it's built to swap directly for a Redis-backed implementation without touching the server code.

`internal/server/server_test.go` includes `TestTransfer_DuplicateKeyReplaysInsteadOfDoubleSpending`, which submits the same idempotency key twice and asserts funds moved exactly once — the core correctness property of this layer.

## Running the server

```bash
go mod tidy   # resolves google.golang.org/grpc and google.golang.org/protobuf from the public proxy
go run ./cmd/server
# in another terminal, if you have grpcurl:
grpcurl -plaintext -d '{"account_id":"alice","amount":1000}' localhost:50051 ledger.v1.LedgerService/Deposit
```

## Persistence layer (week 5-6)

- **WAL** ([`internal/wal/wal.go`](internal/wal/wal.go)) — drains the engine's event channel, batches events, and flushes on either a batch-size or time threshold (whichever comes first), so the hot transfer path never blocks on I/O. Tested for both trigger conditions and for flush-on-shutdown.
- **PostgresStore** ([`internal/wal/postgres_store.go`](internal/wal/postgres_store.go)) — durable sink for Neon. Multi-row batched inserts with `ON CONFLICT (seq) DO NOTHING`, so a retried flush after a transient failure can't create duplicate rows.
- **Redis Streams event bus** ([`internal/streaming/redis_streams.go`](internal/streaming/redis_streams.go)) — publishes and reads back events via Upstash's REST API using only `net/http`, no third-party Redis client. Tested against a mock HTTP server covering the exact request/response shapes Upstash's API uses.
- **Audit worker** ([`internal/audit/worker.go`](internal/audit/worker.go)) — the "real-time mathematical auditing" piece. It never reads the engine's live balances; it *only* replays the append-only event log into its own independently-derived account map, then checks that the sum of all derived balances equals total deposits ever made — the definition of "transfers only move value, never create or destroy it." Tested for conservation holding, and for what a genuine drift would look like.
- **Local fallback, zero config needed:** with no `DATABASE_URL` or Upstash credentials set, the server still runs the full pipeline — WAL flushes to an in-memory store, and the audit worker is wired directly in-process via `audit.LocalPublisher` instead of through Redis. Set the environment variables below to switch to the real Neon/Upstash path.

```bash
export DATABASE_URL="postgres://user:pass@your-neon-host/dbname?sslmode=require"
export UPSTASH_REDIS_REST_URL="https://your-db.upstash.io"
export UPSTASH_REDIS_REST_TOKEN="your-upstash-token"
go run ./cmd/server
```

## Load testing and observability (week 7)

- **`/stats` endpoint** ([`internal/metrics/`](internal/metrics/)) — the server exposes live per-RPC metrics as JSON at `http://localhost:8080/stats`: success/failure counts and p50/p95/p99/max latency, per method. Wired in as a gRPC unary interceptor ([`interceptor.go`](internal/metrics/interceptor.go)), so no individual RPC handler needed to change.
- **`cmd/loadgen`** ([`cmd/loadgen/main.go`](cmd/loadgen/main.go)) — a standalone gRPC client that seeds a pool of accounts, then fires concurrent `Transfer` requests against a running server for a fixed duration, reporting real network-measured throughput and latency percentiles. This is distinct from (and more honest than) the in-process engine benchmark from week 1-2 — it includes actual gRPC serialization and network round-trip cost, not just the mutex/map operations.

```bash
# terminal 1
go run ./cmd/server

# terminal 2
go run ./cmd/loadgen -concurrency 50 -duration 15s
# flags: -addr (default localhost:50051), -accounts (default 20)

# terminal 3, while the load test runs, to watch live stats:
curl http://localhost:8080/stats
```

Run this yourself and paste your actual numbers below — they'll depend on your machine, and that's exactly the point: quote what you measured, not what's written here.

```
=== Aethel Ledger Load Test Results ===
Duration:        15s
Concurrency:     50 workers
Total requests:  691,051
Successful:      691,051
Failed:          0
Throughput:      46,070.1 req/sec
Latency p50:     1.0996ms
Latency p95:     2.0156ms
Latency p99:     2.9854ms
Latency max:     31.5557ms
```

Measured against 20 accounts under deliberately heavy lock contention (50 workers, 20 accounts — every transfer is likely to collide with another in flight). Zero failures across 691K requests, independently confirmed by both the load generator's client-side count and the server's own `/stats` interceptor.

## Roadmap

- [x] Week 1-2: sharded-lock concurrency engine, race-detector test suite, deadlock-freedom proof, benchmarks
- [x] Week 3-4: protobuf schema, gRPC gateway, in-memory idempotency layer (Redis swap-in planned)
- [x] Week 5-6: async batching WAL, Neon Postgres store, Redis Streams event bus, audit worker with independent invariant checking
- [x] Week 7: metrics recorder + `/stats` endpoint, standalone load generator with measured throughput/latency
- [x] Week 8: CI pipeline (GitHub Actions), MIT license, demo script, README polish

## What's next (beyond week 8)

Honest list of what a production version would still need, kept here rather than glossed over:

- Swap the in-memory idempotency store for the originally-planned Redis + Lua atomic implementation (interface is already in place — see `internal/idempotency/store.go`).
- WAL retry-with-backoff and local spill-to-disk on a Postgres outage, instead of the current log-and-drop.
- TLS for the gRPC endpoint (currently plaintext, fine for a local/portfolio demo, not for production).
- Structured logging (e.g. `slog` with JSON output) instead of the current plain `log` calls.
- A real deployment (Cloud Run is the natural fit — see conversation history for why Vercel doesn't work for a stateful gRPC server).
