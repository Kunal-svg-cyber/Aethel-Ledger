# Aethel Ledger

[![CI](https://github.com/Kunal-svg-cyber/aethel-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/Kunal-svg-cyber/aethel-ledger/actions/workflows/ci.yml)
[![Go 1.22](https://img.shields.io/badge/Go-1.22-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A distributed, event-sourced financial ledger engine written in Go, built to demonstrate correct, high-throughput concurrency control for money movement at the level a real payments backend requires.

**Demo video:** _link here (see [DEMO_SCRIPT.md](DEMO_SCRIPT.md))_

**Measured end-to-end through the live gRPC network path** (see [Load testing](#load-testing-and-observability)):

| | |
|---|---|
| Throughput | **46,070 req/sec** sustained, 50 concurrent clients |
| p50 / p99 latency | **1.10ms / 2.99ms** |
| Correctness under load | **691,051 / 691,051** transfers succeeded — zero failures, zero invariant drift |
| Concurrency guarantee | Race-detector-clean; deadlock freedom proven under adversarial concurrent load ([details](#the-concurrency-design)) |

## Why this exists

Most ledger and wallet implementations reach for `SELECT balance FOR UPDATE` and stop there — correct, but throughput is bounded by row-lock contention, and it sidesteps the actual hard problem in concurrent financial systems. Aethel Ledger does balance mutation in memory with fine-grained, deterministically-ordered locks, and treats persistence as an asynchronous, event-sourced side effect. The core engineering problem this project solves: how do you let thousands of goroutines mutate a shared set of account balances concurrently, with zero data races and zero deadlocks, without serializing everything behind a single lock?

## Architecture

```
Client (grpcurl / load generator)
   -> gRPC Gateway (Go, HTTP/2, protobuf)
   -> Idempotency Layer (retry-safe, Redis-backed in production)
   -> Ledger Engine (sharded mutex map, deterministic
      lock ordering by account ID)
   -> WAL (local append-only log, batched async flush)
        -> Neon Postgres (durable store)
        -> Redis Streams (XADD) -> Audit Worker
           (XREADGROUP, recomputes sum(debits) - sum(credits))
```

**Streaming layer note:** the event bus uses Redis Streams (`XADD`/`XREADGROUP` via the Upstash Redis REST API) rather than Kafka. Upstash's managed Kafka offering was deprecated in September 2024 and discontinued in March 2025; Redis Streams provides the same append-only-log-with-consumer-groups semantics the audit worker needs, on infrastructure that remains supported.

## The concurrency design

Implemented in [`internal/ledger/engine.go`](internal/ledger/engine.go).

- **Sharded account map.** Accounts are partitioned across 32 shards by an FNV hash of their ID. Each shard has its own `RWMutex`, so account lookups on unrelated accounts never contend on the same map lock. Lookups of existing accounts only take a read lock; only first-touch creation takes the write lock (double-checked to avoid a create race).
- **Per-account mutex.** Balance mutation is guarded by a mutex on the individual `account`, not the shard — so two transfers landing on the same shard but touching different accounts still don't block each other.
- **Deterministic lock ordering for transfers.** `Transfer(from, to, amount)` never locks in caller-supplied order. It always locks the two accounts in a fixed order derived from comparing their IDs, regardless of which one is the sender. This makes a circular wait — and therefore a deadlock — structurally impossible: deadlock requires two goroutines to acquire the same two locks in opposite orders, and this code never allows that to happen. See the comment above `Transfer` in `engine.go` for the full proof.

### Proof, not assertion

- `TestConcurrentTransfers_ConservesTotalBalance` — 200 goroutines x 200 random transfers across a 12-account pool, run under `-race`, asserting the global invariant (total balance conserved) holds afterward.
- `TestTransfer_NoDeadlockUnderReversedConcurrentPairs` — hammers `A->B` and `B->A` concurrently (the exact pattern that deadlocks a naive "lock `from` then `to`" implementation) with a hard test timeout, so a regression fails loudly instead of hanging.

```
go test -race -v ./...
```

### In-process engine throughput

```
go test -bench=. -benchmem -run=^$ ./internal/ledger/
```

```
BenchmarkTransfer_Parallel   11,026,401 iters   107.1 ns/op   0 B/op   0 allocs/op
```

~9.3M transfers/sec, zero heap allocations per transfer, isolating the concurrency engine from network and serialization cost. For the full-stack, network-measured number, see [Load testing](#load-testing-and-observability) below.

## Running it

```bash
go mod tidy
go build ./...
go test ./...
go run ./cmd/server
```

### A note on the race detector on Windows

If `go test -race` fails locally with `cc1.exe: sorry, unimplemented: 64-bit mode not compiled in`, that indicates a 32-bit-only MinGW gcc on `PATH` — the race detector requires a 64-bit C toolchain for its cgo instrumentation. `go build` and `go test` (without `-race`) are unaffected. The [CI workflow](.github/workflows/ci.yml) runs `-race` on every push via GitHub Actions' Linux runners, which have a correct toolchain, and is the authoritative race-detector result for this project.

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Language | Go | `sync` primitives, goroutines, native concurrency tooling (`-race`, `pprof`) |
| Transport | gRPC + Protocol Buffers | Binary framing over HTTP/2, avoids JSON serialization overhead |
| Cache / idempotency | Redis (Upstash) | Atomic Lua scripts for exactly-once retry handling |
| Event bus | Redis Streams (Upstash) | Append-only log with consumer-group semantics |
| Durable storage | Postgres (Neon, serverless) | Async-batched WAL sink |
| CI/CD | GitHub Actions | `go vet`, `go build`, and `go test -race` on every push |

## gRPC API

Defined in [`proto/ledger/v1/ledger.proto`](proto/ledger/v1/ledger.proto), implemented in [`internal/server/server.go`](internal/server/server.go).

- `Deposit(account_id, amount)` — credits an account, creating it on first touch.
- `Transfer(from_account_id, to_account_id, amount, idempotency_key)` — moves funds via the engine's deadlock-free `Transfer`. Requires a client-generated `idempotency_key`; a retried request with the same key returns the original result (`replayed = true`) instead of moving funds again.
- `GetBalance(account_id)` — reads current balance (0 for an untouched account, not an error).

The idempotency layer ([`internal/idempotency/store.go`](internal/idempotency/store.go)) sits behind a `Store` interface with an in-memory implementation as the default; it's designed to swap directly for a Redis-backed implementation without any change to the server code.

`internal/server/server_test.go` includes `TestTransfer_DuplicateKeyReplaysInsteadOfDoubleSpending`, which submits the same idempotency key twice and asserts funds moved exactly once.

```bash
go run ./cmd/server
# in another terminal, if you have grpcurl:
grpcurl -plaintext -d '{"account_id":"alice","amount":1000}' localhost:50051 ledger.v1.LedgerService/Deposit
```

## Persistence layer

- **WAL** ([`internal/wal/wal.go`](internal/wal/wal.go)) — drains the engine's event channel, batches events, and flushes on either a batch-size or time threshold, whichever comes first, so the hot transfer path never blocks on I/O.
- **PostgresStore** ([`internal/wal/postgres_store.go`](internal/wal/postgres_store.go)) — durable sink for Postgres. Multi-row batched inserts with `ON CONFLICT (seq) DO NOTHING`, so a retried flush after a transient failure can't create duplicate rows.
- **Redis Streams event bus** ([`internal/streaming/redis_streams.go`](internal/streaming/redis_streams.go)) — publishes and reads back events via the Upstash Redis REST API using only `net/http`, no third-party Redis client dependency.
- **Audit worker** ([`internal/audit/worker.go`](internal/audit/worker.go)) — real-time mathematical auditing. It never reads the engine's live balances; it independently replays the append-only event log into its own derived account map, then verifies that the sum of all derived balances equals total deposits ever made — the formal statement of "transfers only move value, never create or destroy it."
- **Zero-config local mode:** with no `DATABASE_URL` or Redis credentials set, the server still runs the complete pipeline — the WAL flushes to an in-memory store, and the audit worker is wired directly in-process via `audit.LocalPublisher` instead of through Redis. Set the environment variables below to switch to the durable path.

```bash
export DATABASE_URL="postgres://user:pass@host/dbname?sslmode=require"
export UPSTASH_REDIS_REST_URL="https://your-db.upstash.io"
export UPSTASH_REDIS_REST_TOKEN="your-upstash-token"
go run ./cmd/server
```

## Load testing and observability

- **`/stats` endpoint** ([`internal/metrics/`](internal/metrics/)) — live per-RPC metrics as JSON at `http://localhost:8080/stats`: success/failure counts and p50/p95/p99/max latency, per method. Wired in as a gRPC unary interceptor ([`interceptor.go`](internal/metrics/interceptor.go)), so no individual RPC handler needs to change.
- **`cmd/loadgen`** ([`cmd/loadgen/main.go`](cmd/loadgen/main.go)) — a standalone gRPC client that seeds a pool of accounts, then fires concurrent `Transfer` requests against a running server for a fixed duration, reporting network-measured throughput and latency percentiles — the full-stack figure, including gRPC serialization and round-trip cost, distinct from the in-process microbenchmark above.

```bash
# terminal 1
go run ./cmd/server

# terminal 2
go run ./cmd/loadgen -concurrency 50 -duration 15s
# flags: -addr (default localhost:50051), -accounts (default 20)

# terminal 3, while the load test runs:
curl http://localhost:8080/stats
```

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

Measured against a 20-account pool under deliberately heavy lock contention (50 workers, 20 accounts — every transfer is likely to collide with another in flight). Zero failures across 691K requests, independently confirmed by both the load generator's client-side count and the server's own `/stats` interceptor.

## Roadmap

Planned hardening beyond the current implementation:

- Swap the in-memory idempotency store for the Redis + Lua atomic implementation (interface already in place — see `internal/idempotency/store.go`).
- WAL retry-with-backoff and local spill-to-disk on a Postgres outage, replacing the current log-and-drop behavior.
- TLS for the gRPC endpoint (currently plaintext, suitable for local development, not production).
- Structured logging (`slog` with JSON output) in place of the current plain `log` calls.
- Container deployment via Cloud Run or an equivalent platform supporting long-lived gRPC servers (a serverless request/response platform like Vercel is not a fit for a stateful, connection-holding server).

## License

MIT — see [LICENSE](LICENSE).
