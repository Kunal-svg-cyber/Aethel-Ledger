# Aethel Ledger

[![CI](https://github.com/Kunal-svg-cyber/aethel-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/Kunal-svg-cyber/aethel-ledger/actions/workflows/ci.yml)
[![Go 1.22](https://img.shields.io/badge/Go-1.22-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A distributed, event-sourced financial ledger engine written in Go — built to demonstrate correct, high-throughput concurrency control for money movement at the level a real payments backend requires, and load-tested against a live remote database until it broke, so the fixes are real.

**Demo video:** _link here_

## Highlights

- **Deadlock-freedom is proven, not assumed.** Deterministic lock ordering makes a circular wait structurally impossible — verified by an adversarial test that would hang under a hard timeout if the property ever broke. [Details ↓](#the-concurrency-design)
- **Four real bugs found and fixed, not staged for a demo.** Three under real concurrent load — a queueing pileup that cut throughput to 5 req/sec, a subtle correctness race inside the fix for that bug, and a connection-pool exhaustion that caused silent request failures — plus a real security vulnerability (a publicly-exposed database table) caught by an automated scanner. Each one found independently, each with measured before/after evidence. [Details ↓](#real-bugs-found-and-fixed)
- **Durability is verified, not claimed.** Balances and idempotency keys survive a full server restart — confirmed by integration tests that simulate exactly that, run against a live database. [Details ↓](#durability-and-persistence)
- **Zero required external dependencies.** The entire system — gRPC API, concurrency engine, idempotency protection, WAL, audit worker — runs standalone with no database or cache. Postgres and Redis are fully implemented, tested, and optional. [Details ↓](#zero-dependency-mode)
- **CI-gated on every push.** GitHub Actions runs `go vet`, `go build`, and `go test -race` on a Linux runner for every commit. [![CI](https://github.com/Kunal-svg-cyber/aethel-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/Kunal-svg-cyber/aethel-ledger/actions/workflows/ci.yml)
- **Honest about its own edges.** Documented, unglossed-over limitations at the bottom of this README — because knowing where your own work ends is part of the engineering. [Details ↓](#known-limitations)
- **Structured, correlated logging.** Every request carries a trace ID through `log/slog`, text or JSON on demand — and idempotency keys now expire on a TTL instead of growing unbounded. [Details ↓](#structured-logging-and-request-tracing)

## Table of contents

- [Why this exists](#why-this-exists)
- [Architecture](#architecture)
- [The concurrency design](#the-concurrency-design)
- [Real bugs found and fixed](#real-bugs-found-and-fixed)
- [Durability and persistence](#durability-and-persistence)
- [Load testing and observability](#load-testing-and-observability)
- [Tech stack](#tech-stack)
- [gRPC API](#grpc-api)
- [Running it](#running-it)
- [Authentication and TLS](#authentication-and-tls)
- [Scaling beyond a single node](#scaling-beyond-a-single-node)
- [Known limitations](#known-limitations)

## Why this exists

Most ledger and wallet implementations reach for `SELECT balance FOR UPDATE` and stop there — correct, but throughput is bounded by row-lock contention, and it sidesteps the actual hard problem in concurrent financial systems. Aethel Ledger does balance mutation in memory with fine-grained, deterministically-ordered locks, and treats persistence as an asynchronous, event-sourced side effect.

**The core engineering question this project answers:** how do you let thousands of goroutines mutate a shared set of account balances concurrently — with zero data races and zero deadlocks — without serializing everything behind a single lock?

## Architecture

```
Client (grpcurl / load generator)
   -> gRPC Gateway (Go, HTTP/2, protobuf)
   -> Idempotency Layer (retry-safe, Postgres-backed in production)
   -> Ledger Engine (sharded mutex map, deterministic
      lock ordering by account ID)
   -> WAL (local append-only log, batched async flush)
        -> Postgres (durable event log; tested against Supabase)
        -> Redis Streams (XADD) -> Audit Worker
           (XREADGROUP, recomputes sum(debits) - sum(credits))
```

**Streaming layer note:** the event bus uses Redis Streams (`XADD`/`XREADGROUP` via the Upstash Redis REST API) rather than Kafka. Upstash's managed Kafka offering was deprecated in September 2024 and discontinued in March 2025; Redis Streams provides the same append-only-log-with-consumer-groups semantics the audit worker needs, on infrastructure that remains supported.

## The concurrency design

Implemented in [`internal/ledger/engine.go`](internal/ledger/engine.go).

| Mechanism | What it does |
|---|---|
| **Sharded account map** | Accounts are partitioned across 32 shards by an FNV hash of their ID. Each shard has its own `RWMutex`, so account lookups on unrelated accounts never contend on the same map lock. Lookups take a read lock; only first-touch creation takes the write lock. |
| **Per-account mutex** | Balance mutation is guarded by a mutex on the individual account, not the shard — two transfers on the same shard but different accounts still don't block each other. |
| **Deterministic lock ordering** | `Transfer(from, to, amount)` never locks in caller-supplied order — it always locks the two accounts in a fixed order derived from comparing their IDs, regardless of who's sending. This makes a circular wait, and therefore a deadlock, structurally impossible. |

### Proof, not assertion

- `TestConcurrentTransfers_ConservesTotalBalance` — 200 goroutines × 200 random transfers across a 12-account pool, run under `-race`, asserting the global invariant (total balance conserved) holds afterward.
- `TestTransfer_NoDeadlockUnderReversedConcurrentPairs` — hammers `A→B` and `B→A` concurrently (the exact pattern that deadlocks a naive "lock `from` then `to`" implementation) under a hard test timeout, so a regression fails loudly instead of hanging.

```bash
go test -race -v ./...
```

**In-process engine throughput:**

```bash
go test -bench=. -benchmem -run=^$ ./internal/ledger/
```
```
BenchmarkTransfer_Parallel   11,026,401 iters   107.1 ns/op   0 B/op   0 allocs/op
```

~9.3M transfers/sec, zero heap allocations per transfer, isolating the concurrency engine from network and serialization cost. See [Load testing](#load-testing-and-observability) for the full-stack, network-measured figure.

## Real bugs found and fixed

This is the part most projects never get to, because they never load-test — or get security-scanned — hard enough to hit it. The first three were found by running `cmd/loadgen` against a live remote Postgres instance under 50-way concurrency; the fourth was caught by Supabase's own automated security scanner. None were staged or anticipated in advance.

| # | Bug | Before | After | Root cause & fix |
|---|---|---|---|---|
| 1 | Durable-ack queueing pileup | **5 req/sec**, 18.5s p50 latency | **60 req/sec**, 0.8s p50 latency | The first durable-ack implementation made every concurrent caller wait for its *own* network round trip to the database, serializing all 50 workers into one queue. Fixed with a group-commit pattern: concurrent callers share one flush instead of paying for their own. |
| 2 | Notification-ordering race | A caller could be told "durable" before its own data was actually flushed | Waiters are snapshotted *before* the flush begins, closing the race | Found while building the fix for bug #1 — a test failed with "events persisted = 1, want 20" on the very first run, catching the bug before it ever shipped. |
| 3 | Connection pool exhaustion | **103 of 376 requests failed** under concurrent load | **0 failures**, safe pool size found by experiment | Adding Postgres-backed idempotency opened a second, unbounded connection pool alongside the WAL's. Fixed by sharing one bounded pool; the safe size (15) was found by deliberately bracketing it — 10 was safe but slow, 30 was faster but reintroduced failures. |
| 4 | Public table exposure (Row-Level Security disabled) | Both Postgres tables were readable, writable, and deletable by **anyone with the project URL**, via Supabase's auto-generated public REST API | **Zero public access**, application unaffected | Supabase exposes every `public`-schema table through a REST API separate from the app's direct Postgres connection. Neither table had Row-Level Security enabled, so the public API had full read/write/delete access to both — completely bypassing the concurrency engine, idempotency checks, and audit worker. Fixed with `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` on both tables; the app's own connection is the table owner, so owner privileges bypass RLS by default and nothing in the application had to change. |

Full technical detail on each, including the exact test names that catch a regression, is in [Durability and persistence](#durability-and-persistence) below.

## Durability and persistence

- **WAL** ([`internal/wal/wal.go`](internal/wal/wal.go)) — drains the engine's event channel, batches events, and flushes on either a batch-size or time threshold, whichever comes first. A synchronous `FlushNow` gives callers a durable-ack guarantee using the group-commit pattern from bug #1 above.
- **PostgresStore** ([`internal/wal/postgres_store.go`](internal/wal/postgres_store.go)) — durable event sink. Multi-row batched inserts with `ON CONFLICT (seq) DO NOTHING`, so a retried flush after a transient failure can't create duplicate rows. Executes as a single statement with no explicit transaction wrapper — a lone SQL statement is already atomic in Postgres, so the wrapper was pure round-trip overhead (removing it roughly tripled throughput on the same connection).
- **Redis Streams event bus** ([`internal/streaming/redis_streams.go`](internal/streaming/redis_streams.go)) — publishes and reads back events via the Upstash Redis REST API using only `net/http`, no third-party Redis client dependency.
- **Audit worker** ([`internal/audit/worker.go`](internal/audit/worker.go)) — real-time mathematical auditing. Never reads the engine's live balances; independently replays the append-only event log into its own derived account map, then verifies the sum of all derived balances equals total deposits ever made.

```bash
export DATABASE_URL="postgres://user:pass@host/dbname?sslmode=require"
export UPSTASH_REDIS_REST_URL="https://your-db.upstash.io"
export UPSTASH_REDIS_REST_TOKEN="your-upstash-token"
go run ./cmd/server
```

### Startup recovery

On startup, `main.go` replays the full persisted event history into both the engine (`Engine.Restore`) and the audit worker before accepting any traffic. Without this, every restart would silently reset all balances to zero despite the event history sitting durably in Postgres, and the sequence counter would restart at 0 — colliding with already-persisted rows and silently dropping every post-restart event. Covered by `TestRestore_RebuildsBalancesFromEventLog` and `TestRestore_ContinuesSequenceWithoutCollision`.

### Graceful shutdown

`main.go` catches `SIGINT`/`SIGTERM`, calls `grpcServer.GracefulStop()`, then cancels the WAL's context and waits for its final flush before exiting. Without this, a normal `Ctrl+C` kills the process immediately — Go does not run deferred functions on an unhandled signal — so buffered-but-unflushed events would be lost on every shutdown.

### Durable-ack tradeoff (bug #1, in detail)

By default, `Deposit` and `Transfer` block on `WAL.FlushNow` before acknowledging success, closing the window where a client could be told "success" for a transaction that hadn't reached durable storage yet. `LedgerServer` accepts a `nil` flusher to opt back into fire-and-forget acking if raw throughput matters more than the guarantee for a given deployment.

| Iteration | Throughput | p50 latency |
|---|---|---|
| Naive (one round trip per caller) | 5 req/sec | 18.5s |
| + Group-commit `FlushNow` | 30 req/sec | 1.74s |
| + Dropped unnecessary transaction wrapper | 60.1 req/sec | 819ms |

Covered by `TestWAL_FlushNowCoalescesConcurrentCallsIntoOneFlush`, `TestWAL_FlushNowIncludesCallersOwnEvent`, and three durable-ack tests in `internal/server/server_test.go`.

### Idempotency durability (a real double-spend, found and fixed)

The idempotency `Store` interface has two implementations: `InMemoryStore` (default) and `PostgresStore`. The in-memory version forgets every key it has seen the moment the process restarts — if a client retries a `Transfer` after a restart, it executes the transfer again. This surfaced directly during testing: a resubmitted transfer after a restart moved funds a second time. `PostgresStore` closes this by persisting each key's state in the same database as the ledger's event log, using `INSERT ... ON CONFLICT DO NOTHING RETURNING` to atomically detect a new key in one round trip. Verified by `TestPostgresStore_SurvivesAcrossInstances`, which commits a key with one store instance and confirms a second, independent instance recognizes it.

### Connection pool exhaustion (bug #3, in detail)

Adding Postgres-backed idempotency initially made things *worse*: a 50-concurrency run that previously had zero failures came back with 103 failures out of 285 requests. Cause: the idempotency store opened its own separate, unbounded connection pool alongside the WAL's — two independent pools racing for the same free-tier database's connection limit. Fixed by sharing one bounded `*sql.DB` between both stores.

| Pool size (`MaxOpenConns`) | Throughput | Failures |
|---|---|---|
| 10 | 15.5 req/sec | 0 |
| 30 | 25.1 req/sec | 21 / 376 |
| **15 (current)** | 10.1 req/sec | **0** |

These three runs happened on different days over the public internet, so the non-monotonic result (15 slower than both neighbors) is most likely network variance, not a property of the pool size — a caveat worth stating rather than overselling a clean trend that isn't fully there. What generalizes regardless: unbounded connections against a real database can exhaust its limit and cause outright failures, and bounding the pool trades some throughput for eliminating that failure mode.

### Balance overflow protection

`Deposit` and `Transfer` reject an operation that would overflow an `int64` balance, checked *before* either balance is mutated. Purely additive — every ordinary amount used anywhere else in this codebase is unaffected, asserted directly by `TestDeposit_OrdinaryAmountsAreUnaffectedByOverflowCheck`.

### Rate limiting

[`internal/ratelimit/`](internal/ratelimit/) implements a token-bucket limiter using only the standard library — no dependency like `golang.org/x/time/rate`. Wired in as a second gRPC interceptor. Default: 5,000 req/sec sustained, burst of 2,000 — well above every load test measured against this project, so it guards against runaway traffic without being reachable by legitimate load. Tested for burst capacity, refill-over-time behavior, and correctness under 100 concurrent callers racing for a burst of 10.

### Public table exposure via disabled Row-Level Security (bug #4, in detail)

Supabase — and Postgres's `PostgREST` layer generally — auto-generates a public REST API for every table in the `public` schema, entirely separate from whatever connection an application uses to talk to Postgres directly. This project's Go server always connected directly over the Postgres wire protocol, so the application itself was never the exposure. But neither `ledger_events` nor `idempotency_keys` had Row-Level Security enabled, which meant the *auto-generated public API* — reachable by anyone with the project URL, no credentials required — had full read, write, and delete access to both tables. That's a real, external-attacker-reachable vulnerability: someone could have read every event and idempotency record, or deleted them outright, without ever touching the gRPC server, the concurrency engine, or the idempotency checks this whole project is built around.

Caught by Supabase's own automated security scanner (`rls_disabled_in_public`), not by anything in this project's own test suite — worth being honest that this class of bug lives outside what unit and integration tests typically cover. Fixed with:

```sql
ALTER TABLE public.ledger_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.idempotency_keys ENABLE ROW LEVEL SECURITY;
```

This didn't require any application-side change: in Postgres, a table's owner bypasses Row-Level Security by default unless `FORCE ROW LEVEL SECURITY` is set, and the Go server's connection is the owner (it's the role that created both tables). Enabling RLS with zero policies defined denies all access to every other role — including the `anon`/`authenticated` roles the public API uses — while leaving the owning application's access completely untouched. Verified after the fact by confirming a full deposit/transfer/restart-recovery cycle still worked identically (unaffected reads and writes as the table owner) while Supabase's scanner separately confirmed the public exposure was closed.

## Load testing and observability

- **`/stats` endpoint** ([`internal/metrics/`](internal/metrics/)) — live per-RPC metrics as JSON at `http://localhost:8080/stats`: success/failure counts and p50/p95/p99/max latency, per method.
- **`cmd/loadgen`** ([`cmd/loadgen/main.go`](cmd/loadgen/main.go)) — a standalone gRPC client that seeds accounts, fires concurrent `Transfer` requests for a fixed duration, and reports network-measured throughput and latency percentiles.

```bash
# terminal 1
go run ./cmd/server

# terminal 2
go run ./cmd/loadgen -concurrency 50 -duration 15s

# terminal 3, while the load test runs:
curl http://localhost:8080/stats
```

**Fire-and-forget baseline** (pre-durable-ack, in-memory store, 20-account pool under deliberately heavy lock contention):

```
Total requests:  691,051
Successful:      691,051
Failed:          0
Throughput:      46,070.1 req/sec
Latency p50:     1.0996ms
Latency p99:     2.9854ms
```

Zero failures across 691K requests, independently confirmed by both the load generator's client-side count and the server's own `/stats` interceptor. See [Durable-ack tradeoff](#durable-ack-tradeoff-bug-1-in-detail) for what changes with full durability enabled — the two numbers measure genuinely different things, not a regression.

### Structured logging and request tracing

All logging goes through [`log/slog`](internal/logging/logging.go) (Go's standard-library structured logger) rather than the plain `log` package — text output by default for readable local development, switchable to JSON with `LOG_FORMAT=json` for log-aggregation pipelines that expect machine-parseable fields.

Every incoming gRPC call gets a random trace ID attached to its context by [`internal/tracing`](internal/tracing/tracing.go), via a dedicated interceptor that runs first in the chain. `Deposit` and `Transfer` log that trace ID alongside the relevant account IDs at the start of each call, so every log line from a single request — including a replayed idempotency hit — can be correlated back to one `trace_id` value, even under concurrent load from many simultaneous callers.

**Honest about where this stops:** the trace ID is generated and logged at the gRPC boundary only — it is not currently threaded through into WAL events or Redis Stream messages, which would need a `trace_id` column added to the event schema. Wiring a real distributed-tracing backend (OpenTelemetry exporting to Grafana Tempo or Honeycomb, both of which have a free tier reachable without any local install) is a natural extension of this same context-propagation mechanism, not yet built here.

### Idempotency key expiry

Both idempotency `Store` implementations now prune records older than a configurable TTL (24 hours by default) via a background goroutine that runs hourly, using the `ExpirableStore` interface — `DeleteExpired(ctx, cutoff)`. Postgres uses the `created_at` column already present in the schema (no migration needed); the in-memory store tracks a creation timestamp per entry. Without this, both stores grew unbounded for the life of the data; an expired key is treated as brand-new if reused, which is the correct behavior — a key old enough to be pruned is, by definition, old enough that no legitimate client retry is still depending on it.

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Language | Go | `sync` primitives, goroutines, native concurrency tooling (`-race`, `pprof`) |
| Transport | gRPC + Protocol Buffers | Binary framing over HTTP/2, avoids JSON serialization overhead |
| Idempotency & event bus | Redis Streams / Postgres (Upstash / any Postgres) | Append-only semantics with consumer groups; REST-only client, zero extra dependency |
| Durable storage | PostgreSQL (tested against Supabase) | Async-batched WAL sink; connection pool tuned by a real bracketing experiment |
| Rate limiting | Custom stdlib token bucket | No external dependency; tested for burst, refill, and concurrency correctness |
| Logging & tracing | `log/slog` + custom trace-ID context propagation | Structured, correlatable logs with zero external dependency; text or JSON on demand |
| CI/CD | GitHub Actions | `go vet`, `go build`, `go test -race` on every push, on a Linux runner with a real 64-bit toolchain |

## gRPC API

Defined in [`proto/ledger/v1/ledger.proto`](proto/ledger/v1/ledger.proto), implemented in [`internal/server/server.go`](internal/server/server.go).

- `Deposit(account_id, amount)` — credits an account, creating it on first touch.
- `Transfer(from_account_id, to_account_id, amount, idempotency_key)` — moves funds via the engine's deadlock-free `Transfer`. A retried request with the same key returns the original result (`replayed = true`) instead of moving funds again.
- `GetBalance(account_id)` — reads current balance (0 for an untouched account, not an error).

```bash
go run ./cmd/server
# in another terminal, if you have grpcurl:
grpcurl -plaintext -d '{"account_id":"alice","amount":1000}' localhost:50051 ledger.v1.LedgerService/Deposit
```

## Running it

### Zero-dependency mode

```bash
go run ./cmd/server
```

With no `DATABASE_URL` or Redis credentials set, this single command runs the complete system — gRPC gateway, concurrency engine, idempotency protection, async WAL, and audit worker — using in-memory stores. Every gRPC call and the deadlock-freedom guarantee work identically to the Postgres-backed path; the only difference is that balances and idempotency records don't survive a process restart, since there's nothing durable to recover from.

This project was developed and load-tested against a real Supabase Postgres instance (see [Real bugs found and fixed](#real-bugs-found-and-fixed) for what that surfaced), but the system was built so that a paused, deleted, or never-configured database doesn't affect the core engineering it demonstrates. Free-tier managed Postgres providers commonly auto-pause after inactivity — that's an infrastructure detail of the demo environment, not a property of the codebase.

### Full setup

```bash
go mod tidy
go build ./...
go test ./...
go run ./cmd/server
```

**Race detector on Windows:** if `go test -race` fails with `cc1.exe: sorry, unimplemented: 64-bit mode not compiled in`, that's a 32-bit-only MinGW gcc on `PATH`, not a code issue — `go build` and `go test` without `-race` are unaffected. The [CI workflow](.github/workflows/ci.yml) runs `-race` on every push via a Linux runner with a correct toolchain, and is the authoritative result.

See [TESTING.md](TESTING.md) for the complete command reference, including the full restart-recovery and load-test walkthrough against a real database.

## Authentication and TLS

Both are opt-in via environment variables, so the zero-dependency mode keeps working unchanged.

| Variable | Effect |
| --- | --- |
| `API_KEY` | When set, every RPC must carry a matching `x-api-key` metadata header; otherwise it is rejected with `Unauthenticated`. Compared in constant time (`crypto/subtle`). |
| `TLS_CERT_FILE`, `TLS_KEY_FILE` | When set, the gRPC listener serves TLS (1.2 minimum). Setting only one is a startup error. |

For local testing, `go run ./cmd/gencert` writes a self-signed `cert.pem` / `key.pem` (ECDSA P-256, valid for `localhost`). Do not commit them. See [TESTING.md](TESTING.md#16-authentication-and-tls) for the full walkthrough.

The auth interceptor runs last in the chain, after tracing, metrics and rate limiting, so rejected requests are still traced and counted.

## Scaling beyond a single node

This is a single-node system today — the design below is how it would extend, not something implemented and running. Worth stating plainly rather than leaving as an implied gap.

**Sharding accounts across nodes.** The engine's own internal sharding (32 buckets, FNV hash of account ID) is already the right primitive — it just needs to operate at the cluster level instead of only within one process. Consistent hashing over account ID would route a given account to one designated ledger node, the same way requests are routed to in-process shards today. The deterministic lock-ordering proof holds unchanged *within* a node; a transfer between two accounts on *different* nodes becomes a distributed transaction, which is the actual hard problem this introduces.

**Cross-node transfers.** A transfer where the sender and receiver live on different nodes can't use a single in-process mutex pair. Two realistic approaches: a two-phase commit across the two owning nodes (simpler, lower throughput, blocks on the slower participant), or restructuring the transfer itself as two independent, node-local events — a debit event on the sender's node and a credit event on the receiver's node — reconciled by the audit worker's existing invariant check rather than enforced synchronously. The second is more in keeping with this project's event-sourced design, at the cost of a brief window where the global invariant is temporarily unbalanced between the two local commits.

**Replication and failover.** Each node's event log is a natural fit for Raft — a well-understood, more operationally tractable choice than Paxos for this shape of problem (log replication with a single leader, not general consensus on arbitrary values). Postgres's own write-ahead log replication, or a Raft-backed store like etcd fronting the event log, would give each shard a leader plus followers that can take over on failure; the WAL/audit-worker split already in this codebase (one component appending events, a separate component independently verifying them) maps naturally onto a leader/follower replication model without a fundamental redesign.

**What doesn't need to change:** the per-account deterministic lock ordering, the event-sourced persistence model, and the independent audit-worker verification all remain correct building blocks at a larger scale — the work is in routing and cross-shard coordination, not in redesigning the core correctness argument this project is built around.

## Known limitations

Kept here deliberately, rather than glossed over — knowing the edges of your own work is part of the engineering:

- The server doesn't validate that a replayed idempotency key's request body matches the original — a client that reuses a key for a genuinely different transfer gets back the first result silently rather than an error.
- Authentication is a single shared API key with no per-client identity or authorization, and `/stats` is unauthenticated. TLS uses certificates you supply; there is no automatic rotation.
- Single-node: see [Scaling beyond a single node](#scaling-beyond-a-single-node) above for the design, not yet the implementation.
- Request tracing stops at the gRPC boundary (see [Structured logging and request tracing](#structured-logging-and-request-tracing)) — it doesn't yet extend into WAL events or Redis Stream messages, and there's no real distributed-tracing backend wired up, only context-propagated trace IDs in logs.
- The audit worker logs on invariant drift but doesn't page, alert, or halt traffic.
- WAL flush failures are logged and the batch is dropped, with no retry-with-backoff or local spill-to-disk yet.
- No containerized deployment yet — Cloud Run or an equivalent platform supporting long-lived gRPC servers is the natural fit (a serverless request/response platform like Vercel is not, since this is a stateful, connection-holding server).

## License

MIT — see [LICENSE](LICENSE).

---

**Kunal** · [GitHub](https://github.com/Kunal-svg-cyber) · [LinkedIn](https://www.linkedin.com/in/kunalsharma2028)
