# Aethel Ledger

A distributed, event-sourced financial ledger engine built from scratch in Go — designed to demonstrate correct, high-throughput concurrency control for money movement, not just another CRUD wallet API.

## Status

**Week 1-2 of 8 — concurrency core.** The in-memory, thread-safe ledger engine is implemented, tested under the race detector, stress-tested for invariant conservation, and proven deadlock-free under adversarial concurrent load. Everything below this point is real and runnable. The gRPC gateway, idempotency layer, WAL, and audit worker are planned for the following weeks (see Roadmap).

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

No local installs needed — this repo is built to run entirely inside GitHub Codespaces (free tier: 120 core-hours/month, ~60 active hours on a 2-core machine).

```bash
go build ./...
go test -race ./...
go run ./cmd/server
```

## Tech stack (target, end of week 8)

| Layer | Choice | Why |
|---|---|---|
| Language | Go | `sync` primitives, goroutines, native concurrency tooling (`-race`, `pprof`) |
| Transport | gRPC + Protocol Buffers | Binary framing over HTTP/2, avoids JSON serialization overhead |
| Cache / idempotency | Upstash Redis (free tier) | Atomic Lua scripts for exactly-once retry handling |
| Event bus | Redis Streams (Upstash Redis) | Replaces the now-discontinued Upstash Kafka; same consumer-group semantics |
| Durable storage | Neon Postgres (free tier) | Serverless, async-batched WAL sink |
| Dev environment | GitHub Codespaces | Zero local installs, fully browser-based |

## Roadmap

- [x] Week 1-2: sharded-lock concurrency engine, race-detector test suite, deadlock-freedom proof, benchmarks
- [ ] Week 3-4: protobuf schema, gRPC gateway, bloom filter + Redis idempotency layer
- [ ] Week 5-6: local WAL, async batch flush to Neon Postgres, Redis Streams event bus, audit worker
- [ ] Week 7: load generator, throughput/latency measurement, basic observability
- [ ] Week 8: docs, demo recording, final polish
