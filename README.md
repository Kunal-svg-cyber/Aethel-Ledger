# Aethel Ledger

A distributed, event-sourced financial ledger engine built from scratch in Go — designed to demonstrate correct, high-throughput concurrency control for money movement, not just another CRUD wallet API.

## Status

**Week 3-4 of 8 — gRPC gateway + idempotency layer.** The concurrency engine (week 1-2) is unchanged and still fully tested. On top of it: a protobuf schema, generated gRPC client/server code, a `LedgerServer` implementation wiring RPCs to the engine, and an idempotency layer that makes `Transfer` safe against client retries. The WAL, Redis Streams event bus, and audit worker are planned for weeks 5-6 (see Roadmap).

**Verification note:** the `internal/ledger` package (week 1-2) has been fully built and tested — `go vet`, `go test -race`, and the benchmark all ran clean. The week 3-4 additions (`internal/server`, `cmd/server`, the generated `internal/genproto` code) were generated with `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` and checked for syntax correctness and exact interface-signature matching against the generated code, but have not yet been build-verified end-to-end against the real `google.golang.org/grpc` module (that requires network access to the Go module proxy, which wasn't available in the environment that assembled this). Run `go mod tidy && go build ./...` after uploading — this resolves real dependency versions from the public proxy and will surface anything that needs fixing.

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

## Roadmap

- [x] Week 1-2: sharded-lock concurrency engine, race-detector test suite, deadlock-freedom proof, benchmarks
- [x] Week 3-4: protobuf schema, gRPC gateway, in-memory idempotency layer (Redis swap-in planned)
- [ ] Week 5-6: local WAL, async batch flush to Neon Postgres, Redis Streams event bus, audit worker
- [ ] Week 7: load generator, throughput/latency measurement, basic observability
- [ ] Week 8: docs, demo recording, final polish
