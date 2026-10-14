# Changelog

All notable changes are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-10-15

### Added
- Sharded in-memory ledger engine with deterministic lock ordering, overflow guards and event replay.
- Asynchronous write-ahead log with group-commit durable acknowledgment, backed by Postgres or memory.
- Idempotency store (memory and Postgres) with key expiry and release of failed reservations.
- gRPC API: Deposit, Transfer, GetBalance, with reflection.
- Audit worker verifying the global balance invariant; optional Redis Streams event bus.
- Rate limiting, per-RPC latency metrics, structured logging and request trace IDs.
- Opt-in API key authentication and TLS, including a local certificate generator.
- Dockerfile, Cloud Run deployment guide, and CI (format, vet, build, race tests, container build).

### Fixed
- Durable-acknowledgment queueing that limited throughput, and a waiter-ordering race in its fix.
- Connection-pool exhaustion from separate pools; one bounded pool is now shared.
- Public exposure of database tables (Row-Level Security enabled).
- Events silently dropped when the log channel was full; callers now wait instead.
- Idempotency keys stuck in flight after a failed transfer.
- Unbounded latency sample growth in metrics.
- Authentication not applied to streaming (reflection) calls.
