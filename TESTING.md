# Testing Guide

Complete command reference for verifying every part of Aethel Ledger, from unit tests through a full restart-recovery and load test against a real Postgres database.

**Sections 1 is all you need for a working, fully-functional server** — the project runs completely in-memory with zero external services (see the README's "Zero-dependency mode"). Sections 2 onward exercise the Postgres-backed durability path specifically, and require a real database connection string (any Postgres works — Supabase, Neon, RDS, a local instance); skip them entirely if you just want to confirm the core system works.

Use two terminal windows: **Terminal A** runs the server, **Terminal B** runs everything else.

## 1. Build and unit tests (no database required)

```powershell
go mod tidy
go build ./...
go test ./...
```

`go test -race ./...` also works, with one caveat: on Windows without a 64-bit MinGW toolchain, this fails with `cc1.exe: sorry, unimplemented: 64-bit mode not compiled in`. That's a local toolchain limitation, not a code issue — the [CI workflow](.github/workflows/ci.yml) runs `-race` on every push via GitHub Actions' Linux runners, which have a correct toolchain, and is the authoritative race-detector result.

## 2. (Optional, requires Postgres) Set your database connection string

Required in every new terminal window — it doesn't persist across sessions. If you don't have a database handy, skip to running the server directly with no `DATABASE_URL` set — it works identically, just without cross-restart durability.

```powershell
$env:DATABASE_URL = "postgresql://user:password@host:5432/dbname"
```

For Supabase specifically: use the **Session pooler** connection string (not Direct connection, not Transaction pooler) from the **Connect** button on your project's dashboard — Direct connection is IPv6-only unless you pay for the IPv4 add-on. URL-encode any special characters in your password (`!` → `%21`, `+` → `%2B`, `?` → `%3F`, etc.).

## 3. Terminal A — start the server

```powershell
go run ./cmd/server
```

Expected startup lines:
```
WAL persisting to Postgres
Idempotency keys persisting to Postgres
recovered N events from durable storage; sequence resumes at N   (only if history exists)
Aethel Ledger gRPC server listening on :50051
Stats endpoint listening on http://localhost:8080/stats
```

If `DATABASE_URL` isn't set, you'll see in-memory fallback messages instead — the server still runs, just without durability across restarts.

Leave this running. Every log line after this point prints automatically — you never type them.

## 4. Terminal B — create request files (one-time)

```powershell
'{"account_id":"alice","amount":1000}' | Out-File -Encoding utf8 deposit.json
'{"from_account_id":"alice","to_account_id":"bob","amount":300,"idempotency_key":"test-key-1"}' | Out-File -Encoding utf8 transfer.json
'{"account_id":"alice"}' | Out-File -Encoding utf8 balance.json
```

## 5. Terminal B — basic deposit, transfer, balance

```powershell
Get-Content deposit.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/Deposit
Get-Content transfer.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/Transfer
Get-Content balance.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/GetBalance
```

## 6. Terminal B — idempotency within a session

Run the exact same transfer again, no restart in between:

```powershell
Get-Content transfer.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/Transfer
```

Expect `"replayed": true` and unchanged balances — funds must not move twice.

## 7. Terminal A — graceful shutdown

Press **Ctrl+C** (a keystroke, not typed text). Expect, printed automatically:
```
shutdown signal received: draining in-flight requests
gRPC server stopped; flushing remaining WAL events
shutdown complete
```

## 8. Terminal A — restart

```powershell
$env:DATABASE_URL = "postgresql://user:password@host:5432/dbname"
go run ./cmd/server
```

Expect `recovered N events from durable storage; sequence resumes at N`.

## 9. Terminal B — confirm balance survived the restart

```powershell
Get-Content balance.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/GetBalance
```

Must match the pre-restart balance, not reset to zero.

## 10. Terminal B — confirm idempotency survived the restart

Resubmit the **same** `transfer.json` (same `idempotency_key`) from before the restart:

```powershell
Get-Content transfer.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/Transfer
```

Expect `"replayed": true` with unchanged balances — a resubmitted request must not double-spend just because the server restarted in between.

## 11. Terminal B — Postgres integration tests

Run these directly against your real database — they're skipped automatically in `go test ./...` unless `DATABASE_URL` is set:

```powershell
$env:DATABASE_URL = "postgresql://user:password@host:5432/dbname"
go test ./internal/wal/ -run TestPostgresStore -v
go test ./internal/idempotency/ -run TestPostgresStore -v
```

## 12. Terminal B — load test (server must be running)

```powershell
go run ./cmd/loadgen -concurrency 50 -duration 15s
```

Reports real network-measured throughput and p50/p95/p99 latency, not the in-memory-only engine benchmark. Numbers will vary run to run against a real remote database — that's normal network variance, not a bug.

## 13. Terminal B — live stats while the load test runs

```powershell
curl http://localhost:8080/stats
```

Returns per-RPC success/failure counts and latency percentiles as JSON.

## 14. Inspect the raw event log directly (optional)

In your database's SQL editor:

```sql
SELECT seq, type, account, amount FROM ledger_events ORDER BY seq;
SELECT key, committed, created_at FROM idempotency_keys ORDER BY created_at;
```

Confirms `seq` values are continuous with no duplicates or gaps across restarts, and that idempotency keys are actually being persisted.
