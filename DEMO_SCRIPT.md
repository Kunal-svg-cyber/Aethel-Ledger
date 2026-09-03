# Demo Script

A ~2 minute recorded walkthrough proving the project works, end to end.
Every command below is copy-paste ready for **Windows PowerShell** — no
improvising quotes on camera.

Record with OBS Studio (free) or Windows' built-in Xbox Game Bar
(Win+G). Capture just your terminal window(s), not the full desktop.

## Setup (before you hit record)

Open two PowerShell windows side by side. In the folder you extracted
the repo to:

```powershell
go build ./...
```

Confirm it prints nothing and returns to the prompt — you want this
already working before recording starts, so the video shows a clean run.

## The recording

**[Terminal 1] Start the server, narrate what's running:**
```powershell
go run ./cmd/server
```
Say something like: *"This starts the gRPC gateway. It's wired to an
in-memory concurrency engine, an async write-ahead log, and a background
audit worker — you can see it logging that the invariant holds every ten
seconds, with zero external services required."*

**[Terminal 2] Prove the concurrency engine is race-free:**
```powershell
go test -race -v ./internal/ledger/
```
Narrate the two tests that matter most as they scroll by:
`TestConcurrentTransfers_ConservesTotalBalance` and
`TestTransfer_NoDeadlockUnderReversedConcurrentPairs`. Say: *"The second
one specifically stresses the exact pattern that deadlocks a naive
lock-ordering implementation — A-to-B and B-to-A transfers firing
concurrently — with a hard timeout, so a regression fails loudly instead
of hanging."*

**[Terminal 2] Send a real deposit:**
```powershell
'{"account_id":"alice","amount":1000}' | Out-File -Encoding utf8 deposit.json
Get-Content deposit.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/Deposit
```

**[Terminal 2] Prove idempotency — send the same transfer twice:**
```powershell
'{"from_account_id":"alice","to_account_id":"bob","amount":300,"idempotency_key":"demo-key-1"}' | Out-File -Encoding utf8 transfer.json
Get-Content transfer.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/Transfer
Get-Content transfer.json | grpcurl -plaintext -d "@" localhost:50051 ledger.v1.LedgerService/Transfer
```
Point at the second response's `"replayed": true` on camera. Say: *"Same
idempotency key, sent twice — the second call returns the original
result instead of moving funds again. This is what makes the API safe
against a client or load balancer retrying a request."*

**[Terminal 2] Run the load test, let it complete:**
```powershell
go run ./cmd/loadgen -concurrency 50 -duration 15s
```
While it runs, say: *"Fifty concurrent workers transferring between a
pool of twenty accounts on purpose — that's deliberately heavy lock
contention."* When it finishes, read the throughput and p99 latency
numbers out loud.

**[Terminal 1] Stop the server:** `Ctrl+C`

## After recording

Trim dead air at the start/end. Upload to YouTube as **Unlisted** (not
Private — Unlisted links are viewable by anyone with the link but
don't appear in your public channel or search). Add the link to
the "Demo" section at the top of `README.md`.
