# Contributing

Issues and pull requests are welcome.

## Setup

Requires Go 1.22 or newer.

```bash
go mod download
go build ./...
go test ./...
```

Postgres integration tests run only when `DATABASE_URL` is set and use their own test keys and rows. See [TESTING.md](TESTING.md) for the full walkthrough.

## Before opening a pull request

- `gofmt -l .` prints nothing and `go vet ./...` passes.
- `go test -race ./...` passes (CI runs it on Linux).
- New behaviour has tests; concurrency changes keep the lock-ordering invariant documented in the README.
- Keep comments to what the code cannot say for itself.

## Commit messages

Short imperative subject line, with detail in the body when the reason is not obvious.
