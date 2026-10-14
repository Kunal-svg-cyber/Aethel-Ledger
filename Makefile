.PHONY: build test race vet fmt check run docker

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test ./...

run:
	go run ./cmd/server

docker:
	docker build -t aethel-ledger .
