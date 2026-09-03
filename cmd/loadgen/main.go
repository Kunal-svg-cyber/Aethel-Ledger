// Command loadgen is a standalone gRPC load generator for Aethel
// Ledger. It seeds a pool of accounts, then fires concurrent Transfer
// requests against a running server for a fixed duration, measuring
// real network-measured throughput and latency, including gRPC
// serialization and round-trip cost.
//
// Usage:
//
//	go run ./cmd/loadgen -addr localhost:50051 -concurrency 50 -duration 15s
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ledgerv1 "github.com/Kunal-svg-cyber/aethel-ledger/internal/genproto/ledger/v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server address")
	concurrency := flag.Int("concurrency", 50, "number of concurrent client workers")
	duration := flag.Duration("duration", 15*time.Second, "how long to run the load test")
	numAccounts := flag.Int("accounts", 20, "size of the account pool transfers move between")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to %s: %v", *addr, err)
	}
	defer conn.Close()

	client := ledgerv1.NewLedgerServiceClient(conn)
	ctx := context.Background()

	accounts := seedAccounts(ctx, client, *numAccounts)
	log.Printf("seeded %d accounts, starting load test: %d workers for %s", len(accounts), *concurrency, *duration)

	var successCount, failureCount int64
	var mu sync.Mutex
	var latencies []time.Duration

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	wg.Add(*concurrency)

	for workerID := 0; workerID < *concurrency; workerID++ {
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))

			for time.Now().Before(deadline) {
				from := accounts[r.Intn(len(accounts))]
				to := accounts[r.Intn(len(accounts))]
				if from == to {
					continue
				}
				key := fmt.Sprintf("loadgen-%d-%d-%d", seed, time.Now().UnixNano(), r.Int63())

				start := time.Now()
				_, err := client.Transfer(ctx, &ledgerv1.TransferRequest{
					FromAccountId:  from,
					ToAccountId:    to,
					Amount:         int64(r.Intn(10) + 1),
					IdempotencyKey: key,
				})
				elapsed := time.Since(start)

				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()

				if err != nil {
					atomic.AddInt64(&failureCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(int64(workerID))
	}
	wg.Wait()

	report(*duration, *concurrency, successCount, failureCount, latencies)
}

// seedAccounts deposits a large starting balance into numAccounts fresh
// accounts so the load test doesn't hit insufficient-funds errors.
func seedAccounts(ctx context.Context, client ledgerv1.LedgerServiceClient, numAccounts int) []string {
	accounts := make([]string, numAccounts)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("loadtest-acct-%02d-%d", i, time.Now().UnixNano())
		if _, err := client.Deposit(ctx, &ledgerv1.DepositRequest{
			AccountId: accounts[i], Amount: 1_000_000_000,
		}); err != nil {
			log.Fatalf("seed deposit for %s failed: %v", accounts[i], err)
		}
	}
	return accounts
}

func report(duration time.Duration, concurrency int, success, failure int64, latencies []time.Duration) {
	total := success + failure

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)
	max := percentile(latencies, 1.0)

	fmt.Println()
	fmt.Println("=== Aethel Ledger Load Test Results ===")
	fmt.Printf("Duration:        %s\n", duration)
	fmt.Printf("Concurrency:     %d workers\n", concurrency)
	fmt.Printf("Total requests:  %d\n", total)
	fmt.Printf("Successful:      %d\n", success)
	fmt.Printf("Failed:          %d\n", failure)
	if total > 0 {
		fmt.Printf("Throughput:      %.1f req/sec\n", float64(total)/duration.Seconds())
	}
	fmt.Printf("Latency p50:     %s\n", p50)
	fmt.Printf("Latency p95:     %s\n", p95)
	fmt.Printf("Latency p99:     %s\n", p99)
	fmt.Printf("Latency max:     %s\n", max)
	fmt.Println()
	fmt.Println("This is the real, network-measured throughput, distinct from")
	fmt.Println("the in-process engine microbenchmark.")
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
