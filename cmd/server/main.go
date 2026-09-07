// Command server starts the Aethel Ledger gRPC gateway, wired to the
// concurrency engine, async WAL, idempotency layer, event bus, and
// audit worker. Degrades gracefully with zero configuration: without
// DATABASE_URL or Upstash Redis credentials, it runs entirely
// in-process with an in-memory store.
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/audit"
	ledgerv1 "github.com/Kunal-svg-cyber/aethel-ledger/internal/genproto/ledger/v1"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/idempotency"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/metrics"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/server"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/streaming"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/wal"
)

const (
	listenAddr = ":50051"
	statsAddr  = ":8080"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	auditWorker := audit.NewWorker()

	dsn := os.Getenv("DATABASE_URL")
	store := buildStore(ctx, dsn)
	publisher := buildPublisher(ctx, auditWorker)

	// Recover state from durable storage before serving any traffic.
	// Without this, every restart would silently reset all balances to
	// zero despite the full event history sitting in Postgres.
	history, err := store.LoadAll(ctx)
	if err != nil {
		log.Fatalf("failed to load event history: %v", err)
	}

	w := wal.New(store, publisher, wal.DefaultConfig())
	go w.Run(ctx)

	engine := ledger.NewEngine(w.Events())
	engine.Restore(history)
	for _, ev := range history {
		auditWorker.Apply(ev)
	}
	if len(history) > 0 {
		log.Printf("recovered %d events from durable storage; sequence resumes at %d", len(history), engine.CurrentSeq())
	}

	idemStore := buildIdempotencyStore(ctx, dsn)
	ledgerServer := server.New(engine, idemStore, w)

	go logInvariantPeriodically(ctx, auditWorker)

	recorder := metrics.NewRecorder()
	go serveStats(recorder)

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(recorder.UnaryServerInterceptor()))
	ledgerv1.RegisterLedgerServiceServer(grpcServer, ledgerServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", listenAddr, err)
	}

	// On SIGINT/SIGTERM, stop accepting new RPCs and let in-flight ones
	// finish, then cancel the WAL/audit context so WAL.Run performs its
	// final flush before the process exits. Without this, a normal
	// Ctrl+C or container SIGTERM kills the process immediately and any
	// buffered-but-unflushed events are lost, since Go does not run
	// deferred functions on an unhandled signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutdown signal received: draining in-flight requests")
		grpcServer.GracefulStop()
	}()

	log.Printf("Aethel Ledger gRPC server listening on %s", listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc server error: %v", err)
	}

	log.Println("gRPC server stopped; flushing remaining WAL events")
	cancel()
	<-w.Done()
	log.Println("shutdown complete")
}

// buildStore picks Postgres if dsn is non-empty, otherwise an
// in-memory store.
func buildStore(ctx context.Context, dsn string) wal.Store {
	if dsn == "" {
		log.Println("DATABASE_URL not set — using in-memory store (not durable across restarts)")
		return wal.NewInMemoryStore()
	}

	pgStore, err := wal.NewPostgresStore(dsn)
	if err != nil {
		log.Fatalf("failed to connect to Postgres: %v", err)
	}
	if err := pgStore.EnsureSchema(ctx); err != nil {
		log.Fatalf("failed to create ledger_events schema: %v", err)
	}
	log.Println("WAL persisting to Postgres")
	return pgStore
}

// buildIdempotencyStore picks Postgres if dsn is non-empty, otherwise an
// in-memory store. An in-memory idempotency store forgets every key it
// has ever seen on restart, which can let a resubmitted request with a
// previously-committed idempotency key execute a second time — the
// Postgres-backed store closes that gap by persisting records
// alongside the ledger's own event log.
func buildIdempotencyStore(ctx context.Context, dsn string) idempotency.Store {
	if dsn == "" {
		log.Println("DATABASE_URL not set — idempotency keys are in-memory only (do not survive a restart)")
		return idempotency.NewInMemoryStore()
	}

	pgStore, err := idempotency.NewPostgresStore(dsn)
	if err != nil {
		log.Fatalf("failed to connect idempotency store to Postgres: %v", err)
	}
	if err := pgStore.EnsureSchema(ctx); err != nil {
		log.Fatalf("failed to create idempotency_keys schema: %v", err)
	}
	log.Println("Idempotency keys persisting to Postgres")
	return pgStore
}

// buildPublisher picks Redis Streams if Upstash credentials are set,
// otherwise wires the audit worker directly in-process.
func buildPublisher(ctx context.Context, auditWorker *audit.Worker) wal.Publisher {
	redisURL := os.Getenv("UPSTASH_REDIS_REST_URL")
	redisToken := os.Getenv("UPSTASH_REDIS_REST_TOKEN")

	if redisURL == "" || redisToken == "" {
		log.Println("Upstash Redis not configured — audit worker wired in-process")
		return &audit.LocalPublisher{Worker: auditWorker}
	}

	bus := streaming.NewRedisStreamsBus(redisURL, redisToken, "ledger:events")
	log.Println("Event bus: Redis Streams (Upstash)")

	consumer := audit.NewRedisConsumer(bus, auditWorker)
	go consumer.Run(ctx, 2*time.Second)

	return bus
}

func logInvariantPeriodically(ctx context.Context, w *audit.Worker) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			drift, n := w.CheckInvariant()
			if drift != 0 {
				log.Printf("AUDIT ALERT: invariant drift = %d after %d events", drift, n)
			} else {
				log.Printf("audit: invariant OK (drift=0) after %d events processed", n)
			}
		}
	}
}

// serveStats exposes per-RPC counts and latency percentiles as JSON at
// http://localhost:8080/stats.
func serveStats(recorder *metrics.Recorder) {
	mux := http.NewServeMux()
	mux.Handle("/stats", recorder.Handler())
	log.Printf("Stats endpoint listening on http://localhost%s/stats", statsAddr)
	if err := http.ListenAndServe(statsAddr, mux); err != nil {
		log.Printf("stats server error: %v", err)
	}
}
