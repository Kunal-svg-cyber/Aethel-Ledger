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

	store := buildStore(ctx)
	publisher := buildPublisher(ctx, auditWorker)

	w := wal.New(store, publisher, wal.DefaultConfig())
	go w.Run(ctx)

	engine := ledger.NewEngine(w.Events())
	idemStore := idempotency.NewInMemoryStore()
	ledgerServer := server.New(engine, idemStore)

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

	log.Printf("Aethel Ledger gRPC server listening on %s", listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc server error: %v", err)
	}
}

// buildStore picks Postgres if DATABASE_URL is set, otherwise an
// in-memory store.
func buildStore(ctx context.Context) wal.Store {
	dsn := os.Getenv("DATABASE_URL")
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
