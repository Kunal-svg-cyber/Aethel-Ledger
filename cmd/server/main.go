
package main

import (
	"context"
	"database/sql"
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
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ratelimit"
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
	store, idemStore := buildStores(ctx, dsn)
	publisher := buildPublisher(ctx, auditWorker)

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

	ledgerServer := server.New(engine, idemStore, w)

	go logInvariantPeriodically(ctx, auditWorker)

	recorder := metrics.NewRecorder()
	go serveStats(recorder)

	limiter := ratelimit.NewLimiter(5000, 2000)

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		recorder.UnaryServerInterceptor(),
		limiter.UnaryServerInterceptor(),
	))
	ledgerv1.RegisterLedgerServiceServer(grpcServer, ledgerServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", listenAddr, err)
	}

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

func buildStores(ctx context.Context, dsn string) (wal.Store, idempotency.Store) {
	if dsn == "" {
		log.Println("DATABASE_URL not set — using in-memory stores (not durable across restarts)")
		return wal.NewInMemoryStore(), idempotency.NewInMemoryStore()
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("failed to open Postgres connection: %v", err)
	}
	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		log.Fatalf("failed to connect to Postgres: %v", err)
	}

	walStore := wal.NewPostgresStoreFromDB(db)
	if err := walStore.EnsureSchema(ctx); err != nil {
		log.Fatalf("failed to create ledger_events schema: %v", err)
	}
	log.Println("WAL persisting to Postgres")

	idemStore := idempotency.NewPostgresStoreFromDB(db)
	if err := idemStore.EnsureSchema(ctx); err != nil {
		log.Fatalf("failed to create idempotency_keys schema: %v", err)
	}
	log.Println("Idempotency keys persisting to Postgres (shared connection pool with WAL)")

	return walStore, idemStore
}

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

func serveStats(recorder *metrics.Recorder) {
	mux := http.NewServeMux()
	mux.Handle("/stats", recorder.Handler())
	log.Printf("Stats endpoint listening on http://localhost%s/stats", statsAddr)
	if err := http.ListenAndServe(statsAddr, mux); err != nil {
		log.Printf("stats server error: %v", err)
	}
}

