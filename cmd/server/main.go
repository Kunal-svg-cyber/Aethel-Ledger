// Command server starts the Aethel Ledger gRPC gateway, wired to the
// concurrency engine, async WAL, idempotency layer, event bus, and
// audit worker. Degrades gracefully with zero configuration: without
// DATABASE_URL or Upstash Redis credentials, it runs entirely
// in-process with in-memory stores.
package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/audit"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/auth"
	ledgerv1 "github.com/Kunal-svg-cyber/aethel-ledger/internal/genproto/ledger/v1"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/idempotency"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/logging"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/metrics"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ratelimit"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/server"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/streaming"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/tlsconfig"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/tracing"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/wal"
)

const (
	listenAddr = ":50051"
	statsAddr  = ":8080"

	// idempotencyKeyTTL bounds how long an idempotency record is kept.
	// Long enough that any realistic client retry (network blip, load
	// balancer resend) still finds its original key; short enough that
	// the store doesn't grow forever. Tune per deployment via the
	// cleanup interval and this constant together.
	idempotencyKeyTTL = 24 * time.Hour
	cleanupInterval   = 1 * time.Hour
)

func main() {
	logging.Init() // structured logging via log/slog; set LOG_FORMAT=json for machine-parseable output

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	auditWorker := audit.NewWorker()

	dsn := os.Getenv("DATABASE_URL")
	store, idemStore := buildStores(ctx, dsn)
	publisher := buildPublisher(ctx, auditWorker)

	// Recover state from durable storage before serving any traffic.
	// Without this, every restart would silently reset all balances to
	// zero despite the full event history sitting in Postgres.
	history, err := store.LoadAll(ctx)
	if err != nil {
		slog.Error("failed to load event history", "error", err)
		os.Exit(1)
	}

	w := wal.New(store, publisher, wal.DefaultConfig())
	go w.Run(ctx)

	engine := ledger.NewEngine(w.Events())
	engine.Restore(history)
	for _, ev := range history {
		auditWorker.Apply(ev)
	}
	if len(history) > 0 {
		slog.Info("recovered events from durable storage", "count", len(history), "resume_seq", engine.CurrentSeq())
	}

	// Periodically prune expired idempotency records. Only runs if the
	// configured store actually supports expiry (both InMemoryStore and
	// PostgresStore do); a type assertion keeps this optional rather
	// than forcing every Store implementation to support it.
	if expirable, ok := idemStore.(idempotency.ExpirableStore); ok {
		go cleanupExpiredIdempotencyKeys(ctx, expirable, idempotencyKeyTTL, cleanupInterval)
	}

	ledgerServer := server.New(engine, idemStore, w)

	go logInvariantPeriodically(ctx, auditWorker)

	recorder := metrics.NewRecorder()
	go serveStats(recorder)

	// A generous default: 5,000 req/sec sustained, burst of 2,000 — see
	// internal/ratelimit for why this number was chosen.
	limiter := ratelimit.NewLimiter(5000, 2000)

	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			tracing.UnaryServerInterceptor(),
			recorder.UnaryServerInterceptor(),
			limiter.UnaryServerInterceptor(),
			auth.UnaryServerInterceptor(os.Getenv("API_KEY")),
		),
	}
	if os.Getenv("API_KEY") != "" {
		slog.Info("API key authentication enabled")
	} else {
		slog.Warn("API_KEY not set: authentication disabled")
	}
	certFile, keyFile := os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE")
	if certFile != "" || keyFile != "" {
		tlsCfg, err := tlsconfig.Load(certFile, keyFile)
		if err != nil {
			slog.Error("failed to load TLS configuration", "error", err)
			os.Exit(1)
		}
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
		slog.Info("TLS enabled", "min_version", "1.2")
	} else {
		slog.Warn("TLS_CERT_FILE/TLS_KEY_FILE not set: serving plaintext")
	}
	grpcServer := grpc.NewServer(serverOpts...)
	ledgerv1.RegisterLedgerServiceServer(grpcServer, ledgerServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		slog.Error("failed to listen", "addr", listenAddr, "error", err)
		os.Exit(1)
	}

	// On SIGINT/SIGTERM, stop accepting new RPCs and let in-flight ones
	// finish, then cancel the WAL/audit context so WAL.Run performs its
	// final flush before the process exits.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("shutdown signal received: draining in-flight requests")
		grpcServer.GracefulStop()
	}()

	slog.Info("Aethel Ledger gRPC server listening", "addr", listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("grpc server error", "error", err)
		os.Exit(1)
	}

	slog.Info("gRPC server stopped; flushing remaining WAL events")
	cancel()
	<-w.Done()
	slog.Info("shutdown complete")
}

// buildStores constructs the WAL and idempotency stores, sharing one
// bounded *sql.DB connection pool between both when dsn is set (see the
// README's "Connection pool exhaustion" section for why that sharing
// matters). With dsn empty, both fall back to independent in-memory
// stores.
func buildStores(ctx context.Context, dsn string) (wal.Store, idempotency.Store) {
	if dsn == "" {
		slog.Info("DATABASE_URL not set — using in-memory stores (not durable across restarts)")
		return wal.NewInMemoryStore(), idempotency.NewInMemoryStore()
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		slog.Error("failed to open Postgres connection", "error", err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		slog.Error("failed to connect to Postgres", "error", err)
		os.Exit(1)
	}

	walStore := wal.NewPostgresStoreFromDB(db)
	if err := walStore.EnsureSchema(ctx); err != nil {
		slog.Error("failed to create ledger_events schema", "error", err)
		os.Exit(1)
	}
	slog.Info("WAL persisting to Postgres")

	idemStore := idempotency.NewPostgresStoreFromDB(db)
	if err := idemStore.EnsureSchema(ctx); err != nil {
		slog.Error("failed to create idempotency_keys schema", "error", err)
		os.Exit(1)
	}
	slog.Info("Idempotency keys persisting to Postgres", "shared_pool_with", "WAL")

	return walStore, idemStore
}

// buildPublisher picks Redis Streams if Upstash credentials are set,
// otherwise wires the audit worker directly in-process.
func buildPublisher(ctx context.Context, auditWorker *audit.Worker) wal.Publisher {
	redisURL := os.Getenv("UPSTASH_REDIS_REST_URL")
	redisToken := os.Getenv("UPSTASH_REDIS_REST_TOKEN")

	if redisURL == "" || redisToken == "" {
		slog.Info("Upstash Redis not configured — audit worker wired in-process")
		return &audit.LocalPublisher{Worker: auditWorker}
	}

	bus := streaming.NewRedisStreamsBus(redisURL, redisToken, "ledger:events")
	slog.Info("event bus configured", "type", "Redis Streams (Upstash)")

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
				slog.Error("AUDIT ALERT: invariant drift detected", "drift", drift, "events_processed", n)
			} else {
				slog.Info("audit invariant OK", "drift", 0, "events_processed", n)
			}
		}
	}
}

// cleanupExpiredIdempotencyKeys periodically prunes idempotency records
// older than ttl. Runs in its own goroutine for the life of the
// process; a failed cleanup pass is logged and retried on the next
// tick, never fatal — a transient cleanup failure should not take down
// the server.
func cleanupExpiredIdempotencyKeys(ctx context.Context, store idempotency.ExpirableStore, ttl, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-ttl)
			removed, err := store.DeleteExpired(ctx, cutoff)
			if err != nil {
				slog.Error("idempotency cleanup failed", "error", err)
				continue
			}
			if removed > 0 {
				slog.Info("pruned expired idempotency keys", "removed", removed, "ttl", ttl.String())
			}
		}
	}
}

// serveStats exposes per-RPC counts and latency percentiles as JSON at
// http://localhost:8080/stats.
func serveStats(recorder *metrics.Recorder) {
	mux := http.NewServeMux()
	mux.Handle("/stats", recorder.Handler())
	slog.Info("stats endpoint listening", "addr", "http://localhost"+statsAddr+"/stats")
	if err := http.ListenAndServe(statsAddr, mux); err != nil {
		slog.Error("stats server error", "error", err)
	}
}
