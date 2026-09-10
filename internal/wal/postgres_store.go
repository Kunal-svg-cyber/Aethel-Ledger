package wal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

// PostgresStore durably persists WAL batches to Postgres via
// database/sql and lib/pq.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore opens a connection pool against dsn, applies
// conservative pool limits (see configurePool), and verifies
// connectivity with a Ping. Use this when wal is the only component
// talking to Postgres; when idempotency also persists to the same
// database, prefer NewPostgresStoreFromDB with a shared *sql.DB instead
// — see the comment there for why.
func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("wal: open postgres: %w", err)
	}
	configurePool(db)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("wal: ping postgres: %w", err)
	}
	return &PostgresStore{db: db}, nil
}

// NewPostgresStoreFromDB wraps an existing, already-configured *sql.DB.
// Used by main.go to share one connection pool between the WAL and
// idempotency Postgres stores instead of each opening its own —
// avoiding two independent, unbounded pools racing for the same
// database's connection limit under concurrent load.
func NewPostgresStoreFromDB(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// configurePool bounds how many connections this process will open
// against Postgres. An unbounded pool (database/sql's default) lets Go
// open a new connection for every concurrent request with no idle one
// available — under real concurrent load against a provider with a
// connection cap (as most managed/pooled Postgres services have,
// including Supabase's session pooler), this can exhaust the limit and
// turn into outright request failures rather than just added latency.
func configurePool(db *sql.DB) {
	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
}

const createTableSQL = `
CREATE TABLE IF NOT EXISTS ledger_events (
    seq             BIGINT PRIMARY KEY,
    type            TEXT NOT NULL,
    account         TEXT NOT NULL,
    counter_account TEXT NOT NULL DEFAULT '',
    amount          BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);`

// EnsureSchema creates the ledger_events table if it doesn't exist.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, createTableSQL)
	return err
}

// FlushBatch inserts an entire batch as a single multi-row INSERT, with
// ON CONFLICT (seq) DO NOTHING on the primary key so a retried flush
// after a partial failure can't create duplicate rows. Executed as a
// single statement without an explicit transaction: a lone SQL
// statement is already atomic in Postgres, so wrapping it in
// BeginTx/Commit would only add two extra network round trips for no
// additional safety -- a real cost against a remote database, not a
// free correctness improvement.
func (s *PostgresStore) FlushBatch(ctx context.Context, batch []ledger.Event) error {
	if len(batch) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ledger_events (seq, type, account, counter_account, amount) VALUES ")
	args := make([]interface{}, 0, len(batch)*5)
	for i, ev := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 5
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4, base+5)
		args = append(args, ev.Seq, string(ev.Type), ev.Account, ev.CounterAccount, ev.Amount)
	}
	sb.WriteString(" ON CONFLICT (seq) DO NOTHING")

	if _, err := s.db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("wal: batch insert: %w", err)
	}
	return nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// LoadAll returns every persisted event in seq order, for rebuilding
// engine state at startup.
func (s *PostgresStore) LoadAll(ctx context.Context) ([]ledger.Event, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT seq, type, account, counter_account, amount FROM ledger_events ORDER BY seq ASC")
	if err != nil {
		return nil, fmt.Errorf("wal: load events: %w", err)
	}
	defer rows.Close()

	var events []ledger.Event
	for rows.Next() {
		var ev ledger.Event
		var evType string
		if err := rows.Scan(&ev.Seq, &evType, &ev.Account, &ev.CounterAccount, &ev.Amount); err != nil {
			return nil, fmt.Errorf("wal: scan event row: %w", err)
		}
		ev.Type = ledger.EventType(evType)
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("wal: iterate event rows: %w", err)
	}
	return events, nil
}
