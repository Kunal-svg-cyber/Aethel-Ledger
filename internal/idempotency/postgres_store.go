package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// PostgresStore durably persists idempotency records to Postgres, so a
// key remains known across a server restart — closing the gap where an
// in-memory store forgets every key it has ever seen the moment the
// process restarts, silently allowing a resubmitted request with a
// previously-committed key to execute a second time.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore opens a connection pool against dsn, applies
// conservative pool limits, and verifies connectivity with a Ping. When
// the WAL also persists to the same database, prefer
// NewPostgresStoreFromDB with a shared *sql.DB instead — see the
// comment there for why.
func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("idempotency: open postgres: %w", err)
	}
	configurePool(db)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("idempotency: ping postgres: %w", err)
	}
	return &PostgresStore{db: db}, nil
}

// NewPostgresStoreFromDB wraps an existing, already-configured *sql.DB.
// Used by main.go to share one connection pool between the idempotency
// and WAL Postgres stores instead of each opening its own. This isn't
// just an efficiency nicety: under concurrent load, two independent,
// unbounded connection pools against the same database can together
// exceed the provider's connection limit even if either alone would
// have been fine — this was measured directly, not theoretical: adding
// a second, separately-pooled Postgres store (this one) produced
// request failures under the same load profile that the WAL alone
// handled cleanly.
func NewPostgresStoreFromDB(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// configurePool bounds how many connections this process opens against
// Postgres, so concurrent load can't exhaust a provider's connection
// limit — see the equivalent comment in internal/wal/postgres_store.go.
func configurePool(db *sql.DB) {
	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
}

const createTableSQL = `
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key        TEXT PRIMARY KEY,
    committed  BOOLEAN NOT NULL DEFAULT FALSE,
    result     BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

// EnsureSchema creates the idempotency_keys table if it doesn't exist.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, createTableSQL)
	return err
}

// CheckAndReserve atomically reserves key if it's new, using
// INSERT ... ON CONFLICT DO NOTHING RETURNING to detect the outcome in
// a single round trip: if a row comes back, this call won the race and
// the key was new; if not (a unique-constraint conflict occurred
// instead), some caller — possibly concurrently, possibly in a past
// process — has already touched this key, and we fall through to
// checking its committed state.
func (s *PostgresStore) CheckAndReserve(ctx context.Context, key string) ([]byte, bool, error) {
	var returnedKey string
	err := s.db.QueryRowContext(ctx,
		"INSERT INTO idempotency_keys (key) VALUES ($1) ON CONFLICT (key) DO NOTHING RETURNING key",
		key,
	).Scan(&returnedKey)
	if err == nil {
		return nil, false, nil // reserved: this is a brand-new key
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, fmt.Errorf("idempotency: reserve: %w", err)
	}

	var committed bool
	var result []byte
	err = s.db.QueryRowContext(ctx,
		"SELECT committed, result FROM idempotency_keys WHERE key = $1", key,
	).Scan(&committed, &result)
	if err != nil {
		return nil, false, fmt.Errorf("idempotency: lookup: %w", err)
	}
	if committed {
		return result, true, nil
	}
	return nil, true, nil // reserved but not yet committed (concurrent in-flight request)
}

// Commit stores the result for a previously reserved key.
func (s *PostgresStore) Commit(ctx context.Context, key string, result []byte) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE idempotency_keys SET committed = TRUE, result = $2 WHERE key = $1",
		key, result,
	)
	if err != nil {
		return fmt.Errorf("idempotency: commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
