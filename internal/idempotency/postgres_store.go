package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type PostgresStore struct {
	db *sql.DB
}

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

func NewPostgresStoreFromDB(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

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

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, createTableSQL)
	return err
}

func (s *PostgresStore) CheckAndReserve(ctx context.Context, key string) ([]byte, bool, error) {
	var returnedKey string
	err := s.db.QueryRowContext(ctx,
		"INSERT INTO idempotency_keys (key) VALUES ($1) ON CONFLICT (key) DO NOTHING RETURNING key",
		key,
	).Scan(&returnedKey)
	if err == nil {
		return nil, false, nil
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
	return nil, true, nil
}

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

