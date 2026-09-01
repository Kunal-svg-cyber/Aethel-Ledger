package wal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("wal: open postgres: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("wal: ping postgres: %w", err)
	}
	return &PostgresStore{db: db}, nil
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

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, createTableSQL)
	return err
}

func (s *PostgresStore) FlushBatch(ctx context.Context, batch []ledger.Event) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("wal: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op if Commit succeeded

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

	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("wal: batch insert: %w", err)
	}
	return tx.Commit()
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
