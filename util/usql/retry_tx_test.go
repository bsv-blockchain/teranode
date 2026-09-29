package usql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func newRetryTxTestDB(t *testing.T) *DB {
	t.Helper()

	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// One connection, so every transaction sees the same in-memory database.
	db.SetMaxOpenConns(1)
	db.SetRetryConfig(RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, Enabled: true})

	_, err = db.ExecContext(context.Background(), `CREATE TABLE items (v INTEGER NOT NULL)`)
	require.NoError(t, err)

	return db
}

func countItems(t *testing.T, db *DB) int {
	t.Helper()

	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT count(*) FROM items`).Scan(&n))

	return n
}

// TestRetryTx_RetriesTheWholeTransaction: a retriable failure after a write rolls that
// attempt back and runs fn again from BEGIN, so only the successful attempt's write lands.
func TestRetryTx_RetriesTheWholeTransaction(t *testing.T) {
	db := newRetryTxTestDB(t)

	attempts := 0
	err := db.RetryTx(context.Background(), nil, func(tx *sql.Tx) error {
		attempts++

		if _, err := tx.Exec(`INSERT INTO items (v) VALUES (?)`, attempts); err != nil {
			return err
		}

		if attempts == 1 {
			return &pgconn.PgError{Code: PgErrSerializationFail}
		}

		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Equal(t, 1, countItems(t, db), "the failed attempt's insert was rolled back")
}

// TestRetryTx_NonRetriableErrorIsReturnedOnce: a business error is not retried, and its
// writes are rolled back.
func TestRetryTx_NonRetriableErrorIsReturnedOnce(t *testing.T) {
	db := newRetryTxTestDB(t)

	attempts := 0
	sentinel := errors.NewInvalidArgumentError("not retriable")

	err := db.RetryTx(context.Background(), nil, func(tx *sql.Tx) error {
		attempts++

		if _, err := tx.Exec(`INSERT INTO items (v) VALUES (1)`); err != nil {
			return err
		}

		return sentinel
	})

	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 1, attempts)
	require.Equal(t, 0, countItems(t, db))
}

// TestRetryTx_OpenCircuitSkipsTheTransaction: with the breaker open, fn never runs.
func TestRetryTx_OpenCircuitSkipsTheTransaction(t *testing.T) {
	db := newRetryTxTestDB(t)

	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		HalfOpenMax:      1,
		Cooldown:         time.Hour,
		FailureWindow:    time.Hour,
		Enabled:          true,
	})
	require.NotNil(t, cb)
	cb.RecordFailure()
	db.SetCircuitBreaker(cb)

	ran := false
	err := db.RetryTx(context.Background(), nil, func(tx *sql.Tx) error {
		ran = true
		return nil
	})

	require.ErrorIs(t, err, ErrCircuitOpen)
	require.False(t, ran)
}
