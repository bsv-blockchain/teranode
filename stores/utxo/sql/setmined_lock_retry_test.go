package sql

import (
	"context"
	"database/sql/driver"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

// A lock error on a SetMinedMulti chunk is retried, not returned. Under SQLite's
// shared cache a lock cycle between connections fails one side with
// SQLITE_LOCKED; when the failed side is SetMinedMulti, setTxMined failed the
// whole block over it. Each chunk is its own transaction and is rolled back on
// failure, so running it again is safe. The write is the real chunk write; the
// first attempts are refused with the error text the engine raised in
// services/blockvalidation's overtaken-tip repair tests.
func TestSetMinedMultiRetriesLockError(t *testing.T) {
	ctx := context.Background()
	db, tx := setup(ctx, t)

	_, err := db.Create(ctx, tx, 0)
	require.NoError(t, err)

	hashes := []*chainhash.Hash{tx.TxIDChainHash()}
	info := utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 1, OnLongestChain: true}
	lockErr := errors.NewStorageError("database table is locked: database is deadlocked (6)")

	t.Run("a refused write succeeds on a later attempt", func(t *testing.T) {
		attempts := 0

		var blockIDs map[chainhash.Hash][]uint32

		err := retryOnLockError(ctx, ulogger.TestLogger{}, setMinedMultiRetries, "[SetMinedMulti]", func() error {
			attempts++
			if attempts < setMinedMultiRetries {
				return lockErr
			}

			var err error

			blockIDs, err = db.setMinedMultiChunk(ctx, hashes, info)

			return err
		})
		require.NoError(t, err)
		require.Equal(t, setMinedMultiRetries, attempts)
		require.Equal(t, []uint32{7}, blockIDs[*hashes[0]], "the tx is mined in the block once the lock clears")
	})

	t.Run("a lock that never clears returns the lock error", func(t *testing.T) {
		attempts := 0

		err := retryOnLockError(ctx, ulogger.TestLogger{}, setMinedMultiRetries, "[SetMinedMulti]", func() error {
			attempts++
			return lockErr
		})
		require.ErrorIs(t, err, lockErr)
		require.Equal(t, setMinedMultiRetries, attempts)
	})

	t.Run("any other error is not retried", func(t *testing.T) {
		attempts := 0
		other := errors.NewTxNotFoundError("transaction not found")

		err := retryOnLockError(ctx, ulogger.TestLogger{}, setMinedMultiRetries, "[SetMinedMulti]", func() error {
			attempts++
			return other
		})
		require.ErrorIs(t, err, other)
		require.Equal(t, 1, attempts)
	})

	t.Run("a cancelled context stops the retries", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		attempts := 0

		err := retryOnLockError(cctx, ulogger.TestLogger{}, setMinedMultiRetries, "[SetMinedMulti]", func() error {
			attempts++
			cancel()

			return lockErr
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, attempts)
	})
}

// setMinedLockRefusals is how many more block_ids inserts the
// setmined_lock_refusal trigger refuses with SQLite's shared-cache lock error.
var setMinedLockRefusals atomic.Int32

// Registered in init so every connection the sqlite driver opens has it.
func init() {
	sqlite.MustRegisterScalarFunction("setmined_lock_refusal", 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
		if setMinedLockRefusals.Add(-1) >= 0 {
			return nil, errors.NewStorageError("database table is locked: database is deadlocked (6)")
		}

		return int64(0), nil
	})
}

// SetMinedMulti itself retries a chunk the engine refuses with a lock error. A
// trigger refuses the chunk's first block_ids insert with the error SQLite's
// shared cache raises for a lock cycle; the chunk rolls back and the retried
// attempt mines the tx. Without the retry at the SetMinedMulti call site the
// call returns the lock error.
func TestSetMinedMultiRetriesLockErrorFromChunk(t *testing.T) {
	ctx := context.Background()
	db, tx := setup(ctx, t)

	_, err := db.Create(ctx, tx, 0)
	require.NoError(t, err)

	_, err = db.db.ExecContext(ctx, `CREATE TRIGGER setmined_lock_refusal BEFORE INSERT ON block_ids
		BEGIN SELECT setmined_lock_refusal(); END`)
	require.NoError(t, err)

	t.Cleanup(func() {
		setMinedLockRefusals.Store(0)
		_, _ = db.db.ExecContext(ctx, "DROP TRIGGER IF EXISTS setmined_lock_refusal")
	})

	setMinedLockRefusals.Store(1)

	hash := tx.TxIDChainHash()

	blockIDs, err := db.SetMinedMulti(ctx, []*chainhash.Hash{hash}, utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 1, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, blockIDs[*hash])
	require.Equal(t, int32(-1), setMinedLockRefusals.Load(), "the first insert was refused and the second ran")
}

// A PostgreSQL deadlock (SQLSTATE 40P01) is still recognised after
// setMinedMultiChunk wraps it in a teranode error, which keeps only the
// driver error's text.
func TestIsDeadlockWrappedPostgresError(t *testing.T) {
	pgErr := &pgconn.PgError{Severity: "ERROR", Code: "40P01", Message: "deadlock detected"}

	require.True(t, isDeadlock(pgErr))
	require.True(t, isDeadlock(errors.NewStorageError("SQL error inserting block_ids: %v", pgErr)))
	require.False(t, isDeadlock(errors.NewStorageError("SQL error inserting block_ids: %v", &pgconn.PgError{Severity: "ERROR", Code: "23505", Message: "duplicate key"})))
}
