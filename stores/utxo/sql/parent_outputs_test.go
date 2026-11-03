package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestParentOutputsForValidationSQLite(t *testing.T) {
	ctx := context.Background()

	t.Run("contract", func(t *testing.T) {
		db, _ := setup(ctx, t)
		tests.ParentOutputsForValidation(t, db)
	})

	t.Run("reads outputs, never inputs", func(t *testing.T) {
		db, _ := setup(ctx, t)
		tests.ParentOutputsReadsOutputsNotInputs(t, db)
	})
}

func TestParentOutputsForValidationPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	t.Run("contract", func(t *testing.T) {
		db, _ := setupPostgresStore(t)
		tests.ParentOutputsForValidation(t, db)
	})

	t.Run("reads outputs, never inputs", func(t *testing.T) {
		db, _ := setupPostgresStore(t)
		tests.ParentOutputsReadsOutputsNotInputs(t, db)
	})
}

// A store fault must come back as a per-outpoint Err, never as TxNotFound: the
// caller turns TxNotFound into a missing-parent verdict, and a transient fault
// reported that way would make a valid block look incomplete forever.
func TestParentOutputsForValidationFaultIsErrNotNotFound(t *testing.T) {
	ctx := context.Background()
	db, _ := setup(ctx, t)

	require.NoError(t, db.db.Close())

	answers, err := db.ParentOutputsForValidation(ctx, []utxo.Outpoint{{TxID: *tests.TXHash, Vout: 0}, {TxID: *tests.TXHash, Vout: 1}})
	require.NoError(t, err)
	require.Len(t, answers, 2)

	for _, a := range answers {
		require.Error(t, a.Err)
		require.Equal(t, utxo.ParentOutputUnknown, a.Status)
	}
}

// A lock error on the read is retried, not reported. Under SQLite's shared
// cache a lock cycle between connections fails one side with SQLITE_LOCKED;
// when the failed side is the reader, a parent that exists must still come back
// as found, not as a per-outpoint Err that fails the transaction. The read is
// the real chunk read; the first attempts are refused with the error text the
// spend side got in services/propagation Test_handleMultipleTx.
func TestParentOutputsForValidationRetriesLockError(t *testing.T) {
	ctx := context.Background()
	db, tx := setup(ctx, t)

	_, err := db.Create(ctx, tx, 0)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	chunk := []outpointPair{{hash: h[:], idx: 0}}
	lockErr := errors.NewStorageError("database table is locked: database is deadlocked (6)")

	t.Run("a refused read succeeds on a later attempt", func(t *testing.T) {
		attempts := 0

		var found map[utxo.Outpoint]utxo.ParentOutput

		err := retryReadOnLockError(ctx, ulogger.TestLogger{}, len(chunk), func() error {
			attempts++
			if attempts < parentOutputsReadRetries {
				return lockErr
			}

			var err error

			found, err = db.parentOutputsChunk(ctx, chunk)

			return err
		})
		require.NoError(t, err)
		require.Equal(t, parentOutputsReadRetries, attempts)

		answer, ok := found[utxo.Outpoint{TxID: *h, Vout: 0}]
		require.True(t, ok, "the parent must be found once the lock clears")
		require.Equal(t, utxo.ParentOutputNotMined, answer.Status)
		require.Equal(t, tx.Outputs[0].Satoshis, answer.Satoshis)
	})

	t.Run("a lock that never clears returns the lock error", func(t *testing.T) {
		attempts := 0

		err := retryReadOnLockError(ctx, ulogger.TestLogger{}, len(chunk), func() error {
			attempts++
			return lockErr
		})
		require.ErrorIs(t, err, lockErr)
		require.Equal(t, parentOutputsReadRetries, attempts)
	})

	t.Run("any other error is not retried", func(t *testing.T) {
		attempts := 0
		other := errors.NewStorageError("no such table: outputs")

		err := retryReadOnLockError(ctx, ulogger.TestLogger{}, len(chunk), func() error {
			attempts++
			return other
		})
		require.ErrorIs(t, err, other)
		require.Equal(t, 1, attempts)
	})

	t.Run("a cancelled context stops the retries", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		attempts := 0

		err := retryReadOnLockError(cctx, ulogger.TestLogger{}, len(chunk), func() error {
			attempts++
			cancel()

			return lockErr
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, attempts)
	})
}
