package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestFreezeWritesRefuseAMissedUpdate pins the RowsAffected guards on the freeze,
// unfreeze and reassign writes (issue #1422). Each write is an UPDATE pinned to the
// freeze state its SELECT observed, so a write that touches no row lost a race with
// another write to the same output and must be reported, never committed as a success.
//
// The race is made deterministic with a SQLite trigger that silently skips every UPDATE
// of outputs (RAISE(IGNORE)): the statement then succeeds and affects no row, which is
// exactly what a lost race looks like to the caller. Without the guards each call below
// would return nil.
func TestFreezeWritesRefuseAMissedUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	spends, err := utxo.GetSpends(utxo2.GetSpendingTx(tx, 0, 1))
	require.NoError(t, err)
	require.Len(t, spends, 2)

	frozen, unfrozen := spends[0], spends[1]

	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{frozen}, store.settings))

	_, err = store.db.ExecContext(ctx, `CREATE TRIGGER skip_output_updates BEFORE UPDATE ON outputs BEGIN SELECT RAISE(IGNORE); END`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = store.db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS skip_output_updates`)
	})

	t.Run("freeze", func(t *testing.T) {
		err := store.FreezeUTXOs(ctx, []*utxo.Spend{unfrozen}, store.settings)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	t.Run("unfreeze", func(t *testing.T) {
		err := store.UnFreezeUTXOs(ctx, []*utxo.Spend{frozen}, store.settings)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	t.Run("reassign", func(t *testing.T) {
		newHash := chainhash.HashH([]byte("reassigned"))
		newUtxo := *frozen
		newUtxo.UTXOHash = &newHash

		err := store.ReAssignUTXO(ctx, frozen, &newUtxo, store.settings)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})
}
