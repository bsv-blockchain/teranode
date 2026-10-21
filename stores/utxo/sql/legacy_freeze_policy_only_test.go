package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestLegacyFreezeIsPolicyOnly pins how a row frozen before the window columns existed
// reads: frozen = TRUE with a NULL freezeFrom. It is policy only — it keeps the coin out
// of the mempool, but carries no consensus window, so a block spending it is not a
// consensus violation and block validation's freeze-record read does not report it. The
// Aerospike store reads a sentinel-only output the same way, so the two stores cannot
// reach different verdicts on the same block (issue #1422).
func TestLegacyFreezeIsPolicyOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	spendTx := utxo2.GetSpendingTx(tx, 0)
	spends, err := utxo.GetSpends(spendTx)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{spends[0]}, store.settings))

	// Rewind the row to its pre-upgrade shape: the marker without a window.
	_, err = store.db.ExecContext(ctx, `UPDATE outputs SET freezeFrom = NULL, freezeUntil = NULL, freezePolicyExpires = NULL
		WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1) AND idx = 0`, tx.TxIDChainHash()[:])
	require.NoError(t, err)

	freezeFields := []fields.FieldName{fields.UtxoFreezeFrom, fields.UtxoFreezeUntil, fields.UtxoFreezeExp}

	data, err := store.Get(ctx, tx.TxIDChainHash(), freezeFields...)
	require.NoError(t, err)
	require.Empty(t, data.FreezeRecords, "a legacy freeze has no consensus record")

	items := []*utxo.UnresolvedMetaData{{Hash: *tx.TxIDChainHash(), Idx: 0}}
	require.NoError(t, store.BatchDecorate(ctx, items, freezeFields...))
	require.NoError(t, items[0].Err)
	require.Empty(t, items[0].Data.FreezeRecords, "the batched read agrees")

	_, err = store.Spend(ctx, spendTx, 100)
	require.ErrorIs(t, err, errors.ErrFrozen, "the policy tier still holds the coin")
	require.NotErrorIs(t, err, errors.ErrUtxoConsensusFrozen)

	_, err = store.Spend(ctx, spendTx, 100, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
	require.NoError(t, err, "a block spend of a legacy freeze is not a consensus violation")
}
