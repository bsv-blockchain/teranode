package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestFinalizeTransaction verifies that FinalizeTransaction clears the tentative
// creating state (making outputs spendable), and is idempotent / a no-op for
// transactions that were never created in the creating state.
func TestFinalizeTransaction(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	// Finalize clears the creating state...
	require.NoError(t, store.FinalizeTransaction(ctx, tx))

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.False(t, got.Creating, "finalize must clear the creating flag")

	// ...making the outputs spendable.
	child := spendableChildTx(t, tx, 0)
	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)

	// Idempotent: finalizing again is a no-op success.
	require.NoError(t, store.FinalizeTransaction(ctx, tx))

	// Finalizing a tx that was never created in the creating state is also a no-op.
	require.NoError(t, store.FinalizeTransaction(ctx, tests.ParentTx))
}

// TestFinalizeTransaction_MultiRecord finalizes a paginated (multi-record) tx.
func TestFinalizeTransaction_MultiRecord(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2 // tx has 6 outputs → multiple records

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	require.NoError(t, store.FinalizeTransaction(ctx, tx))

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.False(t, got.Creating, "finalize must clear the creating flag on a multi-record tx")

	child := spendableChildTx(t, tx, 0)
	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)
}

// TestQueryStaleCreatingTxs verifies the sweeper query returns only creating-state
// txs whose unminedSince is strictly below the cutoff.
func TestQueryStaleCreatingTxs(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	// Three distinct txids (clones differ by version) sharing the same shape.
	staleTx := tx.Clone()
	staleTx.Version = 10

	freshTx := tx.Clone()
	freshTx.Version = 20

	finalTx := tx.Clone()
	finalTx.Version = 30

	// unminedSince is written from the create blockHeight for unmined creates.
	_, err = store.Create(ctx, staleTx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	_, err = store.Create(ctx, freshTx, 200, utxo.WithCreating(true))
	require.NoError(t, err)

	_, err = store.Create(ctx, finalTx, 100) // not creating
	require.NoError(t, err)

	hashes, err := store.QueryStaleCreatingTxs(ctx, 150, 0)
	require.NoError(t, err)
	require.Len(t, hashes, 1, "only the stale creating tx (unminedSince=100 < 150) must be returned")
	require.Equal(t, *staleTx.TxIDChainHash(), hashes[0])

	// A cutoff above the fresh tx's height picks up both creating txs, never the finalized one.
	hashes, err = store.QueryStaleCreatingTxs(ctx, 250, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []chainhash.Hash{*staleTx.TxIDChainHash(), *freshTx.TxIDChainHash()}, hashes)
}

// TestCreatingRecordWithMinedBlockInfo_UnminedSinceLifecycle verifies the sweep can see a
// creating record created with mined block info, and that finalize hands it back as a plain
// mined record: unminedSince is the sweep's selector while the record is creating, and is
// removed again once the record has block IDs and is no longer creating.
func TestCreatingRecordWithMinedBlockInfo_UnminedSinceLifecycle(t *testing.T) {
	for _, batchSize := range []int{0, 2} {
		name := "single-record"
		if batchSize > 0 {
			name = "multi-record"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)

			if batchSize > 0 {
				tSettings.UtxoStore.UtxoBatchSize = batchSize
			}

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
				BlockID: 7, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
			}))
			require.NoError(t, err)

			_, unminedSince := readMasterBins(t, client, store.GetNamespace(), store.GetName(), tx.TxIDChainHash())
			require.Equal(t, 100, unminedSince, "a creating record with block IDs carries unminedSince for the sweep")

			stale, err := store.QueryStaleCreatingTxs(ctx, 150, 0)
			require.NoError(t, err)
			require.Contains(t, stale, *tx.TxIDChainHash())

			require.NoError(t, store.FinalizeTransaction(ctx, tx))

			_, unminedSince = readMasterBins(t, client, store.GetNamespace(), store.GetName(), tx.TxIDChainHash())
			require.Nil(t, unminedSince, "finalize removes unminedSince from a record that has block IDs")

			it, err := store.GetUnminedTxIterator()
			require.NoError(t, err)

			require.NotContains(t, drainConflictingIterator(t, ctx, it), *tx.TxIDChainHash(), "a finalized mined record is not unmined")

			stale, err = store.QueryStaleCreatingTxs(ctx, 150, 0)
			require.NoError(t, err)
			require.NotContains(t, stale, *tx.TxIDChainHash())
		})
	}
}

// TestFinalizeTransaction_KeepsUnminedSinceWithoutBlockIDs verifies finalize leaves
// unminedSince alone on a record that has no block IDs: such a record is genuinely unmined.
func TestFinalizeTransaction_KeepsUnminedSinceWithoutBlockIDs(t *testing.T) {
	for _, batchSize := range []int{0, 2} {
		name := "single-record"
		if batchSize > 0 {
			name = "multi-record"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)

			if batchSize > 0 {
				tSettings.UtxoStore.UtxoBatchSize = batchSize
			}

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)

			require.NoError(t, store.FinalizeTransaction(ctx, tx))

			_, unminedSince := readMasterBins(t, client, store.GetNamespace(), store.GetName(), tx.TxIDChainHash())
			require.Equal(t, 100, unminedSince)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
			require.False(t, got.Creating)
		})
	}
}
