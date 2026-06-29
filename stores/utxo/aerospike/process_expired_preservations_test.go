package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestProcessExpiredPreservations verifies the setter-side layer: delete_at_height is only
// stamped at preservation expiry when the transaction is genuinely safe to drop (mined, on the
// longest chain, AND fully spent). ProcessExpiredPreservations writes the deleteAtHeight bin
// directly (bypassing the Lua setDeleteAtHeight self-heal), so without an eligibility check it
// would stamp a transaction that still has live outputs — and the DAH pruner deletes purely on
// that stamp. preserveUntil is set directly here (not via PreserveTransactions) so this exercises
// the expiry path in isolation, independent of the Phase-1 prune-eligibility gate.
func TestProcessExpiredPreservations(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 100
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	// ProcessExpiredPreservations selects expired records via a NUMERIC secondary index on
	// preserveUntil. The store creates this index at startup (see New's indexOnce block), built in
	// the background — processExpiryUntilProcessed retries to absorb the index-build lag.

	const currentHeight = uint32(200)
	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	expiredPreserveUntil := int(currentHeight - 10)

	txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// markPreservedExpired writes an already-expired preserveUntil directly and clears
	// deleteAtHeight, matching the record state left by a preservation (and bypassing the
	// Phase-1 eligibility gate so the expiry path can be tested for any record state).
	markPreservedExpired := func(t *testing.T) {
		t.Helper()
		wp := util.GetAerospikeWritePolicy(tSettings, 0)
		wp.RecordExistsAction = aerospike.UPDATE
		require.NoError(t, client.PutBins(wp, txKey,
			aerospike.NewBin(fields.PreserveUntil.String(), expiredPreserveUntil),
			aerospike.NewBin(fields.DeleteAtHeight.String(), nil)))
	}

	// processExpiryUntilProcessed runs ProcessExpiredPreservations, retrying to absorb
	// secondary-index build lag, until the record's preserveUntil bin is cleared (proving the
	// expired record was actually found and processed by the query).
	processExpiryUntilProcessed := func(t *testing.T) {
		t.Helper()
		require.Eventually(t, func() bool {
			require.NoError(t, store.ProcessExpiredPreservations(ctx, currentHeight))
			rec, getErr := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
			require.NoError(t, getErr)
			return rec.Bins[fields.PreserveUntil.String()] == nil
		}, 15*time.Second, 250*time.Millisecond, "expired preservation should be processed")
	}

	t.Run("ineligible_unmined_unspent_tx_is_not_stamped", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0) // unmined, unspent
		require.NoError(t, err)
		markPreservedExpired(t)

		processExpiryUntilProcessed(t)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[fields.PreserveUntil.String()], "preserveUntil must be cleared")
		require.Nil(t, rec.Bins[fields.DeleteAtHeight.String()], "unmined/unspent tx must NOT be stamped for deletion")
	})

	t.Run("eligible_mined_fully_spent_tx_is_stamped", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// Fully spend, then mine on the longest chain → eligible.
		_, err = store.Spend(ctx, spendTxAll, 1)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
			BlockID: 1, BlockHeight: 123, SubtreeIdx: 1, OnLongestChain: true,
		})
		require.NoError(t, err)

		markPreservedExpired(t) // simulate the preserved state (DAH cleared, preserveUntil set)

		processExpiryUntilProcessed(t)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[fields.PreserveUntil.String()], "preserveUntil must be cleared")
		require.Equal(t, int(currentHeight+retention), rec.Bins[fields.DeleteAtHeight.String()],
			"eligible tx must be stamped with currentHeight+retention")
	})

	t.Run("partial_spend_parent_is_not_stamped", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// Mine on the longest chain but spend only output 0 — the parent still has live
		// outputs, so it is NOT fully spent and must not be stamped.
		_, err = store.Spend(ctx, spendTx, 1)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
			BlockID: 1, BlockHeight: 123, SubtreeIdx: 1, OnLongestChain: true,
		})
		require.NoError(t, err)

		markPreservedExpired(t)

		processExpiryUntilProcessed(t)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[fields.PreserveUntil.String()], "preserveUntil must be cleared")
		require.Nil(t, rec.Bins[fields.DeleteAtHeight.String()],
			"partially-spent parent must NOT be stamped — it still has live UTXOs")
		// Note: the reorg-window scenario (a parent fully spent + preserved, then un-spent by a
		// reorg) has the SAME end state as this case at expiry time — mined, not fully spent,
		// preserveUntil set — so this subtest also covers the setter's behaviour there. The full
		// preserve→reorg→expiry narrative is exercised end-to-end in the SQL store tests
		// (TestProcessExpiredPreservations_ReorgUnspendDuringWindow).
	})

	t.Run("conflicting_tx_is_stamped_without_being_mined", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		// Conflicting txs get a DAH regardless of mined/spent state (the isConflicting branch
		// of the expression). Set the conflicting bin directly; markPreservedExpired clears the
		// DAH so the "conflicting AND no existing DAH" condition holds.
		wp := util.GetAerospikeWritePolicy(tSettings, 0)
		wp.RecordExistsAction = aerospike.UPDATE
		require.NoError(t, client.PutBins(wp, txKey, aerospike.NewBin(fields.Conflicting.String(), true)))

		markPreservedExpired(t)

		processExpiryUntilProcessed(t)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[fields.PreserveUntil.String()], "preserveUntil must be cleared")
		require.Equal(t, int(currentHeight+retention), rec.Bins[fields.DeleteAtHeight.String()],
			"conflicting tx must be stamped without being mined")
	})
}

// TestPreserveTransactions_OnlyPreservesPruneEligible verifies the Phase-1 layer for Aerospike:
// PreserveTransactions only preserves transactions that already carry a deleteAtHeight stamp.
// A transaction with no stamp is not at risk of pruning, so it must be left untouched rather than
// pointlessly preserved (and dragged into the expiry path).
func TestPreserveTransactions_OnlyPreservesPruneEligible(t *testing.T) {
	run := func(t *testing.T, useExpressions bool) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Aerospike.EnablePreserveFilterExpressions = useExpressions

		client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
		defer deferFn()
		cleanDB(t, client)

		_, createErr := store.Create(ctx, tx, 0)
		require.NoError(t, createErr)

		txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
		require.NoError(t, err)

		const preserveUntilHeight = uint32(1000)

		// Ineligible: no deleteAtHeight stamp → must NOT be preserved.
		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, preserveUntilHeight))
		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[fields.PreserveUntil.String()],
			"tx with no DAH is not at risk and must not be preserved")

		// Now give it a deleteAtHeight stamp (prune-eligible) and preserve again.
		wp := util.GetAerospikeWritePolicy(tSettings, 0)
		wp.RecordExistsAction = aerospike.UPDATE
		require.NoError(t, client.PutBins(wp, txKey, aerospike.NewBin(fields.DeleteAtHeight.String(), 500)))

		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, preserveUntilHeight))
		rec, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, int(preserveUntilHeight), rec.Bins[fields.PreserveUntil.String()],
			"prune-eligible tx must be preserved")
		require.Nil(t, rec.Bins[fields.DeleteAtHeight.String()],
			"preservation clears the DAH so the tx is held")
	}

	t.Run("lua", func(t *testing.T) { run(t, false) })
	t.Run("expressions", func(t *testing.T) { run(t, true) })
}
