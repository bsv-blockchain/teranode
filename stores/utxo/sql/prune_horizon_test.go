package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// The parent-depth guard in utxo.GetCounterConflictingTxHashes tolerates an absent
// counter-conflicting record when the parent output slot it occupied is confirmed
// within `retention` blocks of tip. That is only sound if the store never deletes a
// transaction mined on the longest chain before mined_height + retention: a counter
// mined on the parent's slot has mined_height >= parent_height, so its deletion
// height is at or beyond the parent's.
//
// These tests pin that horizon at its exact boundary, on a real store driven by the
// real pruner service. The earliest DAH the code can produce for a mined transaction
// comes from SetMinedMulti (sql.go: newDAH = minedBlockInfo.BlockHeight + retention);
// the spend path stamps tip+1+retention, which is strictly later. So a transaction
// mined with all outputs already spent is the worst case, and it is what is built here.
func buildMinedFullySpent(ctx context.Context, t *testing.T, store *Store, tx *bt.Tx, mineHeight uint32) *chainhash.Hash {
	t.Helper()

	_, _, err := store.SpendAndCreate(ctx, tx, mineHeight, utxostore.WithCreateOnly())
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Spend every output BEFORE the mined transition, so SetMinedMulti sees an
	// all-spent record and stamps the earliest DAH the store can produce.
	for i, out := range tx.Outputs {
		spendTx := bt.NewTx()
		require.NoError(t, spendTx.From(txHash.String(), uint32(i), out.LockingScript.String(), out.Satoshis))
		require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, _, err = store.SpendAndCreate(ctx, spendTx, mineHeight, utxostore.WithSpendOnly())
		require.NoError(t, err, "spending output %d", i)
	}

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    mineHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.NoError(t, err, "setup: tx must exist before pruning")

	return txHash
}

func horizonStore(ctx context.Context, t *testing.T) (*Store, pruner.Service, uint32) {
	t.Helper()

	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	store, _ := setup(ctx, t)

	provider, ok := any(store).(pruner.PrunerServiceProvider)
	require.True(t, ok)

	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)
	prunerSvc.Start(ctx)

	retention := store.settings.GetUtxoStoreBlockHeightRetention()
	require.NotZero(t, retention, "retention must be configured for this test to mean anything")

	// tests.Tx spends tests.ParentTx; the parent must exist for SetConflicting to
	// record conflicting_children against it.
	_, _, err = store.SpendAndCreate(ctx, tests.ParentTx, 1, utxostore.WithCreateOnly())
	require.NoError(t, err)

	return store, prunerSvc, retention
}

// One block BEFORE the horizon the guard relies on, the mined transaction must
// still be present. If this fails, the guard can tolerate an absent counter that
// was in fact mined and pruned, and the tolerance is unsound.
func TestPruneHorizon_MinedTxSurvivesUntilMinedHeightPlusRetention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, prunerSvc, retention := horizonStore(ctx, t)

	const mineHeight uint32 = 1000

	txHash := buildMinedFullySpent(ctx, t, store, tests.Tx, mineHeight)

	tip := mineHeight + retention - 1
	require.NoError(t, store.SetBlockHeight(tip))

	_, err := prunerSvc.Prune(ctx, tip, "<horizon-below>")
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.NoError(t, err,
		"a tx mined at %d must survive pruning at tip %d (mined_height + retention - 1); "+
			"the counter-conflicting parent-depth guard depends on this", mineHeight, tip)
}

// At the horizon itself the transaction is deleted. This is the other half of the
// boundary: it shows the horizon really is mined_height + retention and not something
// longer that would make the test above pass for the wrong reason.
func TestPruneHorizon_MinedTxDeletedAtMinedHeightPlusRetention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, prunerSvc, retention := horizonStore(ctx, t)

	const mineHeight uint32 = 1000

	txHash := buildMinedFullySpent(ctx, t, store, tests.Tx, mineHeight)

	tip := mineHeight + retention
	require.NoError(t, store.SetBlockHeight(tip))

	_, err := prunerSvc.Prune(ctx, tip, "<horizon-at>")
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.Error(t, err,
		"a tx mined at %d must be pruned at tip %d (mined_height + retention)", mineHeight, tip)
}

// The hazard path: a transaction flagged conflicting while still unmined is
// stamped DAH = flag_height + retention (sql.go SetConflicting, and the
// "conflicting and no existing DAH" branch of teranode.lua setDeleteAtHeight).
// If it is later mined on the longest chain at a greater height, that earlier
// stamp must not survive — otherwise a mined transaction becomes deletable
// before mined_height + retention and the parent-depth guard's assumption breaks.
//
// The conflicting flag and its early stamp are written with direct SQL rather
// than through Store.SetConflicting: that method opens a transaction at
// sql.go:4325 and then calls s.GetSpend on the pool at sql.go:4383, which
// deadlocks on SQLite. What is under test here is the DAH arithmetic in
// SetMinedMulti, and this sets up its precondition exactly.
func TestPruneHorizon_ConflictingThenMinedDoesNotKeepEarlierStamp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, prunerSvc, retention := horizonStore(ctx, t)

	const (
		flagHeight uint32 = 1000
		mineHeight uint32 = 1500
	)

	tx := tests.Tx
	txHash := tx.TxIDChainHash()

	require.NoError(t, store.SetBlockHeight(flagHeight))

	_, _, err := store.SpendAndCreate(ctx, tx, flagHeight, utxostore.WithCreateOnly())
	require.NoError(t, err)

	// Precondition: conflicting, stamped for deletion at flag_height + retention.
	earlyDAH := flagHeight + retention
	_, err = store.db.ExecContext(ctx,
		`UPDATE transactions SET conflicting = true, delete_at_height = $1 WHERE hash = $2`,
		int64(earlyDAH), txHash[:])
	require.NoError(t, err)

	var stamped int64
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT delete_at_height FROM transactions WHERE hash = $1`, txHash[:]).Scan(&stamped))
	require.Equal(t, int64(earlyDAH), stamped, "setup: early stamp must be in place")

	// The outputs are deliberately NOT spent: the store refuses to spend a
	// conflicting transaction's outputs (TX_CONFLICTING), so the all-spent branch is
	// unreachable while the flag is set. The bump under test does not need them —
	// it fires purely because the existing stamp is below the new one.

	require.NoError(t, store.SetBlockHeight(mineHeight))

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID:        200,
		BlockHeight:    mineHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT delete_at_height FROM transactions WHERE hash = $1`, txHash[:]).Scan(&stamped))
	require.GreaterOrEqual(t, stamped, int64(mineHeight+retention),
		"SetMinedMulti must bump the stale flag-height stamp forward to at least mined_height + retention")

	// One block before the horizon measured from the MINED height. If the stale
	// flag-height stamp had survived, the tx would already be gone here.
	tip := mineHeight + retention - 1
	require.NoError(t, store.SetBlockHeight(tip))

	_, err = prunerSvc.Prune(ctx, tip, "<horizon-conflicting>")
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.NoError(t, err,
		"tx flagged conflicting at %d then mined at %d must survive to %d; "+
			"a surviving flag-height DAH stamp would break the parent-depth guard",
		flagHeight, mineHeight, tip)
}
