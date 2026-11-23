package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// These tests pin the mined-record retention horizon against the real SQL store
// and pruner: a record mined at h must survive until h + retention.
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
// The conflicting flag and its early stamp are written with direct SQL, which sets
// up the precondition exactly. The opposite order, mined first and flagged
// conflicting second, goes through Store.SetConflicting in the test below.
func TestPruneHorizon_ConflictingThenMinedDoesNotKeepEarlierStamp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, prunerSvc, retention := horizonStore(ctx, t)

	const (
		flagHeight uint32 = 1000
		forkHeight uint32 = 1500
		mineHeight uint32 = 1200
	)

	tx := tests.Tx
	txHash := tx.TxIDChainHash()

	require.NoError(t, store.SetBlockHeight(flagHeight))

	_, _, err := store.SpendAndCreate(ctx, tx, flagHeight, utxostore.WithCreateOnly())
	require.NoError(t, err)

	// The tx is placed on a fork at the high height first, so the floor is 1500 +
	// retention. Only the floor can then lift the stamp of the lower longest-chain block.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID:        150,
		BlockHeight:    forkHeight,
		SubtreeIdx:     0,
		OnLongestChain: false,
	})
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
	require.GreaterOrEqual(t, stamped, int64(forkHeight+retention),
		"SetMinedMulti must bump the stale flag-height stamp forward to at least highest block height + retention")

	// One block before the horizon measured from the highest block. If the stale
	// flag-height stamp had survived, the tx would already be gone here.
	tip := forkHeight + retention - 1
	require.NoError(t, store.SetBlockHeight(tip))

	_, err = prunerSvc.Prune(ctx, tip, "<horizon-conflicting>")
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.NoError(t, err,
		"tx flagged conflicting at %d then mined at %d must survive to %d; "+
			"a surviving flag-height DAH stamp would break the parent-depth guard",
		flagHeight, forkHeight, tip)
}

// A fully spent tx placed on a fork at a high block, then mined on the longest chain at
// a much lower one, must outlive the high block by the retention window. Run through the
// real pruner.
func TestPruneHorizon_ForkThenLowerLongestChainBlockKeepsHighFloor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, prunerSvc, retention := horizonStore(ctx, t)

	const (
		cachedTip  uint32 = 500
		forkHeight uint32 = 1500
	)

	tx := tests.Tx
	txHash := tx.TxIDChainHash()

	require.NoError(t, store.SetBlockHeight(cachedTip))

	_, _, err := store.SpendAndCreate(ctx, tx, cachedTip, utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID: 100, BlockHeight: forkHeight, SubtreeIdx: 0, OnLongestChain: false,
	})
	require.NoError(t, err)

	for i, out := range tx.Outputs {
		spendTx := bt.NewTx()
		require.NoError(t, spendTx.From(txHash.String(), uint32(i), out.LockingScript.String(), out.Satoshis))
		require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, _, err = store.SpendAndCreate(ctx, spendTx, cachedTip+1, utxostore.WithSpendOnly())
		require.NoError(t, err, "spending output %d", i)
	}

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID: 101, BlockHeight: cachedTip, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	var stamped int64
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT delete_at_height FROM transactions WHERE hash = $1`, txHash[:]).Scan(&stamped))
	require.Equal(t, int64(forkHeight+retention), stamped)

	tip := cachedTip + retention
	require.NoError(t, store.SetBlockHeight(tip))

	_, err = prunerSvc.Prune(ctx, tip, "<horizon-fork-floor>")
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.NoError(t, err, "a tx placed at %d must survive a prune at %d", forkHeight, tip)
}

// The opposite order: a transaction is mined at a height far above the node's cached
// tip, then flagged conflicting. SetConflicting must not stamp from the lagging
// cached tip, because the pruner deletes purely on the stamp. Run through the real
// Store.SetConflicting and the real pruner.
func TestPruneHorizon_MinedThenConflictingDoesNotStampFromLaggingTip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, prunerSvc, retention := horizonStore(ctx, t)

	const (
		cachedTip  uint32 = 500
		mineHeight uint32 = 1500
	)

	tx := tests.Tx
	txHash := tx.TxIDChainHash()

	require.NoError(t, store.SetBlockHeight(cachedTip))

	_, _, err := store.SpendAndCreate(ctx, tx, cachedTip, utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID:        300,
		BlockHeight:    mineHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)

	var stamped int64
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT delete_at_height FROM transactions WHERE hash = $1`, txHash[:]).Scan(&stamped))
	require.GreaterOrEqual(t, stamped, int64(mineHeight+retention),
		"SetConflicting must stamp at least mined_height + retention, not cached_tip + 1 + retention")

	// The tip at which a stamp from the lagging cached tip would already delete it.
	laggingStamp := cachedTip + 1 + retention
	require.NoError(t, store.SetBlockHeight(laggingStamp))

	_, err = prunerSvc.Prune(ctx, laggingStamp, "<horizon-lagging>")
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.NoError(t, err,
		"tx mined at %d then flagged conflicting at cached tip %d must survive pruning at tip %d",
		mineHeight, cachedTip, laggingStamp)

	tip := mineHeight + retention - 1
	require.NoError(t, store.SetBlockHeight(tip))

	_, err = prunerSvc.Prune(ctx, tip, "<horizon-below>")
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.NoError(t, err, "tx must survive pruning at tip %d (mined_height + retention - 1)", tip)

	tip = mineHeight + retention
	require.NoError(t, store.SetBlockHeight(tip))

	_, err = prunerSvc.Prune(ctx, tip, "<horizon-at>")
	require.NoError(t, err)

	_, err = store.Get(ctx, txHash)
	require.Error(t, err, "tx must be pruned at tip %d (mined_height + retention)", tip)
}

// TestPruneHorizon_DefensiveKeepsParentOfChildInsideSafetyWindow: in defensive
// mode a parent past its delete height is deleted only once every child that
// spends it is mined at least the safety window below the prune height. P is
// mined and fully spent by C. At P's delete height, a C mined one block inside
// the window keeps P, and a C mined exactly at the window's edge lets P go. A
// window passed as zero would take P in both cases.
func TestPruneHorizon_DefensiveKeepsParentOfChildInsideSafetyWindow(t *testing.T) {
	for _, tc := range []struct {
		name string
		// depthBelowWindow is how many blocks below the window's edge C is mined; -1
		// puts it one block inside the window.
		depthBelowWindow int
		kept             bool
	}{
		{name: "child mined inside the window", depthBelowWindow: -1, kept: true},
		{name: "child mined at the window's edge", depthBelowWindow: 0, kept: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)

			store, _ := setup(ctx, t)
			store.settings.Pruner.UTXODefensiveEnabled = true

			provider, ok := any(store).(pruner.PrunerServiceProvider)
			require.True(t, ok)

			prunerSvc, err := provider.GetPrunerService()
			require.NoError(t, err)

			// NewService falls back to the global retention when no window is given,
			// and the store gives none.
			window := store.settings.GlobalBlockHeightRetention
			require.NotZero(t, window, "the safety window must be non-zero for this test to mean anything")

			const parentHeight uint32 = 1000

			require.NoError(t, store.SetBlockHeight(parentHeight))

			parent := bt.NewTx()
			require.NoError(t, parent.From("5555555555555555555555555555555555555555555555555555555555555555", 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

			_, err = store.Create(ctx, parent, parentHeight)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			_, _, err = store.SpendAndCreate(ctx, child, parentHeight)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()}, utxostore.MinedBlockInfo{
				BlockID: 1000, BlockHeight: parentHeight, OnLongestChain: true,
			})
			require.NoError(t, err)

			var dah *uint32

			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT delete_at_height FROM transactions WHERE hash = $1", parent.TxIDChainHash()[:]).Scan(&dah))
			require.NotNil(t, dah, "fixture: the mined, fully spent parent has a delete height")

			pruneHeight := *dah
			childHeight := uint32(int64(pruneHeight) - int64(window) - int64(tc.depthBelowWindow))
			require.GreaterOrEqual(t, childHeight, parentHeight, "fixture: the child is mined no lower than its parent")

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{child.TxIDChainHash()}, utxostore.MinedBlockInfo{
				BlockID: 1001, BlockHeight: childHeight, OnLongestChain: true,
			})
			require.NoError(t, err)

			require.NoError(t, store.SetBlockHeight(pruneHeight))

			_, err = prunerSvc.Prune(ctx, pruneHeight, "<defensive-window>")
			require.NoError(t, err)

			_, err = store.Get(ctx, parent.TxIDChainHash())
			if tc.kept {
				require.NoError(t, err, "a parent whose child was mined at %d, inside the %d-block window below %d, must be kept", childHeight, window, pruneHeight)
			} else {
				require.ErrorIs(t, err, errors.ErrTxNotFound, "a parent whose child was mined at %d, %d blocks below %d, must be deleted", childHeight, window, pruneHeight)
			}
		})
	}
}
