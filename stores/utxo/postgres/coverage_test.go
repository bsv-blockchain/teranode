package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestBatchedModeExercisesBatchers drives Create / Get / Spend / SetLocked through
// the BATCHED code paths (which only run after Start initialises the batchers — the
// default setupTestStore store runs in direct mode), then BatchSizeSnapshot and Close.
// This covers createBatched/sendCreateBatch, sendGetBatch, the bulk sendSpendBatch
// path, sendUnlockBatch, BatchSizeSnapshot and Close in one flow.
func TestBatchedModeExercisesBatchers(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	// GetBatcherSize > 1 makes Start create the get batcher, so metadata-only Gets
	// go through sendGetBatch.
	store.settings.UtxoStore.GetBatcherSize = 4

	// Start initialises the create/spend/get/unlock batchers.
	store.Start(ctx)

	// Create via the batcher.
	parent := makeThreeOutputTx(t) // vout0 and vout2 spendable
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)
	parentHash := parent.TxIDChainHash()

	// Metadata-only Get → get batcher (sendGetBatch).
	meta, err := store.Get(ctx, parentHash, fields.BlockIDs)
	require.NoError(t, err)
	require.NotNil(t, meta)

	// Get requesting Utxos forces the output-decoration path (batchDecorateOutputs).
	got, err := store.Get(ctx, parentHash, fields.Utxos)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Spend a 2-input tx so the batch carries >1 item and hits the bulk spend path.
	child := getSpendingTx(t, parent, 0, 2)
	_, err = store.Create(ctx, child, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)

	// SetLocked(false) on a single hash goes through the unlock batcher.
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parentHash}, true))
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parentHash}, false))

	// BatchSizeSnapshot reports per-batcher running averages.
	snap := store.BatchSizeSnapshot()
	require.NotNil(t, snap)

	// Close drains the batchers and releases the pool (idempotent with cleanup Stop).
	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, store.Close(closeCtx))
}

// TestConflictingChildrenAndIterator covers GetConflictingChildren,
// GetCounterConflicting, GetConflictingTxIterator (+ Next/Err/Close) and
// RemoveFromConflictingChildren, using the SetConflicting setup pattern.
func TestConflictingChildrenAndIterator(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	parent := testExtendedTx(t)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)
	parentHash := parent.TxIDChainHash()

	// A spending child of the parent's output 0.
	child := getSpendingTx(t, parent, 0)
	_, err = store.Create(ctx, child, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)
	childHash := child.TxIDChainHash()

	// Mark the parent conflicting — records the spending child in conflicting_children.
	_, childHashes, err := store.SetConflicting(ctx, []chainhash.Hash{*parentHash}, true)
	require.NoError(t, err)
	require.Len(t, childHashes, 1)

	// GetConflictingChildren walks spending children / conflicting_children.
	kids, err := store.GetConflictingChildren(ctx, *parentHash)
	require.NoError(t, err)
	require.Contains(t, hashStrings(kids), childHash.String())

	// GetCounterConflicting walks inputs to find counter-conflicting txs (no error).
	_, err = store.GetCounterConflicting(ctx, *childHash)
	require.NoError(t, err)

	// GetConflictingTxIterator yields the conflicting parent.
	it, err := store.GetConflictingTxIterator()
	require.NoError(t, err)
	require.NotNil(t, it)
	seen := drainIterator(ctx, t, it)
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Contains(t, seen, parentHash.String(), "iterator must yield the conflicting tx")

	// RemoveFromConflictingChildren scrubs the (parent, child) pair (idempotent).
	require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
		{ParentHash: parentHash, ChildHash: childHash},
	}))
}

// TestUnminedIteratorAndScan covers the unmined iterator (Next/Err/Close) and the
// no-op ScanInconsistentUnminedTxs.
func TestUnminedIteratorAndScan(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	unmined := newUnminedSingleOutputTx(t, store)

	// GetUnminedTxIterator reads from the pending_unmined side-table populated by
	// the write-behind projector; flush it so the created tx is projected before
	// the reload read.
	require.NoError(t, store.flushPendingUnmined(ctx))

	it, err := store.GetUnminedTxIterator()
	require.NoError(t, err)
	require.NotNil(t, it)
	seen := drainIterator(ctx, t, it)
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Contains(t, seen, unmined.TxIDChainHash().String())

	// Postgres has no full-scan inconsistency to repair: documented no-op.
	scanIt, err := store.ScanInconsistentUnminedTxs()
	require.NoError(t, err)
	require.Nil(t, scanIt)
}

// TestPreviousOutputsDecorateCoverage covers PreviousOutputsDecorate /
// BatchPreviousOutputsDecorate / batchDecorateOutputs by decorating a spending tx
// whose input references a stored parent output.
func TestPreviousOutputsDecorateCoverage(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	parent := makeThreeOutputTx(t)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	// A spending tx with one input referencing parent vout0, with its previous
	// output script cleared so decoration has something to fill in.
	spend := getSpendingTx(t, parent, 0)
	for _, in := range spend.Inputs {
		in.PreviousTxScript = nil
		in.PreviousTxSatoshis = 0
	}

	require.NoError(t, store.PreviousOutputsDecorate(ctx, spend))
	require.NotNil(t, spend.Inputs[0].PreviousTxScript, "input must be decorated with its prev output script")
}

// TestPrunerServiceAddObserver covers GetPrunerService + AddObserver.
func TestPrunerServiceAddObserver(t *testing.T) {
	store, _ := setupTestStore(t)

	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc)

	svc.AddObserver(&recordingObserver{})
}

// TestBatchDecorateOutputs covers the bulk BatchDecorate path (batchDecorateChunk →
// batchDecorateOutputs), which reconstructs outputs from raw_tx.
func TestBatchDecorateOutputs(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	tx := makeThreeOutputTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	items := []*utxo.UnresolvedMetaData{
		{Hash: *tx.TxIDChainHash(), Idx: 0, Fields: []fields.FieldName{fields.Tx}},
	}
	require.NoError(t, store.BatchDecorate(ctx, items, fields.Tx))
	require.NotNil(t, items[0].Data)
	require.NotNil(t, items[0].Data.Tx)
	require.Len(t, items[0].Data.Tx.Outputs, 3, "decorated tx must carry all 3 outputs")
}

// TestGetFieldCombinations exercises the getInternal decoration branches
// (needInputs / needOutputs / TxInpoints / Utxos) by requesting each field set.
func TestGetFieldCombinations(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	tx := makeThreeOutputTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)
	h := tx.TxIDChainHash()

	for _, f := range []fields.FieldName{
		fields.Tx, fields.Inputs, fields.Outputs, fields.Utxos, fields.TxInpoints, fields.BlockIDs,
	} {
		got, err := store.Get(ctx, h, f)
		require.NoError(t, err, "Get with field %s", f)
		require.NotNil(t, got, "Get with field %s", f)
	}
}

// TestSpendValidationFailures drives the diagnoseSpendFailure branches in direct
// mode by spending parents that are coinbase-immature, locked, or conflicting.
func TestSpendValidationFailures(t *testing.T) {
	t.Run("coinbase_immature", func(t *testing.T) {
		store, ctx := setupTestStore(t)
		require.NoError(t, store.SetBlockHeight(100))

		parent := testExtendedTx(t)
		_, err := store.Create(ctx, parent, 100, utxo.WithSetCoinbase(true))
		require.NoError(t, err)

		// CoinbaseMaturity is 1 in test settings, so the output matures at height 101;
		// spending at 100 is still immature.
		child := getSpendingTx(t, parent, 0)
		_, err = store.Spend(ctx, child, 100)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "got: %v", err)
	})

	t.Run("locked", func(t *testing.T) {
		store, ctx := setupTestStore(t)
		require.NoError(t, store.SetBlockHeight(100))

		parent := testExtendedTx(t)
		_, err := store.Create(ctx, parent, 100, utxo.WithLocked(true))
		require.NoError(t, err)

		child := getSpendingTx(t, parent, 0)
		_, err = store.Spend(ctx, child, 101)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxLocked), "got: %v", err)
	})

	t.Run("conflicting", func(t *testing.T) {
		store, ctx := setupTestStore(t)
		require.NoError(t, store.SetBlockHeight(100))

		parent := testExtendedTx(t)
		_, err := store.Create(ctx, parent, 100, utxo.WithConflicting(true))
		require.NoError(t, err)

		child := getSpendingTx(t, parent, 0)
		_, err = store.Spend(ctx, child, 101)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxConflicting), "got: %v", err)
	})
}

// TestBatchedBulkConcurrent fires many concurrent Create and Get calls so the
// batcher coalesces multiple items per batch, exercising the bulk multi-row loops
// in createBatched/sendCreateBatch and the bulk sendGetBatch path (including the
// block_ids population for a mined tx) rather than the single-item fast paths.
func TestBatchedBulkConcurrent(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))
	store.settings.UtxoStore.GetBatcherSize = 16
	store.Start(ctx)

	// A mined root so at least one fetched row carries block_ids (covers the
	// block_ids-population branch in the bulk get/decorate scanners).
	root := newMinedSingleOutputTx(t, store, 100)
	rootMeta, err := store.Get(ctx, root.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Contains(t, rootMeta.BlockIDs, uint32(100))

	const n = 32
	txs := make([]*bt.Tx, n)
	for i := range txs {
		txs[i] = getSpendingTx(t, root, 0) // random outputs → distinct txids
	}

	// Concurrent creates → multi-item create batches.
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range txs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _, errs[i] = store.Create(ctx, txs[i], 100) }(i)
	}
	wg.Wait()
	for i := range errs {
		require.NoError(t, errs[i], "concurrent create %d", i)
	}

	// Concurrent metadata-only gets → multi-item get batches (sendGetBatch bulk).
	for i := range txs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := store.Get(ctx, txs[i].TxIDChainHash(), fields.BlockIDs)
			errs[i] = e
		}(i)
	}
	wg.Wait()
	for i := range errs {
		require.NoError(t, errs[i], "concurrent get %d", i)
	}
}

// TestBatchedSpendBulkDispatch covers the bulk-path result dispatch in
// trySendSpendBatch (frozen / locked / conflicting / coinbase-immature / success)
// by spending, in batched mode, a single child whose inputs come from parents in
// each of those states — so one Spend enqueues several items that flush together
// and hit every branch.
func TestBatchedSpendBulkDispatch(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))
	store.Start(ctx)

	root := testExtendedTx(t)
	_, err := store.Create(ctx, root, 100)
	require.NoError(t, err)

	// Distinct parents (random outputs → distinct txids), each in a different state.
	mkParent := func(opts ...utxo.CreateOption) *bt.Tx {
		p := getSpendingTx(t, root, 0)
		_, cerr := store.Create(ctx, p, 100, opts...)
		require.NoError(t, cerr)
		return p
	}
	normal := mkParent()
	frozen := mkParent(utxo.WithFrozen(true))
	locked := mkParent(utxo.WithLocked(true))
	conflicting := mkParent(utxo.WithConflicting(true))
	coinbase := mkParent(utxo.WithSetCoinbase(true))

	// One child spending output 0 of every parent → batched into one bulk spend.
	child := bt.NewTx()
	for _, p := range []*bt.Tx{normal, frozen, locked, conflicting, coinbase} {
		require.NoError(t, child.From(
			p.TxIDChainHash().String(), 0,
			p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis,
		))
	}
	_ = child.PayToAddress(testSpendScript, 1000)
	for _, in := range child.Inputs {
		if in.UnlockingScript == nil || len(*in.UnlockingScript) == 0 {
			in.UnlockingScript = bscript.NewFromBytes([]byte{0x30, 0x44})
		}
	}

	// Spend at height 100: coinbase (matures at 101) is still immature. Multiple
	// inputs fail validation, so Spend returns an aggregate error — that's expected;
	// the point is to exercise every dispatch branch.
	_, err = store.Spend(ctx, child, 100)
	require.Error(t, err, "spend mixing frozen/locked/conflicting/immature inputs must fail")
}

// TestGetNotFound covers the TxNotFound branch of getInternal.
func TestGetNotFound(t *testing.T) {
	store, ctx := setupTestStore(t)
	var h chainhash.Hash
	h[0] = 0xCD
	_, err := store.Get(ctx, &h)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "got: %v", err)
}

// TestSpendDoubleSpendAndIdempotent covers the idempotent-retry and double-spend
// (different spender) branches of diagnoseSpendFailure.
func TestSpendDoubleSpendAndIdempotent(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	parent := testExtendedTx(t)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	childA := getSpendingTx(t, parent, 0)
	_, err = store.Create(ctx, childA, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, childA, 101)
	require.NoError(t, err)

	// Re-spending the same outpoint with the same spender is idempotent.
	_, err = store.Spend(ctx, childA, 101)
	require.NoError(t, err, "idempotent re-spend by the same spender must succeed")

	// A different spender of the same outpoint is a double-spend.
	childB := getSpendingTx(t, parent, 0)
	_, err = store.Create(ctx, childB, 100)
	require.NoError(t, err)
	_, err = store.Spend(ctx, childB, 101)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrSpent), "got: %v", err)
}

// TestBatchDecorateMinedBlockIDs covers the block_ids-population branch in the
// bulk decorate scanner (only taken for a mined tx).
func TestBatchDecorateMinedBlockIDs(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	mined := newMinedSingleOutputTx(t, store, 100)
	items := []*utxo.UnresolvedMetaData{
		{Hash: *mined.TxIDChainHash(), Idx: 0, Fields: []fields.FieldName{fields.Tx}},
	}
	require.NoError(t, store.BatchDecorate(ctx, items, fields.Tx))
	require.NotNil(t, items[0].Data)
	require.Contains(t, items[0].Data.BlockIDs, uint32(100))
}

// TestMarkTransactionsOnLongestChainMissing covers the not-found error branch.
func TestMarkTransactionsOnLongestChainMissing(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	var missing chainhash.Hash
	missing[0] = 0xEF
	err := store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{missing}, true)
	require.Error(t, err, "marking a non-existent tx must surface a not-found error")
}

// TestBatchedUnlockConcurrent fires concurrent single-hash SetLocked(false) calls so
// the unlock batcher coalesces multiple items, exercising the bulk sendUnlockBatch
// path rather than the single-item case.
func TestBatchedUnlockConcurrent(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))
	store.Start(ctx)

	root := testExtendedTx(t)
	_, err := store.Create(ctx, root, 100)
	require.NoError(t, err)

	const n = 16
	hashes := make([]chainhash.Hash, n)
	for i := range hashes {
		tx := getSpendingTx(t, root, 0) // distinct random tx
		_, cerr := store.Create(ctx, tx, 100, utxo.WithLocked(true))
		require.NoError(t, cerr)
		hashes[i] = *tx.TxIDChainHash()
	}

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range hashes {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = store.SetLocked(ctx, []chainhash.Hash{hashes[i]}, false) }(i)
	}
	wg.Wait()
	for i := range errs {
		require.NoError(t, errs[i], "concurrent unlock %d", i)
	}
}

// TestSpendIgnoreFlags covers the IgnoreLocked / IgnoreConflicting success paths:
// a locked or conflicting parent IS spendable when the caller passes the flag.
func TestSpendIgnoreFlags(t *testing.T) {
	t.Run("ignore_locked", func(t *testing.T) {
		store, ctx := setupTestStore(t)
		require.NoError(t, store.SetBlockHeight(100))

		parent := testExtendedTx(t)
		_, err := store.Create(ctx, parent, 100, utxo.WithLocked(true))
		require.NoError(t, err)

		child := getSpendingTx(t, parent, 0)
		_, err = store.Spend(ctx, child, 101, utxo.IgnoreFlags{IgnoreLocked: true})
		require.NoError(t, err, "IgnoreLocked must allow spending a locked parent")
	})

	t.Run("ignore_conflicting", func(t *testing.T) {
		store, ctx := setupTestStore(t)
		require.NoError(t, store.SetBlockHeight(100))

		parent := testExtendedTx(t)
		_, err := store.Create(ctx, parent, 100, utxo.WithConflicting(true))
		require.NoError(t, err)

		child := getSpendingTx(t, parent, 0)
		_, err = store.Spend(ctx, child, 101, utxo.IgnoreFlags{IgnoreConflicting: true})
		require.NoError(t, err, "IgnoreConflicting must allow spending a conflicting parent")
	})
}

// TestUnspendFlagAsLocked covers Unspend's optional lock-the-parents branch.
func TestUnspendFlagAsLocked(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	parent := testExtendedTx(t)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := getSpendingTx(t, parent, 0)
	_, err = store.Create(ctx, child, 100)
	require.NoError(t, err)
	spends, err := store.Spend(ctx, child, 101)
	require.NoError(t, err)

	// Unspend with flagAsLocked=true reverses the spend AND locks the parent.
	require.NoError(t, store.Unspend(ctx, spends, true))

	got, err := store.Get(ctx, parent.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked, "Unspend(flagAsLocked=true) must lock the parent")
}

// TestEmptyInputGuards covers the len==0 early-return guards.
func TestEmptyInputGuards(t *testing.T) {
	store, ctx := setupTestStore(t)

	require.NoError(t, store.Unspend(ctx, nil))
	require.NoError(t, store.PreserveTransactions(ctx, nil, 100))
	require.NoError(t, store.RemoveFromConflictingChildren(ctx, nil))
	require.NoError(t, store.BatchDecorate(ctx, nil))
	require.NoError(t, store.SetLocked(ctx, nil, true))
	_, _, err := store.SetConflicting(ctx, nil, true)
	require.NoError(t, err)
}

// TestOperationsOnClosedPoolReturnErrors is a resilience test: with the connection
// pool closed under it, every store operation must return an error (not panic). It
// also exercises the otherwise-hard-to-reach storage-error branches across the store.
func TestOperationsOnClosedPoolReturnErrors(t *testing.T) {
	store, ctx := setupTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	// Well-formed arguments so each op reaches its DB call rather than failing on
	// input validation first.
	tx := testExtendedTx(t)
	child := getSpendingTx(t, tx, 0)
	h := tx.TxIDChainHash()
	spends := []*utxo.Spend{{
		TxID:         h,
		Vout:         0,
		UTXOHash:     h,
		SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0),
	}}

	// Take the pool down. (pgxpool.Close is idempotent, so the cleanup Stop is safe.)
	store.pool.Close()

	requireErr := func(name string, err error) {
		require.Error(t, err, "%s must return an error when the pool is closed", name)
	}

	_, err := store.Create(ctx, tx, 100)
	requireErr("Create", err)
	_, err = store.Get(ctx, h)
	requireErr("Get", err)
	_, err = store.Spend(ctx, child, 101)
	requireErr("Spend", err)
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*h}, true)
	requireErr("SetConflicting", err)
	requireErr("Unspend", store.Unspend(ctx, spends))
	requireErr("PreserveTransactions", store.PreserveTransactions(ctx, []chainhash.Hash{*h}, 200))
	requireErr("ProcessExpiredPreservations", store.ProcessExpiredPreservations(ctx, 200))
	requireErr("SetLocked", store.SetLocked(ctx, []chainhash.Hash{*h}, true))
	requireErr("RemoveFromConflictingChildren", store.RemoveFromConflictingChildren(ctx,
		[]utxo.ConflictingChildRemoval{{ParentHash: h, ChildHash: child.TxIDChainHash()}}))
	requireErr("FreezeUTXOs", store.FreezeUTXOs(ctx, spends, nil))
	requireErr("BatchDecorate", store.BatchDecorate(ctx,
		[]*utxo.UnresolvedMetaData{{Hash: *h, Fields: []fields.FieldName{fields.Tx}}}))
	_, err = store.GetUnminedTxIterator()
	requireErr("GetUnminedTxIterator", err)
	_, err = store.GetConflictingTxIterator()
	requireErr("GetConflictingTxIterator", err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{h}, utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1})
	requireErr("SetMinedMulti", err)
	requireErr("Delete", store.Delete(ctx, h))
	requireErr("UnFreezeUTXOs", store.UnFreezeUTXOs(ctx, spends, nil))
	// Clear the prev-output script so decoration actually queries the (closed) pool
	// instead of short-circuiting on an already-decorated input.
	child.Inputs[0].PreviousTxScript = nil
	requireErr("PreviousOutputsDecorate", store.PreviousOutputsDecorate(ctx, child))
	_, err = store.QueryOldUnminedTransactions(ctx, 200)
	requireErr("QueryOldUnminedTransactions", err)
	_, err = store.GetCounterConflicting(ctx, *h)
	requireErr("GetCounterConflicting", err)
	_, err = store.GetConflictingChildren(ctx, *h)
	requireErr("GetConflictingChildren", err)
	requireErr("MarkTransactionsOnLongestChain", store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*h}, true))
}

// --- test helpers ---

type recordingObserver struct{ calls int }

func (o *recordingObserver) OnPruneComplete(_ uint32, _ int64) { o.calls++ }

func hashStrings(hs []chainhash.Hash) []string {
	out := make([]string, 0, len(hs))
	for i := range hs {
		out = append(out, hs[i].String())
	}
	return out
}

// drainIterator pulls every batch from an unmined/conflicting iterator and returns
// the set of tx-hash strings it yielded.
func drainIterator(ctx context.Context, t *testing.T, it utxo.UnminedTxIterator) []string {
	t.Helper()
	var seen []string
	for {
		batch, err := it.Next(ctx)
		require.NoError(t, err)
		if len(batch) == 0 {
			break
		}
		for _, u := range batch {
			seen = append(seen, u.Hash.String())
		}
	}
	return seen
}
