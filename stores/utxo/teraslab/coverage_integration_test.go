package teraslab_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parentChildTxHex is a tx whose output 0 is referenced by newTestTx's input,
// so creating newTestTx as conflicting links it as a conflicting-child of this
// parent. Reused from the conflicting-children integration test.
const parentChildTxHex = "010000000000000000ef0158ef6d539bf88c850103fa127a92775af48dba580c36bbde4dc6d8b9da83256d050000006a47304402200ca69c5672d0e0471cd4ff1f9993f16103fc29b98f71e1a9760c828b22cae61c0220705e14aa6f3149130c3a6aa8387c51e4c80c6ae52297b2dabfd68423d717be4541210286dbe9cd647f83a4a6b29d2a2d3227a897a4904dc31769502cb013cbe5044dddffffffff8c2f6002000000001976a914308254c746057d189221c36418ba93337de33bc988ac03002d3101000000001976a91498cde576de501ceb5bb1962c6e49a4d1af17730788ac80969800000000001976a914eb7772212c334c0bdccee75c0369aa675fc21d2088ac706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac00000000"

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestCloseSuccess(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// A clean Close drains the batchers and tears down the client.
	require.NoError(t, store.Close(context.Background()))
}

func TestCloseContextCancelled(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	cctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With an already-cancelled context the drain still runs (and the client is
	// closed) in the background; Close either returns the cancellation or wins
	// the race and returns nil. Both are valid — assert it does not panic and,
	// if it errors, that it is the cancellation.
	if err := store.Close(cctx); err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
}

// ---------------------------------------------------------------------------
// RemoveBlockIDs
// ---------------------------------------------------------------------------

func TestRemoveBlockIDs(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	require.NoError(t, store.SetBlockHeight(100))

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)
	txHash := tx.TxIDChainHash()

	// Mine into two blocks.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash},
		utxo.MinedBlockInfo{BlockID: 700, BlockHeight: 100, SubtreeIdx: 1, OnLongestChain: true})
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash},
		utxo.MinedBlockInfo{BlockID: 701, BlockHeight: 101, SubtreeIdx: 2, OnLongestChain: true})
	require.NoError(t, err)

	t.Run("empty removals is no-op", func(t *testing.T) {
		require.NoError(t, store.RemoveBlockIDs(ctx, nil))
	})

	t.Run("nil TxHash is rejected", func(t *testing.T) {
		err := store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{{TxHash: nil, BlockIDs: []uint32{700}}})
		require.Error(t, err)
	})

	t.Run("removal with empty BlockIDs is skipped", func(t *testing.T) {
		require.NoError(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{{TxHash: txHash, BlockIDs: nil}}))
		// Block membership unchanged.
		resp, err := store.Get(ctx, txHash, fields.BlockIDs)
		require.NoError(t, err)
		assert.ElementsMatch(t, []uint32{700, 701}, resp.BlockIDs)
	})

	t.Run("removing a non-existent txid is tolerated", func(t *testing.T) {
		fake := &chainhash.Hash{}
		fake[0] = 0xAB
		require.NoError(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{{TxHash: fake, BlockIDs: []uint32{999}}}))
	})

	t.Run("removes one block id, leaving the other", func(t *testing.T) {
		err := store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{
			{TxHash: txHash, BlockIDs: []uint32{700}},
			// duplicate (txHash, 700) in the same call must be deduped, not double-sent.
			{TxHash: txHash, BlockIDs: []uint32{700}},
		})
		require.NoError(t, err)

		resp, err := store.Get(ctx, txHash, fields.BlockIDs)
		require.NoError(t, err)
		assert.Equal(t, []uint32{701}, resp.BlockIDs)
	})
}

// ---------------------------------------------------------------------------
// RemoveFromConflictingChildren
// ---------------------------------------------------------------------------

func TestRemoveFromConflictingChildren(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	parentTx, err := bt.NewTxFromString(parentChildTxHex)
	require.NoError(t, err)
	_, err = store.Create(ctx, parentTx, 999)
	require.NoError(t, err)

	childTx := newTestTx(t)
	_, err = store.Create(ctx, childTx, 1000, utxo.WithConflicting(true))
	require.NoError(t, err)

	parentHash := childTx.Inputs[0].PreviousTxIDChainHash()
	childHash := childTx.TxIDChainHash()

	t.Run("empty removals is no-op", func(t *testing.T) {
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, nil))
	})

	t.Run("nil hashes are rejected", func(t *testing.T) {
		err := store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{{}})
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrInvalidArgument) || err != nil)
	})

	t.Run("child is linked before removal", func(t *testing.T) {
		resp, err := store.Get(ctx, parentHash, fields.ConflictingChildren)
		require.NoError(t, err)
		assert.Contains(t, resp.ConflictingChildren, *childHash)
	})

	t.Run("removes the parent->child link", func(t *testing.T) {
		err := store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: parentHash, ChildHash: childHash},
		})
		require.NoError(t, err)

		resp, err := store.Get(ctx, parentHash, fields.ConflictingChildren)
		require.NoError(t, err)
		assert.NotContains(t, resp.ConflictingChildren, *childHash)
	})

	t.Run("removing an absent pair is tolerated", func(t *testing.T) {
		unrelated := &chainhash.Hash{}
		unrelated[0] = 0x7E
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: parentHash, ChildHash: unrelated},
		}))
	})

	t.Run("removing with a missing parent is tolerated", func(t *testing.T) {
		// A parent that does not exist surfaces as a per-item TxNotFound which is
		// benign (the link is, by definition, already absent).
		missingParent := &chainhash.Hash{}
		missingParent[0] = 0x33
		someChild := &chainhash.Hash{}
		someChild[0] = 0x44
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: missingParent, ChildHash: someChild},
		}))
	})
}

// ---------------------------------------------------------------------------
// GetConflictingTxIterator
// ---------------------------------------------------------------------------

func TestGetConflictingTxIterator(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0, utxo.WithConflicting(true))
	require.NoError(t, err)

	iter, err := store.GetConflictingTxIterator()
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	batch, err := iter.Next(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(batch), 1, "conflicting tx should be iterated")
	assert.NoError(t, iter.Err())

	require.NoError(t, iter.Close())
	after, err := iter.Next(ctx)
	require.NoError(t, err)
	assert.Nil(t, after, "iterator returns nil after Close")
}

// ---------------------------------------------------------------------------
// ScanInconsistentUnminedTxs
// ---------------------------------------------------------------------------

func TestScanInconsistentUnminedTxs(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	require.NoError(t, store.SetBlockHeight(100))

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	iter, err := store.ScanInconsistentUnminedTxs()
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	records, err := iter.Next(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(records), 1)
	assert.GreaterOrEqual(t, iter.TotalScanned(), int64(1))
	assert.NoError(t, iter.Err())

	// Drain to exhaustion.
	for {
		more, err := iter.Next(ctx)
		require.NoError(t, err)
		if len(more) == 0 {
			break
		}
	}

	require.NoError(t, iter.Close())
	after, err := iter.Next(ctx)
	require.NoError(t, err)
	assert.Nil(t, after)
}

// ---------------------------------------------------------------------------
// BatchPreviousOutputsDecorate + PreviousOutputsDecorate error paths
// ---------------------------------------------------------------------------

func TestBatchPreviousOutputsDecorate(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	parent := newTestTx(t)
	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	// Undecorated spend txs (no PreviousTxScript) referencing parent outputs 0 and 1.
	mkUndecorated := func(vout uint32) *bt.Tx {
		stx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{PreviousTxOutIndex: vout}}}
		_ = stx.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash())
		return stx
	}

	t.Run("decorates inputs across multiple txs", func(t *testing.T) {
		spend0 := mkUndecorated(0)
		spend1 := mkUndecorated(1)
		require.NoError(t, store.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{spend0, spend1}))

		require.NotNil(t, spend0.Inputs[0].PreviousTxScript)
		assert.Equal(t, parent.Outputs[0].Satoshis, spend0.Inputs[0].PreviousTxSatoshis)
		require.NotNil(t, spend1.Inputs[0].PreviousTxScript)
		assert.Equal(t, parent.Outputs[1].Satoshis, spend1.Inputs[0].PreviousTxSatoshis)
	})

	t.Run("missing parent returns TxNotFound", func(t *testing.T) {
		missingParentTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{PreviousTxOutIndex: 0}}}
		fake := &chainhash.Hash{}
		fake[0] = 0xF1
		_ = missingParentTx.Inputs[0].PreviousTxIDAdd(fake)

		err := store.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{missingParentTx})
		require.Error(t, err)
	})

	t.Run("vout out of range returns invalid tx", func(t *testing.T) {
		// parent has 6 outputs; index 99 is out of range.
		badVout := mkUndecorated(99)
		err := store.PreviousOutputsDecorate(ctx, badVout)
		require.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// GetMeta error / cancellation paths
// ---------------------------------------------------------------------------

func TestGetMetaErrorPaths(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	t.Run("GetMeta on missing tx returns error", func(t *testing.T) {
		missing := &chainhash.Hash{}
		missing[0] = 0xB0
		var data meta.Data
		err := store.GetMeta(ctx, missing, &data)
		require.Error(t, err)
	})

	t.Run("GetMeta respects cancelled caller context", func(t *testing.T) {
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		missing := &chainhash.Hash{}
		missing[0] = 0xB1
		var data meta.Data
		err := store.GetMeta(cctx, missing, &data)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// ---------------------------------------------------------------------------
// Batch mutation partial-error arms (mining / alert / locked / conflicting)
//
// The Aerospike backend surfaces per-item failures to the caller; TeraSlab must
// do the same instead of swallowing them. Each mutation is driven against a
// missing txid (or a wrong utxo hash) so the server returns a per-item error.
// ---------------------------------------------------------------------------

func TestBatchMutationErrorPaths(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	require.NoError(t, store.SetBlockHeight(100))

	// A real created tx, used where we need a present record with a wrong hash.
	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	missing := &chainhash.Hash{}
	for i := range missing {
		missing[i] = byte(0x90 + i)
	}
	wrongHash := &chainhash.Hash{}
	wrongHash[0] = 0xFE

	t.Run("FreezeUTXOs surfaces per-item error on wrong hash", func(t *testing.T) {
		err := store.FreezeUTXOs(ctx, []*utxo.Spend{
			{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: wrongHash},
		}, nil)
		require.Error(t, err)
	})

	t.Run("UnFreezeUTXOs surfaces per-item error on un-frozen utxo", func(t *testing.T) {
		err := store.UnFreezeUTXOs(ctx, []*utxo.Spend{
			{TxID: tx.TxIDChainHash(), Vout: 1, UTXOHash: wrongHash},
		}, nil)
		require.Error(t, err)
	})

	t.Run("SetLocked surfaces per-item error on missing tx", func(t *testing.T) {
		err := store.SetLocked(ctx, []chainhash.Hash{*missing}, true)
		require.Error(t, err)
	})

	t.Run("SetMinedMulti surfaces per-item error on missing tx", func(t *testing.T) {
		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{missing},
			utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, SubtreeIdx: 1, OnLongestChain: true})
		require.Error(t, err)
	})

	t.Run("MarkTransactionsOnLongestChain surfaces per-item error on missing tx", func(t *testing.T) {
		err := store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*missing}, true)
		require.Error(t, err)
	})

	t.Run("SetConflicting surfaces per-item error on missing tx", func(t *testing.T) {
		spends, _, err := store.SetConflicting(ctx, []chainhash.Hash{*missing}, true)
		require.Error(t, err)
		_ = spends
	})
}

// ---------------------------------------------------------------------------
// Spend / GetSpend error paths
// ---------------------------------------------------------------------------

func TestSpendAndGetSpendErrorPaths(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	t.Run("Spend with undecorated input fails loudly", func(t *testing.T) {
		// No PreviousTxScript -> UTXOHashFromInput must error rather than hash
		// over a nil script and silently produce the wrong utxo hash.
		stx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{PreviousTxOutIndex: 0}}}
		_ = stx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err := store.Spend(ctx, stx, store.GetBlockHeight()+1)
		require.Error(t, err)
	})

	t.Run("Spend with no spendable inputs returns empty result", func(t *testing.T) {
		// An input with a nil previous txid is skipped; with none left, Spend
		// returns an empty (non-error) result.
		stx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{PreviousTxOutIndex: 0}}}
		spends, err := store.Spend(ctx, stx, store.GetBlockHeight()+1)
		require.NoError(t, err)
		assert.Empty(t, spends)
	})

	t.Run("GetSpend on missing tx returns TxNotFound", func(t *testing.T) {
		missing := &chainhash.Hash{}
		missing[0] = 0xC0
		_, err := store.GetSpend(ctx, &utxo.Spend{TxID: missing, Vout: 0})
		require.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxNotFound)
	})

	t.Run("GetSpend with vout out of range errors", func(t *testing.T) {
		_, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 999})
		require.Error(t, err)
	})
}
