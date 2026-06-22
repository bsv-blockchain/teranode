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
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testTx is a shared transaction hex used across integration tests.
const testTxHex = "010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000"

func newTestTx(t *testing.T) *bt.Tx {
	t.Helper()
	tx, err := bt.NewTxFromString(testTxHex)
	require.NoError(t, err)
	return tx
}

func makeSpendTx(t *testing.T, parentTx *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()
	spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: vout,
		PreviousTxScript:   parentTx.Outputs[vout].LockingScript,
		PreviousTxSatoshis: parentTx.Outputs[vout].Satoshis,
	}}, Outputs: []*bt.Output{{
		Satoshis:      parentTx.Outputs[vout].Satoshis,
		LockingScript: parentTx.Outputs[vout].LockingScript,
	}}}
	_ = spendTx.Inputs[0].PreviousTxIDAdd(parentTx.TxIDChainHash())
	return spendTx
}

// ---------------------------------------------------------------------------
// GetMeta
// ---------------------------------------------------------------------------

func TestGetMeta(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	var data meta.Data
	err = store.GetMeta(ctx, tx.TxIDChainHash(), &data)
	require.NoError(t, err)
	assert.Equal(t, uint64(215), data.Fee)
	assert.Equal(t, uint64(328), data.SizeInBytes)
	assert.False(t, data.IsCoinbase)
	// GetMeta returns metadata only — it must not reconstruct the full tx body.
	assert.Nil(t, data.Tx, "GetMeta must not populate data.Tx")
}

// ---------------------------------------------------------------------------
// GetMeta with specific fields
// ---------------------------------------------------------------------------

func TestGetWithSpecificFields(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	t.Run("only fee", func(t *testing.T) {
		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Fee)
		require.NoError(t, err)
		assert.Equal(t, uint64(215), resp.Fee)
	})

	t.Run("only IsCoinbase via Flags", func(t *testing.T) {
		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.IsCoinbase)
		require.NoError(t, err)
		assert.False(t, resp.IsCoinbase)
	})

	t.Run("only Conflicting", func(t *testing.T) {
		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		assert.False(t, resp.Conflicting)
	})

	t.Run("only LockTime", func(t *testing.T) {
		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.LockTime)
		require.NoError(t, err)
		assert.Equal(t, tx.LockTime, resp.LockTime)
	})

	t.Run("multiple fields", func(t *testing.T) {
		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Fee, fields.SizeInBytes, fields.IsCoinbase)
		require.NoError(t, err)
		assert.Equal(t, uint64(215), resp.Fee)
		assert.Equal(t, uint64(328), resp.SizeInBytes)
		assert.False(t, resp.IsCoinbase)
	})
}

// ---------------------------------------------------------------------------
// SetConflicting / GetConflictingChildren / GetCounterConflicting
// ---------------------------------------------------------------------------

func TestSetConflictingDirect(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	t.Run("SetConflicting marks tx as conflicting", func(t *testing.T) {
		_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
		require.NoError(t, err)

		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		assert.True(t, resp.Conflicting)
	})

	t.Run("SetConflicting clears conflicting", func(t *testing.T) {
		_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false)
		require.NoError(t, err)

		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		assert.False(t, resp.Conflicting)
	})

	t.Run("SetConflicting empty list", func(t *testing.T) {
		_, _, err := store.SetConflicting(ctx, []chainhash.Hash{}, true)
		require.NoError(t, err)
	})
}

func TestGetConflictingChildren(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	parentTx, _ := bt.NewTxFromString("010000000000000000ef0158ef6d539bf88c850103fa127a92775af48dba580c36bbde4dc6d8b9da83256d050000006a47304402200ca69c5672d0e0471cd4ff1f9993f16103fc29b98f71e1a9760c828b22cae61c0220705e14aa6f3149130c3a6aa8387c51e4c80c6ae52297b2dabfd68423d717be4541210286dbe9cd647f83a4a6b29d2a2d3227a897a4904dc31769502cb013cbe5044dddffffffff8c2f6002000000001976a914308254c746057d189221c36418ba93337de33bc988ac03002d3101000000001976a91498cde576de501ceb5bb1962c6e49a4d1af17730788ac80969800000000001976a914eb7772212c334c0bdccee75c0369aa675fc21d2088ac706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac00000000")
	_, err := store.Create(ctx, parentTx, 999)
	require.NoError(t, err)

	childTx := newTestTx(t)
	_, err = store.Create(ctx, childTx, 1000, utxo.WithConflicting(true))
	require.NoError(t, err)

	t.Run("GetConflictingChildren returns child", func(t *testing.T) {
		children, err := store.GetConflictingChildren(ctx, *childTx.Inputs[0].PreviousTxIDChainHash())
		require.NoError(t, err)
		require.Len(t, children, 1)
		assert.Equal(t, *childTx.TxIDChainHash(), children[0])
	})

	t.Run("GetCounterConflicting returns spenders of a shared parent UTXO", func(t *testing.T) {
		// Counter-conflicting txs are those spending the SAME parent UTXOs as
		// the queried tx (double-spend siblings) — NOT the queried tx's own
		// conflicting children. Build a tx that spends parentTx output 0, store
		// and spend it, then query its counter-conflicting set: it must include
		// the spender itself (the recorded spender of parentTx[0]).
		//
		// (The prior assertion queried the parent and expected its children,
		// which conflated counter-conflicting with conflicting-children — an
		// artifact of the old hand-rolled implementation.)
		spenderTx := bt.NewTx()
		require.NoError(t, spenderTx.From(
			parentTx.TxIDChainHash().String(), 0,
			parentTx.Outputs[0].LockingScript.String(), parentTx.Outputs[0].Satoshis))
		require.NoError(t, spenderTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, err := store.Create(ctx, spenderTx, 1001)
		require.NoError(t, err)
		_, err = store.Spend(ctx, spenderTx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		counter, err := store.GetCounterConflicting(ctx, *spenderTx.TxIDChainHash())
		require.NoError(t, err)
		assert.Contains(t, counter, *spenderTx.TxIDChainHash())
	})

	t.Run("GetConflictingChildren on non-parent returns empty", func(t *testing.T) {
		children, err := store.GetConflictingChildren(ctx, *childTx.TxIDChainHash())
		require.NoError(t, err)
		assert.Len(t, children, 0)
	})
}

// ---------------------------------------------------------------------------
// PreviousOutputsDecorate
// ---------------------------------------------------------------------------

func TestPreviousOutputsDecorate(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	spendTx := makeSpendTx(t, tx, 0)
	err = store.PreviousOutputsDecorate(ctx, spendTx)
	require.NoError(t, err)

	t.Run("nil tx is no-op", func(t *testing.T) {
		err := store.PreviousOutputsDecorate(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("tx with no inputs is no-op", func(t *testing.T) {
		err := store.PreviousOutputsDecorate(ctx, &bt.Tx{})
		require.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// Unspend with flagAsLocked
// ---------------------------------------------------------------------------

func TestUnspendWithFlagAsLocked(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxoHash0, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	spendTx := makeSpendTx(t, tx, 0)

	_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)

	// Unspend with flagAsLocked=true
	spends := []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spend.NewSpendingData(spendTx.TxIDChainHash(), 0),
	}}
	err = store.Unspend(ctx, spends, true)
	require.NoError(t, err)

	// The tx should be locked after unspend
	resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	assert.True(t, resp.Locked)
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

func TestGetUnminedTxIterator(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	err := store.SetBlockHeight(100)
	require.NoError(t, err)

	tx := newTestTx(t)
	_, err = store.Create(context.Background(), tx, 100)
	require.NoError(t, err)

	t.Run("full scan iterator", func(t *testing.T) {
		iter, err := store.GetUnminedTxIterator()
		require.NoError(t, err)
		require.NotNil(t, iter)
		defer iter.Close()

		batch, err := iter.Next(context.Background())
		require.NoError(t, err)
		// Should return at least our unmined tx
		require.GreaterOrEqual(t, len(batch), 1)
		assert.Nil(t, iter.Err())

		// The embedded *subtree.Node must be populated — block assembly reads the
		// promoted Hash (and Fee/SizeInBytes) and nil-panics otherwise.
		ut := batch[0]
		require.NotNil(t, ut.Node, "UnminedTransaction.Node must be set")
		assert.Equal(t, tx.TxIDChainHash().String(), ut.Hash.String())
		// TxInpoints must be non-nil and carry the tx's parents — the pruner uses
		// ParentTxHashes to preserve parents of unmined txs.
		require.NotNil(t, ut.TxInpoints, "UnminedTransaction.TxInpoints must be set")
		assert.NotEmpty(t, ut.TxInpoints.ParentTxHashes, "TxInpoints must carry parent hashes")
	})

	t.Run("prunable iterator with matching cutoff", func(t *testing.T) {
		iter, err := store.GetPrunableUnminedTxIterator(200)
		require.NoError(t, err)
		require.NotNil(t, iter)
		defer iter.Close()

		batch, err := iter.Next(context.Background())
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(batch), 1)
	})

	t.Run("prunable iterator with low cutoff returns empty", func(t *testing.T) {
		iter, err := store.GetPrunableUnminedTxIterator(50)
		require.NoError(t, err)
		require.NotNil(t, iter)
		defer iter.Close()

		batch, err := iter.Next(context.Background())
		require.NoError(t, err)
		assert.Len(t, batch, 0)
	})

	t.Run("iterator exhaustion returns nil", func(t *testing.T) {
		iter, err := store.GetPrunableUnminedTxIterator(50)
		require.NoError(t, err)
		defer iter.Close()

		// First call returns empty (no matching txs)
		batch, err := iter.Next(context.Background())
		require.NoError(t, err)
		assert.Len(t, batch, 0)

		// Second call should also return nil (exhausted)
		batch, err = iter.Next(context.Background())
		require.NoError(t, err)
		assert.Nil(t, batch)
	})

	t.Run("close then next returns nil", func(t *testing.T) {
		iter, err := store.GetUnminedTxIterator()
		require.NoError(t, err)

		err = iter.Close()
		require.NoError(t, err)

		batch, err := iter.Next(context.Background())
		require.NoError(t, err)
		assert.Nil(t, batch)
	})
}

// ---------------------------------------------------------------------------
// PreserveTransactions with actual data
// ---------------------------------------------------------------------------

func TestPreserveTransactionsWithData(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	err := store.SetBlockHeight(100)
	require.NoError(t, err)

	tx := newTestTx(t)
	_, err = store.Create(ctx, tx, 100)
	require.NoError(t, err)

	err = store.PreserveTransactions(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, 500)
	require.NoError(t, err)

	// Verify the tx still exists after preservation
	resp, err := store.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

// ---------------------------------------------------------------------------
// Spend multiple outputs of same tx
// ---------------------------------------------------------------------------

func TestSpendMultipleOutputs(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// Spend outputs 0 and 1 in one tx
	spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{
		{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   tx.Outputs[0].LockingScript,
			PreviousTxSatoshis: tx.Outputs[0].Satoshis,
		},
		{
			PreviousTxOutIndex: 1,
			PreviousTxScript:   tx.Outputs[1].LockingScript,
			PreviousTxSatoshis: tx.Outputs[1].Satoshis,
		},
	}, Outputs: []*bt.Output{{
		Satoshis:      tx.Outputs[0].Satoshis + tx.Outputs[1].Satoshis,
		LockingScript: tx.Outputs[0].LockingScript,
	}}}
	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
	_ = spendTx.Inputs[1].PreviousTxIDAdd(tx.TxIDChainHash())

	spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)
	assert.Len(t, spends, 2)

	// Verify both are spent
	utxoHash0, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	utxoHash1, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[1], 1)

	resp0, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0})
	require.NoError(t, err)
	assert.Equal(t, int(utxo.Status_SPENT), resp0.Status)

	resp1, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 1, UTXOHash: utxoHash1})
	require.NoError(t, err)
	assert.Equal(t, int(utxo.Status_SPENT), resp1.Status)
}

// ---------------------------------------------------------------------------
// Batch operations with many transactions
// ---------------------------------------------------------------------------

func TestBatchCreateAndGet(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	// Create 20 unique transactions
	txs := make([]*bt.Tx, 20)
	for i := 0; i < 20; i++ {
		stx := bt.NewTx()
		require.NoError(t, stx.FromUTXOs(&bt.UTXO{
			TxIDHash:      newTestTx(t).TxIDChainHash(),
			Vout:          0,
			LockingScript: newTestTx(t).Inputs[0].PreviousTxScript,
			Satoshis:      newTestTx(t).Inputs[0].PreviousTxSatoshis,
		}))
		err := stx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(i+1)*1000)
		require.NoError(t, err)
		txs[i] = stx

		_, err = store.Create(ctx, stx, 100)
		require.NoError(t, err)
	}

	// BatchDecorate all 20
	items := make([]*utxo.UnresolvedMetaData, len(txs))
	for i, tx := range txs {
		items[i] = &utxo.UnresolvedMetaData{Hash: *tx.TxIDChainHash(), Idx: i}
	}

	err := store.BatchDecorate(ctx, items, fields.Fee, fields.SizeInBytes)
	require.NoError(t, err)

	for i, item := range items {
		require.NotNil(t, item.Data, "item %d should have data", i)
		assert.Greater(t, item.Data.Fee, uint64(0), "item %d should have non-zero fee", i)
	}
}

// ---------------------------------------------------------------------------
// Freeze then spend returns ErrFrozen, not ErrUtxoError at top level
// ---------------------------------------------------------------------------

func TestFreezeSpendErrorType(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxoHash0, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	err = store.FreezeUTXOs(ctx, []*utxo.Spend{{
		TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0,
	}}, tSettings)
	require.NoError(t, err)

	spendTx := makeSpendTx(t, tx, 0)
	spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.Len(t, spends, 1)
	require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)
}

// ---------------------------------------------------------------------------
// Delete non-existent tx is not an error
// ---------------------------------------------------------------------------

func TestDeleteNonExistent(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	fakeHash := &chainhash.Hash{}
	fakeHash[0] = 0xDE
	fakeHash[1] = 0xAD

	err := store.Delete(ctx, fakeHash)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Create with locked flag
// ---------------------------------------------------------------------------

func TestCreateWithLocked(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0, utxo.WithLocked(true))
	require.NoError(t, err)

	resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	assert.True(t, resp.Locked)

	// Spending locked tx should fail
	spendTx := makeSpendTx(t, tx, 0)
	_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// SetMined then Get block entries with specific field
// ---------------------------------------------------------------------------

func TestSetMinedThenGetBlockEntries(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	err := store.SetBlockHeight(100)
	require.NoError(t, err)

	tx := newTestTx(t)
	_, err = store.Create(ctx, tx, 0)
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 999, BlockHeight: 100, SubtreeIdx: 5, OnLongestChain: true})
	require.NoError(t, err)

	// Get only block IDs
	resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Len(t, resp.BlockIDs, 1)
	assert.Equal(t, uint32(999), resp.BlockIDs[0])
}
