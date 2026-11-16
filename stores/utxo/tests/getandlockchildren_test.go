// Package tests provides comprehensive tests for the GetAndLockChildren function.
//
// This test suite validates the GetAndLockChildren function that recursively finds
// and locks all child transactions for a given transaction. The tests ensure that:
//
// 1. The function correctly handles the coinbase placeholder hash (frozen transactions)
// 2. Transactions with no children are handled properly
// 3. Single child relationships are tracked and locked
// 4. Multiple children and grandchildren are recursively processed
// 5. Error conditions are handled gracefully
// 6. Already locked transactions don't cause failures
// 7. Empty spending data is handled correctly
//
// The function is critical for conflicting transaction processing as it ensures
// that all related transactions in a chain are properly locked before processing
// conflicts to maintain consistency.
package tests

import (
	"context"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupSQLiteMemoryStore(ctx context.Context, t *testing.T) utxo.Store {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	return utxoStore
}

func TestGetAndLockChildren(t *testing.T) {
	ctx := context.Background()

	t.Run("should return error for coinbase placeholder hash", func(t *testing.T) {
		store := setupSQLiteMemoryStore(ctx, t)

		children, err := utxo.GetAndLockChildren(ctx, store, subtree.CoinbasePlaceholderHashValue)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tx is frozen")
		assert.Nil(t, children)
	})

	t.Run("should handle transaction with no children", func(t *testing.T) {
		store := setupSQLiteMemoryStore(ctx, t)

		// Create a simple transaction with no spending children
		tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
			"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
			"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
			"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
			"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
			"2f6b52de3d7c88ac00000000")
		require.NoError(t, err)

		// Create the transaction in store
		_, _, err = store.SpendAndCreate(ctx, tx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		txHash := *tx.TxIDChainHash()
		children, err := utxo.GetAndLockChildren(ctx, store, txHash)

		require.NoError(t, err)
		assert.Len(t, children, 0)              // Should return empty slice since there are no children
		assert.NotContains(t, children, txHash) // Original transaction should not be included

		// Verify the transaction is locked
		txMeta, err := store.Get(ctx, &txHash, fields.Locked)
		require.NoError(t, err)
		assert.True(t, txMeta.Locked)
	})
	t.Run("should handle transaction with single child", func(t *testing.T) {
		store := setupSQLiteMemoryStore(ctx, t)

		// Create parent transaction
		parentTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
			"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
			"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
			"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
			"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
			"2f6b52de3d7c88ac00000000")
		require.NoError(t, err)

		// Create child transaction spending from parent
		childTx := bt.NewTx()
		err = childTx.From(parentTx.TxIDChainHash().String(), 0, parentTx.Outputs[0].LockingScript.String(), uint64(parentTx.Outputs[0].Satoshis))
		require.NoError(t, err)
		err = childTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50000)
		require.NoError(t, err)

		// Add a basic unlocking script to satisfy database constraints
		childTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

		// Create both transactions in store
		_, _, err = store.SpendAndCreate(ctx, parentTx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		_, _, err = store.SpendAndCreate(ctx, childTx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		// Spend the parent's output with child transaction
		_, _, err = store.SpendAndCreate(ctx, childTx, store.GetBlockHeight()+1, utxo.WithSpendOnly())
		require.NoError(t, err)

		parentHash := *parentTx.TxIDChainHash()
		childHash := *childTx.TxIDChainHash()

		children, err := utxo.GetAndLockChildren(ctx, store, parentHash)

		require.NoError(t, err)
		assert.Len(t, children, 1)                  // Should include only the child, not parent
		assert.NotContains(t, children, parentHash) // Parent should not be included
		assert.Contains(t, children, childHash)     // Child should be included

		// Verify both transactions are locked
		parentMeta, err := store.Get(ctx, &parentHash, fields.Locked)
		require.NoError(t, err)
		assert.True(t, parentMeta.Locked)

		childMeta, err := store.Get(ctx, &childHash, fields.Locked)
		require.NoError(t, err)
		assert.True(t, childMeta.Locked)
	})

	t.Run("should handle transaction with multiple children and grandchildren", func(t *testing.T) {
		store := setupSQLiteMemoryStore(ctx, t)

		// Create parent transaction with multiple outputs
		parentTx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
			"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
			"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
			"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
			"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
			"2f6b52de3d7c88ac00000000")
		require.NoError(t, err)

		// Create first child spending output 0
		child1Tx := bt.NewTx()
		err = child1Tx.From(parentTx.TxIDChainHash().String(), 0, parentTx.Outputs[0].LockingScript.String(), uint64(parentTx.Outputs[0].Satoshis))
		require.NoError(t, err)
		err = child1Tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 40000)
		require.NoError(t, err)
		child1Tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

		// Create second child spending output 1
		child2Tx := bt.NewTx()
		err = child2Tx.From(parentTx.TxIDChainHash().String(), 1, parentTx.Outputs[1].LockingScript.String(), uint64(parentTx.Outputs[1].Satoshis))
		require.NoError(t, err)
		err = child2Tx.AddP2PKHOutputFromAddress("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2", 30000)
		require.NoError(t, err)
		child2Tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

		// Create grandchild spending from child1
		grandchildTx := bt.NewTx()
		err = grandchildTx.From(child1Tx.TxIDChainHash().String(), 0, child1Tx.Outputs[0].LockingScript.String(), uint64(child1Tx.Outputs[0].Satoshis))
		require.NoError(t, err)
		err = grandchildTx.AddP2PKHOutputFromAddress("1HLoD9E4SDFFPDiYfNYnkBLQ85Y51J3Zb1", 20000)
		require.NoError(t, err)
		grandchildTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

		// Create all transactions in store
		_, _, err = store.SpendAndCreate(ctx, parentTx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
		_, _, err = store.SpendAndCreate(ctx, child1Tx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
		_, _, err = store.SpendAndCreate(ctx, child2Tx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)
		_, _, err = store.SpendAndCreate(ctx, grandchildTx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		// Spend outputs to establish parent-child relationships
		_, _, err = store.SpendAndCreate(ctx, child1Tx, store.GetBlockHeight()+1, utxo.WithSpendOnly())
		require.NoError(t, err)
		_, _, err = store.SpendAndCreate(ctx, child2Tx, store.GetBlockHeight()+1, utxo.WithSpendOnly())
		require.NoError(t, err)
		_, _, err = store.SpendAndCreate(ctx, grandchildTx, store.GetBlockHeight()+1, utxo.WithSpendOnly())
		require.NoError(t, err)

		parentHash := *parentTx.TxIDChainHash()
		child1Hash := *child1Tx.TxIDChainHash()
		child2Hash := *child2Tx.TxIDChainHash()
		grandchildHash := *grandchildTx.TxIDChainHash()

		children, err := utxo.GetAndLockChildren(ctx, store, parentHash)

		require.NoError(t, err)
		assert.Len(t, children, 3)                  // Should include 2 children and 1 grandchild, not parent
		assert.NotContains(t, children, parentHash) // Parent should not be included
		assert.Contains(t, children, child1Hash)
		assert.Contains(t, children, child2Hash)
		assert.Contains(t, children, grandchildHash)

		// Verify all transactions are locked
		for _, hash := range []chainhash.Hash{parentHash, child1Hash, child2Hash, grandchildHash} {
			txMeta, err := store.Get(ctx, &hash, fields.Locked)
			require.NoError(t, err)
			assert.True(t, txMeta.Locked, "Transaction %s should be locked", hash.String())
		}
	})

	t.Run("should handle recursive call errors gracefully", func(t *testing.T) {
		store := setupSQLiteMemoryStore(ctx, t)

		// Try to get children of a non-existent transaction
		nonExistentHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)

		children, err := utxo.GetAndLockChildren(ctx, store, *nonExistentHash)

		assert.Error(t, err)
		assert.Nil(t, children)
	})

	t.Run("should handle locking failure", func(t *testing.T) {
		store := setupSQLiteMemoryStore(ctx, t)

		// Create a transaction
		tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
			"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
			"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
			"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
			"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
			"2f6b52de3d7c88ac00000000")
		require.NoError(t, err)

		_, _, err = store.SpendAndCreate(ctx, tx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		txHash := *tx.TxIDChainHash()

		// First, lock the transaction manually to cause locking failure
		err = store.SetLocked(ctx, []chainhash.Hash{txHash}, true)
		require.NoError(t, err)

		// This should still work as the transaction is already locked
		children, err := utxo.GetAndLockChildren(ctx, store, txHash)
		require.NoError(t, err)
		assert.NotContains(t, children, txHash) // Original transaction should not be included
	})

	t.Run("should handle empty spending data", func(t *testing.T) {
		store := setupSQLiteMemoryStore(ctx, t)

		// Create a transaction with outputs that haven't been spent
		tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
			"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
			"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
			"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
			"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
			"2f6b52de3d7c88ac00000000")
		require.NoError(t, err)

		_, _, err = store.SpendAndCreate(ctx, tx, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		txHash := *tx.TxIDChainHash()

		children, err := utxo.GetAndLockChildren(ctx, store, txHash)

		require.NoError(t, err)
		assert.Len(t, children, 0)              // Should return empty slice since there are no children
		assert.NotContains(t, children, txHash) // Original transaction should not be included
	})
}

// A parent's spending data can name a child whose record is gone. GetAndLockChildrenSkippingMissing
// walks past it, GetAndLockChildren still fails on it.
func TestGetAndLockChildrenSkippingMissing(t *testing.T) {
	ctx := context.Background()

	const parentHex = "010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000"

	// spend creates a tx with two outputs that spends the given outputs
	spend := func(t *testing.T, store utxo.Store, from ...spentOutput) *bt.Tx {
		t.Helper()

		tx := bt.NewTx()

		var total uint64

		for _, in := range from {
			out := in.tx.Outputs[in.vout]
			require.NoError(t, tx.From(in.tx.TxIDChainHash().String(), in.vout, out.LockingScript.String(), out.Satoshis))

			total += out.Satoshis
		}

		// each output takes a quarter of the inputs, the rest is the fee
		require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", total/4))
		require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", total/4))

		for i := range tx.Inputs {
			tx.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{})
		}

		_, _, err := store.SpendAndCreate(ctx, tx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		return tx
	}

	// parent has two outputs: gone spends the first, kept spends the second
	build := func(t *testing.T) (store utxo.Store, parent, gone, kept *bt.Tx) {
		t.Helper()

		store = setupSQLiteMemoryStore(ctx, t)

		parent, err := bt.NewTxFromString(parentHex)
		require.NoError(t, err)

		_, _, err = store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		gone = spend(t, store, spentOutput{parent, 0})
		kept = spend(t, store, spentOutput{parent, 1})

		return store, parent, gone, kept
	}

	// as build, and the record of gone is deleted while parent still names it as a spender
	setup := func(t *testing.T) (store utxo.Store, parent, gone, kept *bt.Tx) {
		t.Helper()

		store, parent, gone, kept = build(t)

		require.NoError(t, store.Delete(ctx, gone.TxIDChainHash()))

		return store, parent, gone, kept
	}

	t.Run("skips a descendant whose record is gone and reports it", func(t *testing.T) {
		store, parent, gone, kept := setup(t)

		before := lockWalkMissingCount(t)

		children, missing, err := utxo.GetAndLockChildrenSkippingMissing(ctx, store, *parent.TxIDChainHash())
		require.NoError(t, err)
		require.Equal(t, []chainhash.Hash{*kept.TxIDChainHash()}, children)
		require.Equal(t, []chainhash.Hash{*gone.TxIDChainHash()}, missing)
		require.Equal(t, before+1, lockWalkMissingCount(t))

		for name, tx := range map[string]*bt.Tx{"parent": parent, "kept": kept} {
			txMeta, getErr := store.Get(ctx, tx.TxIDChainHash(), fields.Locked)
			require.NoError(t, getErr)
			require.True(t, txMeta.Locked, "%s is locked by the walk", name)
		}
	})

	// Aerospike reports a missing record from SetLocked itself, where the SQL store only finds out at
	// Get. The record is still in the SQL store here, so only the failing SetLocked can make it missing.
	t.Run("skips a descendant that SetLocked reports as not found", func(t *testing.T) {
		store, parent, gone, kept := setup(t)

		_, _, err := store.SpendAndCreate(ctx, gone, store.GetBlockHeight()+1, utxo.WithCreateOnly())
		require.NoError(t, err)

		wrapped := &setLockedNotFound{Store: store, target: *gone.TxIDChainHash()}

		children, missing, err := utxo.GetAndLockChildrenSkippingMissing(ctx, wrapped, *parent.TxIDChainHash())
		require.NoError(t, err)
		require.Equal(t, []chainhash.Hash{*kept.TxIDChainHash()}, children)
		require.Equal(t, []chainhash.Hash{*gone.TxIDChainHash()}, missing)

		wrapped.target = *parent.TxIDChainHash()

		_, _, err = utxo.GetAndLockChildrenSkippingMissing(ctx, wrapped, *parent.TxIDChainHash())
		require.ErrorIs(t, err, errors.ErrTxNotFound, "the root is never skipped")
	})

	// both spends gone and kept, so the walk reaches it through kept. only spends gone alone, so
	// nothing that exists names it and the walk cannot reach it.
	t.Run("reaches a descendant of the missing record only through another parent", func(t *testing.T) {
		store, parent, gone, kept := build(t)

		both := spend(t, store, spentOutput{gone, 0}, spentOutput{kept, 0})
		only := spend(t, store, spentOutput{gone, 1})

		require.NoError(t, store.Delete(ctx, gone.TxIDChainHash()))

		children, missing, err := utxo.GetAndLockChildrenSkippingMissing(ctx, store, *parent.TxIDChainHash())
		require.NoError(t, err)
		require.ElementsMatch(t, []chainhash.Hash{*kept.TxIDChainHash(), *both.TxIDChainHash()}, children)
		require.Equal(t, []chainhash.Hash{*gone.TxIDChainHash()}, missing)
		require.NotContains(t, children, *only.TxIDChainHash(), "a descendant reachable only through the missing record is not reached")

		bothMeta, getErr := store.Get(ctx, both.TxIDChainHash(), fields.Locked)
		require.NoError(t, getErr)
		require.True(t, bothMeta.Locked, "the descendant reached through kept is locked")

		onlyMeta, getErr := store.Get(ctx, only.TxIDChainHash(), fields.Locked)
		require.NoError(t, getErr)
		require.False(t, onlyMeta.Locked, "the descendant reached only through the missing record is not locked")
	})

	// A store that runs SpendAndCreate as a spend then a create names the child on the parent before
	// the child's record exists. The walk must read such a child again once everything else is locked,
	// and walk it if it is there by then, instead of settling it as missing on the first read. kept has
	// a chain two deep below it, so the second read comes after the walk locked the deepest of them.
	t.Run("reads a missing descendant again and walks it if its create has landed", func(t *testing.T) {
		store, parent, gone, kept := build(t)

		grandchild := spend(t, store, spentOutput{gone, 0})
		k1 := spend(t, store, spentOutput{kept, 0})
		k2 := spend(t, store, spentOutput{k1, 0})

		wrapped := &setLockedNotFound{Store: store, target: *gone.TxIDChainHash(), failures: 1}

		before := lockWalkMissingCount(t)

		children, missing, err := utxo.GetAndLockChildrenSkippingMissing(ctx, wrapped, *parent.TxIDChainHash())
		require.NoError(t, err)
		require.ElementsMatch(t, []chainhash.Hash{*gone.TxIDChainHash(), *kept.TxIDChainHash(), *grandchild.TxIDChainHash(),
			*k1.TxIDChainHash(), *k2.TxIDChainHash()}, children)
		require.Empty(t, missing)
		require.Equal(t, before, lockWalkMissingCount(t), "a record found on the second read is not counted as missing")
		require.Equal(t, 2, wrapped.calls, "the late record is read exactly twice")

		order := wrapped.lockOrder()
		lockedK2 := slices.Index(order, *k2.TxIDChainHash())

		var goneLocks []int

		for i := range order {
			if order[i] == *gone.TxIDChainHash() {
				goneLocks = append(goneLocks, i)
			}
		}

		require.GreaterOrEqual(t, lockedK2, 0, "precondition: the walk locked k2")
		require.Len(t, goneLocks, 2, "precondition: the walk tried to lock gone twice")
		require.Greater(t, goneLocks[1], lockedK2, "the second read of the missing record waits until the walk has locked everything else")

		for name, tx := range map[string]*bt.Tx{"gone": gone, "grandchild": grandchild} {
			txMeta, getErr := store.Get(ctx, tx.TxIDChainHash(), fields.Locked)
			require.NoError(t, getErr)
			require.True(t, txMeta.Locked, "%s is locked by the walk", name)
		}
	})

	t.Run("still fails when the root is absent", func(t *testing.T) {
		store := setupSQLiteMemoryStore(ctx, t)

		absent := chainhash.HashH([]byte("absent"))

		children, missing, err := utxo.GetAndLockChildrenSkippingMissing(ctx, store, absent)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.Nil(t, children)
		require.Nil(t, missing)
	})

	t.Run("the strict walk fails on a descendant whose record is gone", func(t *testing.T) {
		store, parent, _, _ := setup(t)

		_, err := utxo.GetAndLockChildren(ctx, store, *parent.TxIDChainHash())
		require.ErrorIs(t, err, errors.ErrTxNotFound)
	})
}

// spentOutput names one output of a tx for a test spend.
type spentOutput struct {
	tx   *bt.Tx
	vout uint32
}

// setLockedNotFound wraps a real store and fails SetLocked for one hash the way Aerospike does for a
// record that is not there. With failures set it fails only that many calls for the hash and then lets
// them through, the way a record whose create lands late looks to the walk; with failures zero it fails
// every call.
// Every hash it is asked to lock is recorded in call order.
type setLockedNotFound struct {
	utxo.Store
	target   chainhash.Hash
	failures int
	calls    int
	mu       sync.Mutex
	order    []chainhash.Hash
}

func (s *setLockedNotFound) lockOrder() []chainhash.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]chainhash.Hash(nil), s.order...)
}

func (s *setLockedNotFound) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if value {
		s.order = append(s.order, txHashes...)
	}

	for i := range txHashes {
		if txHashes[i] == s.target {
			s.calls++

			if s.failures == 0 || s.calls <= s.failures {
				return errors.NewTxNotFoundError("transaction not found: %s", txHashes[i].String())
			}
		}
	}

	return s.Store.SetLocked(ctx, txHashes, value)
}

// lockWalkMissingCount reads teranode_utxo_lock_walk_missing_descendants_total from the default registry.
func lockWalkMissingCount(t *testing.T) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() == "teranode_utxo_lock_walk_missing_descendants_total" {
			return family.GetMetric()[0].GetCounter().GetValue()
		}
	}

	require.FailNow(t, "metric teranode_utxo_lock_walk_missing_descendants_total is not registered")

	return 0
}
