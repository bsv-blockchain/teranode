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
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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
		_, err = store.Create(ctx, tx, 1)
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
		_, err = store.Create(ctx, parentTx, 1)
		require.NoError(t, err)

		_, err = store.Create(ctx, childTx, 1)
		require.NoError(t, err)

		// Spend the parent's output with child transaction
		_, err = store.Spend(ctx, childTx, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
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
		_, err = store.Create(ctx, parentTx, 1)
		require.NoError(t, err)
		_, err = store.Create(ctx, child1Tx, 1)
		require.NoError(t, err)
		_, err = store.Create(ctx, child2Tx, 1)
		require.NoError(t, err)
		_, err = store.Create(ctx, grandchildTx, 1)
		require.NoError(t, err)

		// Spend outputs to establish parent-child relationships
		_, err = store.Spend(ctx, child1Tx, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
		require.NoError(t, err)
		_, err = store.Spend(ctx, child2Tx, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
		require.NoError(t, err)
		_, err = store.Spend(ctx, grandchildTx, store.GetBlockHeight()+1, utxo.IgnoreFlags{})
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

		_, err = store.Create(ctx, tx, 1)
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

		_, err = store.Create(ctx, tx, 1)
		require.NoError(t, err)

		txHash := *tx.TxIDChainHash()

		children, err := utxo.GetAndLockChildren(ctx, store, txHash)

		require.NoError(t, err)
		assert.Len(t, children, 0)              // Should return empty slice since there are no children
		assert.NotContains(t, children, txHash) // Original transaction should not be included
	})
}
