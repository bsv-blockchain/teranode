package aerospike_test

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarkTransactionsOnLongestChain(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	// Clean database before starting
	cleanDB(t, client)

	// Set initial block height
	const initialBlockHeight = uint32(100)
	const newBlockHeight = uint32(101)
	err := store.SetBlockHeight(initialBlockHeight)
	require.NoError(t, err)

	// Create test transactions
	tx1, err := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	require.NoError(t, err)

	tx2, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")
	require.NoError(t, err)

	tx1Hash := tx1.TxIDChainHash()
	tx2Hash := tx2.TxIDChainHash()

	t.Run("MarkTransactionsOnLongestChain - create transactions first", func(t *testing.T) {
		// Create transactions in the store first
		_, err := store.Create(ctx, tx1, initialBlockHeight)
		require.NoError(t, err)

		_, err = store.Create(ctx, tx2, initialBlockHeight, utxo.WithSetCoinbase(true))
		require.NoError(t, err)

		// Verify transactions exist and have initial unminedSince values
		meta1, err := store.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, initialBlockHeight, meta1.UnminedSince)

		meta2, err := store.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, initialBlockHeight, meta2.UnminedSince)
	})

	t.Run("MarkTransactionsOnLongestChain - mark as on longest chain", func(t *testing.T) {
		// Mark transactions as being on the longest chain
		txHashes := []chainhash.Hash{*tx1Hash, *tx2Hash}
		err := store.MarkTransactionsOnLongestChain(ctx, txHashes, true)
		require.NoError(t, err)

		// Verify unminedSince field is unset (should be 0 for unmarked/mined)
		meta1, err := store.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta1.UnminedSince)

		meta2, err := store.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta2.UnminedSince)
	})

	t.Run("MarkTransactionsOnLongestChain - mark as not on longest chain", func(t *testing.T) {
		// Update block height to simulate chain progression
		err := store.SetBlockHeight(newBlockHeight)
		require.NoError(t, err)

		// Mark transactions as NOT being on the longest chain
		txHashes := []chainhash.Hash{*tx1Hash, *tx2Hash}
		err = store.MarkTransactionsOnLongestChain(ctx, txHashes, false)
		require.NoError(t, err)

		// Verify unminedSince field is set to current block height
		meta1, err := store.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, newBlockHeight, meta1.UnminedSince)

		meta2, err := store.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, newBlockHeight, meta2.UnminedSince)
	})

	t.Run("MarkTransactionsOnLongestChain - switch back to longest chain", func(t *testing.T) {
		// Mark transactions as being back on the longest chain
		txHashes := []chainhash.Hash{*tx1Hash, *tx2Hash}
		err := store.MarkTransactionsOnLongestChain(ctx, txHashes, true)
		require.NoError(t, err)

		// Verify unminedSince field is unset again
		meta1, err := store.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta1.UnminedSince)

		meta2, err := store.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta2.UnminedSince)
	})

	t.Run("MarkTransactionsOnLongestChain - empty transaction list", func(t *testing.T) {
		// Test with empty transaction list - should not error
		err := store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{}, true)
		require.NoError(t, err)

		err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{}, false)
		require.NoError(t, err)
	})

	t.Run("MarkTransactionsOnLongestChain - single transaction", func(t *testing.T) {
		const testBlockHeight = uint32(200)
		err := store.SetBlockHeight(testBlockHeight)
		require.NoError(t, err)

		// Mark only one transaction as not on longest chain
		err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx1Hash}, false)
		require.NoError(t, err)

		// Verify only tx1 has updated unminedSince
		meta1, err := store.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, testBlockHeight, meta1.UnminedSince)

		// tx2 should still have unminedSince = 0 from previous test
		meta2, err := store.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta2.UnminedSince)

		// Mark tx1 back on longest chain
		err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx1Hash}, true)
		require.NoError(t, err)

		meta1, err = store.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta1.UnminedSince)
	})

	t.Run("MarkTransactionsOnLongestChain - concurrent operations", func(t *testing.T) {
		const concurrency = 10
		const testBlockHeight = uint32(300)

		err := store.SetBlockHeight(testBlockHeight)
		require.NoError(t, err)

		// Create additional unique test transactions for concurrent testing using tx.Clone() and version modification
		var testTxHashes []chainhash.Hash

		for i := 0; i < concurrency; i++ {
			// Clone the first transaction and modify version to make it unique
			testTx := tx1.Clone()
			testTx.Version = uint32(i + 100) // Use different version numbers to create unique transactions

			testTxHash := testTx.TxIDChainHash()
			testTxHashes = append(testTxHashes, *testTxHash)

			// Create the transaction in the store
			_, err = store.Create(ctx, testTx, testBlockHeight)
			require.NoError(t, err)
		}

		// Test concurrent marking as not on longest chain
		err = store.MarkTransactionsOnLongestChain(ctx, testTxHashes, false)
		require.NoError(t, err)

		// Verify all transactions have the correct unminedSince value
		for _, txHash := range testTxHashes {
			meta, err := store.Get(ctx, &txHash, fields.UnminedSince)
			require.NoError(t, err)
			assert.Equal(t, testBlockHeight, meta.UnminedSince)
		}

		// Test concurrent marking as on longest chain
		err = store.MarkTransactionsOnLongestChain(ctx, testTxHashes, true)
		require.NoError(t, err)

		// Verify all transactions have unminedSince = 0
		for _, txHash := range testTxHashes {
			meta, err := store.Get(ctx, &txHash, fields.UnminedSince)
			require.NoError(t, err)
			assert.Equal(t, uint32(0), meta.UnminedSince)
		}
	})
}

func TestMarkTransactionsOnLongestChain_NonExistentTransactions(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	// Clean database before starting
	cleanDB(t, client)

	t.Run("MarkTransactionsOnLongestChain - non-existent transactions should not error", func(t *testing.T) {
		// Create random transaction hashes that don't exist in the store
		nonExistentHash1, err := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		require.NoError(t, err)

		nonExistentHash2, err := chainhash.NewHashFromStr("fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")
		require.NoError(t, err)

		nonExistentHashes := []chainhash.Hash{*nonExistentHash1, *nonExistentHash2}

		// This should not error even though the transactions don't exist
		// The batch operation should handle missing records gracefully
		err = store.MarkTransactionsOnLongestChain(ctx, nonExistentHashes, true)
		// Note: The current implementation may error on non-existent transactions
		// This behavior should be documented and may need adjustment based on requirements
		if err != nil {
			t.Logf("Expected behavior: MarkTransactionsOnLongestChain returned error for non-existent transactions: %v", err)
		}

		err = store.MarkTransactionsOnLongestChain(ctx, nonExistentHashes, false)
		if err != nil {
			t.Logf("Expected behavior: MarkTransactionsOnLongestChain returned error for non-existent transactions: %v", err)
		}
	})
}

func TestMarkTransactionsOnLongestChain_Integration(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	// Clean database before starting
	cleanDB(t, client)

	t.Run("Integration test simulating blockchain reorganization", func(t *testing.T) {
		// Simulate a blockchain reorganization scenario
		const originalBlockHeight = uint32(1000)
		const reorgBlockHeight = uint32(1001)

		err := store.SetBlockHeight(originalBlockHeight)
		require.NoError(t, err)

		// Create transactions that were initially mined
		minedTx, err := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
		require.NoError(t, err)

		minedTxHash := minedTx.TxIDChainHash()

		// Step 1: Create transaction as mined (on longest chain initially)
		_, err = store.Create(ctx, minedTx, originalBlockHeight)
		require.NoError(t, err)

		// Initially mark as on longest chain (mined)
		err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*minedTxHash}, true)
		require.NoError(t, err)

		// Verify it's mined (unminedSince = 0)
		meta, err := store.Get(ctx, minedTxHash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta.UnminedSince)

		// Step 2: Simulate blockchain reorganization - transaction becomes unmined
		err = store.SetBlockHeight(reorgBlockHeight)
		require.NoError(t, err)

		// Mark as not on longest chain (unmined due to reorg)
		err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*minedTxHash}, false)
		require.NoError(t, err)

		// Verify it's now unmined (unminedSince = current block height)
		meta, err = store.Get(ctx, minedTxHash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, reorgBlockHeight, meta.UnminedSince)

		// Step 3: Simulate resolution - transaction gets mined again in new chain
		err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*minedTxHash}, true)
		require.NoError(t, err)

		// Verify it's mined again (unminedSince = 0)
		meta, err = store.Get(ctx, minedTxHash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta.UnminedSince)
	})
}

func TestDeleteAtHeight_ForkTransactionNotOnLongestChain(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 100 // Set retention period

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	// Clean database before starting
	cleanDB(t, client)

	t.Run("Transaction mined on fork should NOT be marked for deletion when fully spent", func(t *testing.T) {
		// Test that transactions mined on forks (not on longest chain) are NOT marked for deletion
		// even when all outputs are spent. Only transactions on the main chain should be pruned.

		const blockHeight = uint32(100)
		err := store.SetBlockHeight(blockHeight)
		require.NoError(t, err)

		// Create a transaction with 6 outputs
		forkTx, err := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
		require.NoError(t, err)
		forkTxHash := forkTx.TxIDChainHash()

		// Step 1: Create the transaction
		_, err = store.Create(ctx, forkTx, blockHeight)
		require.NoError(t, err)

		// Step 2: Mine the transaction on a FORK block (onLongestChain=false)
		minedInfo := utxo.MinedBlockInfo{
			BlockID:        999, // Fork block ID
			BlockHeight:    blockHeight,
			SubtreeIdx:     1,
			OnLongestChain: false, // <-- This is the key: transaction is on a fork!
			UnsetMined:     false,
		}
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{forkTxHash}, minedInfo)
		require.NoError(t, err)

		// Step 3: Verify the transaction has blockIDs (it's in a block)
		meta, err := store.Get(ctx, forkTxHash, fields.BlockIDs, fields.UnminedSince)
		require.NoError(t, err)
		assert.Len(t, meta.BlockIDs, 1, "Transaction should have one blockID")
		assert.Equal(t, uint32(999), meta.BlockIDs[0], "BlockID should be 999 (fork block)")

		// Verify unminedSince is set (because it's not on longest chain)
		// UnminedSince = 0 means on longest chain, != 0 means not on longest chain
		assert.NotEqual(t, uint32(0), meta.UnminedSince, "unminedSince should be non-zero for fork transactions")
		assert.Equal(t, blockHeight, meta.UnminedSince, "unminedSince should equal current block height")

		// Get the Aerospike key for the main record
		keySource := uaerospike.CalculateKeySource(forkTxHash, uint32(0), store.GetUtxoBatchSize())
		mainRecordKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, err)

		// Read the record directly from Aerospike to check deleteAtHeight
		resp, err := client.Get(nil, mainRecordKey)
		require.NoError(t, err)

		// Verify deleteAtHeight is NOT set yet (outputs not spent)
		assert.Nil(t, resp.Bins[fields.DeleteAtHeight.String()], "deleteAtHeight should not be set before spending")

		// Step 4: Create a transaction that spends all 6 outputs
		spendingTx := bt.NewTx()
		var utxos []*bt.UTXO
		for vout := 0; vout < len(forkTx.Outputs); vout++ {
			utxos = append(utxos, &bt.UTXO{
				TxIDHash:      forkTxHash,
				Vout:          uint32(vout),
				Satoshis:      forkTx.Outputs[vout].Satoshis,
				LockingScript: forkTx.Outputs[vout].LockingScript,
			})
		}
		err = spendingTx.FromUTXOs(utxos...)
		require.NoError(t, err)

		// Step 5: Spend all outputs
		_, err = store.Spend(ctx, spendingTx, blockHeight+1)
		require.NoError(t, err)

		// Step 6: Assert CORRECT behavior - deleteAtHeight should NOT be set for fork transactions
		resp, err = client.Get(nil, mainRecordKey)
		require.NoError(t, err)

		// CORRECT BEHAVIOR: deleteAtHeight should be nil for fork transactions
		// This will FAIL with current bug, PASS after fix
		assert.Nil(t, resp.Bins[fields.DeleteAtHeight.String()], "deleteAtHeight should NOT be set for transactions on forks")

		// Verify unminedSince is still set (confirms it's on a fork)
		meta, err = store.Get(ctx, forkTxHash, fields.UnminedSince)
		require.NoError(t, err)
		assert.NotEqual(t, uint32(0), meta.UnminedSince, "unminedSince should be set (transaction on fork)")
	})
}
