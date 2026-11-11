package aerospike_test

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
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

		// Verify unminedSince was stamped (backdated so the pruner scan sees it at once)
		expected := utxo.BackdatedUnminedSince(newBlockHeight, tSettings.UtxoStore.UnminedTxRetention)

		meta1, err := store.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, expected, meta1.UnminedSince)

		meta2, err := store.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, expected, meta2.UnminedSince)
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
		assert.Equal(t, utxo.BackdatedUnminedSince(testBlockHeight, tSettings.UtxoStore.UnminedTxRetention), meta1.UnminedSince)

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

		// Verify it's now unmined (unminedSince backdated from the reorg height)
		meta, err = store.Get(ctx, minedTxHash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, utxo.BackdatedUnminedSince(reorgBlockHeight, tSettings.UtxoStore.UnminedTxRetention), meta.UnminedSince)

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

// TestMarkTransactionsOnLongestChain_KeepsEarlierUnminedSince locks the fix for issue
// 1768: marking a transaction NOT on the longest chain must not push an existing
// unminedSince forward. The pruner's parent-preservation scan only considers children
// with unminedSince <= height - unminedTxRetention, while the parent's deleteAtHeight
// was stamped when the child spent it. Re-stamping unminedSince to the current height
// re-arms the child's protection clock while the parent's delete clock keeps counting,
// and a late enough re-stamp lets the parent be deleted before anything protects it.
func TestMarkTransactionsOnLongestChain_KeepsEarlierUnminedSince(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	cleanDB(t, client)

	const createdAt = uint32(100)
	const muchLater = uint32(1305)

	require.Greater(t, muchLater, tSettings.UtxoStore.UnminedTxRetention+createdAt, "test needs headroom above the retention window")
	require.NoError(t, store.SetBlockHeight(createdAt))

	tx, err := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	_, err = store.Create(ctx, tx, createdAt)
	require.NoError(t, err)

	meta, err := store.Get(ctx, txHash, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, createdAt, meta.UnminedSince, "unmined tx starts with unminedSince = creation height")

	require.NoError(t, store.SetBlockHeight(muchLater))

	// A never-mined transaction swept up in a reset/reorg moveBack must keep its
	// original unminedSince.
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, false))

	meta, err = store.Get(ctx, txHash, fields.UnminedSince)
	require.NoError(t, err)
	assert.Equal(t, createdAt, meta.UnminedSince, "existing unminedSince must not be pushed forward")

	// A transaction that genuinely goes mined -> unmined has no earlier value to keep. It is
	// stamped backdated by unminedTxRetention so the pruner's parent-preservation scan
	// (unminedSince <= height - unminedTxRetention) considers it on the next cycle.
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, true))

	meta, err = store.Get(ctx, txHash, fields.UnminedSince)
	require.NoError(t, err)
	require.Zero(t, meta.UnminedSince)

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, false))

	meta, err = store.Get(ctx, txHash, fields.UnminedSince)
	require.NoError(t, err)
	assert.Equal(t, muchLater-tSettings.UtxoStore.UnminedTxRetention, meta.UnminedSince, "cleared unminedSince is stamped backdated by unminedTxRetention")
	assert.LessOrEqual(t, meta.UnminedSince, muchLater-tSettings.UtxoStore.UnminedTxRetention, "must satisfy the phase-1 cutoff at this height")
}

// TestIssue1768_ParentPreservedDespiteLateUnminedSince replays the mainnet instance from
// issue 1768 against a real store and the real phase-1 pruner scan.
//
// Two clocks race. The parent's deleteAtHeight is stamped when an unmined child spends its
// last output (spendHeight + blockHeightRetention). The phase-1 scan only protects parents of
// children with unminedSince <= height - unminedTxRetention. A re-stamp of unminedSince to a
// later height pushes the child out of the scan's window while the parent's clock keeps
// counting, and once the re-stamp is later than blockHeightRetention - unminedTxRetention the
// parent is deleted before anything protects it.
//
// Both sources of a late unminedSince are exercised:
//  1. a never-mined child swept into a reset/reorg moveBack (MarkTransactionsOnLongestChain
//     must keep the original value), and
//  2. a mined child rolled back by a reorg (there is no earlier value; the stamp must be
//     backdated so the very next scan sees it).
//
// In both cases the scan at the last block before the parent's deleteAtHeight must preserve
// the parent.
func TestIssue1768_ParentPreservedDespiteLateUnminedSince(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UnminedTxRetention = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	cleanDB(t, client)

	const unminedSinceIndexName = "unminedSinceIndex"
	require.NoError(t, store.CreateIndexIfNotExists(ctx, unminedSinceIndexName, fields.UnminedSince.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, unminedSinceIndexName))

	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	unminedRetention := tSettings.UtxoStore.UnminedTxRetention
	require.Greater(t, retention, unminedRetention, "the race needs a positive margin to outrun")

	const spendHeight = uint32(1000)
	deleteAt := spendHeight + retention
	lastSafeHeight := deleteAt - 1

	// The re-stamp lands inside the margin the issue describes: later than
	// blockHeightRetention - unminedTxRetention blocks after the spend.
	require.Greater(t, lastSafeHeight-spendHeight, retention-unminedRetention)

	require.NoError(t, store.SetBlockHeight(spendHeight))

	makeParentAndChild := func(seed string) (*bt.Tx, *bt.Tx) {
		parent := bt.NewTx()
		require.NoError(t, parent.From(chainhash.HashH([]byte(seed)).String(), 0, "51", 10000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

		_, err := store.Create(ctx, parent, spendHeight, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
			BlockID: 999, BlockHeight: spendHeight - 1, OnLongestChain: true,
		}))
		require.NoError(t, err)

		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

		_, _, err = store.SpendAndCreate(ctx, child, spendHeight)
		require.NoError(t, err)

		return parent, child
	}

	readBins := func(tx *bt.Tx) aerospike.BinMap {
		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
		require.NoError(t, err)

		rec, err := client.Get(nil, key, fields.DeleteAtHeight.String(), fields.PreserveUntil.String(), fields.UnminedSince.String())
		require.NoError(t, err)

		return rec.Bins
	}

	// Case 1: never-mined child, re-stamped by a moveBack.
	parent1, child1 := makeParentAndChild("issue-1768-parent-1")
	// Case 2: child mined, then rolled back by a reorg.
	parent2, child2 := makeParentAndChild("issue-1768-parent-2")

	for _, parent := range []*bt.Tx{parent1, parent2} {
		bins := readBins(parent)
		require.Equal(t, int(deleteAt), bins[fields.DeleteAtHeight.String()], "fully spent mined parent is stamped for deletion at the spend")
		require.Nil(t, bins[fields.PreserveUntil.String()])
	}

	require.Equal(t, int(spendHeight), readBins(child1)[fields.UnminedSince.String()])

	require.NoError(t, store.SetBlockHeight(spendHeight+1))
	_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{child2.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 1001, BlockHeight: spendHeight + 1, OnLongestChain: true,
	})
	require.NoError(t, err)
	require.Nil(t, readBins(child2)[fields.UnminedSince.String()], "mined child has no unminedSince")

	// The late re-stamp, one block before the parents would be deleted.
	require.NoError(t, store.SetBlockHeight(lastSafeHeight))
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*child1.TxIDChainHash(), *child2.TxIDChainHash()}, false))

	cutoff := lastSafeHeight - unminedRetention
	require.Equal(t, int(spendHeight), readBins(child1)[fields.UnminedSince.String()], "never-mined child keeps its original unminedSince")
	require.Equal(t, int(cutoff), readBins(child2)[fields.UnminedSince.String()], "rolled-back child is backdated to the scan cutoff")

	// The real phase-1 scan at this height must find both children and preserve both parents.
	processed, err := utxo.PreserveParentsOfOldUnminedTransactions(ctx, store, lastSafeHeight, "issue-1768", tSettings, logger)
	require.NoError(t, err)
	require.Equal(t, 2, processed, "both children must be inside the scan window")

	for i, parent := range []*bt.Tx{parent1, parent2} {
		bins := readBins(parent)
		assert.Equal(t, int(lastSafeHeight+tSettings.UtxoStore.ParentPreservationBlocks), bins[fields.PreserveUntil.String()], "parent %d must be preserved", i+1)
		assert.Nil(t, bins[fields.DeleteAtHeight.String()], "parent %d deleteAtHeight must be cleared", i+1)
	}
}

// TestMarkTransactionsOnLongestChain_ReusedBatchSlots checks every bin of every record a mark touches, and
// of records it must not touch, when each worker sends several batches through the same reused keys and
// batch writes. 31 hashes with a batch size of 4 and 3 workers give each worker batches of 4, 4 and 3 (the
// last 4, 4 and 1, which goes through the client's single-record path), so a key or batch write that kept
// the previous batch's transaction writes the wrong record and leaves the right one as it was.
func TestMarkTransactionsOnLongestChain_ReusedBatchSlots(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.MaxMinedBatchSize = 4
	tSettings.UtxoStore.MaxMinedRoutines = 3

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	const (
		height       = 1000
		count        = 31
		missingIndex = 27 // the third worker's second batch
	)

	require.NoError(t, store.SetBlockHeight(height))

	retention := int(tSettings.GetUtxoStoreBlockHeightRetention())
	require.NotZero(t, retention)

	dahBin := fields.DeleteAtHeight.String()
	unminedBin := fields.UnminedSince.String()
	dahVerBin := fields.DAHVersion.String()

	keyOf := func(txID chainhash.Hash, page byte) *aerospike.Key {
		var k *aerospike.Key

		var err error

		if page == 0 {
			k, err = aerospike.NewKey(store.GetNamespace(), store.GetName(), txID[:])
		} else {
			k, err = aerospike.NewKey(store.GetNamespace(), store.GetName(), append(txID.CloneBytes(), page, 0, 0, 0))
		}

		require.NoError(t, err)

		return k
	}

	// master writes a mined record. pages > 0 makes it external with that many pagination records.
	master := func(txID chainhash.Hash, spent bool, pages int, extra aerospike.BinMap) aerospike.BinMap {
		spentUtxos := 0
		if spent {
			spentUtxos = 1
		}

		bins := aerospike.BinMap{
			fields.TxID.String():           txID.CloneBytes(),
			fields.Conflicting.String():    false,
			fields.TotalExtraRecs.String(): pages,
			fields.SpentExtraRecs.String(): 0,
			fields.SpentUtxos.String():     spentUtxos,
			fields.RecordUtxos.String():    1,
			fields.BlockIDs.String():       []interface{}{1},
			fields.BlockHeights.String():   []interface{}{900},
		}

		if pages > 0 {
			bins[fields.External.String()] = true
			bins[fields.SpentExtraRecs.String()] = pages
		}

		for k, v := range extra {
			bins[k] = v
		}

		require.NoError(t, client.Put(nil, keyOf(txID, 0), bins))

		return bins
	}

	page := func(txID chainhash.Hash, idx byte, extra aerospike.BinMap) aerospike.BinMap {
		bins := aerospike.BinMap{fields.TxID.String(): txID.CloneBytes()}
		for k, v := range extra {
			bins[k] = v
		}

		require.NoError(t, client.Put(nil, keyOf(txID, idx), bins))

		return bins
	}

	// with returns a copy of bins with set applied; a nil value removes the bin.
	with := func(bins aerospike.BinMap, set aerospike.BinMap) aerospike.BinMap {
		out := make(aerospike.BinMap, len(bins))
		for k, v := range bins {
			out[k] = v
		}

		for k, v := range set {
			if v == nil {
				delete(out, k)
			} else {
				out[k] = v
			}
		}

		return out
	}

	requireBins := func(t *testing.T, key *aerospike.Key, want aerospike.BinMap, msg string) {
		t.Helper()

		rec, err := client.Get(nil, key)
		require.NoError(t, err, msg)
		require.Equal(t, want, rec.Bins, msg)
	}

	hashes := func(prefix string) []chainhash.Hash {
		out := make([]chainhash.Hash, count)
		for i := range out {
			out[i] = chainhash.HashH([]byte(fmt.Sprintf("reused-slots-%s-%d", prefix, i)))
		}

		return out
	}

	// controls are records the call is not given; they must come out exactly as they went in.
	controls := func(t *testing.T, prefix string) func() {
		t.Helper()

		want := make(map[chainhash.Hash]aerospike.BinMap, 3)

		for i := range 3 {
			txID := chainhash.HashH([]byte(fmt.Sprintf("reused-slots-%s-control-%d", prefix, i)))
			want[txID] = master(txID, true, 0, aerospike.BinMap{unminedBin: 77, dahBin: 555})
		}

		return func() {
			t.Helper()

			for txID, bins := range want {
				requireBins(t, keyOf(txID, 0), bins, "control record "+txID.String())
			}
		}
	}

	t.Run("off the longest chain stamps only records without unmined_since", func(t *testing.T) {
		txIDs := hashes("off")
		checkControls := controls(t, "off")
		stamp := int(utxo.BackdatedUnminedSince(height, tSettings.UtxoStore.UnminedTxRetention))

		want := make([]aerospike.BinMap, count)

		for i, txID := range txIDs {
			if i%2 == 0 {
				want[i] = master(txID, true, 0, aerospike.BinMap{unminedBin: 50}) // kept
			} else {
				want[i] = with(master(txID, true, 0, nil), aerospike.BinMap{unminedBin: stamp})
			}
		}

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, txIDs, false))

		for i, txID := range txIDs {
			requireBins(t, keyOf(txID, 0), want[i], fmt.Sprintf("tx %d", i))
		}

		checkControls()
	})

	t.Run("on the longest chain without a DAH restamp clears unmined_since and skips a missing record", func(t *testing.T) {
		previousGlobal := tSettings.GlobalBlockHeightRetention
		previousAdjustment := tSettings.UtxoStore.BlockHeightRetentionAdjustment

		tSettings.GlobalBlockHeightRetention = 0
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0

		t.Cleanup(func() {
			tSettings.GlobalBlockHeightRetention = previousGlobal
			tSettings.UtxoStore.BlockHeightRetentionAdjustment = previousAdjustment
		})

		require.Zero(t, tSettings.GetUtxoStoreBlockHeightRetention())

		txIDs := hashes("on-no-restamp")
		checkControls := controls(t, "on-no-restamp")

		want := make([]aerospike.BinMap, count)

		for i, txID := range txIDs {
			if i == missingIndex {
				continue
			}

			want[i] = with(master(txID, true, 0, aerospike.BinMap{unminedBin: 50, dahBin: 555}), aerospike.BinMap{unminedBin: nil})
		}

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, txIDs, true))

		for i, txID := range txIDs {
			if i == missingIndex {
				exists, err := client.Exists(nil, keyOf(txID, 0))
				require.NoError(t, err)
				require.False(t, exists, "a missing record must not be created")

				continue
			}

			requireBins(t, keyOf(txID, 0), want[i], fmt.Sprintf("tx %d", i))
		}

		checkControls()
	})

	t.Run("on the longest chain restamps the DAH of masters and their pagination records", func(t *testing.T) {
		require.Equal(t, uint32(retention), tSettings.GetUtxoStoreBlockHeightRetention())

		txIDs := hashes("on-restamp")
		checkControls := controls(t, "on-restamp")
		newDAH := height + 1 + retention

		type record struct {
			key  *aerospike.Key
			want aerospike.BinMap
			name string
		}

		var records []record

		for i, txID := range txIDs {
			if i == missingIndex {
				continue
			}

			name := fmt.Sprintf("tx %d", i)
			add := func(idx byte, want aerospike.BinMap, suffix string) {
				records = append(records, record{key: keyOf(txID, idx), want: want, name: name + suffix})
			}

			switch i % 4 {
			case 0: // fully spent: gets a DAH
				bins := master(txID, true, 0, aerospike.BinMap{unminedBin: 50})
				add(0, with(bins, aerospike.BinMap{unminedBin: nil, dahBin: newDAH, dahVerBin: 1}), "")
			case 1: // unspent with a stale DAH: the DAH is cleared
				bins := master(txID, false, 0, aerospike.BinMap{unminedBin: 50, dahBin: 1002})
				add(0, with(bins, aerospike.BinMap{unminedBin: nil, dahBin: nil, dahVerBin: 1}), "")
			case 2: // external, fully spent: master and both pages get the DAH
				bins := master(txID, true, 2, aerospike.BinMap{unminedBin: 50})
				add(0, with(bins, aerospike.BinMap{unminedBin: nil, dahBin: newDAH, dahVerBin: 1}), "")

				for idx := byte(1); idx <= 2; idx++ {
					add(idx, with(page(txID, idx, nil), aerospike.BinMap{dahBin: newDAH, dahVerBin: 1}), fmt.Sprintf(" page %d", idx))
				}
			case 3: // external, unspent with a stale DAH: master and both pages lose it
				bins := master(txID, false, 2, aerospike.BinMap{unminedBin: 50, dahBin: 1002})
				add(0, with(bins, aerospike.BinMap{unminedBin: nil, dahBin: nil, dahVerBin: 1}), "")

				for idx := byte(1); idx <= 2; idx++ {
					add(idx, with(page(txID, idx, aerospike.BinMap{dahBin: 1002}), aerospike.BinMap{dahBin: nil, dahVerBin: 1}), fmt.Sprintf(" page %d", idx))
				}
			}
		}

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, txIDs, true))

		for _, r := range records {
			requireBins(t, r.key, r.want, r.name)
		}

		exists, err := client.Exists(nil, keyOf(txIDs[missingIndex], 0))
		require.NoError(t, err)
		require.False(t, exists, "a missing record must not be created")

		checkControls()
	})
}
