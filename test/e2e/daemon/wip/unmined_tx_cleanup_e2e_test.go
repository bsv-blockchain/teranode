package smoke

import (
	"context"
	"database/sql"
	"encoding/hex"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnminedTransactionCleanup tests the unmined transaction cleanup functionality
// This test validates that:
// 1. Unmined transactions are tracked with unminedSince field
// 2. Old unmined transactions have their parents preserved after retention period
func TestUnminedTransactionCleanup(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	nonce := uint32(100)

	// Test with very short retention periods for deterministic testing
	const (
		unminedTxRetention       = 3 // 3 blocks retention (instead of default 1008)
		parentPreservationBlocks = 3 // 5 blocks preservation (instead of default 1440)
		utxoRetentionHeight      = 5 // 5 blocks (instead of default 288)
	)

	// Initialize test daemon with custom settings for short cleanup periods
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.UtxoStore.UnminedTxRetention = unminedTxRetention
				s.UtxoStore.ParentPreservationBlocks = parentPreservationBlocks
				s.GlobalBlockHeightRetention = utxoRetentionHeight
				s.UtxoStore.BlockHeightRetention = utxoRetentionHeight
			},
		),
	})
	defer td.Stop(t)

	var db interface {
		QueryRow(query string, args ...interface{}) *sql.Row
	}
	// Use the underlying DB from the UTXO store if available
	if sqlStore, ok := td.UtxoStore.(*utxosql.Store); ok {
		db = sqlStore.RawDB()
	}

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Get a spendable coinbase transaction
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create a valid "parent" transaction
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 5e8),
	)

	// Submit parent transaction and mine it
	parentTxHex := hex.EncodeToString(parentTx.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []interface{}{parentTxHex})
	require.NoError(t, err)

	// Mine a block to confirm the parent transaction
	_ = td.MineAndWait(t, 1)
	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)
	bestHeightA, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Best height: %d", bestHeightA)

	// Create an "unmined" transaction that spends from the parent
	// This transaction will be sent to the UTXO store but NOT mined
	unminedTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 45000000), // 0.45 BSV (leaving fee)
	)

	// Submit unmined transaction to propagation service (but don't mine it)
	unminedTxHex := hex.EncodeToString(unminedTx.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []interface{}{unminedTxHex})
	require.NoError(t, err)

	// Wait for transaction to be processed and stored as unmined
	time.Sleep(2 * time.Second)

	// Record the current block height when the unmined transaction was stored
	unminedStoredAtHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)

	// Verify the transaction is in the UTXO store as unmined
	unminedTxHash := unminedTx.TxIDChainHash()
	utxoMeta, err := td.UtxoStore.Get(td.Ctx, unminedTxHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotNil(t, utxoMeta.UnminedSince, "Transaction should be marked as unmined")
	require.Equal(t, utxoMeta.UnminedSince, unminedStoredAtHeight, "UnminedSince should be the current time")
	prevBlock := block3

	// Mine enough blocks to trigger cleanup (beyond retention period)
	blocksToMineBeforePreservation := uint32(unminedTxRetention) // Mine past retention period
	// use CreateTestBlock to mine blocks
	for i := uint32(0); i < blocksToMineBeforePreservation; i++ {
		nonce++
		_, prevBlock = td.CreateTestBlock(t, prevBlock, nonce)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, prevBlock, prevBlock.Height, "", "legacy"),
			"Failed to process block")
	}

	parentPreservationCutoffHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	assert.Equal(t, parentPreservationCutoffHeight, unminedStoredAtHeight+blocksToMineBeforePreservation, "Block height should have increased after mining")

	// Verify both transactions exist BEFORE cleanup
	parentTxHash := parentTx.TxIDChainHash()

	// Parent transaction should exist (it was mined)
	parentTxMetaData, err := td.UtxoStore.Get(td.Ctx, parentTxHash)
	require.NoError(t, err)
	require.NotNil(t, parentTxMetaData, "Parent transaction should exist before cleanup")
	t.Logf("Before cleanup: Parent transaction exists in UTXO store")

	// Unmined transaction should exist but be marked as unmined
	unminedMetaBefore, err := td.UtxoStore.Get(td.Ctx, unminedTxHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotNil(t, unminedMetaBefore.UnminedSince, "Unmined transaction should be marked as unmined")
	require.Equal(t, unminedMetaBefore.UnminedSince, unminedStoredAtHeight, "UnminedSince should be the current time")
	t.Logf("Before cleanup: Unmined transaction exists and marked as unmined (since block %d)", unminedMetaBefore.UnminedSince)

	// Manually trigger cleanup using PreserveParentsOfOldUnminedTransactions
	// This is the key function that actually performs the cleanup
	t.Logf("Triggering cleanup manually with current height %d", parentPreservationCutoffHeight)
	cleanupCount, err := utxo.PreserveParentsOfOldUnminedTransactions(td.Ctx, td.UtxoStore, parentPreservationCutoffHeight, "<test-hash>", td.Settings, td.Logger)
	require.NoError(t, err)
	t.Logf("Cleanup completed: processed %d old unmined transactions", cleanupCount)

	// Verify transaction states AFTER cleanup
	// Unmined transaction should still exist
	unminedMetaAfter, err := td.UtxoStore.Get(td.Ctx, unminedTxHash, fields.UnminedSince)
	require.NoError(t, err)
	assert.NotNil(t, unminedMetaAfter.UnminedSince, "Unmined transaction should still exist after cleanup")
	t.Logf("After cleanup: Unmined transaction still exists")

	// Parent transaction should still exist (protected by cleanup)
	parentMetaAfter, err := td.UtxoStore.Get(td.Ctx, parentTxHash)
	require.NoError(t, err)
	require.NotNil(t, parentMetaAfter, "Parent transaction should still exist after cleanup")
	t.Logf("After cleanup: Parent transaction still exists (protected from deletion)")

	// query the db for preserv_until
	var preserveUntil sql.NullInt32
	err = db.QueryRow("SELECT preserve_until FROM transactions WHERE hash = ?", parentTxHash[:]).Scan(&preserveUntil)
	require.NoError(t, err)
	t.Logf("PreserveUntil: %d", preserveUntil.Int32)
	preserveUntilHeight := parentPreservationCutoffHeight + parentPreservationBlocks
	assert.Equal(t, int32(preserveUntilHeight), preserveUntil.Int32)

	// query the db for delete_at_height
	var deleteAtHeight sql.NullInt32
	err = db.QueryRow("SELECT delete_at_height FROM transactions WHERE hash = ?", parentTxHash[:]).Scan(&deleteAtHeight)
	require.NoError(t, err)
	t.Logf("DeleteAtHeight: %d", deleteAtHeight.Int32)
	// delete at height should be 0
	assert.Equal(t, int32(0), deleteAtHeight.Int32)

	// Step 5: Verify we can query old unmined transactions
	if queryable, ok := td.UtxoStore.(interface {
		QueryOldUnminedTransactions(ctx context.Context, cutoffHeight uint32) ([]chainhash.Hash, error)
	}); ok {
		oldUnminedTxs, err := queryable.QueryOldUnminedTransactions(td.Ctx, parentPreservationCutoffHeight)
		require.NoError(t, err)

		assert.Equal(t, 1, len(oldUnminedTxs))
		assert.Equal(t, *unminedTxHash, oldUnminedTxs[0])
	}

	t.Logf("✓ Test completed successfully - unmined transaction cleanup lifecycle verified")

	// spend the unmined transaction
	spendingUnminedTxA := td.CreateTransactionWithOptions(t,
		transactions.WithInput(unminedTx, 0),
		transactions.WithP2PKHOutputs(1, 40000000),
	)
	spendingUnminedTxAHex := hex.EncodeToString(spendingUnminedTxA.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []any{spendingUnminedTxAHex})
	require.NoError(t, err)

	// spend the parent transaction again
	spendingParentTxB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 40000000),
	)

	// create a test block
	nonce++
	_, block4b := td.CreateTestBlock(t, block3, nonce, spendingParentTxB)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy")
	require.NoError(t, err)
	time.Sleep(2 * time.Second)
	prevBlockB := block4b

	_, err = td.UtxoStore.Get(td.Ctx, spendingParentTxB.TxIDChainHash())
	require.NoError(t, err)
	t.Logf("spending tx: %s", spendingParentTxB.TxIDChainHash().String())

	// mine a block and kick of delete at height for the unmined transaction
	_ = td.MineAndWait(t, 1)

	unminedTxMinedAtHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Unmined transaction mined at height: %d", unminedTxMinedAtHeight)

	// get the delete_at_height for the unmined transaction
	err = db.QueryRow("SELECT delete_at_height FROM transactions WHERE hash = ?", unminedTxHash[:]).Scan(&deleteAtHeight)
	require.NoError(t, err)
	t.Logf("DeleteAtHeight: %d", deleteAtHeight.Int32)
	// delete at height should not be 0, and should be the current height + block height retention
	expectedDeleteAtHeight := parentPreservationCutoffHeight + uint32(td.Settings.GlobalBlockHeightRetention)
	assert.Equal(t, int32(expectedDeleteAtHeight), deleteAtHeight.Int32)

	// verify the tx is not in blockassembly
	td.VerifyNotInBlockAssembly(t, unminedTx)

	// delete at height of the parent transaction should still be 0
	err = db.QueryRow("SELECT delete_at_height FROM transactions WHERE hash = ?", parentTxHash[:]).Scan(&deleteAtHeight)
	require.NoError(t, err)
	t.Logf("DeleteAtHeight: %d", deleteAtHeight.Int32)
	assert.Equal(t, int32(0), deleteAtHeight.Int32)

	// mine blocks upto the delete at height
	td.MineAndWait(t, uint32(td.Settings.GlobalBlockHeightRetention))
	time.Sleep(2 * time.Second)

	// the unmined transaction should be deleted
	utxoMeta, err = td.UtxoStore.Get(td.Ctx, unminedTxHash)
	require.Error(t, err)
	// expected TX_NOT_FOUND (30)
	require.ErrorContains(t, err, "TX_NOT_FOUND")

	// get the best height
	unminedTxDeletedAtHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Best height after unmined tx deletion: %d", unminedTxDeletedAtHeight)

	blocksToMineForChainB := unminedTxDeletedAtHeight + 2 - prevBlockB.Height
	t.Logf("Blocks to mine for chain B: %d", blocksToMineForChainB)

	// this block is at height 4, we need to mine more blocks to get past the existing chain (bestheight + 1)
	// for i := uint32(0); i < blocksToMineForChainB; i++ {
	// 	nonce++
	// 	_, prevBlockB = td.CreateTestBlock(t, prevBlockB, nonce)
	// 	err = td.BlockValidationClient.ProcessBlock(td.Ctx, prevBlockB, prevBlockB.Height)
	// 	require.NoError(t, err)
	// 	time.Sleep(2 * time.Second)
	// }

	// td.WaitForBlockHeight(t, prevBlockB, blockWait)
}

// TestUnminedTransactionCleanupAerospike tests the unmined transaction cleanup functionality
// with Aerospike as the UTXO store backend.
// This test validates that:
// 1. Unmined transactions are tracked with unminedSince field
// 2. Old unmined transactions have their parents preserved after retention period
func TestUnminedTransactionCleanupAerospike(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Initialize Aerospike container
	utxoStoreURL, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	nonce := uint32(100)

	// Test with very short retention periods for deterministic testing
	const (
		unminedTxRetention       = 3 // 3 blocks retention (instead of default 1008)
		parentPreservationBlocks = 3 // 3 blocks preservation (instead of default 1440)
		utxoRetentionHeight      = 5 // 5 blocks (instead of default 288)
	)

	// Initialize test daemon with custom settings for short cleanup periods
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				parsedURL, _ := url.Parse(utxoStoreURL)
				s.UtxoStore.UtxoStore = parsedURL
				s.UtxoStore.UnminedTxRetention = unminedTxRetention
				s.UtxoStore.ParentPreservationBlocks = parentPreservationBlocks
				s.GlobalBlockHeightRetention = utxoRetentionHeight
				s.UtxoStore.BlockHeightRetention = utxoRetentionHeight
			},
		),
	})
	defer td.Stop(t)

	// Set run state
	err = td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Get a spendable coinbase transaction
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create a valid "parent" transaction
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 5e8),
	)

	// Submit parent transaction and mine it
	parentTxHex := hex.EncodeToString(parentTx.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []interface{}{parentTxHex})
	require.NoError(t, err)

	// Mine a block to confirm the parent transaction
	_ = td.MineAndWait(t, 1)
	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)
	bestHeightA, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Best height: %d", bestHeightA)

	// Create an "unmined" transaction that spends from the parent
	// This transaction will be sent to the UTXO store but NOT mined
	unminedTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 45000000), // 0.45 BSV (leaving fee)
	)

	// Submit unmined transaction to propagation service (but don't mine it)
	unminedTxHex := hex.EncodeToString(unminedTx.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []interface{}{unminedTxHex})
	require.NoError(t, err)

	// Wait for transaction to be processed and stored as unmined
	time.Sleep(2 * time.Second)

	// Record the current block height when the unmined transaction was stored
	unminedStoredAtHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)

	// Verify the transaction is in the UTXO store as unmined
	unminedTxHash := unminedTx.TxIDChainHash()
	utxoMeta, err := td.UtxoStore.Get(td.Ctx, unminedTxHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotNil(t, utxoMeta.UnminedSince, "Transaction should be marked as unmined")
	require.Equal(t, utxoMeta.UnminedSince, unminedStoredAtHeight, "UnminedSince should be the current height")
	prevBlock := block3

	// Mine enough blocks to trigger cleanup (beyond retention period)
	blocksToMineBeforePreservation := uint32(unminedTxRetention) // Mine past retention period
	// use CreateTestBlock to mine blocks
	for i := uint32(0); i < blocksToMineBeforePreservation; i++ {
		nonce++
		_, prevBlock = td.CreateTestBlock(t, prevBlock, nonce)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, prevBlock, prevBlock.Height, "", "legacy"),
			"Failed to process block")
	}

	parentPreservationCutoffHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	assert.Equal(t, parentPreservationCutoffHeight, unminedStoredAtHeight+blocksToMineBeforePreservation, "Block height should have increased after mining")

	// Verify both transactions exist BEFORE cleanup
	parentTxHash := parentTx.TxIDChainHash()

	// Parent transaction should exist (it was mined)
	parentTxMetaData, err := td.UtxoStore.Get(td.Ctx, parentTxHash)
	require.NoError(t, err)
	require.NotNil(t, parentTxMetaData, "Parent transaction should exist before cleanup")
	t.Logf("Before cleanup: Parent transaction exists in UTXO store")

	// Unmined transaction should exist but be marked as unmined
	unminedMetaBefore, err := td.UtxoStore.Get(td.Ctx, unminedTxHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotNil(t, unminedMetaBefore.UnminedSince, "Unmined transaction should be marked as unmined")
	require.Equal(t, unminedMetaBefore.UnminedSince, unminedStoredAtHeight, "UnminedSince should be the stored height")
	t.Logf("Before cleanup: Unmined transaction exists and marked as unmined (since block %d)", unminedMetaBefore.UnminedSince)

	// Manually trigger cleanup using PreserveParentsOfOldUnminedTransactions
	// This is the key function that actually performs the cleanup
	t.Logf("Triggering cleanup manually with current height %d", parentPreservationCutoffHeight)
	cleanupCount, err := utxo.PreserveParentsOfOldUnminedTransactions(td.Ctx, td.UtxoStore, parentPreservationCutoffHeight, "<test-hash>", td.Settings, td.Logger)
	require.NoError(t, err)
	t.Logf("Cleanup completed: processed %d old unmined transactions", cleanupCount)

	// Verify transaction states AFTER cleanup
	// Unmined transaction should still exist
	unminedMetaAfter, err := td.UtxoStore.Get(td.Ctx, unminedTxHash, fields.UnminedSince)
	require.NoError(t, err)
	assert.NotNil(t, unminedMetaAfter.UnminedSince, "Unmined transaction should still exist after cleanup")
	t.Logf("After cleanup: Unmined transaction still exists")

	// Parent transaction should still exist (protected by cleanup)
	parentMetaAfter, err := td.UtxoStore.Get(td.Ctx, parentTxHash)
	require.NoError(t, err)
	require.NotNil(t, parentMetaAfter, "Parent transaction should still exist after cleanup")
	t.Logf("After cleanup: Parent transaction still exists (protected from deletion)")

	// For Aerospike, we can't query the database directly like SQL
	// But we can verify through the store interface
	// Check that we can query old unmined transactions
	if queryable, ok := td.UtxoStore.(interface {
		QueryOldUnminedTransactions(ctx context.Context, cutoffHeight uint32) ([]chainhash.Hash, error)
	}); ok {
		oldUnminedTxs, err := queryable.QueryOldUnminedTransactions(td.Ctx, parentPreservationCutoffHeight)
		require.NoError(t, err)

		assert.Equal(t, 1, len(oldUnminedTxs))
		assert.Equal(t, *unminedTxHash, oldUnminedTxs[0])
	}

	t.Logf("✓ Test completed successfully - unmined transaction cleanup lifecycle verified with Aerospike")

	// Test spending scenarios
	// Spend the unmined transaction
	spendingUnminedTxA := td.CreateTransactionWithOptions(t,
		transactions.WithInput(unminedTx, 0),
		transactions.WithP2PKHOutputs(1, 40000000),
	)
	spendingUnminedTxAHex := hex.EncodeToString(spendingUnminedTxA.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []any{spendingUnminedTxAHex})
	require.NoError(t, err)

	// Spend the parent transaction again
	spendingParentTxB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 40000000),
	)

	// Create a test block
	nonce++
	_, block4b := td.CreateTestBlock(t, block3, nonce, spendingParentTxB)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy")
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	_, err = td.UtxoStore.Get(td.Ctx, spendingParentTxB.TxIDChainHash())
	require.NoError(t, err)
	t.Logf("spending tx: %s", spendingParentTxB.TxIDChainHash().String())

	// Mine a block and kick off delete at height for the unmined transaction
	_ = td.MineAndWait(t, 1)

	unminedTxMinedAtHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Unmined transaction mined at height: %d", unminedTxMinedAtHeight)

	// Verify the tx is not in blockassembly
	td.VerifyNotInBlockAssembly(t, unminedTx)

	// Mine blocks up to the delete at height
	td.MineAndWait(t, uint32(td.Settings.GlobalBlockHeightRetention)+1)
	time.Sleep(10 * time.Second)

	// The unmined transaction should be deleted
	utxoMeta, err = td.UtxoStore.Get(td.Ctx, unminedTxHash)
	require.Error(t, err)
	// expected TX_NOT_FOUND (30)
	require.ErrorContains(t, err, "TX_NOT_FOUND")

	// Get the best height
	unminedTxDeletedAtHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Best height after unmined tx deletion: %d", unminedTxDeletedAtHeight)
}
