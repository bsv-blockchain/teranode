package doublespendtest

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestForkPruningDoubleSpend tests a scenario where:
//
// Test scenario:
//
//	/ 4a [txA] (initially longest)
//
// 0 -> 1 -> 2 -> 3 [parentTx]
//
//	\ 4b [txB - conflicts with txA]
//
// Steps:
// 1. Mine to maturity (creates blocks 0, 1, 2)
// 2. Create parentTx and mine it in block 3
// 3. Create txA (spends parentTx:0) and mine in block 4a
// 4. Create fork block 4b with txB (spends parentTx:0, conflicts with txA)
// 5. Delete txB from UTXO store (simulate pruning/deletion at DeleteAtHeight)
// 6. Extend fork to make it longer: 4b -> 5b -> 6b
// 7. Verify system handles the reorg correctly AND all blocks have processed_at set
func TestForkPruningDoubleSpendPostgres(t *testing.T) {
	t.Run("fork_pruning_double_spend", func(t *testing.T) {
		testForkPruningDoubleSpend(t, "postgres")
	})
}

func TestForkPruningDoubleSpendAerospike(t *testing.T) {
	t.Run("fork_pruning_double_spend", func(t *testing.T) {
		testForkPruningDoubleSpend(t, "aerospike")
	})
}

func testForkPruningDoubleSpend(t *testing.T, utxoStore string) {
	const blockWait = 10 * time.Second

	// Create test daemon with short retention periods to allow pruning
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:      utxoStore,
		// EnableErrorLogging: true,
		SettingsOverrideFunc: test.ComposeSettings(
			externalTxSettingsFunc(),
			func(s *settings.Settings) {
				// Short retention to enable pruning test
				s.GlobalBlockHeightRetention = 5
				s.UtxoStore.UnminedTxRetention = 2
			},
		),
	})
	defer func() {
		td.Stop(t)
	}()

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks and get spendable coinbase
	// CoinbaseMaturity = 1, so this mines 2 blocks (genesis + block 1, then waits for block 2)
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// ========== Create Parent Transaction ==========
	// Create parent transaction with multiple outputs
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, coinbaseTx.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("ParentTx: %s (%d outputs)", parentTx.TxIDChainHash().String(), len(parentTx.Outputs))

	// Propagate and mine parent transaction
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTx))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(parentTx, blockWait))
	td.MineAndWait(t, 1)

	blockParent, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)
	t.Logf("Block 3 mined with parentTx")

	// 0 -> 1 -> 2 -> 3 [parentTx]

	// ========== Create Chain A: txA spends parentTx:0 ==========
	txA := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, parentTx.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("TxA: %s - spends parentTx:0", txA.TxIDChainHash().String())

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txA, blockWait))
	td.MineAndWait(t, 1)

	block4a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	// 0 -> 1 -> 2 -> 3 [parentTx] -> 4a [txA] (*)

	t.Log("Chain A established with txA")

	// Verify txA is on longest chain
	td.VerifyOnLongestChainInUtxoStore(t, txA)

	// ========== Create Fork: Chain B with txB (conflicts with txA) ==========
	txB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0), // CONFLICT with txA!
		transactions.WithInput(parentTx, 1), // Additional input to differentiate
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, parentTx.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("TxB: %s - spends parentTx:0 (CONFLICTS with txA)", txB.TxIDChainHash().String())

	// Create fork block 4b starting from block 3 (before 4a)
	_, block4b := td.CreateTestBlock(t, blockParent, 20401, txB)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy"),
		"Failed to process fork block 4b")

	//                 / 4a [txA] (*)
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB]

	// Verify 4a is still the winning chain
	td.WaitForBlockHeight(t, block4a, blockWait, true)

	// Verify txB is marked as conflicting (losing chain)
	td.VerifyConflictingInUtxoStore(t, true, txB)
	t.Log("TxB is conflicting (as expected on losing chain)")

	// ========== Simulate pruning by deleting txB from UTXO store ==========
	// This simulates the scenario where:
	// - txB UTXOs reach DeleteAtHeight and are pruned
	// - OR txB is manually deleted for some reason
	txAHash := txA.TxIDChainHash()
	err = td.UtxoStore.Delete(td.Ctx, txAHash)
	require.NoError(t, err)
	t.Logf("Deleted txA from UTXO store (simulating pruning)")

	// Verify txA is deleted
	meta, err := td.UtxoStore.Get(td.Ctx, txAHash)
	require.Error(t, err, "txA should be deleted from UTXO store")
	require.Nil(t, meta, "txA metadata should be nil after deletion")

	// ========== Extend Chain B to make it longer ==========
	// Create empty blocks to make Chain B longer than Chain A
	t.Log("Extending Chain B to trigger reorg...")

	_, block5b := td.CreateTestBlock(t, block4b, 20501)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"))

	_, block6b := td.CreateTestBlock(t, block5b, 20601)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6b, block6b.Height, "", "legacy"))

	//                 / 4a [txA]
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b -> 6b (*)

	// Wait for reorg to complete
	td.WaitForBlockHeight(t, block6b, blockWait, true)

	t.Log("Chain B is now the longest chain")

	// ========== Verify State After Reorg ==========

	// Verify Chain B is now the winner
	bestBlock, bestMeta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, block6b.Header.Hash().String(), bestBlock.Hash().String(),
		"Best block should be block6b after reorg")

	// Verify txA is now conflicting (losing chain)
	td.VerifyConflictingInUtxoStore(t, true, txA)
	t.Log("TxA is now conflicting (moved to losing chain)")

	// ========== Critical Verification: processed_at should be set ==========
	// This is the key test - ensure processed_at is set for all blocks after reorg
	// The bug was: "Failure during moveForward, causing a reset, failing and then skipping processed_at"

	// Check block 4b - retrieve it to get metadata
	block4bRetrieved, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)
	block4bHeader, block4bMeta, err := td.BlockchainClient.GetBlockHeader(td.Ctx, block4bRetrieved.Header.Hash())
	require.NoError(t, err)
	require.NotNil(t, block4bHeader, "Block 4b header should exist")
	require.NotNil(t, block4bMeta.ProcessedAt,
		"Block 4b processed_at should be set after reorg")
	t.Logf("Block 4b processed_at: %v ✓", block4bMeta.ProcessedAt)

	// Check block 5b
	block5bRetrieved, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 5)
	require.NoError(t, err)
	block5bHeader, block5bMeta, err := td.BlockchainClient.GetBlockHeader(td.Ctx, block5bRetrieved.Header.Hash())
	require.NoError(t, err)
	require.NotNil(t, block5bHeader, "Block 5b header should exist")
	require.NotNil(t, block5bMeta.ProcessedAt,
		"Block 5b processed_at should be set after reorg")
	t.Logf("Block 5b processed_at: %v ✓", block5bMeta.ProcessedAt)

	// Check block 6b
	block6bRetrieved, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 6)
	require.NoError(t, err)
	block6bHeader, block6bMeta, err := td.BlockchainClient.GetBlockHeader(td.Ctx, block6bRetrieved.Header.Hash())
	require.NoError(t, err)
	require.NotNil(t, block6bHeader, "Block 6b header should exist")
	require.NotNil(t, block6bMeta.ProcessedAt,
		"Block 6b processed_at should be set after reorg")
	t.Logf("Block 6b processed_at: %v ✓", block6bMeta.ProcessedAt)

	t.Log("✅ All blocks have processed_at set correctly after reorg with pruned transaction")

	// Verify height matches expected
	require.Equal(t, uint32(6), bestMeta.Height,
		"Blockchain height should be 6 after reorg")

	t.Log("Test completed successfully - no processed_at skipping occurred")
}

// TestForkPruningDoubleSpendLosingChain tests the inverse scenario where
// the losing chain's transactions are pruned and then the original chain extends.
//
// ADDITIONAL SCENARIO:
// This test ensures that pruning transactions on the LOSING chain doesn't cause
// processed_at issues when the winning chain continues to extend. This verifies
// that the system correctly handles cleanup of pruned conflicting transactions
// without impacting the main chain's block processing.
func TestForkPruningDoubleSpendLosingChainPostgres(t *testing.T) {
	t.Run("fork_pruning_losing_chain", func(t *testing.T) {
		testForkPruningDoubleSpendLosingChain(t, "postgres")
	})
}

func TestForkPruningDoubleSpendLosingChainAerospike(t *testing.T) {
	t.Run("fork_pruning_losing_chain", func(t *testing.T) {
		testForkPruningDoubleSpendLosingChain(t, "aerospike")
	})
}

func testForkPruningDoubleSpendLosingChain(t *testing.T, utxoStore string) {
	const blockWait = 10 * time.Second

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:      utxoStore,
		// EnableErrorLogging: true,
		SettingsOverrideFunc: test.ComposeSettings(
			externalTxSettingsFunc(),
			func(s *settings.Settings) {
				s.GlobalBlockHeightRetention = 5
				s.UtxoStore.UnminedTxRetention = 2
			},
		),
	})
	defer func() {
		td.Stop(t)
	}()

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// CoinbaseMaturity = 1, so this mines 2 blocks
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create parent transaction
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, coinbaseTx.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("ParentTx: %s", parentTx.TxIDChainHash().String())

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTx))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(parentTx, blockWait))
	td.MineAndWait(t, 1)

	blockParent, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	// Create main chain transaction
	txA := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(2, parentTx.Outputs[0].Satoshis/2-100),
	)
	t.Logf("TxA: %s", txA.TxIDChainHash().String())

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txA, blockWait))
	td.MineAndWait(t, 1)

	block4a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	// Extend main chain further
	_, block5a := td.CreateTestBlock(t, block4a, 10401)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5a, block5a.Height, "", "legacy"))
	td.WaitForBlockHeight(t, block5a, blockWait, true)

	//                       / 4a [txA] -> 5a (*)
	// 0 -> 1 -> 2 -> 3 [parentTx] ->

	// Create competing fork with txB (conflicts with txA)
	txB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0), // CONFLICT
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(2, parentTx.Outputs[0].Satoshis/2-100),
	)
	t.Logf("TxB: %s - conflicts with txA", txB.TxIDChainHash().String())

	_, block4b := td.CreateTestBlock(t, blockParent, 30301, txB)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy"))

	//                       / 4a [txA] -> 5a (*)
	// 0 -> 1 -> 2 -> 3 [parentTx] ->
	//                       \ 4b [txB]

	// Verify 5a is still winning
	td.WaitForBlockHeight(t, block5a, blockWait, true)

	// Verify txB is conflicting (on losing chain)
	td.VerifyConflictingInUtxoStore(t, true, txB)

	// Delete txB from UTXO store (simulate pruning of losing chain)
	txBHash := txB.TxIDChainHash()
	err = td.UtxoStore.Delete(td.Ctx, txBHash)
	require.NoError(t, err)
	t.Logf("Deleted txB (losing chain tx) from UTXO store")

	// Extend the winning chain even more
	_, block6a := td.CreateTestBlock(t, block5a, 10501)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6a, block6a.Height, "", "legacy"))

	_, block7a := td.CreateTestBlock(t, block6a, 10601)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block7a, block7a.Height, "", "legacy"))

	//                       / 4a [txA] -> 5a -> 6a -> 7a (*)
	// 0 -> 1 -> 2 -> 3 [parentTx] ->
	//                       \ 4b [txB - deleted]

	td.WaitForBlockHeight(t, block7a, blockWait, true)

	// Verify all blocks have processed_at set
	for _, block := range []*model.Block{block5a, block6a, block7a} {
		_, blockMeta, err := td.BlockchainClient.GetBlockHeader(td.Ctx, block.Header.Hash())
		require.NoError(t, err)
		require.NotNil(t, blockMeta.ProcessedAt,
			"Block %d processed_at should be set", block.Height)
		t.Logf("Block %d processed_at: %v ✓", block.Height, blockMeta.ProcessedAt)
	}

	// Verify txA is still valid (on winning chain)
	td.VerifyConflictingInUtxoStore(t, false, txA)


	// now extend the losing chain further to make it longer
	// use a loop to go from 4b to 9b
	currentBlock := block4b
	for i := 0; i < 5; i++ {
		_, nextBlock := td.CreateTestBlock(t, currentBlock, uint32(30401+i*100))
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, nextBlock, uint32(nextBlock.Height), "", "legacy"))
		currentBlock = nextBlock
	}
	block9b := currentBlock

	//                       / 4a [txA] -> 5a -> 6a -> 7a (*)
	// 0 -> 1 -> 2 -> 3 [parentTx] ->
	//                       \ 4b [txB - deleted] -> 8b -> 9b

	td.WaitForBlockHeight(t, block9b, blockWait, true)

	// Verify all blocks have processed_at set
	for _, block := range []*model.Block{block9b} {
		_, blockMeta, err := td.BlockchainClient.GetBlockHeader(td.Ctx, block.Header.Hash())
		require.NoError(t, err)
		require.NotNil(t, blockMeta.ProcessedAt,
			"Block %d processed_at should be set", block.Height)
		t.Logf("Block %d processed_at: %v ✓", block.Height, blockMeta.ProcessedAt)
	}

	t.Log("✅ Test completed - winning chain extended successfully with pruned losing chain")
}
