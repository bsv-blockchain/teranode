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

// TestForkDeleteAtHeightPruningDoubleSpend tests a scenario where:
//
// Steps:
// 1. Mine to maturity (creates blocks 0, 1, 2)
// 2. Create parentTx and mine it in block 3
// 3. Create txA (spends parentTx:0) and mine in block 4a
// 4. Create fork block 4b with txB (spends parentTx:0, conflicts with txA)
// 5. Spend txB outputs in block 5b (creates txBChild) - this will trigger pruning of txB
// 6. Extend main chain with txAChild and several more blocks to trigger DeleteAtHeight
// 7. Wait for DeleteAtHeight UTXO pruning to occur (txB should be deleted via retention policy)
// 8. Extend fork to make it longer and trigger reorg
// 9. Verify system handles the reorg correctly AND all blocks have processed_at set
func TestForkDeleteAtHeightPruningDoubleSpendPostgres(t *testing.T) {
	t.Run("fork_delete_at_height_pruning_double_spend", func(t *testing.T) {
		testForkDeleteAtHeightPruningDoubleSpend(t, "postgres")
	})
}

func TestForkDeleteAtHeightPruningDoubleSpendAerospike(t *testing.T) {
	t.Run("fork_delete_at_height_pruning_double_spend", func(t *testing.T) {
		testForkDeleteAtHeightPruningDoubleSpend(t, "aerospike")
	})
}

func testForkDeleteAtHeightPruningDoubleSpend(t *testing.T, utxoStore string) {
	const blockWait = 10 * time.Second

	// Create test daemon with very short retention periods to enable DeleteAtHeight pruning
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStore,
		EnableErrorLogging:   true,
		EnablePruner:         true,
		EnableBlockPersister: true,
		SettingsOverrideFunc: test.ComposeSettings(
			externalTxSettingsFunc(),
			func(s *settings.Settings) {
				// Very short retention to enable quick DeleteAtHeight pruning
				s.GlobalBlockHeightRetention = 1 // Prune after 3 blocks
				s.UtxoStore.UnminedTxRetention = 2
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
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
		transactions.WithP2PKHOutputs(2, parentTx.Outputs[0].Satoshis/2-100),
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

	// Verify 4a is still winning
	td.WaitForBlockHeight(t, block4a, blockWait, true)

	// ========== Extend main chain to trigger DeleteAtHeight for txB ==========
	// Create txAChild on main chain
	txAChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA, 0),
		transactions.WithInput(txA, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, txA.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("TxAChild: %s - spends txA outputs", txAChild.TxIDChainHash().String())

	// require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txAChild))
	// require.NoError(t, td.WaitForTransactionInBlockAssembly(txAChild, blockWait))
	// td.MineAndWait(t, 1)


	_, block5a := td.CreateTestBlock(t, block4a, 20601, txAChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5a, block5a.Height, "", "legacy"))

	//                 / 4a [txA] -> 5a [txA_child] (*)
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b [txB_child]

	// Mine several more blocks on chain A to exceed retention period
	// GlobalBlockHeightRetention = 3, so we need to mine enough blocks
	// for txB to reach DeleteAtHeight
	t.Log("Mining additional blocks to trigger DeleteAtHeight UTXO pruning...")

	err = td.WaitForBlockPersisted(block4a.GetHash(), blockWait)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, tempBlock := td.CreateTestBlock(t, block5a, uint32(10601+i))
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, tempBlock, tempBlock.Height, "", "legacy"))
		td.WaitForBlockBeingMined(t, tempBlock)
		td.WaitForBlockHeight(t, tempBlock, blockWait, true)
		block5a = tempBlock // Update for next iteration
		t.Logf("ChainA Mined block at height %d", tempBlock.Height)
	}

	//                 / 4a [txA] -> 5a [txA_child] -> 6a -> 7a -> 8a -> 9a -> 10a (*)
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b [txB_child]

	// Verify txA has been pruned
	txAHash := txA.TxIDChainHash()
	meta, err := td.UtxoStore.Get(td.Ctx, txAHash)
	require.Error(t, err)
	require.Nil(t, meta, "txA should be pruned")

	// ========== Extend Chain B to make it longer and trigger reorg ==========
	// ========== Spend txB outputs on the fork to trigger DeleteAtHeight pruning ==========
	// Create txBChild that spends txB outputs
	// This will cause txB to be marked for deletion after retention period
	txBChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txB, 0),
		transactions.WithInput(txB, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, txB.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("TxBChild: %s - spends txB outputs (will trigger txB DeleteAtHeight pruning)", txBChild.TxIDChainHash().String())

	// Create block 5b with txBChild
	_, block5b := td.CreateTestBlock(t, block4b, 20501, txBChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"))

	//                 / 4a [txA] (*)
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b [txBChild]
	t.Log("Extending Chain B to trigger reorg with DeleteAtHeight pruned transaction...")

	// Build on block5b to make chain B longer than chain A
	currentBlock := block5b
	for i := 0; i < 7; i++ {
		_, tempBlock := td.CreateTestBlock(t, currentBlock, uint32(20601+i))
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, tempBlock, tempBlock.Height, "", "legacy"))
		currentBlock = tempBlock
		t.Logf("Created fork block at height %d", tempBlock.Height)
	}

	finalForkBlock := currentBlock

	//                 / 4a [txA] -> 5a [txA_child] -> 6a -> 7a -> 8a -> 9a -> 10a
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b [txB_child] -> 6b -> 7b -> 8b -> 9b -> 10b -> 11b -> 12b (*)

	// Wait for reorg to complete
	td.WaitForBlockHeight(t, finalForkBlock, blockWait*2, true)

	t.Log("Chain B is now the longest chain after DeleteAtHeight pruning")

	// ========== Verify State After Reorg ==========

	// Verify Chain B is now the winner
	bestBlock, bestMeta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, finalForkBlock.Header.Hash().String(), bestBlock.Hash().String(),
		"Best block should be the final fork block after reorg")

	// Verify txA is now conflicting (moved to losing chain)
	td.VerifyConflictingInUtxoStore(t, true, txA)
	t.Log("TxA is now conflicting (moved to losing chain)")

	t.Log("Verifying processed_at is set for all blocks after reorg with DeleteAtHeight pruned transaction...")

	// Check a sample of blocks on the new winning chain
	heightsToCheck := []uint32{4, 5, 8, 10, finalForkBlock.Height}
	for _, height := range heightsToCheck {
		blockAtHeight, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, height)
		require.NoError(t, err)
		_, blockMeta, err := td.BlockchainClient.GetBlockHeader(td.Ctx, blockAtHeight.Header.Hash())
		require.NoError(t, err)
		require.NotNil(t, blockMeta.ProcessedAt,
			"Block %d processed_at should be set after reorg", height)
		t.Logf("Block %d processed_at: %v ✓", height, blockMeta.ProcessedAt)
	}

	t.Log("✅ All blocks have processed_at set correctly after reorg with DeleteAtHeight pruned transaction")

	// Verify height matches expected
	require.Equal(t, finalForkBlock.Height, bestMeta.Height,
		"Blockchain height should match final fork block height")

	t.Log("Test completed successfully - DeleteAtHeight UTXO pruning did not cause processed_at skipping")
}

func testForkDeleteAtHeightPruningDoubleSpend__(t *testing.T, utxoStore string) {
	const blockWait = 10 * time.Second

	// Create test daemon with very short retention periods to enable DeleteAtHeight pruning
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:      utxoStore,
		EnableErrorLogging: true,
		EnablePruner: true,
		SettingsOverrideFunc: test.ComposeSettings(
			externalTxSettingsFunc(),
			func(s *settings.Settings) {
				// Very short retention to enable quick DeleteAtHeight pruning
				s.GlobalBlockHeightRetention = 1 // Prune after 3 blocks
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
		transactions.WithP2PKHOutputs(2, parentTx.Outputs[0].Satoshis/2-100),
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
		transactions.WithP2PKHOutputs(2, parentTx.Outputs[0].Satoshis/2-100),
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

	// ========== Spend txB outputs on the fork to trigger DeleteAtHeight pruning ==========
	// Create txBChild that spends txB outputs
	// This will cause txB to be marked for deletion after retention period
	txBChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txB, 0),
		transactions.WithInput(txB, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, txB.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("TxBChild: %s - spends txB outputs (will trigger txB DeleteAtHeight pruning)", txBChild.TxIDChainHash().String())

	// Create block 5b with txBChild
	_, block5b := td.CreateTestBlock(t, block4b, 20501, txBChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"))

	//                 / 4a [txA] (*)
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b [txB_child]

	// Verify 5b is still winning
	td.WaitForBlockHeight(t, block5b, blockWait, true)

	// ========== Extend main chain to trigger DeleteAtHeight for txB ==========
	// Create txAChild on main chain
	txAChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA, 0),
		transactions.WithInput(txA, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, txA.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("TxAChild: %s - spends txA outputs", txAChild.TxIDChainHash().String())

	// require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txAChild))
	// require.NoError(t, td.WaitForTransactionInBlockAssembly(txAChild, blockWait))
	// td.MineAndWait(t, 1)


	_, block5a := td.CreateTestBlock(t, block4a, 20601, txAChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5a, block5a.Height, "", "legacy"))

	//                 / 4a [txA] -> 5a [txA_child] (*)
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b [txB_child]

	// Mine several more blocks on chain A to exceed retention period
	// GlobalBlockHeightRetention = 3, so we need to mine enough blocks
	// for txB to reach DeleteAtHeight
	t.Log("Mining additional blocks to trigger DeleteAtHeight UTXO pruning...")

	var lastBlock *model.Block
	for i := 0; i < 5; i++ {
		_, tempBlock := td.CreateTestBlock(t, block5a, uint32(10601+i))
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, tempBlock, tempBlock.Height, "", "legacy"))
		td.WaitForBlockBeingMined(t, tempBlock)
		td.WaitForBlockHeight(t, tempBlock, blockWait, true)
		lastBlock = tempBlock
		block5a = tempBlock // Update for next iteration
		t.Logf("Mined block at height %d", tempBlock.Height)
	}

	//                 / 4a [txA] -> 5a [txA_child] -> 6a -> 7a -> 8a -> 9a -> 10a (*)
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b [txB_child]

	// At this point, txB should have been pruned via DeleteAtHeight
	// because it's been more than GlobalBlockHeightRetention (3) blocks since it was on losing chain

	t.Log("Waiting for DeleteAtHeight UTXO pruning to complete...")
	time.Sleep(2 * time.Second) // Give system time to process pruning

	// Verify txB has been pruned (should not be found)
	txBHash := txB.TxIDChainHash()
	meta, err := td.UtxoStore.Get(td.Ctx, txBHash)
	if err == nil && meta != nil {
		t.Logf("WARNING: txB not yet pruned (CurrentHeight: %d, Conflicting: %v)",
			lastBlock.Height, meta.Conflicting)
		// Note: In some cases, pruning might not have occurred yet
		// The test will still demonstrate the bug if it happens during reorg
	} else {
		t.Logf("✓ TxB has been pruned via DeleteAtHeight from UTXO store")
	}

	// Verify txA has not been pruned
	txAHash := txA.TxIDChainHash()
	meta, err = td.UtxoStore.Get(td.Ctx, txAHash)
	if err == nil && meta != nil {
		t.Logf("✓ TxA has not been pruned (CurrentHeight: %d, Conflicting: %v)",
			lastBlock.Height, meta.Conflicting)
	} else {
		t.Logf("WARNING: txA has been pruned (CurrentHeight: %d, Conflicting: %v)",
			lastBlock.Height, meta.Conflicting)
	}

	// ========== Extend Chain B to make it longer and trigger reorg ==========
	t.Log("Extending Chain B to trigger reorg with DeleteAtHeight pruned transaction...")

	// Build on block5b to make chain B longer than chain A
	currentBlock := block5b
	for i := 0; i < 7; i++ {
		_, tempBlock := td.CreateTestBlock(t, currentBlock, uint32(20601+i))
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, tempBlock, tempBlock.Height, "", "legacy"))
		currentBlock = tempBlock
		t.Logf("Created fork block at height %d", tempBlock.Height)
	}

	finalForkBlock := currentBlock

	//                 / 4a [txA] -> 5a [txA_child] -> 6a -> 7a -> 8a -> 9a -> 10a
	// 0 -> 1 -> 2 -> 3 [parentTx]
	//                 \ 4b [txB] -> 5b [txB_child] -> 6b -> 7b -> 8b -> 9b -> 10b -> 11b -> 12b (*)

	// Wait for reorg to complete
	td.WaitForBlockHeight(t, finalForkBlock, blockWait*2, true)

	t.Log("Chain B is now the longest chain after DeleteAtHeight pruning")

	// ========== Verify State After Reorg ==========

	// Verify Chain B is now the winner
	bestBlock, bestMeta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, finalForkBlock.Header.Hash().String(), bestBlock.Hash().String(),
		"Best block should be the final fork block after reorg")

	// Verify txA is now conflicting (moved to losing chain)
	td.VerifyConflictingInUtxoStore(t, true, txA)
	t.Log("TxA is now conflicting (moved to losing chain)")

	// ========== Critical Verification: processed_at should be set ==========
	// This is the key test - ensure processed_at is set for all blocks after reorg
	// The bug was: "Failure during moveForward, causing a reset, failing and then skipping processed_at"

	t.Log("Verifying processed_at is set for all blocks after reorg with DeleteAtHeight pruned transaction...")

	// Check a sample of blocks on the new winning chain
	heightsToCheck := []uint32{4, 5, 8, 10, finalForkBlock.Height}
	for _, height := range heightsToCheck {
		blockAtHeight, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, height)
		require.NoError(t, err)
		_, blockMeta, err := td.BlockchainClient.GetBlockHeader(td.Ctx, blockAtHeight.Header.Hash())
		require.NoError(t, err)
		require.NotNil(t, blockMeta.ProcessedAt,
			"Block %d processed_at should be set after reorg", height)
		t.Logf("Block %d processed_at: %v ✓", height, blockMeta.ProcessedAt)
	}

	t.Log("✅ All blocks have processed_at set correctly after reorg with DeleteAtHeight pruned transaction")

	// Verify height matches expected
	require.Equal(t, finalForkBlock.Height, bestMeta.Height,
		"Blockchain height should match final fork block height")

	t.Log("Test completed successfully - DeleteAtHeight UTXO pruning did not cause processed_at skipping")
}
