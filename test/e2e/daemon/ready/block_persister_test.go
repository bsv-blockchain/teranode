package smoke

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestBlockPersister tests that the block persister correctly persists blocks and subtrees
// after mining on a single node.
//
// Test scenario:
// 1. Start a single node with blockchain, blockassembly, and blockpersister services
// 2. Mine to maturity and get a spendable coinbase
// 3. Create transactions and mine blocks containing them
// 4. After each block is mined, verify that:
//   - The block has been marked as persisted in the blockchain database
//   - The block file exists in the block store
//   - The subtree data files exist in the subtree store
func TestBlockPersister(t *testing.T) {
	t.Log("=== Test: Block Persister Single Node ===")
	t.Log("This test verifies that blocks and subtrees are correctly persisted after mining")

	// Create a single node with block persister enabled
	node := newBlockPersisterTestNode(t)
	defer node.Stop(t)

	// Get the block store for verification
	blockStore, err := node.GetBlockStore()
	require.NoError(t, err, "Failed to get block persister store")

	// Phase 1: Mine to maturity and get spendable coinbase
	t.Log("Phase 1: Mining to maturity to get spendable coinbase...")
	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, node.Ctx)
	t.Logf("Coinbase transaction: %s", coinbaseTx.TxIDChainHash().String())

	// Get current height after maturity mining
	_, meta, err := node.BlockchainClient.GetBestBlockHeader(node.Ctx)
	require.NoError(t, err)
	startHeight := meta.Height
	t.Logf("Starting height after maturity: %d", startHeight)

	// Phase 2: Create and mine blocks with transactions
	t.Log("Phase 2: Creating transactions and mining blocks...")

	txChain := make([]*bt.Tx, 0)
	minedBlocks := make([]*model.Block, 0)
	currentTx := coinbaseTx

	// Mine 5 blocks with 1 transaction each
	numBlocksToMine := 5
	for i := 0; i < numBlocksToMine; i++ {
		// Create a new transaction spending from the previous one
		newTx := node.CreateTransactionWithOptions(t,
			transactions.WithInput(currentTx, 0),
			transactions.WithP2PKHOutputs(1, currentTx.TotalOutputSatoshis()),
		)

		txChain = append(txChain, newTx)

		// Send the transaction
		err := node.PropagationClient.ProcessTransaction(node.Ctx, newTx)
		require.NoError(t, err, "Failed to send transaction %d", i)

		t.Logf("Created and sent transaction %d: %s", i+1, newTx.TxIDChainHash().String())

		// Wait for the transaction to be processed by block assembly
		err = node.WaitForTransactionInBlockAssembly(newTx, 10*time.Second)
		require.NoError(t, err, "Timeout waiting for transaction to be processed by block assembly")

		// Mine a block with this transaction
		block := node.MineAndWait(t, 1)
		minedBlocks = append(minedBlocks, block)
		t.Logf("Mined block %d at height %d: %s", i+1, startHeight+uint32(i+1), block.Hash().String())

		// Update current transaction for next iteration
		currentTx = newTx
	}

	t.Logf("Successfully mined %d blocks with transactions", len(minedBlocks))

	// Phase 3: Verify block persistence
	t.Log("Phase 3: Verifying block persistence...")

	for i, block := range minedBlocks {
		blockHash := block.Hash()
		expectedHeight := startHeight + uint32(i+1)

		t.Logf("Checking persistence for block %d (height %d): %s", i+1, expectedHeight, blockHash.String())

		// Wait for the block to be persisted
		err := node.WaitForBlockPersisted(blockHash, 30*time.Second)
		require.NoError(t, err, "Block %d was not persisted within timeout", i+1)

		// Verify block file exists in block store
		blockExists, err := blockStore.Exists(node.Ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
		require.NoError(t, err, "Failed to check if block file exists")
		require.True(t, blockExists, "Block file should exist in block store for block %d", i+1)
		t.Logf("  Block file exists in store")

		// Verify subtree data files exist
		fullBlock, err := node.BlockchainClient.GetBlock(node.Ctx, blockHash)
		require.NoError(t, err, "Failed to get full block data")

		if len(fullBlock.Subtrees) > 0 {
			for j, subtreeHash := range fullBlock.Subtrees {
				subtreeExists, err := node.SubtreeStore.Exists(node.Ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
				require.NoError(t, err, "Failed to check if subtree data file exists")
				require.True(t, subtreeExists, "Subtree data file should exist for subtree %d in block %d", j, i+1)
			}
			t.Logf("  %d subtree data files exist in store", len(fullBlock.Subtrees))
		} else {
			t.Logf("  Block has no subtrees (coinbase only)")
		}

		t.Logf("  Block %d persistence verified", i+1)
	}

	// Phase 4: Verify all blocks are marked as persisted in the database
	t.Log("Phase 4: Verifying no unpersisted blocks remain...")

	// Give the persister a moment to finish any remaining work
	time.Sleep(2 * time.Second)

	// Check that there are no unpersisted blocks
	unpersistedBlocks, err := node.BlockchainClient.GetBlocksNotPersisted(node.Ctx, 100)
	require.NoError(t, err, "Failed to get unpersisted blocks")

	if len(unpersistedBlocks) > 0 {
		for _, b := range unpersistedBlocks {
			t.Logf("  Unpersisted block: height=%d, hash=%s", b.Height, b.Hash().String())
		}
	}
	require.Empty(t, unpersistedBlocks, "All blocks should be persisted, but found %d unpersisted", len(unpersistedBlocks))
	t.Log("  All blocks are marked as persisted in the database")

	// Phase 5: Verify block data can be retrieved from store and parsed correctly
	t.Log("Phase 5: Verifying block data can be retrieved from store...")

	for i, block := range minedBlocks {
		blockHash := block.Hash()

		// Read block data from store
		blockData, err := blockStore.Get(node.Ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
		require.NoError(t, err, "Failed to read block data from store for block %d", i+1)
		require.NotEmpty(t, blockData, "Block data should not be empty for block %d", i+1)

		// Verify the block data can be parsed as a valid block
		parsedBlock, err := model.NewBlockFromBytes(blockData)
		require.NoError(t, err, "Failed to parse block data for block %d", i+1)
		require.Equal(t, blockHash.String(), parsedBlock.Hash().String(), "Parsed block hash should match for block %d", i+1)

		t.Logf("  Block %d data retrieved successfully (%d bytes)", i+1, len(blockData))
	}

	t.Log("=== Test Passed: Block Persister correctly persists blocks and subtrees ===")
}

// TestBlockPersisterWithMultipleTransactionsPerBlock tests persistence with blocks containing multiple transactions.
func TestBlockPersisterWithMultipleTransactionsPerBlock(t *testing.T) {
	t.Log("=== Test: Block Persister with Multiple Transactions Per Block ===")

	node := newBlockPersisterTestNode(t)
	defer node.Stop(t)

	blockStore, err := node.GetBlockStore()
	require.NoError(t, err, "Failed to get block persister store")

	// Mine to maturity
	t.Log("Phase 1: Mining to maturity...")
	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, node.Ctx)
	t.Logf("Coinbase tx: %s", coinbaseTx.TxIDChainHash().String())

	// Create multiple transactions from the coinbase
	t.Log("Phase 2: Creating multiple transactions for a single block...")

	// First, create a transaction with multiple outputs that we can spend
	outputAmount := uint64(100000) // 100k satoshis per output
	multiOutputTx := node.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(5, outputAmount), // Create 5 outputs of 100k each
	)

	err = node.PropagationClient.ProcessTransaction(node.Ctx, multiOutputTx)
	require.NoError(t, err, "Failed to send multi-output transaction")

	err = node.WaitForTransactionInBlockAssembly(multiOutputTx, 10*time.Second)
	require.NoError(t, err, "Timeout waiting for multi-output transaction")

	// Mine the block with the multi-output transaction
	block1 := node.MineAndWait(t, 1)
	t.Logf("Mined block with multi-output tx: %s", block1.Hash().String())

	// Now create multiple child transactions from the multi-output transaction
	childTxs := make([]*bt.Tx, 0)
	for i := 0; i < 4; i++ { // Use 4 of the 5 outputs
		childTx := node.CreateTransactionWithOptions(t,
			transactions.WithInput(multiOutputTx, uint32(i)),
			transactions.WithP2PKHOutputs(1, 50000), // 50k satoshi output
		)
		childTxs = append(childTxs, childTx)

		err = node.PropagationClient.ProcessTransaction(node.Ctx, childTx)
		require.NoError(t, err, "Failed to send child transaction %d", i)

		err = node.WaitForTransactionInBlockAssembly(childTx, 10*time.Second)
		require.NoError(t, err, "Timeout waiting for child transaction %d", i)
	}

	t.Logf("Sent %d child transactions to be included in next block", len(childTxs))

	// Mine block with multiple transactions
	block2 := node.MineAndWait(t, 1)
	t.Logf("Mined block with multiple transactions: %s", block2.Hash().String())

	// Verify persistence
	t.Log("Phase 3: Verifying persistence of block with multiple transactions...")

	err = node.WaitForBlockPersisted(block2.Hash(), 30*time.Second)
	require.NoError(t, err, "Block with multiple transactions was not persisted within timeout")

	// Verify block file exists
	blockExists, err := blockStore.Exists(node.Ctx, block2.Hash().CloneBytes(), fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.True(t, blockExists, "Block file should exist")

	// Get full block and verify subtrees
	fullBlock, err := node.BlockchainClient.GetBlock(node.Ctx, block2.Hash())
	require.NoError(t, err)

	t.Logf("Block has %d subtrees", len(fullBlock.Subtrees))

	for i, subtreeHash := range fullBlock.Subtrees {
		subtreeExists, err := node.SubtreeStore.Exists(node.Ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
		require.NoError(t, err, "Failed to check subtree %d", i)
		require.True(t, subtreeExists, "Subtree %d data file should exist", i)
		t.Logf("  Subtree %d verified: %s", i, subtreeHash.String())
	}

	t.Log("=== Test Passed: Block with multiple transactions persisted correctly ===")
}

// newBlockPersisterTestNode creates a new test node configured for block persister testing.
func newBlockPersisterTestNode(t *testing.T) *daemon.TestDaemon {
	t.Log("Creating test node with block persister enabled...")

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableBlockPersister: true,
		EnablePruner:         false,
		EnableP2P:            false,
		EnableRPC:            true,
		EnableValidator:      true,
		UTXOStoreType:        "aerospike",
		EnableErrorLogging:   true,
		PreserveDataDir:      true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(), // Use SQLite for blockchain store instead of PostgreSQL
			test.MultiNodeSettings(1), // Configure block persister store and other node-specific settings
			func(s *settings.Settings) {
				s.ClientName = "BlockPersisterTest"
				s.ChainCfgParams.CoinbaseMaturity = 5
				s.GlobalBlockHeightRetention = 100 // Keep blocks longer for testing
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})

	t.Logf("Test node created: %s", node.Settings.ClientName)

	return node
}

// waitForBlockPersistedWithStore waits for a block file to exist in the blob store.
func waitForBlockPersistedWithStore(ctx context.Context, blockStore blob.Store, blockHash *chainhash.Hash, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	checkInterval := 500 * time.Millisecond

	for time.Now().Before(deadline) {
		exists, err := blockStore.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
		if err == nil && exists {
			return nil
		}
		time.Sleep(checkInterval)
	}

	return context.DeadlineExceeded
}

// waitForSubtreePersisted waits for a subtree data file to exist in the blob store.
func waitForSubtreePersisted(ctx context.Context, subtreeStore blob.Store, subtreeHash *chainhash.Hash, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	checkInterval := 500 * time.Millisecond

	for time.Now().Before(deadline) {
		exists, err := subtreeStore.Exists(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
		if err == nil && exists {
			return nil
		}
		time.Sleep(checkInterval)
	}

	return context.DeadlineExceeded
}

// verifyBlockPersistence is a helper that verifies all aspects of block persistence.
func verifyBlockPersistence(t *testing.T, node *daemon.TestDaemon, blockStore blob.Store, block *model.Block, timeout time.Duration) {
	t.Helper()

	blockHash := block.Hash()

	// 1. Wait for block to be marked as persisted in database
	err := node.WaitForBlockPersisted(blockHash, timeout)
	require.NoError(t, err, "Block was not marked as persisted in database within timeout")

	// 2. Verify block file exists in store
	err = waitForBlockPersistedWithStore(node.Ctx, blockStore, blockHash, timeout)
	require.NoError(t, err, "Block file was not created in store within timeout")

	// 3. Verify subtree data files exist
	fullBlock, err := node.BlockchainClient.GetBlock(node.Ctx, blockHash)
	require.NoError(t, err, "Failed to get full block data")

	for i, subtreeHash := range fullBlock.Subtrees {
		err = waitForSubtreePersisted(node.Ctx, node.SubtreeStore, subtreeHash, timeout)
		require.NoError(t, err, "Subtree %d was not persisted within timeout", i)
	}

	// 4. Verify block data can be read from store and parsed correctly
	blockData, err := blockStore.Get(node.Ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
	require.NoError(t, err, "Failed to read block data from store")
	require.NotEmpty(t, blockData, "Block data should not be empty")

	// 5. Verify block data can be parsed as a valid block
	parsedBlock, err := model.NewBlockFromBytes(blockData)
	require.NoError(t, err, "Failed to parse block data")
	require.Equal(t, blockHash.String(), parsedBlock.Hash().String(), "Parsed block hash should match")

	t.Logf("Block persistence verified: hash=%s, size=%d bytes, subtrees=%d",
		blockHash.String(), len(blockData), len(fullBlock.Subtrees))
}

// TestBlockPersisterRetrievesPersistedData verifies that persisted data can be correctly retrieved and parsed.
func TestBlockPersisterRetrievesPersistedData(t *testing.T) {
	t.Log("=== Test: Block Persister Data Retrieval ===")

	node := newBlockPersisterTestNode(t)
	defer node.Stop(t)

	blockStore, err := node.GetBlockStore()
	require.NoError(t, err)

	// Mine to maturity and create a transaction
	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, node.Ctx)

	newTx := node.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, coinbaseTx.TotalOutputSatoshis()),
	)

	err = node.PropagationClient.ProcessTransaction(node.Ctx, newTx)
	require.NoError(t, err)

	err = node.WaitForTransactionInBlockAssembly(newTx, 10*time.Second)
	require.NoError(t, err)

	block := node.MineAndWait(t, 1)

	// Verify full persistence
	t.Log("Verifying block persistence...")
	verifyBlockPersistence(t, node, blockStore, block, 30*time.Second)

	// Get the full block and verify subtree data can be read
	fullBlock, err := node.BlockchainClient.GetBlock(node.Ctx, block.Hash())
	require.NoError(t, err)

	t.Logf("Block height: %d", fullBlock.Height)
	t.Logf("Block hash: %s", fullBlock.Hash().String())
	t.Logf("Number of subtrees: %d", len(fullBlock.Subtrees))

	for i, subtreeHash := range fullBlock.Subtrees {
		subtreeData, err := node.SubtreeStore.Get(node.Ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
		require.NoError(t, err, "Failed to read subtree data for subtree %d", i)
		require.NotEmpty(t, subtreeData, "Subtree data should not be empty")

		t.Logf("  Subtree %d: hash=%s, size=%d bytes", i, subtreeHash.String(), len(subtreeData))
	}

	t.Log("=== Test Passed: Persisted data can be correctly retrieved ===")
}

// TestBlockPersisterMultipleBlocksSequential verifies that multiple sequential blocks are all persisted correctly.
func TestBlockPersisterMultipleBlocksSequential(t *testing.T) {
	t.Log("=== Test: Block Persister Multiple Sequential Blocks ===")

	node := newBlockPersisterTestNode(t)
	defer node.Stop(t)

	blockStore, err := node.GetBlockStore()
	require.NoError(t, err)

	// Mine to maturity
	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, node.Ctx)
	currentTx := coinbaseTx

	// Mine 10 blocks in sequence, verifying persistence after each
	numBlocks := 10
	t.Logf("Mining and verifying %d blocks sequentially...", numBlocks)

	for i := 0; i < numBlocks; i++ {
		// Create transaction
		newTx := node.CreateTransactionWithOptions(t,
			transactions.WithInput(currentTx, 0),
			transactions.WithP2PKHOutputs(1, currentTx.TotalOutputSatoshis()),
		)

		err := node.PropagationClient.ProcessTransaction(node.Ctx, newTx)
		require.NoError(t, err)

		err = node.WaitForTransactionInBlockAssembly(newTx, 10*time.Second)
		require.NoError(t, err)

		// Mine block
		block := node.MineAndWait(t, 1)

		// Verify persistence
		verifyBlockPersistence(t, node, blockStore, block, 30*time.Second)
		t.Logf("Block %d/%d persisted and verified: %s", i+1, numBlocks, block.Hash().String())

		currentTx = newTx
	}

	// Final check: no unpersisted blocks should remain
	time.Sleep(2 * time.Second) // Give persister time to finish
	unpersistedBlocks, err := node.BlockchainClient.GetBlocksNotPersisted(node.Ctx, 100)
	require.NoError(t, err)
	require.Empty(t, unpersistedBlocks, "All blocks should be persisted")

	t.Logf("=== Test Passed: All %d blocks persisted correctly ===", numBlocks)
}

// TestBlockPersisterWithPruner verifies that block persistence works correctly
// even when the pruner is actively deleting spent transactions.
//
// KNOWN ISSUE: This test exposes a race condition between the block persister and pruner.
// When GlobalBlockHeightRetention is set to 1 (aggressive pruning), the pruner can delete
// transaction data before the block persister has a chance to create subtree data files.
// The block persister's CreateSubtreeDataFileStreaming function needs transaction metadata
// from the UTXO store, but the pruner may have already deleted it.
//
// Test scenario:
// 1. Start a single node with block persister AND pruner enabled
// 2. Mine to maturity and get a spendable coinbase
// 3. Create a transaction chain, mining 1 transaction per block up to height 12
// 4. Wait for all blocks to be persisted (files created in store)
// 5. Wait for the pruner to complete its pruning cycle
// 6. Verify that all blocks are still persisted in the block store
// 7. Verify that block data can still be retrieved and parsed correctly
// 8. Verify that subtree data files still exist
func TestBlockPersisterWithPruner(t *testing.T) {
	t.Log("=== Test: Block Persister with Pruner ===")
	t.Log("This test verifies that block persistence completes before pruner deletes transaction data")

	// Create a single node with both block persister AND pruner enabled
	node := newBlockPersisterWithPrunerTestNode(t)
	defer node.Stop(t)

	// Get the block store for verification
	blockStore, err := node.GetBlockStore()
	require.NoError(t, err, "Failed to get block store")

	// Phase 1: Mine to maturity and get spendable coinbase
	t.Log("Phase 1: Mining to maturity to get spendable coinbase...")
	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, node.Ctx)
	t.Logf("Coinbase transaction: %s", coinbaseTx.TxIDChainHash().String())

	// Get current height after maturity mining
	_, meta, err := node.BlockchainClient.GetBestBlockHeader(node.Ctx)
	require.NoError(t, err)
	startHeight := meta.Height
	t.Logf("Starting height after maturity: %d", startHeight)

	// Phase 2: Create transaction chain with 1 tx per block
	t.Log("Phase 2: Creating transaction chain with 1 tx per block...")

	txChain := make([]*bt.Tx, 0)
	minedBlocks := make([]*model.Block, 0)
	currentTx := coinbaseTx

	// Mine 10 blocks to give pruner enough blocks to work with
	numBlocksToMine := 10
	for i := 0; i < numBlocksToMine; i++ {
		// Create a new transaction spending from the previous one
		newTx := node.CreateTransactionWithOptions(t,
			transactions.WithInput(currentTx, 0),
			transactions.WithP2PKHOutputs(1, currentTx.TotalOutputSatoshis()),
		)

		txChain = append(txChain, newTx)

		// Send the transaction
		err := node.PropagationClient.ProcessTransaction(node.Ctx, newTx)
		require.NoError(t, err, "Failed to send transaction %d", i)

		t.Logf("Created and sent transaction %d: %s", i+1, newTx.TxIDChainHash().String())

		// Wait for the transaction to be processed by block assembly
		err = node.WaitForTransactionInBlockAssembly(newTx, 10*time.Second)
		require.NoError(t, err, "Timeout waiting for transaction to be processed by block assembly")

		// Mine a block with this transaction
		block := node.MineAndWait(t, 1)
		minedBlocks = append(minedBlocks, block)
		t.Logf("Mined block %d at height %d: %s", i+1, startHeight+uint32(i+1), block.Hash().String())

		// Update current transaction for next iteration
		currentTx = newTx
	}

	t.Logf("Successfully mined %d blocks with transactions", len(minedBlocks))

	// Phase 3: Wait for all blocks to be persisted first
	// We wait for the block FILES to exist (not just the database flag) because
	// the database can be marked before the actual file write completes.
	t.Log("Phase 3: Waiting for all blocks to be persisted (checking actual files)...")

	for i, block := range minedBlocks {
		blockHash := block.Hash()
		err := waitForBlockPersistedWithStore(node.Ctx, blockStore, blockHash, 30*time.Second)
		require.NoError(t, err, "Block %d file was not created within timeout", i+1)
		t.Logf("  Block %d file persisted: %s", i+1, blockHash.String())
	}
	t.Log("  All block files exist in the store")

	// Phase 4: Wait for pruner to complete its cycle
	t.Log("Phase 4: Waiting for pruner to complete pruning cycle...")
	node.WaitForPruner(t, 30*time.Second)
	t.Log("Pruner has completed processing")

	// Phase 5: Verify block files still exist in the block store after pruning
	t.Log("Phase 5: Verifying block files still exist in block store after pruning...")
	t.Log("(Note: Pruner deletes UTXOs from utxostore, but should NOT affect persisted blocks)")

	for i, block := range minedBlocks {
		blockHash := block.Hash()
		expectedHeight := startHeight + uint32(i+1)

		t.Logf("Checking persistence for block %d (height %d): %s", i+1, expectedHeight, blockHash.String())

		// Verify block file exists in block store
		blockExists, err := blockStore.Exists(node.Ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
		require.NoError(t, err, "Failed to check if block file exists for block %d", i+1)
		require.True(t, blockExists, "Block file should still exist in block store for block %d after pruning", i+1)
		t.Logf("  Block file exists in store")

		// Verify subtree data files exist
		fullBlock, err := node.BlockchainClient.GetBlock(node.Ctx, blockHash)
		require.NoError(t, err, "Failed to get full block data for block %d", i+1)

		if len(fullBlock.Subtrees) > 0 {
			for j, subtreeHash := range fullBlock.Subtrees {
				subtreeExists, err := node.SubtreeStore.Exists(node.Ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
				require.NoError(t, err, "Failed to check if subtree data file exists")
				require.True(t, subtreeExists, "Subtree data file should still exist for subtree %d in block %d after pruning", j, i+1)
			}
			t.Logf("  %d subtree data files exist in store", len(fullBlock.Subtrees))
		} else {
			t.Logf("  Block has no subtrees (coinbase only)")
		}

		t.Logf("  Block %d persistence verified after pruning", i+1)
	}

	// Phase 6: Verify block data can still be retrieved and parsed correctly
	t.Log("Phase 6: Verifying block data can be retrieved and parsed after pruning...")

	for i, block := range minedBlocks {
		blockHash := block.Hash()

		// Read block data from store
		blockData, err := blockStore.Get(node.Ctx, blockHash.CloneBytes(), fileformat.FileTypeBlock)
		require.NoError(t, err, "Failed to read block data from store for block %d after pruning", i+1)
		require.NotEmpty(t, blockData, "Block data should not be empty for block %d after pruning", i+1)

		// Verify the block data can be parsed as a valid block
		parsedBlock, err := model.NewBlockFromBytes(blockData)
		require.NoError(t, err, "Failed to parse block data for block %d after pruning", i+1)
		require.Equal(t, blockHash.String(), parsedBlock.Hash().String(), "Parsed block hash should match for block %d", i+1)

		t.Logf("  Block %d data retrieved and parsed successfully (%d bytes)", i+1, len(blockData))
	}

	t.Log("=== Test Passed: Block Persister works correctly with Pruner ===")
}

// newBlockPersisterWithPrunerTestNode creates a new test node configured for block persister testing with pruner enabled.
func newBlockPersisterWithPrunerTestNode(t *testing.T) *daemon.TestDaemon {
	t.Log("Creating test node with block persister AND pruner enabled...")

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableBlockPersister: true,
		EnablePruner:         true, // Enable pruner for this test
		EnableP2P:            false,
		EnableRPC:            true,
		EnableValidator:      true,
		UTXOStoreType:        "aerospike",
		EnableErrorLogging:   true,
		PreserveDataDir:      true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(), // Use SQLite for blockchain store instead of PostgreSQL
			test.MultiNodeSettings(1), // Configure block persister store and other node-specific settings
			func(s *settings.Settings) {
				s.ClientName = "BlockPersisterPrunerTest"
				s.ChainCfgParams.CoinbaseMaturity = 1
				// Use aggressive pruning (retention=1) to test that block persister
				// completes before pruner deletes the transaction data it needs.
				// If this test fails, it indicates a race condition between
				// block persister and pruner that needs to be fixed.
				s.GlobalBlockHeightRetention = 1
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})

	t.Logf("Test node created: %s (GlobalBlockHeightRetention: %d)", node.Settings.ClientName, node.Settings.GlobalBlockHeightRetention)

	return node
}

// TestBlockPersisterUTXOAdditionsAndDeletions tests that the block persister correctly persists
// UTXO additions and deletions files for each block.
//
// Test scenario:
// 1. Start a single node with blockchain, blockassembly, and blockpersister services
// 2. Mine to maturity and get a spendable coinbase
// 3. Create a transaction chain where each transaction spends the previous one
// 4. Mine blocks containing these transactions
// 5. After each block is persisted, verify that:
//   - The utxo-additions file exists and contains the expected UTXOs
//   - The utxo-deletions file exists and contains the expected spent outputs
//   - The UTXO data can be read and parsed correctly
func TestBlockPersisterUTXOAdditionsAndDeletions(t *testing.T) {
	t.Log("=== Test: Block Persister UTXO Additions and Deletions ===")
	t.Log("This test verifies that UTXO additions and deletions files are correctly persisted")

	node := newBlockPersisterTestNode(t)
	defer node.Stop(t)

	blockStore, err := node.GetBlockStore()
	require.NoError(t, err, "Failed to get block store")

	// Phase 1: Mine to maturity and get spendable coinbase
	t.Log("Phase 1: Mining to maturity to get spendable coinbase...")
	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, node.Ctx)
	t.Logf("Coinbase transaction: %s", coinbaseTx.TxIDChainHash().String())

	// Get current height after maturity mining
	_, meta, err := node.BlockchainClient.GetBestBlockHeader(node.Ctx)
	require.NoError(t, err)
	startHeight := meta.Height
	t.Logf("Starting height after maturity: %d", startHeight)

	// Phase 2: Create transaction chain and mine blocks
	t.Log("Phase 2: Creating transaction chain and mining blocks...")

	type blockTxInfo struct {
		block       *model.Block
		tx          *bt.Tx
		prevTx      *bt.Tx
		inputIndex  uint32
		blockHeight uint32
	}

	blockInfos := make([]blockTxInfo, 0)
	currentTx := coinbaseTx

	// Mine 3 blocks with 1 transaction each, creating a spending chain
	numBlocksToMine := 3
	for i := 0; i < numBlocksToMine; i++ {
		// Get the satoshis available from output 0 of the current transaction
		inputSatoshis := currentTx.Outputs[0].Satoshis
		// Create 2 outputs, each with roughly half the input amount (minus some for fees)
		outputAmount := (inputSatoshis - 1000) / 2 // Leave 1000 satoshis for fees

		// Create a new transaction spending from the previous one (output 0)
		newTx := node.CreateTransactionWithOptions(t,
			transactions.WithInput(currentTx, 0),
			transactions.WithP2PKHOutputs(2, outputAmount), // Create 2 outputs
		)

		t.Logf("Created transaction %d: %s (spending %s:0)", i+1, newTx.TxIDChainHash().String(), currentTx.TxIDChainHash().String())

		// Send the transaction
		err := node.PropagationClient.ProcessTransaction(node.Ctx, newTx)
		require.NoError(t, err, "Failed to send transaction %d", i)

		// Wait for the transaction to be processed by block assembly
		err = node.WaitForTransactionInBlockAssembly(newTx, 10*time.Second)
		require.NoError(t, err, "Timeout waiting for transaction to be processed by block assembly")

		// Mine a block with this transaction
		block := node.MineAndWait(t, 1)
		expectedHeight := startHeight + uint32(i+1)
		t.Logf("Mined block %d at height %d: %s", i+1, expectedHeight, block.Hash().String())

		blockInfos = append(blockInfos, blockTxInfo{
			block:       block,
			tx:          newTx,
			prevTx:      currentTx,
			inputIndex:  0,
			blockHeight: expectedHeight,
		})

		// Update current transaction for next iteration
		currentTx = newTx
	}

	// Phase 3: Verify UTXO additions and deletions for each block
	t.Log("Phase 3: Verifying UTXO additions and deletions persistence...")

	for i, info := range blockInfos {
		blockHash := info.block.Hash()
		t.Logf("Checking UTXO files for block %d (height %d): %s", i+1, info.blockHeight, blockHash.String())

		// Wait for block to be fully persisted
		err := node.WaitForBlockPersisted(blockHash, 30*time.Second)
		require.NoError(t, err, "Block %d was not persisted within timeout", i+1)

		// Wait for UTXO additions file to exist
		err = waitForUTXOFilesPersisted(node.Ctx, blockStore, blockHash, 30*time.Second)
		require.NoError(t, err, "UTXO files were not persisted within timeout for block %d", i+1)

		// Verify UTXO additions file exists
		additionsExists, err := blockStore.Exists(node.Ctx, blockHash.CloneBytes(), fileformat.FileTypeUtxoAdditions)
		require.NoError(t, err, "Failed to check if utxo-additions file exists for block %d", i+1)
		require.True(t, additionsExists, "utxo-additions file should exist for block %d", i+1)
		t.Logf("  utxo-additions file exists")

		// Verify UTXO deletions file exists
		deletionsExists, err := blockStore.Exists(node.Ctx, blockHash.CloneBytes(), fileformat.FileTypeUtxoDeletions)
		require.NoError(t, err, "Failed to check if utxo-deletions file exists for block %d", i+1)
		require.True(t, deletionsExists, "utxo-deletions file should exist for block %d", i+1)
		t.Logf("  utxo-deletions file exists")

		// Read and verify UTXO additions content
		utxoSet, err := utxopersister.GetUTXOSet(node.Ctx, node.Logger, node.Settings, blockStore, blockHash)
		require.NoError(t, err, "Failed to create UTXO set reader for block %d", i+1)

		// Check additions - should contain the outputs from our transaction
		additionsReader, err := utxoSet.GetUTXOAdditionsReader(node.Ctx)
		require.NoError(t, err, "Failed to get utxo-additions reader for block %d", i+1)
		defer additionsReader.Close()

		additionsCount := 0
		foundOurTx := false
		for {
			utxoWrapper, err := utxopersister.NewUTXOWrapperFromReader(node.Ctx, additionsReader)
			if err == io.EOF {
				break
			}
			require.NoError(t, err, "Failed to read UTXO wrapper from additions file for block %d", i+1)

			additionsCount++
			if utxoWrapper.TxID.String() == info.tx.TxIDChainHash().String() {
				foundOurTx = true
				t.Logf("  Found our transaction in additions: %s", utxoWrapper.TxID.String())
				t.Logf("    Height: %d, Coinbase: %v, UTXOs: %d", utxoWrapper.Height, utxoWrapper.Coinbase, len(utxoWrapper.UTXOs))

				// Verify the UTXO outputs match our transaction
				require.Equal(t, info.blockHeight, utxoWrapper.Height, "UTXO height should match block height")
				require.False(t, utxoWrapper.Coinbase, "Our transaction should not be marked as coinbase")
				require.Equal(t, 2, len(utxoWrapper.UTXOs), "Should have 2 UTXOs (we created 2 outputs)")

				for j, utxo := range utxoWrapper.UTXOs {
					t.Logf("    UTXO %d: index=%d, value=%d, script_len=%d", j, utxo.Index, utxo.Value, len(utxo.Script))
				}
			}
		}

		require.True(t, foundOurTx, "Our transaction should be in the utxo-additions file for block %d", i+1)
		t.Logf("  Total transactions in additions: %d (includes coinbase)", additionsCount)

		// Check deletions - should contain the spent output from the previous transaction
		deletionsReader, err := utxoSet.GetUTXODeletionsReader(node.Ctx)
		require.NoError(t, err, "Failed to get utxo-deletions reader for block %d", i+1)
		defer deletionsReader.Close()

		deletionsCount := 0
		foundSpentOutput := false
		for {
			deletion, err := utxopersister.NewUTXODeletionFromReader(deletionsReader)
			if err == io.EOF {
				break
			}
			if err != nil {
				// Check if we hit the EOF marker (32 zero bytes for TxID)
				break
			}

			// Skip if this is the EOF marker (all zeros)
			isEOFMarker := true
			for _, b := range deletion.TxID[:] {
				if b != 0 {
					isEOFMarker = false
					break
				}
			}
			if isEOFMarker {
				break
			}

			deletionsCount++
			t.Logf("  Deletion: txid=%s, index=%d", deletion.TxID.String(), deletion.Index)

			if deletion.TxID.String() == info.prevTx.TxIDChainHash().String() && deletion.Index == info.inputIndex {
				foundSpentOutput = true
				t.Logf("  Found our spent output in deletions: %s:%d", deletion.TxID.String(), deletion.Index)
			}
		}

		require.True(t, foundSpentOutput, "The spent output should be in the utxo-deletions file for block %d", i+1)
		t.Logf("  Total deletions: %d", deletionsCount)

		t.Logf("  Block %d UTXO files verified successfully", i+1)
	}

	t.Log("=== Test Passed: Block Persister correctly persists UTXO additions and deletions ===")
}

// waitForUTXOFilesPersisted waits for both UTXO additions and deletions files to exist in the store.
func waitForUTXOFilesPersisted(ctx context.Context, store blob.Store, blockHash *chainhash.Hash, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	checkInterval := 500 * time.Millisecond

	for time.Now().Before(deadline) {
		additionsExists, err := store.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeUtxoAdditions)
		if err != nil {
			time.Sleep(checkInterval)
			continue
		}

		deletionsExists, err := store.Exists(ctx, blockHash.CloneBytes(), fileformat.FileTypeUtxoDeletions)
		if err != nil {
			time.Sleep(checkInterval)
			continue
		}

		if additionsExists && deletionsExists {
			return nil
		}

		time.Sleep(checkInterval)
	}

	return context.DeadlineExceeded
}
