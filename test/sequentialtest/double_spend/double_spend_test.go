package doublespendtest

import (
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/test/txregistry"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("SETTINGS_CONTEXT", "test")
}

var (
	blockWait = 5 * time.Second
)

// TestDoubleSpendScenarios tests various double-spend scenarios in a blockchain.
// NOTE: these tests cannot be run in parallel as they rely on the same blockchain instance. They have to be run sequentially.
// All Double spend testcases are covering TNA-5: Teranode must only accept the block if all transactions in it are valid and not already spent

func TestDoubleSpendPostgres(t *testing.T) {
	// t.Skip()
	// start a postgres container
	utxoStore, teardown, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		_ = teardown()
	}()

	t.Run("single_tx_with_one_conflicting_transaction", func(t *testing.T) {
		testSingleDoubleSpend(t, utxoStore)
	})
	// t.Run("multiple conflicting txs in same block", func(t *testing.T) {
	// 	testMarkAsConflictingMultipleSameBlock(t, utxoStore)
	// })
	t.Run("multiple_conflicting_txs_in_different_blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, utxoStore)
	})
	t.Run("conflicting_transaction_chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, utxoStore)
	})
	t.Run("double_spend_fork", func(t *testing.T) {
		testDoubleSpendFork(t, utxoStore)
	})
	// t.Run("double spend in subsequent block", func(t *testing.T) {
	// 	testDoubleSpendInSubsequentBlock(t, utxoStore)
	// })
	t.Run("triple_forked_chain", func(t *testing.T) {
		testTripleForkedChain(t, utxoStore)
	})
	t.Run("test_non_conflicting_tx_after_reorg", func(t *testing.T) {
		testNonConflictingTxReorg(t, utxoStore)
	})
	t.Run("test_conflicting_tx_processed_after_reorg", func(t *testing.T) {
		testConflictingTxReorg(t, utxoStore)
	})
	t.Run("test_non_conflicting_tx_after_block_assembly_reset", func(t *testing.T) {
		testNonConflictingTxBlockAssemblyReset(t, utxoStore)
	})
	t.Run("test_double_spend_fork_with_nested_txs", func(t *testing.T) {
		testDoubleSpendForkWithNestedTXs(t, utxoStore)
	})
	t.Run("test_double_spend_with_frozen_tx", func(t *testing.T) {
		testSingleDoubleSpendFrozenTx(t, utxoStore)
	})
	// this test is not working yet, waiting for #2853
	// t.Run("test_double_spend_not_mined_for_long", func(t *testing.T) {
	// 	testSingleDoubleSpendNotMinedForLong(t, utxoStore)
	// })
}

func TestDoubleSpendAerospike(t *testing.T) {
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	t.Run("single_tx_with_one_conflicting_transaction", func(t *testing.T) {
		testSingleDoubleSpend(t, utxoStore)
	})
	// t.Run("multiple conflicting txs in same block", func(t *testing.T) {
	// 	testMarkAsConflictingMultipleSameBlock(t, utxoStore)
	// })
	t.Run("multiple_conflicting_txs_in_different_blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, utxoStore)
	})
	t.Run("conflicting_transaction_chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, utxoStore)
	})
	t.Run("double_spend_fork", func(t *testing.T) {
		testDoubleSpendFork(t, utxoStore)
	})
	t.Run("double_spend_in_subsequent_block", func(t *testing.T) {
		testDoubleSpendInSubsequentBlock(t, utxoStore)
	})
	t.Run("triple_forked_chain", func(t *testing.T) {
		testTripleForkedChain(t, utxoStore)
	})
	t.Run("test_non_conflicting_tx_after_reorg", func(t *testing.T) {
		testNonConflictingTxReorg(t, utxoStore)
	})
	t.Run("test_conflicting_tx_processed_after_reorg", func(t *testing.T) {
		testConflictingTxReorg(t, utxoStore)
	})
	t.Run("test_non_conflicting_tx_after_block_assembly_reset", func(t *testing.T) {
		testNonConflictingTxBlockAssemblyReset(t, utxoStore)
	})
	t.Run("test_double_spend_fork_with_nested_txs", func(t *testing.T) {
		testDoubleSpendForkWithNestedTXs(t, utxoStore)
	})
	t.Run("test_double_spend_with_frozen_tx", func(t *testing.T) {
		testSingleDoubleSpendFrozenTx(t, utxoStore)
	})
	// this test is not working yet, waiting for #2853
	// t.Run("test_double_spend_not_mined_for_long", func(t *testing.T) {
	// 	testSingleDoubleSpendNotMinedForLong(t, utxoStore)
	// })
}

// testSingleDoubleSpend tests the handling of double-spend transactions and their child
// transactions in a blockchain. The test verifies:
//  1. System can detect and handle double-spend attempts
//  2. Chain reorganization correctly updates transaction conflict status
//  3. Child transactions of double-spends are properly handled when part of longest chain
//
// Test flow:
//   - Creates block102b with a double-spend transaction
//   - Verifies original block102a remains at height 102
//   - Creates block103b with a child of the double-spend transaction
//   - Verifies chain reorganization occurs (block103b becomes tip)
//   - Validates final conflict status:
//   - Original tx becomes conflicting (losing chain)
//   - Double-spend tx becomes non-conflicting (winning chain)
//   - Child tx becomes non-conflicting (winning chain)
func testSingleDoubleSpend(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txA0, txB0, block102a, _ := setupDoubleSpendTest(t, utxoStore, 1)
	defer func() {
		td.Stop(t)
	}()

	// 0 -> 1 ... 101 -> 102a (*)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 10202)

	//                   / 102a (*)
	// 0 -> 1 ... 101 ->
	//                   \ 102b

	// Create block 103b to make the longest chain...
	_, block103b := td.CreateTestBlock(t, block102b, 10302) // Empty block

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block103b, blockWait, true)

	//                   / 102a
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (*)

	// Check the txA0 is marked as conflicting
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)
	td.VerifyConflictingInUtxoStore(t, true, txA0)

	// check the txA0 has been removed from block assembly
	td.VerifyNotInBlockAssembly(t, txA0)

	// Check the txB0 is no longer marked as conflicting
	// it should still be marked as conflicting in the subtree
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0)
	td.VerifyConflictingInUtxoStore(t, false, txB0)

	// check that the txB0 is not in block assembly, it should have been mined and removed
	td.VerifyNotInBlockAssembly(t, txB0)

	// fork back to the original chain and check that everything is processed properly
	_, block103a := td.CreateTestBlock(t, block102a, 10301) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block")

	_, block104a := td.CreateTestBlock(t, block103a, 10401) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block104a, blockWait)

	//                   / 102a -> 103a -> 104a (*)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b

	// check that the txB0 is not in block assembly, it should have been removed, since it was conflicting with chain a
	td.VerifyNotInBlockAssembly(t, txB0)

	// check that txB0 has been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, false, txA0)
	td.VerifyConflictingInUtxoStore(t, true, txB0)

	// check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0)
}

// This is testing a scenario where:
// 1. Block 103 is is continuing from block 102
// 2. The txB0 is in block 103 which means the block is invalid
func testDoubleSpendInSubsequentBlock(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, _, txB0, block102, _ := setupDoubleSpendTest(t, utxoStore, 5)
	defer td.Stop(t)

	// Step 1: Create and validate block with double spend transaction
	_, block103 := td.CreateTestBlock(t, block102, 10301, txB0)

	require.Error(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103, block103.Height, "", "legacy"),
		"Failed to reject invalid block with double spend transaction")
}

// testMarkAsConflictingMultipleSameBlock tests a scenario where:
// 1. Multiple transactions conflict with each other
// 2. All conflicting transactions are in the same block
// 3. All conflicting transactions should be marked as conflicting
func testMarkAsConflictingMultipleSameBlock(t *testing.T, utxoStore string) {
	_ = utxoStore
	t.Errorf("testMarkAsConflictingMultipleSameBlock not implemented")
}

// testMarkAsConflictingMultiple tests a scenario where:
// 1. Multiple transactions conflict with each other
// 2. Conflicting transactions are in different blocks
// 3. All conflicting transactions should be marked as conflicting
func testMarkAsConflictingMultiple(t *testing.T, utxoStore string) {
	// Setup test environment
	td, coinbaseTx1, txA0, txB0, block102a, _ := setupDoubleSpendTest(t, utxoStore, 10)
	defer td.Stop(t)

	// 0 -> 1 ... 101 -> 102a

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 10202)
	assert.NotNil(t, block102b) // temp

	//                   / 102a (*)
	// 0 -> 1 ... 101 ->
	//                   \ 102b

	txC0 := td.CreateTransaction(t, coinbaseTx1)

	// Create block 102c with a different double spend transaction
	block102c := createConflictingBlock(t, td, block102a, []*bt.Tx{txC0}, []*bt.Tx{txA0}, 10203)
	assert.NotNil(t, block102c) // temp

	//                   / 102a (*)
	// 0 -> 1 ... 101 -> - 102b
	//                   \ 102c

	// Create block 103b to make the longest chain...
	_, block103b := td.CreateTestBlock(t, block102b, 10302) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block103b, blockWait)

	//                   / 102a
	// 0 -> 1 ... 101 -> - 102b -> 103b (*)
	//                   \ 102c

	// verify all conflicting transactions are properly marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA0)
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0) // should always be marked as conflicting

	td.VerifyConflictingInUtxoStore(t, false, txB0)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0) // should always be marked as conflicting

	td.VerifyConflictingInUtxoStore(t, true, txC0)
	td.VerifyConflictingInSubtrees(t, block102c.Subtrees[0], txC0) // should always be marked as conflicting
}

// testMarkAsConflictingChains tests a scenario where:
// 1. Two transaction chains conflict with each other
// 2. All transactions in both chains should be marked as conflicting
func testMarkAsConflictingChains(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txA0, _, block102a, _ := setupDoubleSpendTest(t, utxoStore, 15)
	defer td.Stop(t)

	// 0 -> 1 ... 101 -> 102a

	txA1 := td.CreateTransaction(t, txA0, 1)
	txA2 := td.CreateTransaction(t, txA1)
	txA3 := td.CreateTransaction(t, txA2)
	txA4 := td.CreateTransaction(t, txA3)

	// Create block 103a with the original transactions
	subtree103a, block103a := td.CreateTestBlock(t, block102a, 10301, txA1, txA2, txA3, txA4)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block")

	// 0 -> 1 ... 101 -> 102a -> 103a

	// verify all are not conflicting
	td.VerifyConflictingInUtxoStore(t, false, txA1, txA2, txA3, txA4)
	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash()) // Should be empty

	// wait for the block to be processed
	td.WaitForBlockHeight(t, block103a, blockWait)

	// create a conflicting chain, using the same parent tx txA
	txB1 := td.CreateTransaction(t, txA0, 1)
	txB2 := td.CreateTransaction(t, txB1)
	txB3 := td.CreateTransaction(t, txB2)
	txB4 := td.CreateTransaction(t, txB3)

	// Create a conflicting block 103b with double spend transactions
	block103b := createConflictingBlock(t, td, block103a,
		[]*bt.Tx{txB1, txB2, txB3, txB4},
		[]*bt.Tx{txA1, txA2, txA3, txA4},
		10302,
	)
	assert.NotNil(t, block103b)

	// verify 103a is still the valid block
	td.WaitForBlockHeight(t, block103a, blockWait)

	//                / 102a -> 103a (*)
	// 0 -> 1 ... 101
	//                \ 102b -> 103b

	// switch forks by mining 104b
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block")

	// wait for block assembly to reach height 104
	td.WaitForBlockHeight(t, block104b, blockWait)

	//                / 102a -> 103a
	// 0 -> 1 ... 101
	//                \ 102b -> 103b -> 104b (*)

	// verify all txs in 103a have been marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA1, txA2, txA3, txA4)
	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash(), txA1, txA2, txA3, txA4)

	// verify all txs in 103b are not marked as conflicting, while they are still marked as conflicting in the subtrees
	td.VerifyConflictingInUtxoStore(t, false, txB1, txB2, txB3, txB4)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB1, txB2, txB3, txB4)
}

// testDoubleSpendFork tests a scenario with two competing chains:
//
// Transaction Chains:
// Chain A: txA0 -> txA1 -> txA2 -> txA3 -> txA4
// Chain B: txB0 -> txB1 -> txB2 -> txB3 -> txB4
//
// Test Flow:
// 1. Initially chain A (102a->103a) is winning
// 2. Then chain B (102b->103b->104b) becomes winning
// 3. Verify transactions in losing chain are marked as conflicting
func testDoubleSpendFork(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txA0, txB0, block102a, _ := setupDoubleSpendTest(t, utxoStore, 20)
	defer td.Stop(t)

	// Create chain A transactions
	txA1 := td.CreateTransaction(t, txA0)
	txA2 := td.CreateTransaction(t, txA1)
	txA3 := td.CreateTransaction(t, txA2)
	txA4 := td.CreateTransaction(t, txA3)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA1))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA2))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA3))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA4))

	// Create block 103a with chain A transactions
	subtree103a, block103a := td.CreateTestBlock(t, block102a, 10301, txA1, txA2, txA3, txA4)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block103a")

	// 0 -> 1 ... 101 -> 102a -> 103a

	// Create chain B (double spend chain)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create block102b from block101
	_, block102b := td.CreateTestBlock(t, block101, 10202) // Empty block

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height, "", "legacy"),
		"Failed to process block102b")

	//                / 102a -> 103a (*)
	// 0 -> 1 ... 101
	//                \ 102b

	// Create chain B transactions
	txB1 := td.CreateTransaction(t, txB0)
	txB2 := td.CreateTransaction(t, txB1)
	txB3 := td.CreateTransaction(t, txB2)

	// Create block103b with chain B transactions
	_, block103b := td.CreateTestBlock(t, block102b, 10302, txB0, txB1, txB2, txB3)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block103b")

	//                / 102a -> 103a (*)
	// 0 -> 1 ... 101
	//                \ 102b -> 103b

	// verify 103a is still the valid block
	td.WaitForBlockHeight(t, block103a, blockWait)

	// switch forks by mining 104b
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block104b")

	//                / 102a -> 103a
	// 0 -> 1 ... 101
	//                \ 102b -> 103b -> 104b (*)

	// wait for block assembly to reach height 104
	td.WaitForBlockHeight(t, block104b, blockWait)

	// verify all txs in 103a have been marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA1, txA2, txA3, txA4)
	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash(), txA1, txA2, txA3, txA4)

	// verify all txs in 103b are not marked as conflicting
	td.VerifyConflictingInUtxoStore(t, false, txB0, txB1, txB2, txB3)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0, txB1, txB2, txB3)
}

func createConflictingBlock(t *testing.T, td *daemon.TestDaemon, originalBlock *model.Block, blockTxs []*bt.Tx,
	originalTxs []*bt.Tx, nonce uint32, expectBlockError ...bool) *model.Block {
	// Get previous block so we can create an alternate block for this block with a double spend in it.
	previousBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, originalBlock.Height-1)
	require.NoError(t, err)

	// Step 1: Create and validate block with double spend transaction
	newBlockSubtree, newBlock := td.CreateTestBlock(t, previousBlock, nonce, blockTxs...)

	if len(expectBlockError) > 0 && expectBlockError[0] {
		require.Error(t, td.BlockValidationClient.ProcessBlock(td.Ctx, newBlock, newBlock.Height, "", "legacy"),
			"Failed to process block with double spend transaction")

		return nil
	}

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, newBlock, newBlock.Height, "", "legacy"),
		"Failed to process block with double spend transaction")

	td.VerifyBlockByHash(t, newBlock, newBlock.Header.Hash())

	// Verify block 102 is still the original block at height 102
	td.WaitForBlockHeight(t, originalBlock, blockWait, true)

	// Verify conflicting is still set to false
	td.VerifyConflictingInSubtrees(t, originalBlock.Subtrees[0]) // Should be empty
	td.VerifyConflictingInUtxoStore(t, false, originalTxs...)

	// Verify conflicting
	td.VerifyConflictingInSubtrees(t, newBlockSubtree.RootHash(), blockTxs...)
	td.VerifyConflictingInUtxoStore(t, true, blockTxs...)

	return newBlock
}

func createFork(t *testing.T, td *daemon.TestDaemon, originalBlock *model.Block, blockTxs []*bt.Tx, nonce uint32) *model.Block {
	// Get previous block so we can create an alternate bock for this block with no conflicting transactions.
	previousBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, originalBlock.Height-1)
	require.NoError(t, err)

	// Step 1: Create and validate block with double spend transaction
	_, newBlock := td.CreateTestBlock(t, previousBlock, nonce, blockTxs...)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, newBlock, newBlock.Height, "", "legacy"),
		"Failed to process block with double spend transaction")

	td.VerifyBlockByHash(t, newBlock, newBlock.Header.Hash())

	// Verify block 102 is still the original block at height 102
	td.WaitForBlockHeight(t, originalBlock, blockWait)

	// Verify conflicting is set to false
	td.VerifyConflictingInSubtrees(t, originalBlock.Subtrees[0], nil)
	td.VerifyConflictingInUtxoStore(t, false, blockTxs...)

	return newBlock
}

// testTripleForkedChain tests a scenario with three competing chains:
//
//	// Transaction Chains:
//
// Chain A: txA0 -> txA1 -> txA2 -> txA3
// Chain B: txB0 -> txB1 -> txB2 -> txB3
// Chain C: txC0 -> txC1 -> txC2
//
// Block Structure:
//
//	  102a -> 103a
//	 /
//	/
//
// 0 -> 1 ... 101 - 102b -> 103b -> 104b
//
//				  \
//	 			   \
//					102c -> 103c -> 104c -> 105c (*)
//
// Test Flow:
// 1. Initially chain A (102a->103a) is winning
// 2. Then chain B (102b->103b->104b) becomes winning
// 3. Finally chain C (102c->103c->104c->105c) becomes the ultimate winner
// 4. Verify all transactions in losing chains are marked as conflicting
func testTripleForkedChain(t *testing.T, utxoStore string) {
	// Setup test environment
	td, coinbaseTx1, txA0, txB0, block102a, _ := setupDoubleSpendTest(t, utxoStore, 25)
	defer td.Stop(t)

	// Create chain A transactions
	txA1 := td.CreateTransaction(t, txA0)
	txA2 := td.CreateTransaction(t, txA1)
	txA3 := td.CreateTransaction(t, txA2)

	// Create block 103a with chain A transactions
	subtree103a, block103a := td.CreateTestBlock(t, block102a, 10301, txA1, txA2, txA3)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block103a")

	//
	//                               txA1
	//                   / 102a ---> txA2 ---> 103a (*)
	//                  /            txA3
	// 0 -> 1 ... 101 /

	// Create chain B (double spend chain)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create block102b from block101
	_, block102b := td.CreateTestBlock(t, block101, 10202) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height, "", "legacy"),
		"Failed to process block102b")

	// Create chain B transactions
	txB1 := td.CreateTransaction(t, txB0)
	txB2 := td.CreateTransaction(t, txB1)

	// Create block103b with chain B transactions
	subtree103b, block103b := td.CreateTestBlock(t, block102b, 10302, txB0, txB1, txB2)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block103b")

	//                         txA1
	//                 102a -> txA2 -> 103a (*)
	//               /         txA3
	// 0 -> 1 ... 101
	//               \         txB0
	//                 102b -> txB1 -> 103b
	//                         txB2

	// Create chain C (triple spend chain)
	// Create a new transaction that spends the same UTXO as txA0 and txB0
	// txC0 should spend from the same coinbase as txA0 and txB0 to create a triple conflict
	// Use the same coinbase from block 25 that was used in setup
	coinbaseTx25 := coinbaseTx1 // This is from block 25 as setup uses block offset 25

	txC0 := td.CreateTransaction(t, coinbaseTx25, 0) // Same output as txA0 and txB0
	txC1 := td.CreateTransaction(t, txC0)
	txC2 := td.CreateTransaction(t, txC1)

	// Create block102c from block101
	_, block102c := td.CreateTestBlock(t, block101, 10203) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102c, block102c.Height, "", "legacy"),
		"Failed to process block102c")

	// Create block103c with chain C transactions
	_, block103c := td.CreateTestBlock(t, block102c, 10303, txC0, txC1, txC2)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103c, block103c.Height, "", "legacy"),
		"Failed to process block103c")

	//                  102a -> 103a (*)
	//                /
	// 0 -> 1 ... 101 - 102b -> 103b
	//                \
	//                  102c -> 103c

	// Verify 103a is still the valid block at height 103
	td.WaitForBlockHeight(t, block103a, blockWait)

	// Make chain B win temporarily by mining 104b
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block104b")

	//                  102a -> 103a
	//                /
	// 0 -> 1 ... 101 - 102b -> 103b
	//                \
	//                  102c -> 103c -> 104c (*)

	// Verify chain B is now winning
	td.WaitForBlockHeight(t, block104b, blockWait)

	// Make chain C the ultimate winner by mining 104c and 105c
	_, block104c := td.CreateTestBlock(t, block103c, 10403) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104c, block104c.Height, "", "legacy"),
		"Failed to process block104c")

	_, block105c := td.CreateTestBlock(t, block104c, 10503) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105c, block105c.Height, "", "legacy"),
		"Failed to process block105c")

	//                  102a -> 103a
	//                /
	// 0 -> 1 ... 101 - 102b -> 103b
	//                \
	//                  102c -> 103c -> 104c -> 105c (*)

	// Wait for block assembly to reach height 105
	td.WaitForBlockHeight(t, block105c, blockWait)

	// Verify all txs in chain A are marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA1, txA2, txA3)
	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash(), txA1, txA2, txA3)

	// Verify all txs in chain B are marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txB0, txB1, txB2)
	td.VerifyConflictingInSubtrees(t, subtree103b.RootHash(), txB0, txB1, txB2)

	// Verify all txs in chain C are not marked as conflicting (winning chain)
	td.VerifyConflictingInUtxoStore(t, false, txC0, txC1, txC2)
	td.VerifyConflictingInSubtrees(t, block103c.Subtrees[0], txC0, txC1, txC2)
}

func testNonConflictingTxReorg(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txA0, txB0, block102a, txX0 := setupDoubleSpendTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	txregistry.AddTag(txA0, "txA0")
	txregistry.AddTag(txB0, "txB0")

	// 0 -> 1 ... 101 -> 102a (*)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 10202)

	// Create block 103b to make the longest chain...
	_, block103b := td.CreateTestBlock(t, block102b, 10302, txX0)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block103b, blockWait)

	//                   / 102a {txA0}
	// 0 -> 1 ... 101 ->
	//                   \ 102b {txB0} -> 103b (*)

	// Check the txA0 is marked as conflicting
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)
	td.VerifyConflictingInUtxoStore(t, true, txA0)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0)
	td.VerifyConflictingInUtxoStore(t, false, txB0)

	td.VerifyNotInBlockAssembly(t, txA0)
	td.VerifyNotInBlockAssembly(t, txB0)

	// fork back to the original chain and check that everything is processed properly
	_, block103a := td.CreateTestBlock(t, block102a, 10301) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block")

	//                   / 102a {txA0} -> 103a
	// 0 -> 1 ... 101 ->
	//                   \ 102b {txB0} -> 103b (*)

	td.VerifyNotInBlockAssembly(t, txA0)
	td.VerifyNotInBlockAssembly(t, txB0)

	// check that txB0 has been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA0)
	td.VerifyConflictingInUtxoStore(t, false, txB0)

	// check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0)

	_, block104a := td.CreateTestBlock(t, block103a, 10401) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block104a, blockWait)

	//                   / 102a {txA0} -> 103a -> 104a (*)
	// 0 -> 1 ... 101 ->
	//                   \ 102b {txB0} -> 103b

	td.VerifyNotInBlockAssembly(t, txA0)
	td.VerifyNotInBlockAssembly(t, txB0)

	// check that txB0 has been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, false, txA0)
	td.VerifyConflictingInUtxoStore(t, true, txB0)

	// check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0)

	// create another block 105a with the tx2
	_, block105a := td.CreateTestBlock(t, block104a, 10501, txX0)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105a, block105a.Height, "", "legacy"),
		"Failed to process block")

	td.VerifyConflictingInUtxoStore(t, false, txX0)
	td.VerifyConflictingInSubtrees(t, block105a.Subtrees[0]) // Should not have any conflicts
}

func testConflictingTxReorg(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txOriginal, _, block102a, _ := setupDoubleSpendTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	tx1 := td.CreateTransaction(t, txOriginal, 0)
	tx1Conflicting := td.CreateTransaction(t, txOriginal, 0) // Conflicts with tx1

	// 0 -> 1 ... 101 -> 102a (*)

	// send tx1 to the block assembly
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1), "Failed to process transaction tx1")

	// send tx1Conflicting to the block assembly
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1Conflicting), "Failed to reject conflicting transaction tx1Conflicting")

	// Create block 103a with the conflicting tx
	_, block103a := td.CreateTestBlock(t, block102a, 10301, tx1Conflicting)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"), "Failed to process block")

	// Create block 103b with the conflicting tx
	_, block103b := td.CreateTestBlock(t, block102a, 10302, tx1Conflicting)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"), "Failed to process block")

	td.WaitForBlockHeight(t, block103a, blockWait)

	//                   / 103a {tx1Conflicting} (*)
	// 0 -> 1 ... 102a ->
	//                   \ 103b {tx1Conflicting}

	// Check the tx1Conflicting is marked as conflicting
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0], tx1Conflicting)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], tx1Conflicting)
	td.VerifyConflictingInUtxoStore(t, true, tx1)
	td.VerifyConflictingInUtxoStore(t, false, tx1Conflicting)

	td.VerifyNotInBlockAssembly(t, tx1)
	td.VerifyNotInBlockAssembly(t, tx1Conflicting)

	// fork to the new chain and check that everything is processed properly
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"), "Failed to process block")

	// When we reorg, tx1Conflicting should be processed properly and removed from block assembly
	//                   / 103a {tx1Conflicting}
	// 0 -> 1 ... 102a ->
	//                   \ 103b {tx1Conflicting} -> 104b (*)

	td.WaitForBlockHeight(t, block104b, blockWait)

	td.VerifyNotInBlockAssembly(t, tx1Conflicting)

	// check that tx1Conflicting has not been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, false, tx1Conflicting)

	// check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0], tx1Conflicting)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], tx1Conflicting)
}

func testNonConflictingTxBlockAssemblyReset(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txA0, txB0, block102a, txX0 := setupDoubleSpendTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	// 0 -> 1 ... 101 -> 102a (*)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 10202)

	// Create block 103b to make the longest chain...
	_, block103b := td.CreateTestBlock(t, block102b, 10302, txX0)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block103b, blockWait)

	//                   / 102a
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> txB0 -> 103b (*)

	// Check the txA0 is marked as conflicting
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)
	td.VerifyConflictingInUtxoStore(t, true, txA0)

	// check the txA0 has been removed from block assembly
	td.VerifyNotInBlockAssembly(t, txA0)

	// Check the txB0 is no longer marked as conflicting
	// it should still be marked as conflicting in the subtree
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0)
	td.VerifyConflictingInUtxoStore(t, false, txB0)

	// check that the txB0 is not in block assembly, it should have been mined and removed
	td.VerifyNotInBlockAssembly(t, txB0)

	// fork back to the original chain and check that everything is processed properly
	_, block103a := td.CreateTestBlock(t, block102a, 10301) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block")

	_, block104a := td.CreateTestBlock(t, block103a, 10401) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block104a, blockWait)

	//                   / 102a -> 103a -> 104a (*)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b

	// check that the txB0 is not in block assembly, it should have been removed, since it was conflicting with chain a
	td.VerifyNotInBlockAssembly(t, txB0)

	// check that txB0 has been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, false, txA0)
	td.VerifyConflictingInUtxoStore(t, true, txB0)

	// check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0)

	require.NoError(t, td.BlockAssemblyClient.ResetBlockAssembly(td.Ctx), "Failed to reset block assembly")

	// create another block 105a with the tx2
	_, block105a := td.CreateTestBlock(t, block104a, 10501, txX0)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105a, block105a.Height, "", "legacy"),
		"Failed to process block")

	td.VerifyConflictingInUtxoStore(t, false, txX0)
	td.VerifyConflictingInSubtrees(t, block105a.Subtrees[0]) // Should not have any conflicts
}

// testDoubleSpendForkWithNestedTXs tests a scenario with two competing chains and multiple reorganizations:
//
// Chain A: txA0 -> txA1 -> txA2 -> txA3 -> txA4
// Chain B: txB0 -> txB1 -> txB2 -> txB3 -> txB4 -> txB5
//
// Key test stages:
// 1. Chain A wins initially (103a)
// 2. Chain B takes over with extra transactions (105b with txB5,6)
// 3. Chain A wins again (106a), causing all Chain B transactions to be conflicting
//
// Final block structure:
//
//	             txA1
//	 / 102a ---> txA2 ---> 103a -> 104a -> 105a -> 106a (*)
//	/            txA3
//
// 0 -> 1 ... 101 /            txA4
//
//	\
//	 \ 102b ---> txB0  ---> 103b -> 104b -> 105b
//	             txB2         txB3
//
// This test verifies that:
// - All transactions in the losing chain (including late additions 5,6) are marked conflicting
// - Chain reorganization properly handles nested transaction dependencies
func testDoubleSpendForkWithNestedTXs(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txA0, txB0, block102a, _ := setupDoubleSpendTest(t, utxoStore)
	defer td.Stop(t)

	// Create chain A transactions
	txA1 := td.CreateTransaction(t, txA0)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA1))

	// Create block 103a with chain A transactions
	_, block103a := td.CreateTestBlock(t, block102a, 10301, txA1)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block103a")

	//
	//                   / 102a ---> 	txA1 ---> 103a (*)
	// 0 -> 1 ... 101 /

	td.VerifyConflictingInUtxoStore(t, false, txA0)
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0]) // Should not have any conflicts

	td.VerifyConflictingInUtxoStore(t, false, txA1)
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0]) // Should not have any conflicts

	// Create chain B (double spend chain)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create block102b from block101
	_, block102b := td.CreateTestBlock(t, block101, 10202) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height, "", "legacy"),
		"Failed to process block102b")

	//
	//                   / 102a {txA0} ---> 103a {txA1} (*)
	//                  /
	// 0 -> 1 ... 101 /
	//                \
	//                 \ 102b {}

	// Create block103b with chain B transactions
	_, block103b := td.CreateTestBlock(t, block102b, 10302, txB0)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block103b")

	//
	//                   / 102a {txA0} ---> 103a {txA1} (*)
	//                  /
	// 0 -> 1 ... 101 /
	//                \
	//                 \ 102b {} ---> 103b {txB0}

	// verify 103a is still the valid block
	td.WaitForBlockHeight(t, block103a, blockWait)

	td.VerifyConflictingInUtxoStore(t, false, txA0)
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0]) // Should not have any conflicts

	td.VerifyConflictingInUtxoStore(t, false, txA1)
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0]) // Should not have any conflicts

	td.VerifyConflictingInUtxoStore(t, true, txB0)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0)

	// switch forks by mining 104b
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block104b")

	//
	//                   / 102a {txA0} ---> 103a {txA1}
	//                  /
	// 0 -> 1 ... 101 /
	//                \
	//                 \ 102b {} ---> 103b {txB0} --> 104b (*)

	td.WaitForBlockHeight(t, block104b, blockWait)

	td.VerifyConflictingInUtxoStore(t, true, txA0)
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)

	td.VerifyConflictingInUtxoStore(t, true, txA1)
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0], txA1)

	td.VerifyConflictingInUtxoStore(t, false, txB0)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0)

	txB1 := td.CreateTransaction(t, txB0)

	// Process the doubleSpends
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txB1))

	// Add a new block 105b on top of 104b with the new double spends
	_, block105b := td.CreateTestBlock(t, block104b, 10502, txB1)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105b, block105b.Height, "", "legacy"),
		"Failed to process block105b")

	td.WaitForBlockHeight(t, block105b, blockWait)

	// TN1
	//
	//                   102a {txA0} ---> 103a {txA1}
	//                 /
	// 0 -> 1 ... 101 /
	//                \
	//                 \
	//                   102b {} ---> 103b {txB0} --> 104b (*) --> 105b {txB1} (*)

	td.VerifyConflictingInUtxoStore(t, true, txA0)
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)

	td.VerifyConflictingInUtxoStore(t, true, txA1)
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0], txA1)

	td.VerifyConflictingInUtxoStore(t, false, txB0)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0)

	td.VerifyConflictingInUtxoStore(t, false, txB1)
	td.VerifyConflictingInSubtrees(t, block105b.Subtrees[0]) // Should be empty

	// now make the other chain longer
	_, block104a := td.CreateTestBlock(t, block103a, 10401) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"),
		"Failed to process block104a")

	_, block105a := td.CreateTestBlock(t, block104a, 10501) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105a, block105a.Height, "", "legacy"),
		"Failed to process block105a")

	_, block106a := td.CreateTestBlock(t, block105a, 10601) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block106a, block106a.Height, "", "legacy"),
		"Failed to process block106a")

	//
	//                   / 102a {txA0} ---> 103a {txA1} ---> 104a ---> 105a ---> 106a (*)
	//                  /
	// 0 -> 1 ... 101 /
	//                \
	//                 \ 102b {} ---> 103b {txB0} --> 104b --> 105b {txB1}

	td.WaitForBlockHeight(t, block106a, blockWait)

	td.VerifyConflictingInUtxoStore(t, false, txA0)
	td.VerifyConflictingInUtxoStore(t, false, txA1)
	td.VerifyConflictingInUtxoStore(t, true, txB0)
	td.VerifyConflictingInUtxoStore(t, true, txB1)

	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0], txA1)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0)
	td.VerifyConflictingInSubtrees(t, block105b.Subtrees[0], txB1)
}

func testSingleDoubleSpendFrozenTx(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txA0, txB0, block102a, _ := setupDoubleSpendTest(t, utxoStore, 45)
	defer func() {
		td.Stop(t)
	}()

	// freeze utxos of txA0
	outputs := txA0.Outputs
	spends := make([]*utxo.Spend, 0)

	for idx, output := range outputs {
		if output.Satoshis == 0 {
			continue
		}

		// nolint: gosec
		utxoHash, _ := util.UTXOHashFromOutput(txA0.TxIDChainHash(), output, uint32(idx))
		// nolint: gosec
		spend := &utxo.Spend{
			TxID:     txA0.TxIDChainHash(),
			Vout:     uint32(idx),
			UTXOHash: utxoHash,
		}
		spends = append(spends, spend)
	}

	require.NoError(t, td.UtxoStore.FreezeUTXOs(td.Ctx, spends, td.Settings))

	// 0 -> 1 ... 101 -> 102a (*)

	// Create block 102b with a frozen transaction, this should fail
	_ = createConflictingBlock(t, td, block102a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 10202, true)
}

// testSingleDoubleSpendNotMinedForLong simulates a scenario where a transaction is not mined for a long time
// and then a double spend transaction is created. It verifies that the original transaction was removed from the
// block assembly before the double spend transaction was processed
func testSingleDoubleSpendNotMinedForLong(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txA0, _, _, _ := setupDoubleSpendTest(t, utxoStore, 50)
	defer td.Stop(t)

	txA1 := td.CreateTransaction(t, txA0)

	// validate transaction and propagate it
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA1))

	// remove the transaction from block assembly
	// this creates a scenario where the transaction is not mined for a long time
	require.NoError(t, td.BlockAssemblyClient.RemoveTx(t.Context(), txA1.TxIDChainHash()))

	// verify that the transaction is not in block assembly
	td.VerifyNotInBlockAssembly(t, txA1)

	// generate 101 blocks and wait for them to be processed by block assembly
	require.NoError(t, td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101}))

	// verify that the transaction has not been mined
	txMeta, err := td.UtxoStore.Get(t.Context(), txA1.TxIDChainHash())
	require.NoError(t, err)
	require.NotNil(t, txMeta)
	require.Nil(t, txMeta.BlockIDs)

	// get best block
	bestBlockHeader, _, err := td.BlockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)

	// get best block
	bestBlock, err := td.BlockchainClient.GetBlock(t.Context(), bestBlockHeader.Hash())
	require.NoError(t, err)

	// wait for block assembly to process the blocks
	td.WaitForBlockHeight(t, bestBlock, blockWait)

	// verify that the transaction has been removed from the utxo store
	txMeta, err = td.UtxoStore.Get(t.Context(), txA1.TxIDChainHash())
	require.NoError(t, err)

	assert.Nil(t, txMeta)
}
