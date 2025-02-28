//go:build test_sequentially

package doublespendtest

import (
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/test/testdaemon"
	"github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("SETTINGS_CONTEXT", "test")
}

var (
	// DEBUG DEBUG DEBUG
	blockWait = 5000 * time.Second
)

// TestDoubleSpendScenarios tests various double-spend scenarios in a blockchain.
// NOTE: these tests cannot be run in parallel as they rely on the same blockchain instance. They have to be run sequentially.

func TestDoubleSpendSQLite(t *testing.T) {
	utxoStore := "sqlite:///test"

	t.Run("single tx with one conflicting transaction", func(t *testing.T) {
		testSingleDoubleSpend(t, utxoStore)
	})
	// t.Run("multiple conflicting txs in same block", func(t *testing.T) {
	// 	testMarkAsConflictingMultipleSameBlock(t, utxoStore)
	// })
	t.Run("multiple conflicting txs in different blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, utxoStore)
	})
	t.Run("conflicting transaction chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, utxoStore)
	})
	t.Run("double spend fork", func(t *testing.T) {
		testDoubleSpendFork(t, utxoStore)
	})
	// t.Run("double spend in subsequent block", func(t *testing.T) {
	// 	testDoubleSpendInSubsequentBlock(t, utxoStore)
	// })
	t.Run("triple forked chain", func(t *testing.T) {
		testTripleForkedChain(t, utxoStore)
	})
	t.Run("test non-conflicting tx after reorg", func(t *testing.T) {
		testNonConflictingTxReorg(t, utxoStore)
	})
	t.Run("test non-conflicting tx after block assembly reset", func(t *testing.T) {
		testNonConflictingTxBlockAssemblyReset(t, utxoStore)
	})
}

func TestDoubleSpendPostgres(t *testing.T) {
	// t.Skip()
	// start a postgres container
	utxoStore, teardown, err := utils.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		_ = teardown()
	}()

	t.Run("single tx with one conflicting transaction", func(t *testing.T) {
		testSingleDoubleSpend(t, utxoStore)
	})
	// t.Run("multiple conflicting txs in same block", func(t *testing.T) {
	// 	testMarkAsConflictingMultipleSameBlock(t, utxoStore)
	// })
	t.Run("multiple conflicting txs in different blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, utxoStore)
	})
	t.Run("conflicting transaction chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, utxoStore)
	})
	t.Run("double spend fork", func(t *testing.T) {
		testDoubleSpendFork(t, utxoStore)
	})
	// t.Run("double spend in subsequent block", func(t *testing.T) {
	// 	testDoubleSpendInSubsequentBlock(t, utxoStore)
	// })
	t.Run("triple forked chain", func(t *testing.T) {
		testTripleForkedChain(t, utxoStore)
	})
	t.Run("test non-conflicting tx after reorg", func(t *testing.T) {
		testNonConflictingTxReorg(t, utxoStore)
	})
	t.Run("test non-conflicting tx after block assembly reset", func(t *testing.T) {
		testNonConflictingTxBlockAssemblyReset(t, utxoStore)
	})
}

func TestDoubleSpendAerospike(t *testing.T) {
	utxoStore, teardown, err := initAerospike()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	t.Run("single tx with one conflicting transaction", func(t *testing.T) {
		testSingleDoubleSpend(t, utxoStore)
	})
	// t.Run("multiple conflicting txs in same block", func(t *testing.T) {
	// 	testMarkAsConflictingMultipleSameBlock(t, utxoStore)
	// })
	t.Run("multiple conflicting txs in different blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, utxoStore)
	})
	t.Run("conflicting transaction chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, utxoStore)
	})
	t.Run("double spend fork", func(t *testing.T) {
		testDoubleSpendFork(t, utxoStore)
	})
	// t.Run("double spend in subsequent block", func(t *testing.T) {
	// 	testDoubleSpendInSubsequentBlock(t, utxoStore)
	// })
	t.Run("triple forked chain", func(t *testing.T) {
		testTripleForkedChain(t, utxoStore)
	})
	t.Run("test non-conflicting tx after reorg", func(t *testing.T) {
		testNonConflictingTxReorg(t, utxoStore)
	})
	t.Run("test non-conflicting tx after block assembly reset", func(t *testing.T) {
		testNonConflictingTxBlockAssemblyReset(t, utxoStore)
	})
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
	td, _, txOriginal, txDoubleSpend, block102a, _ := setupDoubleSpendTest(t, utxoStore)
	defer func() {
		td.Stop()
	}()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txDoubleSpend}, []*bt.Tx{txOriginal}, 10202)

	// Create block 103b to make the longest chain...
	_, block103b := td.CreateTestBlock(t, []*bt.Tx{}, block102b, 10302)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state := td.WaitForBlockHeight(t, 103, blockWait)

	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

	// At this point we have:
	//                   / 102a (losing)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (winning)

	// Verify block 102b is now the block at height 102
	td.VerifyBlockByHeight(t, block102b, 102)

	// Verify block 103b is the block at height 103
	td.VerifyBlockByHeight(t, block103b, 103)

	// Check the txOriginal is marked as conflicting
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, true)

	// check the txOriginal has been removed from block assembly
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txOriginal.TxIDChainHash()})

	// Check the txDoubleSpend is no longer marked as conflicting
	// it should still be marked as conflicting in the subtree
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, false)

	// check that the txDoubleSpend is not in block assembly, it should have been mined and removed
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// fork back to the original chain and check that everything is processed properly
	_, block103a := td.CreateTestBlock(t, []*bt.Tx{}, block102a, 10301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height),
		"Failed to process block")

	_, block104a := td.CreateTestBlock(t, []*bt.Tx{}, block103a, 10401)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state = td.WaitForBlockHeight(t, 104, blockWait)
	assert.Equal(t, uint32(104), state.CurrentHeight, "Expected block assembly to reach height 104")

	// Verify block 104a is the block at height 104
	td.VerifyBlockByHeight(t, block104a, 104)

	// At this point we have:
	//                   / 102a -> 103a -> 104a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (losing)

	// check that the txDoubleSpend is not in block assembly, it should have been removed, since it was conflicting with chain a
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// check that txDoubleSpend has been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, false)
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, true)

	// check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})
}

// This is testing a scenario where:
// 1. Block 103 is is continuing from block 102
// 2. The txDoubleSpend is in block 103 which means the block is invalid
func testDoubleSpendInSubsequentBlock(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, _, txDoubleSpend, block102, _ := setupDoubleSpendTest(t, utxoStore)
	defer td.Stop()

	// Step 1: Create and validate block with double spend transaction
	_, block103 := td.CreateTestBlock(t, []*bt.Tx{txDoubleSpend}, block102, 10301)

	require.Error(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103, block103.Height),
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
	td, coinbaseTx1, txOriginal, txDoubleSpend, block102a, _ := setupDoubleSpendTest(t, utxoStore)
	defer td.Stop()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	txDoubleSpend2 := td.CreateTransaction(t, coinbaseTx1, 47e8)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txDoubleSpend}, []*bt.Tx{txOriginal}, 10202)
	assert.NotNil(t, block102b) // temp

	// Create block 102c with a different double spend transaction
	block102c := createConflictingBlock(t, td, block102a, []*bt.Tx{txDoubleSpend2}, []*bt.Tx{txOriginal}, 10203)
	assert.NotNil(t, block102c) // temp

	// Create block 103b to make the longest chain...
	_, block103b := td.CreateTestBlock(t, []*bt.Tx{}, block102b, 10302)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state := td.WaitForBlockHeight(t, 103, blockWait)
	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

	// At this point we have:
	//                   / 102a (losing)
	// 0 -> 1 ... 101 -> - 102b -> 103b (winning)
	//                   \ 102c (losing)

	// Verify block 103b is now the block at height 103
	td.VerifyBlockByHeight(t, block103b, 103)

	// verify all conflicting transactions are properly marked as conflicting
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, true)
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()}) // should always be marked as conflicting

	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, false)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}) // should always be marked as conflicting

	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend2.TxIDChainHash()}, true)
	td.VerifyConflictingInSubtrees(t, block102c.Subtrees[0], []chainhash.Hash{*txDoubleSpend2.TxIDChainHash()}) // should always be marked as conflicting
}

// testMarkAsConflictingChains tests a scenario where:
// 1. Two transaction chains conflict with each other
// 2. All transactions in both chains should be marked as conflicting
func testMarkAsConflictingChains(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txOriginal, txDoubleSpend, block102a, _ := setupDoubleSpendTest(t, utxoStore)
	defer td.Stop()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	txOriginal1 := td.CreateTransaction(t, txOriginal, 14e8)
	txOriginal2 := td.CreateTransaction(t, txOriginal1, 13e8)
	txOriginal3 := td.CreateTransaction(t, txOriginal2, 12e8)
	txOriginal4 := td.CreateTransaction(t, txOriginal3, 11e8)

	// Create block 103a with the original transactions
	subtree103a, block103a := td.CreateTestBlock(t, []*bt.Tx{txOriginal1, txOriginal2, txOriginal3, txOriginal4}, block102a, 10301)

	block103aTxHashes := make([]chainhash.Hash, 0, 4)
	for _, tx := range []*bt.Tx{txOriginal1, txOriginal2, txOriginal3, txOriginal4} {
		block103aTxHashes = append(block103aTxHashes, *tx.TxIDChainHash())
	}

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height),
		"Failed to process block")

	// verify all are not conflicting
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{
		*txOriginal1.TxIDChainHash(),
		*txOriginal2.TxIDChainHash(),
		*txOriginal3.TxIDChainHash(),
		*txOriginal4.TxIDChainHash(),
	}, false)

	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash(), nil)

	// wait for the block to be processed
	state := td.WaitForBlockHeight(t, 103, blockWait)
	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a -> 103a (winning)

	txDoubleSpend2 := td.CreateTransaction(t, txDoubleSpend, 32e8)
	txDoubleSpend3 := td.CreateTransaction(t, txDoubleSpend2, 31e8)
	txDoubleSpend4 := td.CreateTransaction(t, txDoubleSpend3, 30e8)

	// Create a conflicting block 103b with double spend transactions
	block103b := createConflictingBlock(t, td, block103a,
		[]*bt.Tx{txDoubleSpend, txDoubleSpend2, txDoubleSpend3, txDoubleSpend4},
		[]*bt.Tx{txOriginal1, txOriginal2, txOriginal3, txOriginal4},
		10302,
	)
	assert.NotNil(t, block103b)

	block103bTxHashes := make([]chainhash.Hash, 0, 4)
	for _, tx := range []*bt.Tx{txDoubleSpend, txDoubleSpend2, txDoubleSpend3, txDoubleSpend4} {
		block103bTxHashes = append(block103bTxHashes, *tx.TxIDChainHash())
	}

	// verify 103a is still the valid block
	td.VerifyBlockByHeight(t, block103a, 103)

	// at this point we have:
	//                   / 102a -> 103a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (losing)

	// switch forks by mining 104b
	_, block104b := td.CreateTestBlock(t, []*bt.Tx{}, block103b, 10402)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height),
		"Failed to process block")

	// wait for block assembly to reach height 104
	state = td.WaitForBlockHeight(t, 104, blockWait)
	assert.Equal(t, uint32(104), state.CurrentHeight, "Expected block assembly to reach height 104")

	// verify 104b is the valid block
	td.VerifyBlockByHeight(t, block104b, 104)

	// verify all txs in 103a have been marked as conflicting
	td.VerifyConflictingInUtxoStore(t, block103aTxHashes, true)
	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash(), block103aTxHashes)

	// verify all txs in 103b are not marked as conflicting, while they are still marked as conflicting in the subtrees
	td.VerifyConflictingInUtxoStore(t, block103bTxHashes, false)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], block103bTxHashes)
}

// testDoubleSpendFork tests a scenario with two competing chains:
//
// Transaction Chains:
// Chain A: txOriginal -> txOriginal1 -> txOriginal2 -> txOriginal3 -> txOriginal4
// Chain B: txDoubleSpend -> txDoubleSpend2 -> txDoubleSpend3 -> txDoubleSpend4
//
// Test Flow:
// 1. Initially chain A (102a->103a) is winning
// 2. Then chain B (102b->103b->104b) becomes winning
// 3. Verify transactions in losing chain are marked as conflicting
func testDoubleSpendFork(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txOriginal, txDoubleSpend, block102a, _ := setupDoubleSpendTest(t, utxoStore)
	defer td.Stop()

	// Create chain A transactions
	txOriginal1 := td.CreateTransaction(t, txOriginal, 14e8)
	txOriginal2 := td.CreateTransaction(t, txOriginal1, 13e8)
	txOriginal3 := td.CreateTransaction(t, txOriginal2, 12e8)
	txOriginal4 := td.CreateTransaction(t, txOriginal3, 11e8)

	td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal1)
	td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal2)
	td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal3)
	td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal4)

	// Create block 103a with chain A transactions
	subtree103a, block103a := td.CreateTestBlock(t, []*bt.Tx{txOriginal1, txOriginal2, txOriginal3, txOriginal4}, block102a, 10301)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height),
		"Failed to process block103a")

	// At this point we have:
	//
	//                               txOriginal1
	//                   / 102a ---> txOriginal2 ---> 103a (winning)
	//                  /            txOriginal3
	// 0 -> 1 ... 101 /            txOriginal4

	block103aTxHashes := make([]chainhash.Hash, 0, 4)
	for _, tx := range []*bt.Tx{txOriginal1, txOriginal2, txOriginal3, txOriginal4} {
		block103aTxHashes = append(block103aTxHashes, *tx.TxIDChainHash())
	}

	// Create chain B (double spend chain)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create block102b from block101
	_, block102b := td.CreateTestBlock(t, []*bt.Tx{}, block101, 10202)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height),
		"Failed to process block102b")

	// Create chain B transactions
	txDoubleSpend2 := td.CreateTransaction(t, txDoubleSpend, 32e8)
	txDoubleSpend3 := td.CreateTransaction(t, txDoubleSpend2, 31e8)
	txDoubleSpend4 := td.CreateTransaction(t, txDoubleSpend3, 30e8)

	// Create block103b with chain B transactions
	_, block103b := td.CreateTestBlock(t, []*bt.Tx{txDoubleSpend, txDoubleSpend2, txDoubleSpend3, txDoubleSpend4}, block102b, 10302)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height),
		"Failed to process block103b")

	// At this point we have:
	//
	//                               txOriginal1
	//                   / 102a ---> txOriginal2 ---> 103a (winning)
	//                  /            txOriginal3
	// 0 -> 1 ... 101 /            txOriginal4
	//                \
	//                 \ 102b ---> txDoubleSpend  ---> 103b
	//                             txDoubleSpend2
	//                             txDoubleSpend3
	//                             txDoubleSpend4

	block103bTxHashes := make([]chainhash.Hash, 0, 4)
	for _, tx := range []*bt.Tx{txDoubleSpend, txDoubleSpend2, txDoubleSpend3, txDoubleSpend4} {
		block103bTxHashes = append(block103bTxHashes, *tx.TxIDChainHash())
	}

	// verify 103a is still the valid block
	td.VerifyBlockByHeight(t, block103a, 103)

	// switch forks by mining 104b
	_, block104b := td.CreateTestBlock(t, []*bt.Tx{}, block103b, 10402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height),
		"Failed to process block104b")

	// At this point we have:
	//
	//                               txOriginal1
	//                   / 102a ---> txOriginal2 ---> 103a
	//                  /            txOriginal3
	// 0 -> 1 ... 101 /            txOriginal4
	//                \
	//                 \ 102b ---> txDoubleSpend  ---> 103b -> 104b (winning)
	//                             txDoubleSpend2
	//                             txDoubleSpend3
	//                             txDoubleSpend4

	// wait for block assembly to reach height 104
	state := td.WaitForBlockHeight(t, 104, blockWait)
	assert.Equal(t, uint32(104), state.CurrentHeight, "Expected block assembly to reach height 104")

	// verify 104b is the valid block
	td.VerifyBlockByHeight(t, block104b, 104)

	// verify all txs in 103a have been marked as conflicting
	td.VerifyConflictingInUtxoStore(t, block103aTxHashes, true)
	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash(), block103aTxHashes)

	// verify all txs in 103b are not marked as conflicting
	td.VerifyConflictingInUtxoStore(t, block103bTxHashes, false)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], block103bTxHashes)
}

func createConflictingBlock(t *testing.T, td *testdaemon.TestDaemon, originalBlock *model.Block, blockTxs []*bt.Tx, originalTxs []*bt.Tx, nonce uint32) *model.Block {
	// Get previous block so we can create an alternate bock for this block with a double spend in it.
	previousBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, originalBlock.Height-1)
	require.NoError(t, err)

	// Step 1: Create and validate block with double spend transaction
	newBlockSubtree, newBlock := td.CreateTestBlock(t, blockTxs, previousBlock, nonce)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, newBlock, newBlock.Height),
		"Failed to process block with double spend transaction")

	td.VerifyBlockByHash(t, newBlock, newBlock.Header.Hash())

	// At this point we have:
	//                   / 102a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b (losing)

	// Verify block 102 is still the original block at height 102
	td.VerifyBlockByHeight(t, originalBlock, originalBlock.Height)

	originalTxHashes := make([]chainhash.Hash, 0, len(originalTxs))
	for _, tx := range originalTxs {
		originalTxHashes = append(originalTxHashes, *tx.TxIDChainHash())
	}

	// Verify conflicting is still set to false
	td.VerifyConflictingInSubtrees(t, originalBlock.Subtrees[0], nil)
	td.VerifyConflictingInUtxoStore(t, originalTxHashes, false)

	doubleSpendTxHashes := make([]chainhash.Hash, 0, len(blockTxs))
	for _, tx := range blockTxs {
		doubleSpendTxHashes = append(doubleSpendTxHashes, *tx.TxIDChainHash())
	}

	// Verify conflicting
	td.VerifyConflictingInSubtrees(t, newBlockSubtree.RootHash(), doubleSpendTxHashes)
	td.VerifyConflictingInUtxoStore(t, doubleSpendTxHashes, true)

	return newBlock
}

func createFork(t *testing.T, td *testdaemon.TestDaemon, originalBlock *model.Block, blockTxs []*bt.Tx, nonce uint32) *model.Block {
	// Get previous block so we can create an alternate bock for this block with no conflicting transactions.
	previousBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, originalBlock.Height-1)
	require.NoError(t, err)

	// Step 1: Create and validate block with double spend transaction
	_, newBlock := td.CreateTestBlock(t, blockTxs, previousBlock, nonce)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, newBlock, newBlock.Height),
		"Failed to process block with double spend transaction")

	td.VerifyBlockByHash(t, newBlock, newBlock.Header.Hash())

	// At this point we have:
	//                   / 102a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b (fork)

	// Verify block 102 is still the original block at height 102
	td.VerifyBlockByHeight(t, originalBlock, originalBlock.Height)

	blockTxHashes := make([]chainhash.Hash, 0, len(blockTxs))
	for _, tx := range blockTxs {
		blockTxHashes = append(blockTxHashes, *tx.TxIDChainHash())
	}

	// Verify conflicting is set to false
	td.VerifyConflictingInSubtrees(t, originalBlock.Subtrees[0], nil)
	td.VerifyConflictingInUtxoStore(t, blockTxHashes, false)

	return newBlock
}

// testTripleForkedChain tests a scenario with three competing chains:
//
// Transaction Chains:
// Chain A: txOriginal -> txOriginal1 -> txOriginal2 -> txOriginal3
// Chain B: txDoubleSpend -> txDoubleSpend2 -> txDoubleSpend3
// Chain C: txTripleSpend -> txTripleSpend2 -> txTripleSpend3
//
// Block Structure:
//
//	              				 txOriginal1
//					 / 102a ---> txOriginal2 ---> 103a
//					/            txOriginal3
//				   /
//
// 0 -> 1 ... 101 -----> 102b -> txDoubleSpend  -> 103b -> 104b
//
//				   \             txDoubleSpend2
//	 				\            txDoubleSpend3
//					 \
//					  \ 102c -> txTripleSpend  -> 103c -> 104c -> 105c (winning)
//								txTripleSpend2
//								txTripleSpend3
//
// Test Flow:
// 1. Initially chain A (102a->103a) is winning
// 2. Then chain B (102b->103b->104b) becomes winning
// 3. Finally chain C (102c->103c->104c->105c) becomes the ultimate winner
// 4. Verify all transactions in losing chains are marked as conflicting
func testTripleForkedChain(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txOriginal, txDoubleSpend, block102a, _ := setupDoubleSpendTest(t, utxoStore)
	defer td.Stop()

	// Create chain A transactions
	txOriginal1 := td.CreateTransaction(t, txOriginal, 14e8)
	txOriginal2 := td.CreateTransaction(t, txOriginal1, 13e8)
	txOriginal3 := td.CreateTransaction(t, txOriginal2, 12e8)

	// Create block 103a with chain A transactions
	subtree103a, block103a := td.CreateTestBlock(t, []*bt.Tx{txOriginal1, txOriginal2, txOriginal3}, block102a, 10301)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height),
		"Failed to process block103a")

	// At this point we have:
	//
	//                               txOriginal1
	//                   / 102a ---> txOriginal2 ---> 103a (winning)
	//                  /            txOriginal3
	// 0 -> 1 ... 101 /

	block103aTxHashes := make([]chainhash.Hash, 0, 3)
	for _, tx := range []*bt.Tx{txOriginal1, txOriginal2, txOriginal3} {
		block103aTxHashes = append(block103aTxHashes, *tx.TxIDChainHash())
	}

	// Create chain B (double spend chain)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create block102b from block101
	_, block102b := td.CreateTestBlock(t, []*bt.Tx{}, block101, 10202)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height),
		"Failed to process block102b")

	// Create chain B transactions
	txDoubleSpend2 := td.CreateTransaction(t, txDoubleSpend, 32e8)
	txDoubleSpend3 := td.CreateTransaction(t, txDoubleSpend2, 31e8)

	// Create block103b with chain B transactions
	subtree103b, block103b := td.CreateTestBlock(t, []*bt.Tx{txDoubleSpend, txDoubleSpend2, txDoubleSpend3}, block102b, 10302)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height),
		"Failed to process block103b")

	// At this point we have:
	//
	//                               txOriginal1
	//                   / 102a ---> txOriginal2 ---> 103a (winning)
	//                  /            txOriginal3
	// 0 -> 1 ... 101 /
	//                |             txDoubleSpend
	//                |\ 102b ---> txDoubleSpend2 ---> 103b
	//                |            txDoubleSpend3
	//                |
	//                \             txTripleSpend
	//                 \ 102c ---> txTripleSpend2 ---> 103c
	//                             txTripleSpend3

	block103bTxHashes := make([]chainhash.Hash, 0, 3)
	for _, tx := range []*bt.Tx{txDoubleSpend, txDoubleSpend2, txDoubleSpend3} {
		block103bTxHashes = append(block103bTxHashes, *tx.TxIDChainHash())
	}

	// Create chain C (triple spend chain)
	// Create a new transaction that spends the same UTXO as txOriginal and txDoubleSpend
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	txTripleSpend := td.CreateTransaction(t, block1.CoinbaseTx, 14e8)
	require.NoError(t, err)

	txTripleSpend2 := td.CreateTransaction(t, txTripleSpend, 14e8)
	txTripleSpend3 := td.CreateTransaction(t, txTripleSpend2, 14e8)

	// err = td.PropagationClient.ProcessTransaction(td.Ctx, txTripleSpend2)
	// require.NoError(t, err)
	// err = td.PropagationClient.ProcessTransaction(td.Ctx, txTripleSpend3)
	// require.NoError(t, err)

	// Create block102c from block101
	_, block102c := td.CreateTestBlock(t, []*bt.Tx{}, block101, 10203)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102c, block102c.Height),
		"Failed to process block102c")

	// Create block103c with chain C transactions
	_, block103c := td.CreateTestBlock(t, []*bt.Tx{txTripleSpend, txTripleSpend2, txTripleSpend3}, block102c,
		10303,
	)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103c, block103c.Height),
		"Failed to process block103c")

	// At this point we have:
	//
	//                               txOriginal1
	//                   / 102a ---> txOriginal2 ---> 103a (winning)
	//                  /            txOriginal3
	// 0 -> 1 ... 101 /
	//                |             txDoubleSpend
	//                |\ 102b ---> txDoubleSpend2 ---> 103b
	//                |            txDoubleSpend3
	//                |
	//                \             txTripleSpend
	//                 \ 102c ---> txTripleSpend2 ---> 103c
	//                             txTripleSpend3

	block103cTxHashes := make([]chainhash.Hash, 0, 3)
	for _, tx := range []*bt.Tx{txTripleSpend, txTripleSpend2, txTripleSpend3} {
		block103cTxHashes = append(block103cTxHashes, *tx.TxIDChainHash())
	}

	// Verify 103a is still the valid block at height 103
	td.VerifyBlockByHeight(t, block103a, 103)

	// Make chain B win temporarily by mining 104b
	_, block104b := td.CreateTestBlock(t, []*bt.Tx{}, block103b, 10402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height),
		"Failed to process block104b")

	// At this point we have:
	//
	//                               txOriginal1
	//                   / 102a ---> txOriginal2 ---> 103a
	//                  /            txOriginal3
	// 0 -> 1 ... 101 /
	//                |             txDoubleSpend
	//                |\ 102b ---> txDoubleSpend2 ---> 103b -> 104b (winning)
	//                |            txDoubleSpend3
	//                |
	//                \             txTripleSpend
	//                 \ 102c ---> txTripleSpend2 ---> 103c
	//                             txTripleSpend3

	// Verify chain B is now winning
	td.VerifyBlockByHeight(t, block104b, 104)

	// Make chain C the ultimate winner by mining 104c and 105c
	_, block104c := td.CreateTestBlock(t, []*bt.Tx{}, block103c, 10403)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104c, block104c.Height),
		"Failed to process block104c")

	_, block105c := td.CreateTestBlock(t, []*bt.Tx{}, block104c, 10503)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105c, block105c.Height),
		"Failed to process block105c")

	// Final fork structure:
	//
	//                               txOriginal1
	//                   / 102a ---> txOriginal2 ---> 103a
	//                  /            txOriginal3
	// 0 -> 1 ... 101 /
	//                |             txDoubleSpend
	//                |\ 102b ---> txDoubleSpend2 ---> 103b -> 104b
	//                |            txDoubleSpend3
	//                |
	//                \             txTripleSpend
	//                 \ 102c ---> txTripleSpend2 ---> 103c -> 104c -> 105c (winning)
	//                             txTripleSpend3

	// Wait for block assembly to reach height 105
	state := td.WaitForBlockHeight(t, 105, blockWait)
	assert.Equal(t, uint32(105), state.CurrentHeight, "Expected block assembly to reach height 105")

	// Verify chain C is the ultimate winner
	td.VerifyBlockByHeight(t, block105c, 105)

	// Verify all txs in chain A are marked as conflicting
	td.VerifyConflictingInUtxoStore(t, block103aTxHashes, true)
	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash(), block103aTxHashes)

	// Verify all txs in chain B are marked as conflicting
	td.VerifyConflictingInUtxoStore(t, block103bTxHashes, true)
	td.VerifyConflictingInSubtrees(t, subtree103b.RootHash(), block103bTxHashes)

	// Verify all txs in chain C are not marked as conflicting (winning chain)
	td.VerifyConflictingInUtxoStore(t, block103cTxHashes, false)
	td.VerifyConflictingInSubtrees(t, block103c.Subtrees[0], block103cTxHashes)
}

func testNonConflictingTxReorg(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txOriginal, txDoubleSpend, block102a, tx2 := setupDoubleSpendTest(t, utxoStore)
	defer func() {
		td.Stop()
	}()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txDoubleSpend}, []*bt.Tx{txOriginal}, 10202)

	// Create block 103b to make the longest chain...
	_, block103b := td.CreateTestBlock(t, []*bt.Tx{tx2}, block102b, 10302)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state := td.WaitForBlockHeight(t, 103, blockWait)

	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

	// At this point we have:
	//                   / 102a (losing)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> tx2 -> 103b (winning)

	// Verify block 102b is now the block at height 102
	td.VerifyBlockByHeight(t, block102b, 102)

	// Verify block 103b is the block at height 103
	td.VerifyBlockByHeight(t, block103b, 103)

	// Check the txOriginal is marked as conflicting
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, true)

	// check the txOriginal has been removed from block assembly
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txOriginal.TxIDChainHash()})

	// Check the txDoubleSpend is no longer marked as conflicting
	// it should still be marked as conflicting in the subtree
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, false)

	// check that the txDoubleSpend is not in block assembly, it should have been mined and removed
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// fork back to the original chain and check that everything is processed properly
	_, block103a := td.CreateTestBlock(t, []*bt.Tx{}, block102a, 10301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height),
		"Failed to process block")

	_, block104a := td.CreateTestBlock(t, []*bt.Tx{}, block103a, 10401)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state = td.WaitForBlockHeight(t, 104, blockWait)
	assert.Equal(t, uint32(104), state.CurrentHeight, "Expected block assembly to reach height 104")

	// Verify block 104a is the block at height 104
	td.VerifyBlockByHeight(t, block104a, 104)

	// At this point we have:
	//                   / 102a -> 103a -> 104a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (losing)

	// check that the txDoubleSpend is not in block assembly, it should have been removed, since it was conflicting with chain a
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// check that txDoubleSpend has been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, false)
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, true)

	// check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// create another block 105a with the tx2
	_, block105a := td.CreateTestBlock(t, []*bt.Tx{tx2}, block104a, 10501)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105a, block105a.Height),
		"Failed to process block")

	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*tx2.TxIDChainHash()}, false)
	td.VerifyConflictingInSubtrees(t, block105a.Subtrees[0], []chainhash.Hash{})
}

func testNonConflictingTxBlockAssemblyReset(t *testing.T, utxoStore string) {
	// Setup test environment
	td, _, txOriginal, txDoubleSpend, block102a, tx2 := setupDoubleSpendTest(t, utxoStore)
	defer func() {
		td.Stop()
	}()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txDoubleSpend}, []*bt.Tx{txOriginal}, 10202)

	// Create block 103b to make the longest chain...
	_, block103b := td.CreateTestBlock(t, []*bt.Tx{tx2}, block102b, 10302)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state := td.WaitForBlockHeight(t, 103, blockWait)

	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

	// At this point we have:
	//                   / 102a (losing)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> tx2 -> 103b (winning)

	// Verify block 102b is now the block at height 102
	td.VerifyBlockByHeight(t, block102b, 102)

	// Verify block 103b is the block at height 103
	td.VerifyBlockByHeight(t, block103b, 103)

	// Check the txOriginal is marked as conflicting
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, true)

	// check the txOriginal has been removed from block assembly
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txOriginal.TxIDChainHash()})

	// Check the txDoubleSpend is no longer marked as conflicting
	// it should still be marked as conflicting in the subtree
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, false)

	// check that the txDoubleSpend is not in block assembly, it should have been mined and removed
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// fork back to the original chain and check that everything is processed properly
	_, block103a := td.CreateTestBlock(t, []*bt.Tx{}, block102a, 10301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height),
		"Failed to process block")

	_, block104a := td.CreateTestBlock(t, []*bt.Tx{}, block103a, 10401)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state = td.WaitForBlockHeight(t, 104, blockWait)
	assert.Equal(t, uint32(104), state.CurrentHeight, "Expected block assembly to reach height 104")

	// Verify block 104a is the block at height 104
	td.VerifyBlockByHeight(t, block104a, 104)

	// At this point we have:
	//                   / 102a -> 103a -> 104a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (losing)

	// check that the txDoubleSpend is not in block assembly, it should have been removed, since it was conflicting with chain a
	td.VerifyNotInBlockAssembly(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// check that txDoubleSpend has been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, false)
	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, true)

	// check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	require.NoError(t, td.BlockAssemblyClient.ResetBlockAssembly(td.Ctx), "Failed to reset block assembly")

	// create another block 105a with the tx2
	_, block105a := td.CreateTestBlock(t, []*bt.Tx{tx2}, block104a, 10501)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105a, block105a.Height),
		"Failed to process block")

	td.VerifyConflictingInUtxoStore(t, []chainhash.Hash{*tx2.TxIDChainHash()}, false)
	td.VerifyConflictingInSubtrees(t, block105a.Subtrees[0], []chainhash.Hash{})
}
