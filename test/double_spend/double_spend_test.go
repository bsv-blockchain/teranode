//go:build test_sequentially

package doublespendtest

import (
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
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
	t.Run("multiple conflicting txs in same block", func(t *testing.T) {
		testMarkAsConflictingMultipleSameBlock(t, utxoStore)
	})
	t.Run("multiple conflicting txs in different blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, utxoStore)
	})
	t.Run("conflicting transaction chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, utxoStore)
	})
	// t.Run("double spend in subsequent block", func(t *testing.T) {
	// 	testDoubleSpendInSubsequentBlock(t, utxoStore)
	// })
}

func TestDoubleSpendPostgres(t *testing.T) {
	// start a postgres container
	utxoStore, teardown, err := utils.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		_ = teardown()
	}()

	t.Run("single tx with one conflicting transaction", func(t *testing.T) {
		testSingleDoubleSpend(t, utxoStore)
	})
	t.Run("multiple conflicting txs in same block", func(t *testing.T) {
		testMarkAsConflictingMultipleSameBlock(t, utxoStore)
	})
	t.Run("multiple conflicting txs in different blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, utxoStore)
	})
	t.Run("conflicting transaction chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, utxoStore)
	})
	// t.Run("double spend in subsequent block", func(t *testing.T) {
	// 	testDoubleSpendInSubsequentBlock(t, utxoStore)
	// })
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
	t.Run("multiple conflicting txs in same block", func(t *testing.T) {
		testMarkAsConflictingMultipleSameBlock(t, utxoStore)
	})
	t.Run("multiple conflicting txs in different blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, utxoStore)
	})
	t.Run("conflicting transaction chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, utxoStore)
	})
	// t.Run("double spend in subsequent block", func(t *testing.T) {
	// 	testDoubleSpendInSubsequentBlock(t, utxoStore)
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
	dst, _, txOriginal, txDoubleSpend, block102a := setupDoubleSpendTest(t, utxoStore)
	defer func() {
		dst.Stop()
	}()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, dst, block102a, []*bt.Tx{txDoubleSpend}, []*bt.Tx{txOriginal}, 10202)

	// Create block 103b to make the longest chain...
	_, block103b := dst.createTestBlock(t, []*bt.Tx{}, block102b, 10302)

	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block103b, block103b.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state := dst.waitForBlockHeight(t, 103, blockWait)

	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

	// At this point we have:
	//                   / 102a (losing)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (winning)

	// Verify block 102b is now the block at height 102
	dst.verifyBlockByHeight(t, block102b, 102)

	// Verify block 103b is the block at height 103
	dst.verifyBlockByHeight(t, block103b, 103)

	// Check the txOriginal is marked as conflicting
	dst.verifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, true)

	// check the txOriginal has been removed from block assembly
	dst.verifyNotInBlockAssembly(t, []chainhash.Hash{*txOriginal.TxIDChainHash()})

	// Check the txDoubleSpend is no longer marked as conflicting
	// it should still be marked as conflicting in the subtree
	dst.verifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, false)

	// check that the txDoubleSpend is not in block assembly, it should have been mined and removed
	dst.verifyNotInBlockAssembly(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// fork back to the original chain and check that everything is processed properly
	_, block103a := dst.createTestBlock(t, []*bt.Tx{}, block102a, 10301)
	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block103a, block103a.Height),
		"Failed to process block")

	_, block104a := dst.createTestBlock(t, []*bt.Tx{}, block103a, 10401)
	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block104a, block104a.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state = dst.waitForBlockHeight(t, 104, blockWait)
	assert.Equal(t, uint32(104), state.CurrentHeight, "Expected block assembly to reach height 104")

	// Verify block 104a is the block at height 104
	dst.verifyBlockByHeight(t, block104a, 104)

	// At this point we have:
	//                   / 102a -> 103a -> 104a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (losing)

	// check that the txDoubleSpend is not in block assembly, it should have been removed, since it was conflicting with chain a
	dst.verifyNotInBlockAssembly(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

	// check that txDoubleSpend has been marked again as conflicting
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, false)
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, true)

	// check that both transactions are still marked as conflicting in the subtrees
	dst.verifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	dst.verifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})
}

// This is testing a scenario where:
// 1. Block 103 is is continuing from block 102
// 2. The txDoubleSpend is in block 103 which means the block is invalid
func testDoubleSpendInSubsequentBlock(t *testing.T, utxoStore string) {
	// Setup test environment
	dst, _, _, txDoubleSpend, block102 := setupDoubleSpendTest(t, utxoStore)
	defer dst.Stop()

	// Step 1: Create and validate block with double spend transaction
	_, block103 := dst.createTestBlock(t, []*bt.Tx{txDoubleSpend}, block102, 103)

	require.Error(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block103, block103.Height),
		"Failed to reject invalid block with double spend transaction")
}

// testMarkAsConflictingMultipleSameBlock tests a scenario where:
// 1. Multiple transactions conflict with each other
// 2. All conflicting transactions are in the same block
// 3. All conflicting transactions should be marked as conflicting
func testMarkAsConflictingMultipleSameBlock(t *testing.T, utxoStore string) {
	t.Errorf("testMarkAsConflictingMultipleSameBlock not implemented")
}

// testMarkAsConflictingMultiple tests a scenario where:
// 1. Multiple transactions conflict with each other
// 2. Conflicting transactions are in different blocks
// 3. All conflicting transactions should be marked as conflicting
func testMarkAsConflictingMultiple(t *testing.T, utxoStore string) {
	// Setup test environment
	dst, coinbaseTx1, txOriginal, txDoubleSpend, block102a := setupDoubleSpendTest(t, utxoStore)
	defer dst.Stop()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	txDoubleSpend2 := createTransaction(t, coinbaseTx1, dst.privKey, 47e8)

	// Create block 102b with a double spend transaction
	block102b := createConflictingBlock(t, dst, block102a, []*bt.Tx{txDoubleSpend}, []*bt.Tx{txOriginal}, 10202)
	assert.NotNil(t, block102b) // temp

	// Create block 102c with a different double spend transaction
	block102c := createConflictingBlock(t, dst, block102a, []*bt.Tx{txDoubleSpend2}, []*bt.Tx{txOriginal}, 10203)
	assert.NotNil(t, block102c) // temp

	// Create block 103b to make the longest chain...
	_, block103b := dst.createTestBlock(t, []*bt.Tx{}, block102b, 10302)

	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block103b, block103b.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state := dst.waitForBlockHeight(t, 103, blockWait)
	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

	// At this point we have:
	//                   / 102a (losing)
	// 0 -> 1 ... 101 -> - 102b -> 103b (winning)
	//                   \ 102c (losing)

	// Verify block 103b is now the block at height 103
	dst.verifyBlockByHeight(t, block103b, 103)

	// verify all conflicting transactions are properly marked as conflicting
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, true)
	dst.verifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()}) // should always be marked as conflicting

	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, false)
	dst.verifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}) // should always be marked as conflicting

	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend2.TxIDChainHash()}, true)
	dst.verifyConflictingInSubtrees(t, block102c.Subtrees[0], []chainhash.Hash{*txDoubleSpend2.TxIDChainHash()}) // should always be marked as conflicting
}

// testMarkAsConflictingChains tests a scenario where:
// 1. Two transaction chains conflict with each other
// 2. All transactions in both chains should be marked as conflicting
func testMarkAsConflictingChains(t *testing.T, utxoStore string) {
	// Setup test environment
	dst, _, txOriginal, txDoubleSpend, block102a := setupDoubleSpendTest(t, utxoStore)
	defer dst.Stop()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	txOriginal1 := createTransaction(t, txOriginal, dst.privKey, 14e8)
	txOriginal2 := createTransaction(t, txOriginal1, dst.privKey, 13e8)
	txOriginal3 := createTransaction(t, txOriginal2, dst.privKey, 12e8)
	txOriginal4 := createTransaction(t, txOriginal3, dst.privKey, 11e8)

	// Create block 103a with the original transactions
	subtree103a, block103a := dst.createTestBlock(t, []*bt.Tx{txOriginal1, txOriginal2, txOriginal3, txOriginal4}, block102a, 10301)

	block103aTxHashes := make([]chainhash.Hash, 0, 4)
	for _, tx := range []*bt.Tx{txOriginal1, txOriginal2, txOriginal3, txOriginal4} {
		block103aTxHashes = append(block103aTxHashes, *tx.TxIDChainHash())
	}

	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block103a, block103a.Height),
		"Failed to process block")

	// verify all are not conflicting
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{
		*txOriginal1.TxIDChainHash(),
		*txOriginal2.TxIDChainHash(),
		*txOriginal3.TxIDChainHash(),
		*txOriginal4.TxIDChainHash(),
	}, false)

	dst.verifyConflictingInSubtrees(t, subtree103a.RootHash(), nil)

	// wait for the block to be processed
	state := dst.waitForBlockHeight(t, 103, blockWait)
	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a -> 103a (winning)

	txDoubleSpend2 := createTransaction(t, txDoubleSpend, dst.privKey, 32e8)
	txDoubleSpend3 := createTransaction(t, txDoubleSpend2, dst.privKey, 31e8)
	txDoubleSpend4 := createTransaction(t, txDoubleSpend3, dst.privKey, 30e8)

	// Create a conflicting block 103b with double spend transactions
	block103b := createConflictingBlock(t, dst, block103a,
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
	dst.verifyBlockByHeight(t, block103a, 103)

	// at this point we have:
	//                   / 102a -> 103a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b -> 103b (losing)

	// switch forks by mining 104b
	_, block104b := dst.createTestBlock(t, []*bt.Tx{}, block103b, 10402)

	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block104b, block104b.Height),
		"Failed to process block")

	// wait for block assembly to reach height 104
	state = dst.waitForBlockHeight(t, 104, blockWait)
	assert.Equal(t, uint32(104), state.CurrentHeight, "Expected block assembly to reach height 104")

	// verify 104b is the valid block
	dst.verifyBlockByHeight(t, block104b, 104)

	// verify all txs in 103a have been marked as conflicting
	dst.verifyConflictingInUtxoStore(t, block103aTxHashes, true)
	dst.verifyConflictingInSubtrees(t, subtree103a.RootHash(), block103aTxHashes)

	// verify all txs in 103b are not marked as conflicting, while they are still marked as conflicting in the subtrees
	dst.verifyConflictingInUtxoStore(t, block103bTxHashes, false)
	dst.verifyConflictingInSubtrees(t, block103b.Subtrees[0], block103bTxHashes)
}

func createConflictingBlock(t *testing.T, dst *DoubleSpendTester, originalBlock *model.Block, blockTxs []*bt.Tx, originalTxs []*bt.Tx, nonce uint32) *model.Block {
	// Get previous block so we can create an alternate bock for this block with a double spend in it.
	previousBlock, err := dst.blockchainClient.GetBlockByHeight(dst.ctx, originalBlock.Height-1)
	require.NoError(t, err)

	// Step 1: Create and validate block with double spend transaction
	newBlockSubtree, newBlock := dst.createTestBlock(t, blockTxs, previousBlock, nonce)

	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, newBlock, newBlock.Height),
		"Failed to process block with double spend transaction")

	dst.verifyBlockByHash(t, newBlock, newBlock.Header.Hash())

	// At this point we have:
	//                   / 102a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b (losing)

	// Verify block 102 is still the original block at height 102
	dst.verifyBlockByHeight(t, originalBlock, originalBlock.Height)

	originalTxHashes := make([]chainhash.Hash, 0, len(originalTxs))
	for _, tx := range originalTxs {
		originalTxHashes = append(originalTxHashes, *tx.TxIDChainHash())
	}

	// Verify conflicting is still set to false
	dst.verifyConflictingInSubtrees(t, originalBlock.Subtrees[0], nil)
	dst.verifyConflictingInUtxoStore(t, originalTxHashes, false)

	doubleSpendTxHashes := make([]chainhash.Hash, 0, len(blockTxs))
	for _, tx := range blockTxs {
		doubleSpendTxHashes = append(doubleSpendTxHashes, *tx.TxIDChainHash())
	}

	// Verify conflicting
	dst.verifyConflictingInSubtrees(t, newBlockSubtree.RootHash(), doubleSpendTxHashes)
	dst.verifyConflictingInUtxoStore(t, doubleSpendTxHashes, true)

	return newBlock
}
