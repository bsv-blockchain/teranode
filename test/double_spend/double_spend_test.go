//go:build test_full

package doublespendtest

import (
	"testing"
	"time"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoubleSpendScenarios(t *testing.T) {
	t.Run("single tx with one conflicting transaction", testSingleDoubleSpend)
	// t.Run("single tx with one conflicting transaction and child", testDoubleSpendAndChildAreMarkedAsConflicting)
	t.Run("multiple conflicting txs in same block", TestMarkAsConflictingMultipleSameBlock)
	t.Run("multiple conflicting txs in different blocks", TestMarkAsConflictingMultiple)
	t.Run("conflicting transaction chains", TestMarkAsConflictingChains)
	// t.Run("double spend in subsequent block", testDoubleSpendInSubsequentBlock)
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
func testSingleDoubleSpend(t *testing.T) {
	// Setup test environment
	dst, _, txOriginal, txDoubleSpend, block102a := setupDoubleSpendTest(t)
	defer dst.d.Stop()

	// At this point we have:
	// 0 -> 1 ... 101 -> 102a (winning)

	// Get block 101 so we can create an alternate bock for height 102 with a double spend in it.
	block101, err := dst.blockchainClient.GetBlockByHeight(dst.ctx, 101)
	require.NoError(t, err)

	// Step 1: Create and validate block with double spend transaction
	subtree102b, block102b := dst.createTestBlock(t, []*bt.Tx{txDoubleSpend}, block101)

	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block102b, block102b.Height),
		"Failed to process block with double spend transaction")

	dst.verifyBlockByHash(t, block102b, block102b.Header.Hash())

	// At this point we have:
	//                   / 102a (winning)
	// 0 -> 1 ... 101 ->
	//                   \ 102b (losing)

	// Verify block 102 is still the original block at height 102
	dst.verifyBlockByHeight(t, block102a, 102)

	// Verify conflicting is still set to false
	// dst.verifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, false)

	// Verify conflicting
	dst.verifyConflictingInSubtrees(t, subtree102b.RootHash(), []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, true)

	// Create block 103b to make the longest chain...
	_, block103b := dst.createTestBlock(t, []*bt.Tx{}, block102b)

	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block103b, block103b.Height),
		"Failed to process block")

	// Verify final state in Block Assembly
	state := dst.waitForBlockHeight(t, 103, 5*time.Second)

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
	// TODO dst.verifyConflictingInSubtrees(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txOriginal.TxIDChainHash()}, true)

	// Check the txDoubleSpend is no longer marked as conflicting
	// it should still be marked as conflicting in the subtree
	dst.verifyConflictingInSubtrees(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})
	dst.verifyConflictingInUtxoStore(t, []chainhash.Hash{*txDoubleSpend.TxIDChainHash()}, false)
}

// func testDoubleSpendAndChildAreMarkedAsConflicting(t *testing.T) {
// 	// Setup test environment
// 	dst, _, txOriginal, txDoubleSpend, block102a := setupDoubleSpendTest(t)
// 	defer dst.d.Stop()

// 	// At this point we have:
// 	// 0 -> 1 ... 101 -> 102a (winning)

// 	// Get block 101 so we can create an alternate bock for height 102 with a double spend in it.
// 	block101, err := dst.blockchainClient.GetBlockByHeight(dst.ctx, 101)
// 	require.NoError(t, err)

// 	// Step 1: Create and validate block with double spend transaction
// 	subtree102b, block102b := dst.createTestBlock(t, []*bt.Tx{txDoubleSpend}, block101)

// 	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block102b, block102b.Height),
// 		"Failed to process block with double spend transaction")

// 	dst.verifyBlockByHash(t, block102b, block102b.Header.Hash())

// 	// At this point we have:
// 	//                   / 102a (winning)
// 	// 0 -> 1 ... 101 ->
// 	//                   \ 102b (losing)

// 	// Verify block 102 is still the original block at height 102
// 	dst.verifyBlockByHeight(t, block102a, 102)

// 	// Verify conflicting
// 	dst.verifyConflicting(t, subtree102b.RootHash(), []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

// 	// Step 2: Create child transaction of the conflicting transaction
// 	txDoubleSpendChild := createTransaction(t, txDoubleSpend, dst.privKey, 47e8)

// 	subtree103b, block103b := dst.createTestBlock(t, []*bt.Tx{txDoubleSpendChild}, block102b)

// 	require.NoError(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block103b, block103b.Height),
// 		"Failed to process block with child of double spend transaction")

// 	// At this point we have:
// 	//                   / 102a (losing)
// 	// 0 -> 1 ... 101 ->
// 	//                   \ 102b -> 103b (winning)

// 	// Verify block 102b is now the block at height 102
// 	dst.verifyBlockByHeight(t, block102b, 102)

// 	// Verify block 103b is the block at height 103
// 	dst.verifyBlockByHeight(t, block103b, 103)

// 	dst.verifyConflicting(t, subtree103b.RootHash(), []chainhash.Hash{*txDoubleSpendChild.TxIDChainHash()})

// 	// Verify final state in Block Assembly
// 	state := dst.waitForBlockHeight(t, 103, 5*time.Second)

// 	assert.Equal(t, uint32(103), state.CurrentHeight, "Expected block assembly to reach height 103")

// 	// Check the txOriginal is marked as conflicting
// 	dst.verifyConflicting(t, block102a.Subtrees[0], []chainhash.Hash{*txOriginal.TxIDChainHash()})

// 	// Check the txDoubleSpend is no longer marked as conflicting
// 	dst.verifyConflicting(t, block102b.Subtrees[0], []chainhash.Hash{*txDoubleSpend.TxIDChainHash()})

// 	// Check the txDoubleSpendChild is no longer marked as conflicting
// 	dst.verifyConflicting(t, block103b.Subtrees[0], []chainhash.Hash{*txDoubleSpendChild.TxIDChainHash()})
// }

// This is testing a scenario where:
// 1. Block 103 is is continuing from block 102
// 2. The txDoubleSpend is in block 103 which means the block is invalid
func testDoubleSpendInSubsequentBlock(t *testing.T) {
	// Setup test environment
	dst, _, _, txDoubleSpend, block102 := setupDoubleSpendTest(t)
	defer dst.d.Stop()

	// Step 1: Create and validate block with double spend transaction
	_, block103 := dst.createTestBlock(t, []*bt.Tx{txDoubleSpend}, block102)

	require.Error(t, dst.blockValidationClient.ProcessBlock(dst.ctx, block103, block103.Height),
		"Failed to reject invalid block with double spend transaction")
}

// TestMarkAsConflictingMultipleSameBlock tests a scenario where:
// 1. Multiple transactions conflict with each other
// 2. All conflicting transactions are in the same block
// 3. All conflicting transactions should be marked as conflicting
func TestMarkAsConflictingMultipleSameBlock(t *testing.T) {
}

// TestMarkAsConflictingMultiple tests a scenario where:
// 1. Multiple transactions conflict with each other
// 2. Conflicting transactions are in different blocks
// 3. All conflicting transactions should be marked as conflicting
func TestMarkAsConflictingMultiple(t *testing.T) {
}

// TestMarkAsConflictingChains tests a scenario where:
// 1. Two transaction chains conflict with each other
// 2. All transactions in both chains should be marked as conflicting
func TestMarkAsConflictingChains(t *testing.T) {
}
