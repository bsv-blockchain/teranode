package smoke

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestPrunerUnminedParentRetention verifies that the pruner service's Phase 1
// (parent preservation) correctly protects parent transactions of old unmined
// transactions from deletion by Phase 2 (DAH pruning).
//
// The pruner runs a two-phase process triggered by block notifications:
//
//	Phase 1: PreserveParentsOfOldUnminedTransactions - finds unmined txs older
//	         than UnminedTxRetention blocks and sets PreserveUntil on their parents,
//	         which also clears DeleteAtHeight to prevent Phase 2 from deleting them.
//	Phase 2: DAH pruning - deletes records past their delete-at-height (respects PreserveUntil).
//
// This test proves Phase 1 is essential by using a small BlockHeightRetention (5 blocks).
// Without Phase 1, the parent tx would be deleted by Phase 2 once its DAH is exceeded.
// With Phase 1, the parent's DAH is cleared and PreserveUntil is set, keeping it alive.
//
// Test flow:
// 1. Create TestDaemon with pruner enabled, short retention periods
// 2. Mine to maturity, create parent tx (mined) → child tx (unmined)
// 3. Mine 8 blocks past unmined retention period
// 4. Verify parent tx still exists (preserved by Phase 1 despite DAH being exceeded)
// 5. Verify unmined child tx still exists
func TestPrunerUnminedParentRetention(t *testing.T) {
	const (
		unminedTxRetention       = 2   // Phase 1 triggers after 2 blocks past UnminedSince
		parentPreservationBlocks = 100 // Parent preserved for 100 blocks after Phase 1
	)

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:     true,
		EnableRPC:        true,
		EnableValidator:  true,
		UTXOStoreType:    "aerospike",
		UseUnifiedLogger: false,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.UtxoStore.UnminedTxRetention = unminedTxRetention
				s.UtxoStore.ParentPreservationBlocks = parentPreservationBlocks
				// Small BlockHeightRetention: parent tx DAH = minedHeight + 5
				// Without Phase 1 protection, parent would be deleted by Phase 2
				s.GlobalBlockHeightRetention = 5
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
				s.ChainCfgParams.CoinbaseMaturity = 5
				// Required: Phase 1 uses GetUnminedTxIterator which only populates
				// TxInpoints (parent hashes) when this is enabled
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()

	// Mine to maturity and get a spendable coinbase tx
	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)

	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	heightAfterMaturity := meta.Height
	t.Logf("Height after maturity: %d", heightAfterMaturity)

	// Create parent tx and submit it
	parentTx := node.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 5e8),
	)
	parentTxHex := hex.EncodeToString(parentTx.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{parentTxHex})
	require.NoError(t, err)

	// Wait for parent to be in block assembly, then mine it
	err = node.WaitForTransactionInBlockAssembly(parentTx, 10*time.Second)
	require.NoError(t, err)
	node.MineAndWait(t, 1)

	_, meta, err = node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	parentMinedHeight := meta.Height
	parentHash := parentTx.TxIDChainHash()
	// With GlobalBlockHeightRetention=5, parent DAH = parentMinedHeight + 5
	t.Logf("Parent tx %s mined at height %d (DAH will be ~%d)",
		parentHash, parentMinedHeight, parentMinedHeight+5)

	// Create unmined child tx that spends parent output 0 (will NOT be mined)
	unminedChildTx := node.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 45000000),
	)
	unminedChildHex := hex.EncodeToString(unminedChildTx.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{unminedChildHex})
	require.NoError(t, err)

	// Wait for unmined child to be in block assembly (implies it's validated and in UTXO store)
	err = node.WaitForTransactionInBlockAssembly(unminedChildTx, 10*time.Second)
	require.NoError(t, err)

	// Verify child is stored as unmined
	unminedHash := unminedChildTx.TxIDChainHash()
	unminedMeta, err := node.UtxoStore.Get(ctx, unminedHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, unminedMeta.UnminedSince, "Child should be marked as unmined")
	t.Logf("Unmined child %s stored with UnminedSince=%d", unminedHash, unminedMeta.UnminedSince)

	// Verify parent exists in UTXO store before pruner runs
	parentMeta, err := node.UtxoStore.Get(ctx, parentHash)
	require.NoError(t, err)
	require.NotNil(t, parentMeta, "Parent should exist in UTXO store")
	t.Log("Verified: parent tx exists before pruner Phase 1")

	// Add 8 empty blocks via block validation (bypassing block assembly).
	// Using CreateTestBlock + ProcessBlock ensures the unmined child is NOT included
	// in any block, keeping it unmined throughout.
	// This will:
	// 1. Pass the unmined retention period (Phase 1 fires when blockHeight - 2 >= UnminedSince)
	// 2. Exceed the parent's DAH (parentMinedHeight + 5), which would delete parent without Phase 1
	// Phase 1 runs BEFORE Phase 2 in prunerProcessor, so parent is preserved before DAH check
	t.Log("Adding 8 empty blocks via block validation to trigger pruner...")
	prevBlock, err := node.BlockchainClient.GetBlockByHeight(ctx, parentMinedHeight)
	require.NoError(t, err)

	for i := 0; i < 8; i++ {
		_, newBlock := node.CreateTestBlock(t, prevBlock, uint32(9000+i))
		err = node.BlockValidationClient.ProcessBlock(ctx, newBlock, newBlock.Height, "", "legacy")
		require.NoError(t, err)
		t.Logf("Processed empty block at height %d", newBlock.Height)
		prevBlock = newBlock
	}

	_, meta, err = node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Current height: %d (parent DAH ~%d would have been exceeded)",
		meta.Height, parentMinedHeight+5)

	// Wait for pruner to finish processing
	// The pruner processes asynchronously - allow time for Phase 1 + Phase 2 to complete
	require.Eventually(t, func() bool {
		// Check if parent tx still exists (Phase 1 should have preserved it)
		_, err := node.UtxoStore.Get(ctx, parentHash)
		if err != nil {
			t.Logf("Parent tx check: %v (waiting for pruner to stabilize)", err)
			return false
		}
		return true
	}, 30*time.Second, 1*time.Second,
		"Parent tx should still exist after pruner (Phase 1 preservation)")
	t.Logf("Verified: parent tx %s preserved by pruner Phase 1 (despite DAH being exceeded)", parentHash)

	// Verify unmined child still exists
	unminedMetaAfter, err := node.UtxoStore.Get(ctx, unminedHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, unminedMetaAfter.UnminedSince, "Unmined child should still be marked as unmined")
	t.Logf("Verified: unmined child %s still exists (UnminedSince=%d)", unminedHash, unminedMetaAfter.UnminedSince)

	t.Log("Pruner unmined parent retention validated successfully")
}

// TestPrunerMixedChildrenParentPreservation verifies that a parent transaction with
// 2 UTXOs is preserved when one output is spent by a mined child and the other by
// an unmined child.
//
// Setup:
//   - Parent tx has 2 outputs (mined into a block)
//   - Child tx1 spends output 0 and is included in a block (mined)
//   - Child tx2 spends output 1 but stays unmined (submitted via RPC, blocks bypass BA)
//
// After exceeding parent's DAH:
//   - Parent is preserved by Phase 1 because it has an unmined child (tx2),
//     even though it also has a mined child (tx1)
func TestPrunerMixedChildrenParentPreservation(t *testing.T) {
	const (
		unminedTxRetention       = 2
		parentPreservationBlocks = 100
	)

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:     true,
		EnableRPC:        true,
		EnableValidator:  true,
		UTXOStoreType:    "aerospike",
		UseUnifiedLogger: false,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.UtxoStore.UnminedTxRetention = unminedTxRetention
				s.UtxoStore.ParentPreservationBlocks = parentPreservationBlocks
				s.GlobalBlockHeightRetention = 5
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
				s.ChainCfgParams.CoinbaseMaturity = 5
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()

	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)

	// Create parent tx with 2 outputs from coinbase
	parentTx := node.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(2, 4e8),
	)
	parentHex := hex.EncodeToString(parentTx.ExtendedBytes())
	_, err := node.CallRPC(ctx, "sendrawtransaction", []any{parentHex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(parentTx, 10*time.Second)
	require.NoError(t, err)
	node.MineAndWait(t, 1)

	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	parentMinedHeight := meta.Height
	parentHash := parentTx.TxIDChainHash()
	t.Logf("Parent tx %s mined at height %d with 2 outputs (DAH ~%d)",
		parentHash, parentMinedHeight, parentMinedHeight+5)

	// Child tx2: spends parent output 1, stays unmined
	// Submit via RPC first so it's in BA / UTXO store, then bypass BA for subsequent blocks
	unminedChild := node.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(1, 3e8),
	)
	unminedChildHex := hex.EncodeToString(unminedChild.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{unminedChildHex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(unminedChild, 10*time.Second)
	require.NoError(t, err)
	unminedChildHash := unminedChild.TxIDChainHash()

	// Verify unmined child is stored as unmined
	unminedMeta, err := node.UtxoStore.Get(ctx, unminedChildHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, unminedMeta.UnminedSince, "Child tx2 should be marked as unmined")
	t.Logf("Unmined child (tx2) %s stored with UnminedSince=%d", unminedChildHash, unminedMeta.UnminedSince)

	// Child tx1: spends parent output 0, will be included in a block
	minedChild := node.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 3e8),
	)
	minedChildHash := minedChild.TxIDChainHash()

	// Verify parent exists before pruning
	_, err = node.UtxoStore.Get(ctx, parentHash)
	require.NoError(t, err, "Parent should exist in UTXO store")
	t.Log("Verified: parent tx exists before pruner runs")

	// Add blocks via ProcessBlock (bypasses BA, keeps unmined child unmined)
	// First block includes minedChild (tx1); remaining blocks are empty
	prevBlock, err := node.BlockchainClient.GetBlockByHeight(ctx, parentMinedHeight)
	require.NoError(t, err)

	// Block 1: includes minedChild (tx1 spending parent output 0)
	_, block := node.CreateTestBlock(t, prevBlock, 9000, minedChild)
	err = node.BlockValidationClient.ProcessBlock(ctx, block, block.Height, "", "legacy")
	require.NoError(t, err)
	t.Logf("Block %d: mined child (tx1) %s included (spends parent output 0)", block.Height, minedChildHash)
	prevBlock = block

	// Blocks 2-8: empty blocks to exceed parent's DAH
	for i := 1; i < 8; i++ {
		_, block = node.CreateTestBlock(t, prevBlock, uint32(9000+i))
		err = node.BlockValidationClient.ProcessBlock(ctx, block, block.Height, "", "legacy")
		require.NoError(t, err)
		prevBlock = block
	}

	_, meta, err = node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Current height: %d (parent DAH ~%d exceeded)", meta.Height, parentMinedHeight+5)

	// Wait for pruner to process — parent should be preserved despite DAH exceeded
	// Phase 1 finds unmined child tx2, sets PreserveUntil on parent, clears its DAH
	// Phase 2 cannot delete parent because DAH was cleared by Phase 1
	require.Eventually(t, func() bool {
		_, getErr := node.UtxoStore.Get(ctx, parentHash)
		if getErr != nil {
			t.Logf("Parent tx check: %v (waiting for pruner to stabilize)", getErr)
			return false
		}
		return true
	}, 30*time.Second, 1*time.Second,
		"Parent tx should still exist (Phase 1 preserves it due to unmined child tx2)")
	t.Logf("Parent tx %s preserved by Phase 1 (has unmined child despite also having mined child)", parentHash)

	// Verify unmined child (tx2) still exists and is still unmined
	unminedMetaAfter, err := node.UtxoStore.Get(ctx, unminedChildHash, fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, unminedMetaAfter.UnminedSince, "Unmined child (tx2) should still be unmined")
	t.Logf("Unmined child (tx2) still unmined (UnminedSince=%d)", unminedMetaAfter.UnminedSince)

	t.Log("Mixed children parent preservation validated successfully")
}
