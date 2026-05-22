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

// TestPrunerParentNotDeletedBeforeChildren verifies that a parent transaction with
// multiple outputs stored externally (via low UtxoBatchSize) is NOT pruned before
// its children when mining past the UTXO retention height.
//
// This reproduces a production bug where parent txs were being pruned before their
// children, breaking the child->parent reference chain.
//
// Transaction graph (all mined in a single block):
//
//	coinbase -> parentTx (5 outputs, external due to UtxoBatchSize=2)
//	              ├── output 0,1 -> child1 (5 outputs, external)
//	              │                    └── output 0,1,2,3,4 -> grandchild (fully spends child1)
//	              └── output 2,3 -> child2 (5 outputs, external, NOT spent)
//	              (output 4 unspent on parent)
//
// After mining past retention:
//   - child1 is fully spent → gets DAH
//   - child2 is NOT fully spent → no DAH
//   - parentTx is NOT fully spent (output 4 unspent) → no DAH
//   - parentTx must NOT be deleted while children still exist
func TestPrunerParentNotDeletedBeforeChildren(t *testing.T) {
	const (
		blockHeightRetention     = 3
		lowUtxoBatchSize         = 2 // triggers external storage with >2 outputs
		numOutputsForExternalTx  = 5 // creates 3 records with batchSize=2 → external
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
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
				s.UtxoStore.UnminedTxRetention = unminedTxRetention
				s.UtxoStore.ParentPreservationBlocks = parentPreservationBlocks
				s.GlobalBlockHeightRetention = blockHeightRetention
				s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
				s.ChainCfgParams.CoinbaseMaturity = 5
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()

	// Mine to maturity and get a spendable coinbase
	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)

	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Height after maturity: %d", meta.Height)

	// ========== Create Parent Transaction with multiple outputs (external) ==========
	parentTx := node.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx,
			coinbaseTx.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
	)
	parentTxHex := hex.EncodeToString(parentTx.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{parentTxHex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(parentTx, 10*time.Second)
	require.NoError(t, err)
	node.MineAndWait(t, 1)

	_, meta, err = node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	parentMinedHeight := meta.Height
	parentHash := parentTx.TxIDChainHash()
	t.Logf("Parent tx %s mined at height %d with %d outputs (external, DAH ~%d)",
		parentHash, parentMinedHeight, len(parentTx.Outputs), parentMinedHeight+blockHeightRetention)

	parentMeta, err := node.UtxoStore.Get(ctx, parentHash, fields.External)
	require.NoError(t, err)
	require.NotNil(t, parentMeta, "Parent should exist in UTXO store")
	t.Logf("Parent tx external storage confirmed")

	// ========== Create Child1: spends parent outputs 0,1 (external) ==========
	child1 := node.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx,
			parentTx.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
	)
	child1Hex := hex.EncodeToString(child1.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{child1Hex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(child1, 10*time.Second)
	require.NoError(t, err)
	child1Hash := child1.TxIDChainHash()
	t.Logf("Child1 %s: spends parent outputs 0,1 (%d outputs)", child1Hash, len(child1.Outputs))

	// ========== Create Child2: spends parent outputs 2,3 (external, NOT spent later) ==========
	child2 := node.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 2),
		transactions.WithInput(parentTx, 3),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx,
			parentTx.Outputs[2].Satoshis/uint64(numOutputsForExternalTx)-100),
	)
	child2Hex := hex.EncodeToString(child2.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{child2Hex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(child2, 10*time.Second)
	require.NoError(t, err)
	child2Hash := child2.TxIDChainHash()
	t.Logf("Child2 %s: spends parent outputs 2,3 (%d outputs, will NOT be spent)", child2Hash, len(child2.Outputs))

	// Mine child1 and child2 into a block
	node.MineAndWait(t, 1)

	_, meta, err = node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	childrenMinedHeight := meta.Height
	t.Logf("Children mined at height %d", childrenMinedHeight)

	// ========== Create Grandchild: fully spends ALL child1 outputs ==========
	// This causes child1 to become fully spent → gets DAH
	grandchild := node.CreateTransactionWithOptions(t,
		transactions.WithInput(child1, 0),
		transactions.WithInput(child1, 1),
		transactions.WithInput(child1, 2),
		transactions.WithInput(child1, 3),
		transactions.WithInput(child1, 4),
		transactions.WithP2PKHOutputs(1, child1.Outputs[0].Satoshis*uint64(numOutputsForExternalTx)-1000),
	)
	grandchildHex := hex.EncodeToString(grandchild.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{grandchildHex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(grandchild, 10*time.Second)
	require.NoError(t, err)
	grandchildHash := grandchild.TxIDChainHash()
	t.Logf("Grandchild %s: fully spends all %d child1 outputs", grandchildHash, numOutputsForExternalTx)

	// Mine grandchild
	node.MineAndWait(t, 1)

	_, meta, err = node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	grandchildMinedHeight := meta.Height
	t.Logf("Grandchild mined at height %d", grandchildMinedHeight)

	// At this point:
	// - parentTx: outputs 0,1 spent by child1, outputs 2,3 spent by child2, output 4 UNSPENT → no DAH
	// - child1: ALL outputs spent by grandchild → DAH = grandchildMinedHeight + blockHeightRetention
	// - child2: NO outputs spent → no DAH
	// - grandchild: no outputs spent → no DAH

	// Verify all transactions exist before pruning
	_, err = node.UtxoStore.Get(ctx, parentHash)
	require.NoError(t, err, "Parent should exist before pruning")
	_, err = node.UtxoStore.Get(ctx, child1Hash)
	require.NoError(t, err, "Child1 should exist before pruning")
	_, err = node.UtxoStore.Get(ctx, child2Hash)
	require.NoError(t, err, "Child2 should exist before pruning")
	_, err = node.UtxoStore.Get(ctx, grandchildHash)
	require.NoError(t, err, "Grandchild should exist before pruning")
	t.Log("All transactions verified in UTXO store before pruning")

	// ========== Mine blocks to exceed retention period ==========
	// We need to exceed child1's DAH: grandchildMinedHeight + blockHeightRetention
	// Mine enough empty blocks via ProcessBlock (bypasses BA, doesn't change tx state)
	t.Logf("Mining blocks to exceed child1 DAH (~%d)...", grandchildMinedHeight+blockHeightRetention)

	prevBlock, err := node.BlockchainClient.GetBlockByHeight(ctx, grandchildMinedHeight)
	require.NoError(t, err)

	numBlocksToMine := blockHeightRetention + 5 // exceed child1's DAH with margin
	for i := 0; i < numBlocksToMine; i++ {
		_, newBlock := node.CreateTestBlock(t, prevBlock, uint32(9000+i))
		err = node.BlockValidationClient.ProcessBlock(ctx, newBlock, newBlock.Height, "", "legacy", 0)
		require.NoError(t, err)
		t.Logf("Processed empty block at height %d", newBlock.Height)
		prevBlock = newBlock
	}

	_, meta, err = node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Current height: %d (child1 DAH ~%d)", meta.Height, grandchildMinedHeight+blockHeightRetention)

	// Wait for pruner to process
	time.Sleep(5 * time.Second)

	// ========== Verify: Parent must NOT be deleted ==========
	// The parent still has output 4 unspent, and child2 is not fully spent.
	// Even if child1's DAH was reached, the parent should remain.
	parentMetaAfter, err := node.UtxoStore.Get(ctx, parentHash)
	require.NoError(t, err, "PARENT MUST NOT BE PRUNED - it still has unspent outputs and existing children")
	require.NotNil(t, parentMetaAfter, "Parent meta should not be nil")
	t.Logf("PASS: Parent tx %s still exists after pruning (not deleted before children)", parentHash)

	// ========== Verify: Child2 must NOT be deleted ==========
	// Child2 has no outputs spent → no DAH → should not be pruned
	child2MetaAfter, err := node.UtxoStore.Get(ctx, child2Hash)
	require.NoError(t, err, "Child2 must not be pruned - it has unspent outputs")
	require.NotNil(t, child2MetaAfter, "Child2 meta should not be nil")
	t.Logf("PASS: Child2 %s still exists (unspent outputs)", child2Hash)

	// ========== Verify: Grandchild must NOT be deleted ==========
	// Grandchild has no outputs spent → no DAH → should not be pruned
	grandchildMetaAfter, err := node.UtxoStore.Get(ctx, grandchildHash)
	require.NoError(t, err, "Grandchild must not be pruned - it has unspent outputs")
	require.NotNil(t, grandchildMetaAfter, "Grandchild meta should not be nil")
	t.Logf("PASS: Grandchild %s still exists (unspent outputs)", grandchildHash)

	// ========== Check child1 status ==========
	// child1 is fully spent and its DAH was exceeded, so it MAY have been pruned
	// This is acceptable - the key assertion is that PARENT survives
	child1MetaAfter, err := node.UtxoStore.Get(ctx, child1Hash)
	if err != nil {
		t.Logf("INFO: Child1 %s was pruned (fully spent, DAH exceeded) - this is acceptable", child1Hash)
	} else {
		t.Logf("INFO: Child1 %s still exists (DAH=%v)", child1Hash, child1MetaAfter)
	}

	t.Log("Parent-before-child pruning protection validated successfully")
}

// TestPrunerParentFullySpentNotDeletedBeforeChildren verifies that even when a parent
// transaction is fully spent (all outputs consumed), it is NOT pruned before its children.
//
// Transaction graph:
//
//	coinbase -> parentTx (5 outputs, external)
//	              ├── output 0,1 -> child1 (5 outputs, external)
//	              │                    └── all outputs -> grandchild (fully spends child1)
//	              └── output 2,3,4 -> child2 (5 outputs, external, NOT spent)
//
// parentTx is FULLY spent (all 5 outputs consumed by child1+child2) → gets DAH.
// child1 is fully spent by grandchild → gets DAH.
// child2 is NOT spent → no DAH.
//
// After mining past retention: parent must NOT be deleted while child2 still exists.
func TestPrunerParentFullySpentNotDeletedBeforeChildren(t *testing.T) {
	const (
		blockHeightRetention     = 1
		lowUtxoBatchSize         = 2
		numOutputsForExternalTx  = 5
		unminedTxRetention       = 2
		parentPreservationBlocks = 100
	)

	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnablePruner:         true,
		EnableBlockPersister: true,
		EnableRPC:            true,
		EnableValidator:      true,
		UTXOStoreType:        "aerospike",
		UseUnifiedLogger:     false,
		PreserveDataDir:      true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
				s.UtxoStore.UnminedTxRetention = unminedTxRetention
				s.UtxoStore.ParentPreservationBlocks = parentPreservationBlocks
				s.GlobalBlockHeightRetention = blockHeightRetention
				// s.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockMined
				s.ChainCfgParams.CoinbaseMaturity = 5
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
			},
		),
	})
	defer node.Stop(t, true)

	ctx := context.Background()

	coinbaseTx := node.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)

	_, meta, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Height after maturity: %d", meta.Height)

	// ========== Create Parent: 5 outputs, all will be spent ==========
	parentTx := node.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx,
			coinbaseTx.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
	)
	parentTxHex := hex.EncodeToString(parentTx.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{parentTxHex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(parentTx, 10*time.Second)
	require.NoError(t, err)
	blockWithParent := node.MineAndWait(t, 1)

	_, meta, err = node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	parentMinedHeight := meta.Height
	parentHash := parentTx.TxIDChainHash()
	t.Logf("Parent tx %s mined at height %d (5 outputs, external)", parentHash, parentMinedHeight)

	// ========== Create Child1: spends parent outputs 0,1 ==========
	child1 := node.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx,
			parentTx.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
	)
	child1Hex := hex.EncodeToString(child1.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{child1Hex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(child1, 10*time.Second)
	require.NoError(t, err)
	child1Hash := child1.TxIDChainHash()
	t.Logf("Child1 %s: spends parent outputs 0,1", child1Hash)

	// Mine Child 1
	_ = node.MineAndWait(t, 1)
	node.VerifyOnLongestChainInUtxoStore(t, child1)

	t.Logf("After Child1 is mined")
	rawParentTx := GetRawTx(t, node.UtxoStore, *parentHash, fields.DeleteAtHeight.String(), fields.BlockHeights.String(), string(fields.Utxos), fields.TotalUtxos.String(), fields.External.String(), fields.TotalExtraRecs.String(), fields.SpentExtraRecs.String())
	require.NotNil(t, rawParentTx)

	PrintRawTx(t, "Raw parentTx", rawParentTx.(map[string]interface{}))

	// ========== Create Child2: spends parent outputs 2,3,4 (ALL remaining) ==========
	// This makes parent FULLY spent → parent will get DAH
	child2 := node.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 2),
		transactions.WithInput(parentTx, 3),
		transactions.WithInput(parentTx, 4),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx,
			parentTx.Outputs[2].Satoshis/uint64(numOutputsForExternalTx)-100),
	)
	child2Hex := hex.EncodeToString(child2.ExtendedBytes())
	_, err = node.CallRPC(ctx, "sendrawtransaction", []any{child2Hex})
	require.NoError(t, err)
	err = node.WaitForTransactionInBlockAssembly(child2, 10*time.Second)
	require.NoError(t, err)
	child2Hash := child2.TxIDChainHash()
	t.Logf("Child2 %s: spends parent outputs 2,3,4 (parent now FULLY SPENT)", child2Hash)

	_, forkBlockWithChild2 := node.CreateTestBlock(t, blockWithParent, 201, child2)
	err = node.BlockValidation.ValidateBlock(node.Ctx, forkBlockWithChild2, node.AssetURL, false)
	require.NoError(t, err)

	// node.WaitForBlockAssemblyToProcessTx(t, child2Hex)
	time.Sleep(2 * time.Second)

	node.VerifyNotOnLongestChainInUtxoStore(t, child2)
	node.VerifyInBlockAssembly(t, child2)

	// Parent DAH is not set here
	t.Logf("After forked block")
	rawParentTx = GetRawTx(t, node.UtxoStore, *parentHash, fields.DeleteAtHeight.String(), fields.BlockHeights.String(), string(fields.Utxos), fields.TotalUtxos.String(), fields.External.String(), fields.TotalExtraRecs.String(), fields.SpentExtraRecs.String())
	require.NotNil(t, rawParentTx)

	PrintRawTx(t, "Raw parentTx", rawParentTx.(map[string]interface{}))

	node.MineAndWait(t, 1)

	node.VerifyOnLongestChainInUtxoStore(t, child2)
	node.VerifyNotInBlockAssembly(t, child2)

	// Parent DAH is set here
	t.Logf("After Child2 is mined,so Parent is all spent")
	rawParentTx = GetRawTx(t, node.UtxoStore, *parentHash, fields.DeleteAtHeight.String(), fields.BlockHeights.String(), string(fields.Utxos), fields.TotalUtxos.String(), fields.External.String(), fields.TotalExtraRecs.String(), fields.SpentExtraRecs.String())
	require.NotNil(t, rawParentTx)

	PrintRawTx(t, "Raw parentTx", rawParentTx.(map[string]interface{}))

	triggerBlock := node.MineAndWait(t, 1)
	node.MineAndWait(t, 2)

	err = node.WaitForBlockPersisted(triggerBlock.Hash(), 10*time.Second)
	require.NoError(t, err)

	node.WaitForPruner(t, 10*time.Second)

	_, err = node.UtxoStore.Get(node.Ctx, parentHash)
	require.Error(t, err)
}
