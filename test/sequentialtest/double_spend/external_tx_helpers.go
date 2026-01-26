package doublespendtest

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

const (
	// lowUtxoBatchSize triggers external storage with fewer outputs.
	// External transactions are created when len(batches) > 1 where batches = ceil(numOutputs / utxoBatchSize).
	lowUtxoBatchSize = 2

	// numOutputsForExternalTx creates enough outputs to trigger external storage.
	// With utxoBatchSize=2, we need >2 outputs to get len(batches) > 1.
	numOutputsForExternalTx = 5

	// outputAmount is the satoshi amount per output in external transactions.
	outputAmount = uint64(100000)
)

var (
	// externalBlockWait is the timeout for waiting for block height in external tx tests.
	externalBlockWait = 5 * time.Second
)

// externalTxSettingsFunc returns a settings override function that configures
// low utxoBatchSize to trigger external transaction storage.
func externalTxSettingsFunc() func(*settings.Settings) {
	return test.ComposeSettings(
		test.SystemTestSettings(),
		func(s *settings.Settings) {
			s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
		},
	)
}

// setupExternalTxDoubleSpendTest creates a test environment for double spend tests
// using external transactions (multi-UTXO record transactions).
//
// It returns:
//   - td: TestDaemon instance
//   - parentTx: Parent transaction with 5 outputs (used to create conflicting txs)
//   - txOriginal: Original transaction spending parentTx outputs 0,1 (external tx with 5 outputs)
//   - txDoubleSpend: Double spend transaction spending parentTx outputs 0,4 (external tx with 5 outputs)
//   - block102: Block at height 102 containing parentTx
//   - tx2: Another transaction from a different block (external tx with 5 outputs)
func setupExternalTxDoubleSpendTest(t *testing.T, utxoStoreType string, blockOffset ...uint32) (
	td *daemon.TestDaemon,
	parentTx, txOriginal, txDoubleSpend *bt.Tx,
	block102 *model.Block,
	tx2 *bt.Tx,
) {
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})

	// Set the FSM state to RUNNING...
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	// Use different block heights for different tests to avoid UTXO conflicts
	blockHeight := uint32(1)
	if len(blockOffset) > 0 && blockOffset[0] > 0 && blockOffset[0] <= 100 {
		blockHeight = blockOffset[0]
	}

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, blockHeight)
	require.NoError(t, err)

	coinbaseTx1 := block1.CoinbaseTx
	// create a parent tx by spending the coinbaseTx
	parentTx = td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx1, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, coinbaseTx1.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
	)

	// propagate
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err)

	// generate a block to include parentTx
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block102, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	require.Equal(t, uint64(2), block102.TransactionCount)

	// Create external transactions: transactions with enough outputs to trigger external storage
	// With utxoBatchSize=2 and numOutputsForExternalTx=5, these will be stored externally
	txOriginal = td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, parentTx.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
	)

	txDoubleSpend = td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithInput(parentTx, 4),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, parentTx.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
	)

	t.Logf("Created external txOriginal %s with %d outputs", txOriginal.TxIDChainHash().String(), len(txOriginal.Outputs))
	t.Logf("Created external txDoubleSpend %s with %d outputs", txDoubleSpend.TxIDChainHash().String(), len(txDoubleSpend.Outputs))

	err1 := td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal)
	require.NoError(t, err1)

	err2 := td.PropagationClient.ProcessTransaction(td.Ctx, txDoubleSpend)
	require.Error(t, err2, "This should fail as it is a double spend")

	// Create another external transaction from a different block
	block2Height := blockHeight + 1
	if block2Height > 100 {
		block2Height = 2
	}

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, block2Height)
	require.NoError(t, err)

	tx2 = td.CreateTransactionWithOptions(t,
		transactions.WithInput(block2.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)

	t.Logf("Created external tx2 %s with %d outputs", tx2.TxIDChainHash().String(), len(tx2.Outputs))

	err = td.PropagationClient.ProcessTransaction(td.Ctx, tx2)
	require.NoError(t, err)

	return td, parentTx, txOriginal, txDoubleSpend, block102, tx2
}

// createExternalTxFromParent creates an external transaction (multi-output) from a parent transaction.
// This creates a transaction with multiple outputs that will be stored externally due to
// the low utxoBatchSize setting.
func createExternalTxFromParent(t *testing.T, td *daemon.TestDaemon, parentTx *bt.Tx, outputIdx uint32) *bt.Tx {
	tx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, outputIdx),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount/uint64(numOutputsForExternalTx)),
	)
	t.Logf("Created external tx %s with %d outputs from parent output %d",
		tx.TxIDChainHash().String(), len(tx.Outputs), outputIdx)
	return tx
}

// createExternalTxChain creates a chain of external transactions where each transaction
// spends from a different output of the parent to create multiple UTXO records.
// Returns a slice of transactions [tx1, tx2, tx3, ...] where each spends from
// different outputs of the previous transaction.
func createExternalTxChain(t *testing.T, td *daemon.TestDaemon, startTx *bt.Tx, chainLength int) []*bt.Tx {
	chain := make([]*bt.Tx, chainLength)
	currentTx := startTx

	for i := 0; i < chainLength; i++ {
		// Spend from output i%len(outputs) to spread across different UTXO records
		outputIdx := uint32(i % len(currentTx.Outputs))
		if currentTx.Outputs[outputIdx].Satoshis == 0 {
			outputIdx = 0 // Fall back to first output if selected output is empty
		}

		nextTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(currentTx, outputIdx),
			transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount/(uint64(numOutputsForExternalTx)*uint64(i+2))),
		)
		t.Logf("Chain tx %d: %s with %d outputs (from parent output %d)",
			i+1, nextTx.TxIDChainHash().String(), len(nextTx.Outputs), outputIdx)

		chain[i] = nextTx
		currentTx = nextTx
	}

	return chain
}

// createConflictingBlockUseExternalRecords creates a block containing external transactions that
// conflict with transactions in another block. This is used for testing double spend
// scenarios with external transactions.
func createConflictingBlockUseExternalRecords(t *testing.T, td *daemon.TestDaemon, originalBlock *model.Block,
	blockTxs []*bt.Tx, originalTxs []*bt.Tx, nonce uint32, expectBlockError ...bool) *model.Block {

	// Get previous block so we can create an alternate block
	previousBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, originalBlock.Height-1)
	require.NoError(t, err)

	// Create block with the conflicting transactions
	newBlockSubtree, newBlock := td.CreateTestBlock(t, previousBlock, nonce, blockTxs...)

	if len(expectBlockError) > 0 && expectBlockError[0] {
		require.Error(t, td.BlockValidationClient.ProcessBlock(td.Ctx, newBlock, newBlock.Height, "", "legacy"),
			"Expected block with double spend transaction to be rejected")
		return nil
	}

	// require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, newBlock, newBlock.Height, "", "legacy"),
	// 	"Failed to process block with double spend transaction")

	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, newBlock, "legacy"),
		"Failed to process block with double spend transaction")

	td.VerifyBlockByHash(t, newBlock, newBlock.Header.Hash())

	// Verify original block is still at height
	td.WaitForBlockHeight(t, originalBlock, externalBlockWait, true)

	// Verify conflicting status (only if original block has subtrees)
	if len(originalBlock.Subtrees) > 0 {
		td.VerifyConflictingInSubtrees(t, originalBlock.Subtrees[0])
	}
	td.VerifyConflictingInUtxoStore(t, false, originalTxs...)

	// Verify new block txs are marked as conflicting
	td.VerifyConflictingInSubtrees(t, newBlockSubtree.RootHash(), blockTxs...)
	td.VerifyConflictingInUtxoStore(t, true, blockTxs...)

	return newBlock
}
