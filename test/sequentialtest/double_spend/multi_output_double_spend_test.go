package doublespendtest

import (
	"crypto/rand"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("SETTINGS_CONTEXT", "test")
}

var (
	multiOutputBlockWait = 5 * time.Second
)

// TestMultiOutputDoubleSpendScenarios tests double-spend scenarios where transactions have
// multiple outputs and utxoBatchSize is set to 2, forcing UTXO records to be stored in
// external transaction files (child records like "txid_1", "txid_2", etc.).
//
// With utxoBatchSize=2:
// - Outputs 0-1 are in the main record
// - Outputs 2-3 are in external record "_1"
// - Outputs 4-5 are in external record "_2"
// - And so on...
//
// This tests that double-spend detection and reorg handling work correctly when
// UTXOs are stored across multiple Aerospike records.

func TestMultiOutputSingleDoubleSpendAerospike(t *testing.T) {
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	t.Run("single_double_spend_with_multi_output_external_records", func(t *testing.T) {
		testSingleDoubleSpendMultiOutput(t, utxoStore)
	})
}
func TestMultiOutputSingleDoubleSpendPostgres(t *testing.T) {
	utxoStore, teardown, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		_ = teardown()
	}()

	t.Run("single_double_spend_with_multi_output_external_records", func(t *testing.T) {
		testSingleDoubleSpendMultiOutput(t, utxoStore)
	})
}

func TestMultiOutputConflictingTxReorgAerospike(t *testing.T) {
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = teardown() })

	t.Run("conflicting_tx_reorg_with_multi_output_external_records", func(t *testing.T) {
		testConflictingTxReorgMultiOutput(t, utxoStore)
	})
}

func TestMultiOutputConflictingTxReorgPostgres(t *testing.T) {
	utxoStore, teardown, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)
	defer func() { _ = teardown() }()

	t.Run("conflicting_tx_reorg_with_multi_output_external_records", func(t *testing.T) {
		testConflictingTxReorgMultiOutput(t, utxoStore)
	})
}


// testSingleDoubleSpendMultiOutput is the multi-output variant of testSingleDoubleSpend.
//
// Key differences from the original:
// 1. Creates a parent transaction with 10 outputs (spans 5 external records with utxoBatchSize=2)
// 2. txA0 and txB0 both spend output 5 from the parent (located in external record "_2", offset 1)
// 3. Tests that double-spend detection works correctly when the UTXO is in an external record
// 4. Tests that reorg handling updates external record states correctly
//
// Test flow (same as testSingleDoubleSpend but with multi-output parent):
//   - Parent tx with 10 outputs is mined in block 4
//   - txA0 spends output 5 and is mined in block 5a
//   - Creates block102b with txB0 (double-spend of output 5)
//   - Verifies original block remains at height 5
//   - Creates 5b,6B to trigger reorg (block6b becomes tip)
//   - Validates conflict status after reorg:
//     - txA0 becomes conflicting (losing chain)
//     - txB0 becomes non-conflicting (winning chain)
//   - Forks back to chain A with blocks 103a and 104a
//   - Validates conflict status after second reorg:
//     - txA0 becomes non-conflicting (winning chain)
//     - txB0 becomes conflicting (losing chain)
func testSingleDoubleSpendMultiOutput(t *testing.T, utxoStore string) {
	// Setup test environment with utxoBatchSize=2 to force external record creation
	// This will create a parent tx with 10 outputs, where txA0 spends output 5 (in external record "_2")
	// and txB0 is a double-spend of the same output
	td, _, txA0, txB0, block5a := setupMultiOutputDoubleSpendTest(t, utxoStore, 1)
	defer func() {
		td.Stop(t)
	}()

	// Now we have:
	// - parentTx with 10 outputs mined in block 4
	// - txA0 spending output 5 (in external record "_2", offset 1) mined in block 5
	// - txB0 is a double-spend attempt (rejected)
	//
	// With utxoBatchSize=2:
	//   - Outputs 0-1: main record
	//   - Outputs 2-3: record "_1"
	//   - Outputs 4-5: record "_2"  <-- Output 5 is here (offset 1)
	//   - Outputs 6-7: record "_3"
	//   - Outputs 8-9: record "_4"

	// 0 -> 1 -> 2 -> 3 -> 4 (parentTx) -> 5a (txA0 spends output 5) (*)

	// Create block 5b with the double spend transaction (competing with block5a)
	block5b := createConflictingBlock(t, td, block5a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 50002)

	//                   / 5a (txA0 spends output 5) (*)
	// 0 -> 1 -> 2 -> 3 -> 4 (parentTx)
	//                   \ 5b (txB0 spends output 5)

	// Create block 6b to make chain B the longest chain
	_, block6b := td.CreateTestBlock(t, block5b, 60002)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6b, block6b.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlock(t, block6b, multiOutputBlockWait, true)

	//                   / 5a (txA0 spends output 5)
	// 0 -> 1 -> 2 -> 3 -> 4 (parentTx)
	//                   \ 5b (txB0 spends output 5) -> 6b (*)

	// After reorg to chain B:
	td.VerifyConflictingInSubtrees(t, block5a.Subtrees[0], txA0)
	td.VerifyConflictingInUtxoStore(t, true, txA0)
	td.VerifyConflictingInSubtrees(t, block5b.Subtrees[0], txB0)
	td.VerifyConflictingInUtxoStore(t, false, txB0)
	td.VerifyOnLongestChainInUtxoStore(t, txB0)
	td.VerifyNotOnLongestChainInUtxoStore(t, txA0)
	td.VerifyNotInBlockAssembly(t, txA0)
	td.VerifyNotInBlockAssembly(t, txB0)

	// Fork back to the original chain and check that everything is processed properly
	_, block6a := td.CreateTestBlock(t, block5a, 60001)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6a, block6a.Height, "", "legacy"),
		"Failed to process block")

	_, block7a := td.CreateTestBlock(t, block6a, 70001)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block7a, block7a.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlock(t, block7a, multiOutputBlockWait)

	//                   / 5a (txA0) -> 6a -> 7a (*)
	// 0 -> 1 -> 2 -> 3 -> 4 (parentTx)
	//                   \ 5b (txB0) -> 6b

	// After reorg back to chain A:
	td.VerifyConflictingInSubtrees(t, block5a.Subtrees[0], txA0)
	td.VerifyConflictingInUtxoStore(t, false, txA0)
	td.VerifyConflictingInSubtrees(t, block5b.Subtrees[0], txB0)
	td.VerifyConflictingInUtxoStore(t, true, txB0)
	td.VerifyOnLongestChainInUtxoStore(t, txA0)
	td.VerifyNotOnLongestChainInUtxoStore(t, txB0)
	td.VerifyNotInBlockAssembly(t, txA0)
	td.VerifyNotInBlockAssembly(t, txB0)
}

// testConflictingTxReorgMultiOutput is the multi-output variant of testConflictingTxReorg.
//
// This test validates that when a transaction conflicts with itself (by spending the same
// outputs from a multi-output parent) and appears in competing blocks, the reorg handling
// correctly updates conflict states in external records.
//
// Key differences from the original:
// 1. Uses a parent transaction with 10 outputs (spans 5 external records with utxoBatchSize=2)
// 2. txOriginal spends output 5 from the parent (located in external record "_2", offset 1)
// 3. tx1 and tx1Conflicting both spend output 0 from txOriginal (also in external record)
// 4. Tests that conflict marking works correctly when UTXOs are in external records
//
// Test flow:
//   - Parent tx with 10 outputs is mined in block 4
//   - txOriginal spends output 5 and is mined in block 5
//   - tx1 is sent to propagation (accepted)
//   - tx1Conflicting is sent to propagation (rejected as double-spend)
//   - Creates block103a with tx1Conflicting
//   - Creates block103b with tx1Conflicting (competing block)
//   - Validates that tx1 is marked as conflicting, tx1Conflicting is not
//   - Creates block104b to make chain B win
//   - Validates conflict states remain correct after reorg
func testConflictingTxReorgMultiOutput(t *testing.T, utxoStore string) {
	// Setup test environment with parent tx (10 outputs), spending output 5 (in external record "_2")
	td, _, txOriginal, _, block5a := setupMultiOutputDoubleSpendTest(t, utxoStore, 1)
	defer func() {
		td.Stop(t)
	}()

	// Now we have:
	// - parentTx with 10 outputs mined in block 4
	// - txOriginal spending multiple outputs (0,1,5,6,7,8) from parentTx mined in block 5
	//
	// With utxoBatchSize=2:
	//   - Outputs 0-1: main record
	//   - Outputs 2-3: record "_1"
	//   - Outputs 4-5: record "_2"  <-- Output 5 from parentTx is here
	//   - Outputs 6-7: record "_3"
	//   - Outputs 8-9: record "_4"
	//
	// txOriginal has 6 outputs (each 100M satoshis), so we can create conflicting transactions spending them

	// Create two transactions that conflict by spending some of the same outputs from txOriginal
	// First transaction spends outputs 0-3 from txOriginal (4 inputs × 100M = 400M satoshis)
	// We create 3 outputs of 100M each plus leave some for fee (300M + ~100M fee)
	tx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txOriginal, 0),
		transactions.WithInput(txOriginal, 1),
		transactions.WithInput(txOriginal, 2),
		transactions.WithInput(txOriginal, 3),
		transactions.WithP2PKHOutputs(3, 100000000),
	)

	// Second transaction also tries to spend outputs 2-4 from txOriginal (conflicts with tx1 on outputs 2,3)
	// 3 inputs × 100M = 300M satoshis, create 2 outputs of 100M each
	tx1Conflicting := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txOriginal, 2),
		transactions.WithInput(txOriginal, 3),
		transactions.WithInput(txOriginal, 4),
		transactions.WithP2PKHOutputs(2, 100000000),
	)

	// 0 -> 1 -> 2 -> 3 -> 4 (parentTx) -> 5 (txOriginal) (*)

	// Send tx1 to the block assembly
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1), "Failed to process transaction tx1")

	// Send tx1Conflicting to the block assembly - should be rejected as double-spend
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1Conflicting), "Failed to reject conflicting transaction tx1Conflicting")

	// Create block 6a with the conflicting tx
	_, block6a := td.CreateTestBlock(t, block5a, 60001, tx1Conflicting)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", nil, true), "Failed to process block")

	// Create block 6b with the conflicting tx (competing with block6a)
	_, block6b := td.CreateTestBlock(t, block5a, 60002, tx1Conflicting)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6b, "legacy", nil, true), "Failed to process block")

	td.WaitForBlock(t, block6a, multiOutputBlockWait)

	//                   / 6a {tx1Conflicting} (*)
	// 0 -> 1 -> 2 -> 3 -> 4 (parentTx) -> 5 (txOriginal)
	//                   \ 6b {tx1Conflicting}

	// Check the tx1Conflicting is marked as conflicting in subtrees
	td.VerifyConflictingInSubtrees(t, block6a.Subtrees[0], tx1Conflicting)
	td.VerifyConflictingInSubtrees(t, block6b.Subtrees[0], tx1Conflicting)

	// tx1 should be marked as conflicting (lost to tx1Conflicting in blocks)
	// tx1Conflicting should NOT be marked as conflicting (it's in the winning chain)
	td.VerifyConflictingInUtxoStore(t, true, tx1)
	td.VerifyConflictingInUtxoStore(t, false, tx1Conflicting)

	td.VerifyNotInBlockAssembly(t, tx1)
	td.VerifyNotInBlockAssembly(t, tx1Conflicting)

	// Fork to the new chain and check that everything is processed properly
	_, block7b := td.CreateTestBlock(t, block6b, 70002) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block7b, block7b.Height, "", "legacy"), "Failed to process block")

	// When we reorg, tx1Conflicting should be processed properly and removed from block assembly
	//                   / 6a {tx1Conflicting}
	// 0 -> 1 -> 2 -> 3 -> 4 (parentTx) -> 5 (txOriginal)
	//                   \ 6b {tx1Conflicting} -> 7b (*)

	td.WaitForBlock(t, block7b, multiOutputBlockWait)

	td.VerifyNotInBlockAssembly(t, tx1Conflicting)

	// Check that tx1Conflicting has not been marked again as conflicting
	// (it's in the winning chain B)
	td.VerifyConflictingInUtxoStore(t, false, tx1Conflicting)

	// Check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block6a.Subtrees[0], tx1Conflicting)
	td.VerifyConflictingInSubtrees(t, block6b.Subtrees[0], tx1Conflicting)

	// verify that longest chain will not accept txOriginal
	// add block 8b with the txOriginal
	_, block8b := td.CreateTestBlock(t, block7b, 80002, txOriginal)
	require.Error(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block8b, block8b.Height, "", "legacy"), "Failed to process block")
}

// setupMultiOutputDoubleSpendTest creates a test environment similar to setupDoubleSpendTest,
// but with utxoBatchSize=2 and a parent transaction with multiple outputs that creates
// a double-spend scenario where some outputs are in external records.
//
// Returns:
//   - td: Test daemon
//   - parentTx: Transaction with outputCount outputs (mined in block at blockOffset)
//   - txOriginal: Transaction spending specific output from parentTx (mined in block 4)
//   - txDoubleSpend: Transaction double-spending same output (rejected)
//   - block4: The block containing txOriginal
//   - outputsToDoubleSpend: Array of output indices that are being double-spent
func setupMultiOutputDoubleSpendTest(t *testing.T, utxoStoreOverride string, blockOffset uint32) (*daemon.TestDaemon, *bt.Tx, *bt.Tx, *bt.Tx, *model.Block) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(tSettings *settings.Settings) {
			url, err := url.Parse(utxoStoreOverride)
			require.NoError(t, err)
			tSettings.UtxoStore.UtxoStore = url

			// Set utxoBatchSize to 2 to force external record creation
			// This means outputs 0-1 go in main record, 2-3 in _1, 4-5 in _2, etc.
			tSettings.UtxoStore.UtxoBatchSize = 2

			// Set coinbase maturity to 2 to avoid generating many blocks
			tSettings.ChainCfgParams.CoinbaseMaturity = 2
		},
	})

	// Set the FSM state to RUNNING
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate minimal blocks for coinbase maturity
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 3})
	require.NoError(t, err)

	// Get a coinbase transaction to use as input for parent tx
	blockHeight := blockOffset
	if blockHeight == 0 || blockHeight > 3 {
		blockHeight = 1
	}

	block, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, blockHeight)
	require.NoError(t, err)

	// Create a transaction with multiple outputs
	// parentTx := createTransactionWithMultipleOutputs(t, td, block.CoinbaseTx, outputCount)
	parentTx := td.CreateTransactionWithOptions(t, 
	transactions.WithInput(block.CoinbaseTx, 0),
	transactions.WithP2PKHOutputs(10, 100000000),
	)

	// Process the parent transaction
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTx))

	// Mine a block to include parentTx
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	// Verify parentTx was mined in block 4
	block4, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block4.TransactionCount, "Block 4 should have coinbase + parentTx")

	// Now create transaction that spends one or more outputs from parentTx
	txOriginal := td.CreateTransactionWithOptions(t, 
	transactions.WithInput(parentTx, 0),
	transactions.WithInput(parentTx, 1),
	transactions.WithInput(parentTx, 5),
	transactions.WithInput(parentTx, 6),
	transactions.WithInput(parentTx, 7),
	transactions.WithInput(parentTx, 8),
	transactions.WithP2PKHOutputs(6, 100000000),
	)

	// Process the original transaction
	err = td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal)
	require.NoError(t, err)

	// Create a double-spend transaction spending the same outputs
	txDoubleSpend := td.CreateTransactionWithOptions(t, 
	transactions.WithInput(parentTx, 0),
	transactions.WithInput(parentTx, 2),
	transactions.WithInput(parentTx, 4),
	transactions.WithInput(parentTx, 6),
	transactions.WithInput(parentTx, 8),
	transactions.WithInput(parentTx, 9),
	transactions.WithP2PKHOutputs(6, 100000000),
	)

	// This should fail as it's a double spend
	err = td.PropagationClient.ProcessTransaction(td.Ctx, txDoubleSpend)
	require.Error(t, err, "Expected double-spend to be rejected")

	// Mine the original transaction
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block5, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 5)
	require.NoError(t, err)

	require.Equal(t, uint64(2), block5.TransactionCount, "Block 5 should have coinbase + txOriginal")

	return td, parentTx, txOriginal, txDoubleSpend, block5
}

// createTransactionWithMultipleOutputs creates a transaction with a specified number of outputs.
// Each output gets an equal share of the input satoshis (minus a small fee).
func createTransactionWithMultipleOutputs(t *testing.T, td *daemon.TestDaemon, inputTx *bt.Tx, outputCount int) *bt.Tx {
	// Calculate satoshis per output (leave some for fees)
	totalInput := inputTx.Outputs[0].Satoshis
	fee := uint64(500)
	satoshisPerOutput := (totalInput - fee) / uint64(outputCount)
	

	// Use CreateTransactionWithOptions which will automatically sign
	return td.CreateTransactionWithOptions(t,
		transactions.WithInput(inputTx, 0),
		transactions.WithP2PKHOutputs(outputCount, satoshisPerOutput),
	)
}

// createTransactionSpendingMultipleOutputs creates a transaction that spends multiple outputs
// from a parent transaction. This is useful for testing scenarios where multiple outputs
// spanning different external records are spent in a single transaction.
func createTransactionSpendingMultipleOutputs(t *testing.T, td *daemon.TestDaemon, parentTx *bt.Tx, vouts []uint32) *bt.Tx {
	// Verify all requested outputs exist
	for _, vout := range vouts {
		require.Less(t, int(vout), len(parentTx.Outputs), "vout %d is out of range for transaction with %d outputs", vout, len(parentTx.Outputs))
	}

	// Calculate total input amount
	var totalInput uint64
	for _, vout := range vouts {
		totalInput += parentTx.Outputs[vout].Satoshis
	}

	// Create output with total input minus fee
	fee := uint64(500)
	outputAmount := totalInput - fee

	// Add random bytes to make each transaction unique
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	require.NoError(t, err)

	// Build transaction options with multiple inputs
	opts := make([]transactions.TxOption, 0, len(vouts)+2)
	for _, vout := range vouts {
		opts = append(opts, transactions.WithInput(parentTx, vout))
	}
	opts = append(opts,
		transactions.WithP2PKHOutputs(1, outputAmount),
		transactions.WithOpReturnData(randomBytes),
	)

	return td.CreateTransactionWithOptions(t, opts...)
}
