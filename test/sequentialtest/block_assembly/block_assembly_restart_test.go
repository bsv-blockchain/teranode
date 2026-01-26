package block_assembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

const (
	// lowUtxoBatchSize is set low so that transactions with more outputs than this
	// will be stored externally. External transactions are created when:
	// len(batches) > 1 - number of UTXOs exceeds utxoBatchSize
	// We use a low batch size to trigger external storage with fewer outputs.
	lowUtxoBatchSize = 2

	// numOutputsForExternalTx is the number of outputs needed to exceed lowUtxoBatchSize
	// and trigger external transaction storage
	numOutputsForExternalTx = 5

	blockWait = 10 * time.Second
)

func verifyTxInpointsViaIterator(t *testing.T, td *daemon.TestDaemon, tx *bt.Tx, expectedParentTxHash *chainhash.Hash) {
	t.Helper()

	it, err := td.UtxoStore.GetUnminedTxIterator(true)
	require.NoError(t, err, "Should be able to get unmined tx iterator")
	defer it.Close()

	expectedTxHash := tx.TxIDChainHash()
	found := false

	for {
		unminedTxBatch, err := it.Next(td.Ctx)
		if err != nil {
			t.Fatalf("Error iterating unmined transactions: %v", err)
		}
		if len(unminedTxBatch) == 0 {
			break
		}

		unminedTx := unminedTxBatch[0]

		if unminedTx.Skip {
			continue
		}

		if unminedTx.Hash.String() == expectedTxHash.String() {
			found = true
			t.Logf("Found transaction %s in iterator with %d parent tx hashes",
				unminedTx.Hash.String(), len(unminedTx.TxInpoints.ParentTxHashes))

			require.Equal(t, 1, len(unminedTx.TxInpoints.ParentTxHashes),
				"Transaction TxInpoints should have exactly 1 parent tx hash. "+
					"If this is 0, ParseInputReferencesOnly failed to parse Extended Format correctly.")

			// Verify the parent tx hash matches
			actualParentTxHash := unminedTx.TxInpoints.ParentTxHashes[0]
			require.Equal(t, expectedParentTxHash.String(), actualParentTxHash.String(),
				"Parent tx hash should match the expected parent transaction")

			break
		}
	}

	require.True(t, found, "Transaction %s should be found in unmined tx iterator", expectedTxHash.String())
}
func TestBlockAssemblyRestartWithExternalTransactionsAerospike(t *testing.T) {
	t.Run("external tx reload after reset", func(t *testing.T) {
		testBlockAssemblyRestartWithExternalTx(t, "aerospike")
	})
}

func testBlockAssemblyRestartWithExternalTx(t *testing.T, utxoStoreType string) {
	// Setup test environment with low utxoBatchSize to force external storage
	// Use SkipContainerCleanup since we'll restart the daemon and reuse the container
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				// Set low batch size so transactions with >2 outputs become external
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
			},
		),
	})

	// Store container manager before any potential stop, so we can reuse it
	containerManager := td.GetContainerManager()
	// Ensure container cleanup happens at the end of the test
	defer func() {
		if containerManager != nil {
			_ = containerManager.Cleanup()
		}
	}()
	defer td.Stop(t)

	// Set the FSM state to RUNNING
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks to get spendable coinbase
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 3})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block3, blockWait, true)

	// Step 1: Create a transaction with multiple outputs to trigger external storage
	// With utxoBatchSize=2, a tx with 5 outputs will have len(batches) > 1, making it external
	t.Logf("Creating transaction with %d outputs (utxoBatchSize=%d) to trigger external storage", numOutputsForExternalTx, lowUtxoBatchSize)

	externalTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block1.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 100000),
	)

	t.Logf("Created transaction %s with %d outputs", externalTx.TxIDChainHash().String(), len(externalTx.Outputs))
	require.Equal(t, numOutputsForExternalTx, len(externalTx.Outputs), "Transaction should have expected number of outputs")

	// Step 2: Submit the transaction
	t.Log("Submitting external transaction to propagation")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, externalTx)
	require.NoError(t, err)

	// Create child
	childTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(externalTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 1000),
	)

	// Step 2: Submit the transaction
	t.Log("Submitting external transaction to propagation")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx)
	require.NoError(t, err)

	// Step 3: Wait for transaction to appear in block assembly
	t.Log("Waiting for transaction to appear in block assembly")
	err = td.WaitForTransactionInBlockAssembly(externalTx, blockWait)
	require.NoError(t, err)

	// Verify it's in block assembly before reset
	td.VerifyInBlockAssembly(t, externalTx)
	t.Logf("Transaction %s is in block assembly before reset", externalTx.TxIDChainHash().String())

	// Step 4: Trigger block assembly reset
	// This simulates a restart scenario where loadUnminedTransactions is called
	t.Log("Triggering block assembly reset (simulating restart)")
	err = td.BlockAssemblyClient.ResetBlockAssemblyFully(td.Ctx)
	require.NoError(t, err, "Block assembly reset should succeed with external transactions")

	// Wait a bit for reset to complete
	time.Sleep(2 * time.Second)

	// Step 5: Verify transaction is still in block assembly after reset
	t.Log("Verifying external transaction is reloaded after reset")
	err = td.WaitForTransactionInBlockAssembly(externalTx, blockWait)
	require.NoError(t, err, "External transaction should be reloaded after block assembly reset")

	td.VerifyInBlockAssembly(t, externalTx, childTx)
	t.Logf("Transaction %s is in block assembly after reset", externalTx.TxIDChainHash().String())

	// restart td - stop the daemon but keep the container alive
	td.Stop(t)
	td.ResetServiceManagerContext(t)

	// Create new daemon instance, reusing the existing container to preserve Aerospike data
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		ContainerManager:     containerManager,
		SkipRemoveDataDir:    true,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				// Set low batch size so transactions with >2 outputs become external
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
			},
		),
	})
	defer td.Stop(t)

	// Step 6: Mine a block and verify the transaction is included
	t.Log("Mining block to confirm external transaction")
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block4, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block4, blockWait, true)

	// Verify transaction is no longer in block assembly (it should be mined)
	td.VerifyNotInBlockAssembly(t, externalTx, childTx)

	// Verify transaction is on longest chain
	td.VerifyOnLongestChainInUtxoStore(t, externalTx)
	td.VerifyOnLongestChainInUtxoStore(t, childTx)

	// t.Logf("External transaction %s successfully mined in block %s", externalTx.TxIDChainHash().String(), block4.Hash().String())
}

func TestBlockAssemblyRestartWithMultipleExternalTransactionsAerospike(t *testing.T) {
	t.Run("multiple external tx reload after reset", func(t *testing.T) {
		testBlockAssemblyRestartWithMultipleExternalTx(t, "aerospike")
	})
}

func testBlockAssemblyRestartWithMultipleExternalTx(t *testing.T, utxoStoreType string) {
	// Setup test environment with low utxoBatchSize
	// Use SkipContainerCleanup since we'll restart the daemon and reuse the container
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
			},
		),
	})

	// Store container manager before any potential stop, so we can reuse it
	containerManager := td.GetContainerManager()
	// Ensure container cleanup happens at the end of the test
	defer func() {
		if containerManager != nil {
			_ = containerManager.Cleanup()
		}
	}()
	defer td.Stop(t)

	// Set the FSM state to RUNNING
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 5})
	require.NoError(t, err)

	block5, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 5)
	require.NoError(t, err)
	td.WaitForBlockHeight(t, block5, blockWait, true)

	// Create multiple external transactions from different coinbases
	t.Log("Creating multiple external transactions (each with outputs > utxoBatchSize)")

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)
	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)
	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	// Create 3 external transactions (each with outputs > utxoBatchSize)
	externalTx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block1.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 100000),
	)

	externalTx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block2.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 100000),
	)

	externalTx3 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block3.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 100000),
	)

	// Create child transactions that spend from external transactions
	childTx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(externalTx1, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 1000),
	)

	childTx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(externalTx2, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 1000),
	)

	childTx3 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(externalTx3, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 1000),
	)

	// Submit all transactions
	t.Log("Submitting external transactions and their children")
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, externalTx1))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, externalTx2))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, externalTx3))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx1))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx2))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx3))

	// Wait for all to appear in block assembly
	require.NoError(t, td.WaitForTransactionInBlockAssembly(externalTx1, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(externalTx2, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(externalTx3, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(childTx1, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(childTx2, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(childTx3, blockWait))

	td.VerifyInBlockAssembly(t, externalTx1, externalTx2, externalTx3, childTx1, childTx2, childTx3)
	t.Log("All external transactions and children are in block assembly")

	// Trigger reset
	t.Log("Triggering block assembly reset")
	err = td.BlockAssemblyClient.ResetBlockAssemblyFully(td.Ctx)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Verify all transactions are reloaded
	t.Log("Verifying all external transactions are reloaded after reset")
	require.NoError(t, td.WaitForTransactionInBlockAssembly(externalTx1, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(externalTx2, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(externalTx3, blockWait))

	td.VerifyInBlockAssembly(t, externalTx1, externalTx2, externalTx3, childTx1, childTx2, childTx3)
	t.Log("All external transactions successfully reloaded after reset")

	// Restart daemon - stop the daemon but keep the container alive
	td.Stop(t)
	td.ResetServiceManagerContext(t)

	// Create new daemon instance, reusing the existing container to preserve Aerospike data
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		ContainerManager:     containerManager,
		SkipRemoveDataDir:    true,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
			},
		),
	})
	defer td.Stop(t)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block6, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 6)
	require.NoError(t, err)
	td.WaitForBlockHeight(t, block6, blockWait, true)

	// Verify all transactions are mined
	td.VerifyNotInBlockAssembly(t, externalTx1, externalTx2, externalTx3, childTx1, childTx2, childTx3)
	td.VerifyOnLongestChainInUtxoStore(t, externalTx1)
	td.VerifyOnLongestChainInUtxoStore(t, externalTx2)
	td.VerifyOnLongestChainInUtxoStore(t, externalTx3)
	td.VerifyOnLongestChainInUtxoStore(t, childTx1)
	td.VerifyOnLongestChainInUtxoStore(t, childTx2)
	td.VerifyOnLongestChainInUtxoStore(t, childTx3)

	t.Log("All external transactions and children successfully mined")
}

func TestBlockAssemblyRestartWithMixedTransactionsAerospike(t *testing.T) {
	t.Run("mixed tx reload after reset", func(t *testing.T) {
		testBlockAssemblyRestartWithMixedTx(t, "aerospike")
	})
}

func testBlockAssemblyRestartWithMixedTx(t *testing.T, utxoStoreType string) {
	// Setup test environment with low utxoBatchSize
	// Use SkipContainerCleanup since we'll restart the daemon and reuse the container
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
			},
		),
	})

	// Store container manager before any potential stop, so we can reuse it
	containerManager := td.GetContainerManager()
	// Ensure container cleanup happens at the end of the test
	defer func() {
		if containerManager != nil {
			_ = containerManager.Cleanup()
		}
	}()
	defer td.Stop(t)

	// Set the FSM state to RUNNING
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 5})
	require.NoError(t, err)

	block5, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 5)
	require.NoError(t, err)
	td.WaitForBlockHeight(t, block5, blockWait, true)

	// Get coinbases for creating transactions
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)
	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)
	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	// Create a mix of regular and external transactions
	t.Log("Creating mixed transactions (regular and external)")

	// Regular transaction (1 output, stored internally since outputs <= utxoBatchSize)
	regularTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block1.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 100000),
	)
	t.Logf("Regular tx has %d outputs (internal storage)", len(regularTx.Outputs))

	// External transaction (5 outputs, stored externally since outputs > utxoBatchSize)
	externalTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block2.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 100000),
	)
	t.Logf("External tx has %d outputs (external storage)", len(externalTx.Outputs))

	// Another regular transaction
	regularTx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block3.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 100000),
	)

	// Create child transaction that spends from external transaction
	childTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(externalTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 1000),
	)

	// Submit all transactions
	t.Log("Submitting mixed transactions")
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, regularTx))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, externalTx))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, regularTx2))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx))

	// Wait for all to appear in block assembly
	require.NoError(t, td.WaitForTransactionInBlockAssembly(regularTx, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(externalTx, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(regularTx2, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(childTx, blockWait))

	td.VerifyInBlockAssembly(t, regularTx, externalTx, regularTx2, childTx)
	t.Log("All mixed transactions are in block assembly")

	// Trigger reset
	t.Log("Triggering block assembly reset")
	err = td.BlockAssemblyClient.ResetBlockAssemblyFully(td.Ctx)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Verify all transactions are reloaded (both regular and external)
	t.Log("Verifying all mixed transactions are reloaded after reset")
	require.NoError(t, td.WaitForTransactionInBlockAssembly(regularTx, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(externalTx, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(regularTx2, blockWait))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(childTx, blockWait))

	td.VerifyInBlockAssembly(t, regularTx, externalTx, regularTx2, childTx)
	t.Log("All mixed transactions successfully reloaded after reset")

	// Restart daemon - stop the daemon but keep the container alive
	td.Stop(t)
	td.ResetServiceManagerContext(t)

	// Create new daemon instance, reusing the existing container to preserve Aerospike data
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		ContainerManager:     containerManager,
		SkipRemoveDataDir:    true,
		SkipContainerCleanup: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
			},
		),
	})
	defer td.Stop(t)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block6, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 6)
	require.NoError(t, err)
	td.WaitForBlockHeight(t, block6, blockWait, true)

	// Verify all transactions are mined
	td.VerifyNotInBlockAssembly(t, regularTx, externalTx, regularTx2, childTx)
	td.VerifyOnLongestChainInUtxoStore(t, regularTx)
	td.VerifyOnLongestChainInUtxoStore(t, externalTx)
	td.VerifyOnLongestChainInUtxoStore(t, regularTx2)
	td.VerifyOnLongestChainInUtxoStore(t, childTx)

	t.Log("All mixed transactions successfully mined")
}
func TestExternalTransactionTxInpointsParsingViaIteratorAerospike(t *testing.T) {
	t.Run("external tx TxInpoints parsing via iterator", func(t *testing.T) {
		testExternalTransactionTxInpointsParsingViaIterator(t, "aerospike")
	})
}

func testExternalTransactionTxInpointsParsingViaIterator(t *testing.T, utxoStoreType string) {
	// Setup test environment with low utxoBatchSize to force external storage
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType: utxoStoreType,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
				// Set low batch size so transactions with >2 outputs become external
				s.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
			},
		),
	})
	defer td.Stop(t)

	// Set the FSM state to RUNNING
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks to get spendable coinbase
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 3})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block3, blockWait, true)

	// Create an external transaction with multiple outputs to trigger external storage
	// This transaction spends from block1's coinbase, so it has 1 input
	t.Logf("Creating external transaction with %d outputs (utxoBatchSize=%d)", numOutputsForExternalTx, lowUtxoBatchSize)

	externalTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(block1.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 100000),
	)

	t.Logf("Created external transaction %s with %d inputs and %d outputs",
		externalTx.TxIDChainHash().String(),
		len(externalTx.Inputs),
		len(externalTx.Outputs))

	// Verify the transaction has exactly 1 input (spending from coinbase)
	require.Equal(t, 1, len(externalTx.Inputs), "Transaction should have exactly 1 input")

	// Submit the transaction
	t.Log("Submitting external transaction to propagation")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, externalTx)
	require.NoError(t, err)

	// Wait for transaction to appear in block assembly
	t.Log("Waiting for transaction to appear in block assembly")
	err = td.WaitForTransactionInBlockAssembly(externalTx, blockWait)
	require.NoError(t, err)

	// Give a bit more time for the transaction to be fully stored
	time.Sleep(2 * time.Second)
	t.Log("Using GetUnminedTxIterator to verify external transaction parsing (same as loadUnminedTransactions)")

	// Verify TxInpoints are correctly parsed via the iterator
	verifyTxInpointsViaIterator(t, td, externalTx, block1.CoinbaseTx.TxIDChainHash())
	t.Logf("Successfully verified TxInpoints parsing for external transaction %s via iterator", externalTx.TxIDChainHash().String())
}
