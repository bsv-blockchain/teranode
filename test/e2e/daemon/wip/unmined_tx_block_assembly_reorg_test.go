package smoke

import (
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("SETTINGS_CONTEXT", "test")
}

// TestUnminedTransactionInBlockAssemblyAfterReorgSQLite tests with SQLite backend
func TestUnminedTransactionInBlockAssemblyAfterReorgSQLite(t *testing.T) {
	utxoStore := "sqlite:///test"

	t.Run("unmined_tx_block_assembly_reorg", func(t *testing.T) {
		testUnminedTransactionInBlockAssemblyAfterReorg(t, utxoStore)
	})
}

// TestUnminedTransactionInBlockAssemblyAfterReorgPostgres tests with PostgreSQL backend
func TestUnminedTransactionInBlockAssemblyAfterReorgPostgres(t *testing.T) {
	// Setup PostgreSQL container
	utxoStore, teardown, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err, "Failed to setup PostgreSQL container")

	defer func() {
		_ = teardown()
	}()

	t.Run("unmined_tx_block_assembly_reorg", func(t *testing.T) {
		testUnminedTransactionInBlockAssemblyAfterReorg(t, utxoStore)
	})
}

// TestUnminedTransactionInBlockAssemblyAfterReorgAerospike tests with Aerospike backend
func TestUnminedTransactionInBlockAssemblyAfterReorgAerospike(t *testing.T) {
	// Setup Aerospike container
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err, "Failed to setup Aerospike container")

	t.Cleanup(func() {
		_ = teardown()
	})

	t.Run("unmined_tx_block_assembly_reorg", func(t *testing.T) {
		testUnminedTransactionInBlockAssemblyAfterReorg(t, utxoStore)
	})
}

// testUnminedTransactionInBlockAssemblyAfterReorg is the shared test implementation
func testUnminedTransactionInBlockAssemblyAfterReorg(t *testing.T, utxoStore string) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Start NodeA with custom UTXO store
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				// Parse and set the UTXO store URL
				parsedURL, err := url.Parse(utxoStore)
				require.NoError(t, err, "Failed to parse UTXO store URL")
				s.UtxoStore.UtxoStore = parsedURL
			},
		),
	})
	defer td.Stop(t)

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err, "Failed to initialize blockchain")

	// Mine blocks to coinbase maturity
	// Note: MineToMaturityAndGetSpendableCoinbaseTx mines CoinbaseMaturity+1 blocks
	// Since CoinbaseMaturity=1 in test daemon, this mines 2 blocks (height 0->2)
	t.Log("Mining blocks to coinbase maturity...")
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	require.NotNil(t, coinbaseTx, "Failed to get spendable coinbase")

	// At this point we're at height 2, get the block at height 2, call this block2A
	height, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err, "Failed to get best height")
	require.Equal(t, uint32(2), height, "Expected blockchain to be at height 2")

	block2A, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err, "Failed to get block at height 2")
	require.NotNil(t, block2A, "Block at height 2 should not be nil")
	t.Logf("Got block2A at height 2: %s", block2A.Header.Hash().String())

	// Create Transaction TXA from coinbase
	t.Log("Creating transaction TXA from coinbase...")
	txA := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 1000000), // 0.01 BSV
	)
	require.NotNil(t, txA, "Failed to create TXA")
	t.Logf("Created TXA: %s", txA.TxIDChainHash().String())

	// Submit TXA to propagation
	err = td.PropagationClient.ProcessTransaction(td.Ctx, txA)
	require.NoError(t, err, "Failed to submit TXA")

	// Wait for TXA to be in block assembly
	err = td.WaitForTransactionInBlockAssembly(txA, 10*time.Second)
	require.NoError(t, err, "TXA should be in block assembly")

	// Create transaction TXB from TXA
	t.Log("Creating transaction TXB from TXA...")
	txB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA, 0),
		transactions.WithP2PKHOutputs(1, 999000), // Minus fee
	)
	require.NotNil(t, txB, "Failed to create TXB")
	t.Logf("Created TXB: %s", txB.TxIDChainHash().String())

	// Submit TXB to propagation
	err = td.PropagationClient.ProcessTransaction(td.Ctx, txB)
	require.NoError(t, err, "Failed to submit TXB")

	// Wait for TXB to be in block assembly
	err = td.WaitForTransactionInBlockAssembly(txB, 10*time.Second)
	require.NoError(t, err, "TXB should be in block assembly")

	// Verify both TXA and TXB are in block assembly before the reorg
	t.Log("Verifying TXA and TXB are in block assembly before reorg...")
	td.VerifyInBlockAssembly(t, txA, txB)

	// Create a block block3A manually from parent block block2A and include TXA in it
	t.Log("Creating block3A manually with TXA...")
	_, block3A := td.CreateTestBlock(t, block2A, 100, txA)
	require.NotNil(t, block3A, "Failed to create block3A")
	t.Logf("Created block3A at height 3: %s", block3A.Header.Hash().String())

	// Process and Validate the block manually
	t.Log("Processing and validating block3A...")
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block3A, block3A.Height, "", "legacy")
	require.NoError(t, err, "Failed to process block3A")

	err = td.BlockValidationClient.ValidateBlock(td.Ctx, block3A, nil)
	require.NoError(t, err, "Failed to validate block3A")

	// Wait for block3A to be at the right height
	td.WaitForBlockHeight(t, block3A, 10*time.Second, true)

	// Verify current height is 3
	height, _, err = td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err, "Failed to get best height after block3A")
	require.Equal(t, uint32(3), height, "Expected blockchain to be at height 3 after block3A")

	// Create a fork block block3B from block2A (competing with block3A)
	t.Log("Creating fork block3B from block2A...")
	_, block3B := td.CreateTestBlock(t, block2A, 200) // Different nonce, no transactions
	require.NotNil(t, block3B, "Failed to create block3B")
	t.Logf("Created fork block3B at height 3: %s", block3B.Header.Hash().String())

	// Process and validate block3B
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block3B, block3B.Height, "", "legacy")
	require.NoError(t, err, "Failed to process block3B")

	err = td.BlockValidationClient.ValidateBlock(td.Ctx, block3B, nil)
	require.NoError(t, err, "Failed to validate block3B")

	// Create a block block4B on block3B to make chainB longer
	t.Log("Creating block4B on block3B to make chainB win...")
	_, block4B := td.CreateTestBlock(t, block3B, 300) // No transactions
	require.NotNil(t, block4B, "Failed to create block4B")
	t.Logf("Created block4B at height 4: %s", block4B.Header.Hash().String())

	// Process and validate block4B
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block4B, block4B.Height, "", "legacy")
	require.NoError(t, err, "Failed to process block4B")

	err = td.BlockValidationClient.ValidateBlock(td.Ctx, block4B, nil)
	require.NoError(t, err, "Failed to validate block4B")

	// Wait for the reorg to complete
	td.WaitForBlockHeight(t, block4B, 10*time.Second, true)

	// Ensure chainB is winning with the best height at 4
	height, _, err = td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err, "Failed to get best height after reorg")
	require.Equal(t, uint32(4), height, "Expected blockchain to be at height 4 after reorg")

	// Verify that the best block at height 4 is block4B
	bestBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err, "Failed to get best block at height 4")
	require.Equal(t, block4B.Header.Hash().String(), bestBlock.Header.Hash().String(),
		"Best block at height 4 should be block4B")

	// Verify that block3B is at height 3 (chainB won)
	blockAt3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err, "Failed to get block at height 3 after reorg")
	require.Equal(t, block3B.Header.Hash().String(), blockAt3.Header.Hash().String(),
		"Block at height 3 should be block3B after reorg")

	t.Log("Reorg successful - chainB is the winning chain")

	// Give some time for block assembly to process the reorg
	time.Sleep(2 * time.Second)

	// Validate that transaction TXB is still in block assembly
	t.Log("Verifying TXB is still in block assembly after reorg...")
	td.VerifyInBlockAssembly(t, txB)

	// Validate that transaction TXA is NOT in block assembly (it was in block3A which got reorged)
	t.Log("Verifying TXA is NOT in block assembly after reorg...")
	td.VerifyInBlockAssembly(t, txA)

	t.Log("Test completed successfully - TXB remains in block assembly, TXA was removed due to reorg")
}
