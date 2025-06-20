package smoke

import (
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlockSubsidy verifies that block subsidy is correctly included in the coinbase transaction
// TNJ4-4
func TestBlockSubsidy(t *testing.T) {
	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	td.Logger.Infof("Generating blocks...")
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate block")

	// Get initial coinbase transaction to create new transactions
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Send multiple transactions to include in next block (to generate fees)
	_, _, err = td.CreateAndSendTxs(t, block1.CoinbaseTx, 32)
	require.NoError(t, err, "Failed to send transactions")

	// Get mining candidate
	mc0, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx)
	require.NoError(t, err, "Error getting mining candidate")

	// Calculate expected block subsidy for current height
	expectedSubsidy := util.GetBlockSubsidyForHeight(mc0.Height, td.Settings.ChainCfgParams)

	// Verify mining candidate includes at least the block subsidy
	assert.Greater(t, mc0.CoinbaseValue, expectedSubsidy,
		"Coinbase value should be at least the block subsidy")

	// Generate a block
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to generate block")

	// Get the generated block
	_, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err, "Error getting best block header")

	block, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, meta.Height)
	require.NoError(t, err, "Error getting block")

	// Verify coinbase transaction outputs match mining candidate value
	coinbaseTx := block.CoinbaseTx
	amount := coinbaseTx.TotalOutputSatoshis()
	assert.Equal(t, mc0.CoinbaseValue, amount,
		"Coinbase transaction output should match mining candidate value")
}
