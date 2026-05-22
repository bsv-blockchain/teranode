package smoke

import (
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
)

// TestForkNavigation creates a blockchain with multiple forks and starts the
// dashboard for interactive testing of the fork navigation feature.
//
// The test builds the following chain structure:
//
//	                        / 8a (fork)
//	0 -> 1 -> ... -> 7 -> 8 -> 9 -> ... -> 14 -> 15 -> ... -> 19 -> 20 -> 21 -> 22
//	                                         \                        \
//	                                          15a (fork)               20a -> 21a (fork branch)
//
// To run:
//
//	INTERACTIVE=true go test -v -run TestForkNavigation ./test/e2e/daemon/ready/
func TestForkNavigation(t *testing.T) {
	if os.Getenv("INTERACTIVE") != "true" {
		t.Skip("Skipping interactive test. Set INTERACTIVE=true to run this test for manual UI testing.")
	}

	ctx := t.Context()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
			},
		),
	})

	defer td.Stop(t, true)

	blockWait := 10 * time.Second

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Mine the full main chain first (to height 22) so forks never trigger a reorg
	block22 := td.MineAndWait(t, 22)
	require.Equal(t, uint32(22), block22.Height)
	t.Logf("Mined main chain to height 22")

	// Fetch parent blocks for the forks
	block7, err := td.BlockchainClient.GetBlockByHeight(ctx, 7)
	require.NoError(t, err)

	block14, err := td.BlockchainClient.GetBlockByHeight(ctx, 14)
	require.NoError(t, err)

	block19, err := td.BlockchainClient.GetBlockByHeight(ctx, 19)
	require.NoError(t, err)

	// === FORK 1: Create competing block at height 8 ===
	_, fork8a := td.CreateTestBlock(t, block7, 8888)
	require.NoError(t, td.BlockValidation.ValidateBlock(ctx, fork8a, "legacy", false))
	td.WaitForBlockBeingMined(t, fork8a)
	t.Logf("Fork 1: created block 8a (hash: %s) forking from height 7", fork8a.Hash())

	// === FORK 2: Create competing block at height 15 ===
	_, fork15a := td.CreateTestBlock(t, block14, 15888)
	require.NoError(t, td.BlockValidation.ValidateBlock(ctx, fork15a, "legacy", false))
	td.WaitForBlockBeingMined(t, fork15a)
	t.Logf("Fork 2: created block 15a (hash: %s) forking from height 14", fork15a.Hash())

	// === FORK 3: Create a 2-block fork branch at heights 20-21 ===
	_, fork20a := td.CreateTestBlock(t, block19, 20888)
	require.NoError(t, td.BlockValidation.ValidateBlock(ctx, fork20a, "legacy", false))
	td.WaitForBlockBeingMined(t, fork20a)
	t.Logf("Fork 3: created block 20a (hash: %s) forking from height 19", fork20a.Hash())

	_, fork21a := td.CreateTestBlock(t, fork20a, 21888)
	require.NoError(t, td.BlockValidation.ValidateBlock(ctx, fork21a, "legacy", false))
	td.WaitForBlockBeingMined(t, fork21a)
	t.Logf("Fork 3: extended fork with block 21a (hash: %s) at height 21", fork21a.Hash())

	// Wait for everything to settle
	td.WaitForBlockHeight(t, block22, blockWait)

	// Fetch block 8 for test URL
	block8, err := td.BlockchainClient.GetBlockByHeight(ctx, 8)
	require.NoError(t, err)

	dashboardURL := strings.TrimSuffix(td.AssetURL, "/api/v1")

	t.Log("\n===========================================")
	t.Log("FORK NAVIGATION TEST READY")
	t.Log("===========================================")
	t.Logf("Dashboard URL: %s", dashboardURL)
	t.Logf("API URL:       %s", td.AssetURL)
	t.Log("")
	t.Log("Chain structure:")
	t.Log("                        / 8a (fork)")
	t.Log("  0 -> 1 -> ... -> 7 -> 8 -> 9 -> ... -> 14 -> 15 -> ... -> 19 -> 20 -> 21 -> 22")
	t.Log("                                           \\                        \\")
	t.Log("                                            15a (fork)               20a -> 21a (fork)")
	t.Log("")
	t.Log("Fork heights: 8, 15, 20")
	t.Log("")
	t.Logf("Test URLs:")
	t.Logf("  Block 7 (parent of fork 1):  %s/viewer/block/?hash=%s", dashboardURL, block7.Hash())
	t.Logf("  Forks from block 7:          %s/forks/?hash=%s", dashboardURL, block7.Hash())
	t.Logf("  Block 8 (no fork here):      %s/forks/?hash=%s", dashboardURL, block8.Hash())
	t.Logf("  Block 14 (parent of fork 2): %s/forks/?hash=%s", dashboardURL, block14.Hash())
	t.Logf("  Block 19 (parent of fork 3): %s/forks/?hash=%s", dashboardURL, block19.Hash())
	t.Log("")
	t.Log("Testing instructions:")
	t.Logf("1. Open dashboard: %s", dashboardURL)
	t.Log("2. Navigate to Blocks page, click on any block")
	t.Log("3. Click the 'forks' link in block details")
	t.Log("4. Use the 'Prev fork' and 'Next fork' buttons to navigate between fork heights")
	t.Log("5. Try starting from a block with no fork (e.g., height 10) and navigate to nearest forks")
	t.Log("===========================================\n")

	t.Log("Press Ctrl+C when done testing...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	t.Log("\nReceived interrupt signal, cleaning up...")
}
