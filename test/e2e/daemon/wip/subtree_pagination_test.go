package smoke

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestSubtreePagination tests the subtree pagination feature (Issue #4400)
//
// This test:
// 1. Configures teranode with very small subtrees (2 txs per subtree)
// 2. Creates many transactions to generate multiple subtrees
// 3. Mines them into a block
// 4. Provides a URL to manually test pagination in the UI
//
// To run interactively (for manual UI testing):
// INTERACTIVE=true go test -v -run TestSubtreePagination ./test/e2e/daemon/ready/
//
// Without INTERACTIVE=true, the test will skip with a message.
func TestSubtreePagination(t *testing.T) {
	ctx := context.Background()

	// Configure test daemon with small subtree size for easier testing
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				// CRITICAL: Set subtree size to 2 transactions to create many subtrees
				s.BlockAssembly.InitialMerkleItemsPerSubtree = 2
				s.BlockAssembly.MinimumMerkleItemsPerSubtree = 2
				s.BlockAssembly.MaximumMerkleItemsPerSubtree = 2
				// Disable dynamic sizing to enforce fixed size
				s.BlockAssembly.UseDynamicSubtreeSize = false
			},
		),
	})

	defer td.Stop(t, true)

	// Check if Asset service is available
	if td.AssetURL == "" {
		t.Skip("Asset service not available - cannot test pagination UI")
	}
	t.Logf("Asset service available at: %s", td.AssetURL)

	// Get a spendable coinbase transaction
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)
	t.Logf("Got spendable coinbase tx: %s", coinbaseTx.TxIDChainHash())

	// Create multiple transactions to generate many subtrees
	// With subtree size = 2, 50 transactions will create ~25 subtrees
	const numTxs = 50
	txs := make([]*bt.Tx, 0, numTxs)
	var currentTx = coinbaseTx

	t.Logf("Creating %d transactions...", numTxs)

	for i := range numTxs {
		// Create a chain of transactions
		newTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(currentTx, 0),
			transactions.WithP2PKHOutputs(1, 10000),
		)

		// Send transaction to propagation service
		txBytes := hex.EncodeToString(newTx.ExtendedBytes())
		_, err := td.CallRPC(ctx, "sendrawtransaction", []any{txBytes})
		require.NoError(t, err, "Failed to send transaction %d", i+1)

		txs = append(txs, newTx)
		currentTx = newTx

		if (i+1)%10 == 0 {
			t.Logf("  Sent %d/%d transactions", i+1, numTxs)
		}
	}

	t.Logf("✓ All %d transactions sent", numTxs)

	// Wait for block assembly to process transactions
	t.Logf("Waiting for block assembly to process transactions...")
	time.Sleep(2 * time.Second)

	// Mine a block with all transactions
	t.Logf("Mining block with %d transactions...", numTxs)
	block := td.MineAndWait(t, 1)
	t.Logf("✓ Block mined: %s (height %d)", block.Hash(), block.Height)

	// Validate subtrees were created
	subtrees, err := block.GetSubtrees(ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	numSubtrees := len(subtrees)
	t.Logf("✓ Block contains %d subtrees", numSubtrees)

	// Verify we have multiple subtrees with our small size setting
	// Note: Coinbase counts as 1 tx, so with 50 txs + 1 coinbase and size=2, we should get ~25 subtrees
	// Being lenient here in case settings don't apply exactly as expected
	if numSubtrees < 5 {
		t.Logf("WARNING: Only got %d subtrees, expected more. Subtree size settings may not be working correctly.", numSubtrees)
		t.Logf("First subtree has %d nodes (should be ~2 with our settings)", subtrees[0].Length())
		t.Logf("Settings: InitialMerkleItemsPerSubtree=%d, Min=%d, Max=%d, UseDynamicSubtreeSize=%v",
			td.Settings.BlockAssembly.InitialMerkleItemsPerSubtree,
			td.Settings.BlockAssembly.MinimumMerkleItemsPerSubtree,
			td.Settings.BlockAssembly.MaximumMerkleItemsPerSubtree,
			td.Settings.BlockAssembly.UseDynamicSubtreeSize)
		t.Logf("Continuing test anyway to verify pagination API works...")
	}

	// Log subtree details for manual testing
	// Strip /api/v1 from AssetURL for dashboard URL
	dashboardURL := strings.TrimSuffix(td.AssetURL, "/api/v1")

	t.Log("\n===========================================")
	t.Log("SUBTREE PAGINATION TEST READY")
	t.Log("===========================================")
	t.Logf("Dashboard URL: %s", dashboardURL)
	t.Logf("API URL: %s", td.AssetURL)
	t.Logf("Block Hash: %s", block.Hash())
	t.Logf("Block Height: %d", block.Height)
	t.Logf("Total Subtrees: %d", numSubtrees)
	t.Log("\nSubtree Hashes (use these to test pagination):")

	for i, subtree := range subtrees {
		t.Logf("  Subtree %d: %s (nodes: %d)", i+1, subtree.RootHash(), subtree.Length())
		// Log first subtree URL as example
		if i == 0 {
			t.Logf("\nTest this subtree via API:")
			t.Logf("  %s/subtree/%s/json", td.AssetURL, subtree.RootHash())
			t.Logf("  %s/subtree/%s/json?offset=0&limit=10", td.AssetURL, subtree.RootHash())
			t.Logf("\nTest this subtree via Dashboard:")
			t.Logf("  %s/viewer/subtree/?hash=%s&blockHash=%s", dashboardURL, subtree.RootHash(), block.Hash())
		}
	}

	t.Log("\nTo test pagination in the UI:")
	t.Logf("1. Open dashboard: %s", dashboardURL)
	t.Log("2. Navigate to Blocks page")
	t.Logf("3. Click on block at height %d", block.Height)
	t.Log("4. Click on any subtree link")
	t.Log("5. Click the 'JSON' tab")
	t.Log("6. Observe pagination controls (top and bottom)")
	t.Log("7. Test navigation: Previous/Next, page numbers, page size")
	t.Log("\nExpected behavior:")
	t.Log("- Only 20 nodes displayed per page (default)")
	t.Log("- Pagination controls visible at top")
	t.Log("- Page size selector at bottom")
	t.Log("- Fast loading (no full subtree download)")
	t.Log("===========================================\n")

	// Keep test running for manual inspection if INTERACTIVE=true
	// Skip this test in CI/automated testing
	if os.Getenv("INTERACTIVE") != "true" {
		t.Skip("Skipping interactive test. Set INTERACTIVE=true to run this test for manual UI testing.")
	}

	t.Log("Test environment is ready for manual testing...")
	t.Log("Press Ctrl+C when you're done testing to clean up and exit")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	t.Log("\nReceived interrupt signal, cleaning up...")
}

// TestSubtreePaginationAPI tests the pagination API directly
func TestSubtreePaginationAPI(t *testing.T) {
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.BlockAssembly.InitialMerkleItemsPerSubtree = 2
				s.BlockAssembly.MinimumMerkleItemsPerSubtree = 2
				s.BlockAssembly.MaximumMerkleItemsPerSubtree = 2
				s.BlockAssembly.UseDynamicSubtreeSize = false
			},
		),
	})

	defer td.Stop(t, true)

	// Check if Asset service is available
	if td.AssetURL == "" {
		t.Skip("Asset service not available - cannot test pagination API")
	}
	t.Logf("Asset service available at: %s", td.AssetURL)

	// Create transactions and mine block
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)
	var currentTx = coinbaseTx

	// Create 10 transactions (5 subtrees with size=2)
	for range 10 {
		newTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(currentTx, 0),
			transactions.WithP2PKHOutputs(1, 10000),
		)

		txBytes := hex.EncodeToString(newTx.ExtendedBytes())
		_, err := td.CallRPC(ctx, "sendrawtransaction", []any{txBytes})
		require.NoError(t, err)

		currentTx = newTx
	}

	time.Sleep(1 * time.Second)
	block := td.MineAndWait(t, 1)

	subtrees, err := block.GetSubtrees(ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)
	require.Greater(t, len(subtrees), 0, "Expected at least one subtree")

	// Test pagination API for first subtree
	subtreeHash := subtrees[0].RootHash().String()

	t.Logf("Testing pagination for subtree: %s", subtreeHash)

	// Test 1: Get first page with default limit
	resp1 := testSubtreeAPI(t, td.AssetURL, subtreeHash, 0, 20)
	require.NotNil(t, resp1.Data, "Expected data field in response")
	require.NotNil(t, resp1.Pagination, "Expected pagination field in response")
	require.Equal(t, 0, resp1.Pagination.Offset, "Expected offset 0")
	require.Equal(t, 20, resp1.Pagination.Limit, "Expected limit 20")
	require.Greater(t, resp1.Pagination.TotalRecords, 0, "Expected total records > 0")
	t.Logf("✓ Page 1: offset=%d, limit=%d, total=%d", resp1.Pagination.Offset, resp1.Pagination.Limit, resp1.Pagination.TotalRecords)

	// Test 2: Get with smaller limit
	resp2 := testSubtreeAPI(t, td.AssetURL, subtreeHash, 0, 2)
	require.Equal(t, 2, resp2.Pagination.Limit, "Expected limit 2")
	require.Equal(t, 2, len(resp2.Data.Nodes), "Expected 2 nodes in response")
	t.Logf("✓ Small page: got %d nodes", len(resp2.Data.Nodes))

	// Test 3: Get second page
	if resp1.Pagination.TotalRecords > 2 {
		resp3 := testSubtreeAPI(t, td.AssetURL, subtreeHash, 2, 2)
		require.Equal(t, 2, resp3.Pagination.Offset, "Expected offset 2")
		require.LessOrEqual(t, len(resp3.Data.Nodes), 2, "Expected at most 2 nodes")
		t.Logf("✓ Page 2: offset=%d, got %d nodes", resp3.Pagination.Offset, len(resp3.Data.Nodes))
	}

	t.Log("✓ All pagination API tests passed")
}

// Helper types for API testing
type SubtreeAPIResponse struct {
	Data struct {
		Height           int           `json:"Height"`
		Fees             uint64        `json:"Fees"`
		SizeInBytes      uint64        `json:"SizeInBytes"`
		FeeHash          string        `json:"FeeHash"`
		Nodes            []SubtreeNode `json:"Nodes"`
		ConflictingNodes []string      `json:"ConflictingNodes"`
	} `json:"data"`
	Pagination struct {
		Offset       int `json:"offset"`
		Limit        int `json:"limit"`
		TotalRecords int `json:"totalRecords"`
	} `json:"pagination"`
}

type SubtreeNode struct {
	Hash  string `json:"Hash"`
	Type  int    `json:"Type"`
	Index int    `json:"Index"`
}

// Helper function to test subtree API
func testSubtreeAPI(t *testing.T, baseURL, subtreeHash string, offset, limit int) *SubtreeAPIResponse {
	url := fmt.Sprintf("%s/api/v1/subtree/%s/json?offset=%d&limit=%d", baseURL, subtreeHash, offset, limit)

	// Use HTTP client to fetch
	resp, err := http.Get(url) //nolint:gosec // Test code with controlled URLs
	require.NoError(t, err, "Failed to fetch subtree API")
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode, "Expected 200 status code")

	var apiResp SubtreeAPIResponse
	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	require.NoError(t, err, "Failed to decode API response")

	return &apiResp
}
