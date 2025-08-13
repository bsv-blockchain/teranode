package bsv

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

// TestBSVGetBlockMultiNode tests getblock RPC across multiple connected nodes
//
// Converted from BSV bsv-getblock-rest-rpc.py test
// Purpose: Test getblock RPC consistency across multiple P2P-connected Teranode nodes
//
// APPROACH: Use existing Teranode multi-node patterns instead of creating new framework
// PATTERN: Multiple daemon.NewTestDaemon() + ConnectToPeer() + RPC consistency testing
//
// TEST BEHAVIOR:
// 1. Create 3 connected Teranode nodes
// 2. Generate blocks on one node
// 3. Wait for P2P propagation
// 4. Test getblock RPC consistency across all nodes
// 5. Test different verbosity levels
//
// This demonstrates the "ComparisonTestFramework" pattern using pure Teranode components!
func TestBSVGetBlockMultiNode(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	t.Log("Testing BSV getblock RPC across multiple connected Teranode nodes...")

	// Create 3 nodes with P2P enabled (like existing Teranode tests)
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		EnableP2P:       true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Validator.UseLocalValidator = true
		},
	})
	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		EnableP2P:       true,
		SettingsContext: "docker.host.teranode2.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 28090
			settings.Validator.UseLocalValidator = true
		},
	})
	defer node2.Stop(t)

	node3 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		EnableP2P:       true,
		SettingsContext: "docker.host.teranode3.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 38090
			settings.Validator.UseLocalValidator = true
		},
	})
	defer node3.Stop(t)

	// Connect nodes in ring topology (like existing Teranode tests)
	t.Log("Connecting nodes in P2P network...")
	node1.ConnectToPeer(t, node2)
	node2.ConnectToPeer(t, node3)
	node3.ConnectToPeer(t, node1)

	// Verify P2P connections
	peers1, err := node1.CallRPC(node1.Ctx, "getpeerinfo", nil)
	require.NoError(t, err, "Failed to get peer info from node1")
	t.Logf("Node1 peers: %v", peers1)

	peers2, err := node2.CallRPC(node2.Ctx, "getpeerinfo", nil)
	require.NoError(t, err, "Failed to get peer info from node2")
	t.Logf("Node2 peers: %v", peers2)

	peers3, err := node3.CallRPC(node3.Ctx, "getpeerinfo", nil)
	require.NoError(t, err, "Failed to get peer info from node3")
	t.Logf("Node3 peers: %v", peers3)

	// Generate blocks on node1 (like existing Teranode tests)
	t.Log("Generating blocks on node1...")
	blocksToGenerate := uint32(10)
	_, err = node1.CallRPC(node1.Ctx, "generate", []any{blocksToGenerate})
	require.NoError(t, err, "Failed to generate blocks on node1")

	// Wait for P2P propagation using proper Teranode helper (like existing tests)
	t.Log("Waiting for P2P block propagation...")
	ctx := context.Background()
	blockWaitTime := 30 * time.Second

	err = helper.WaitForNodeBlockHeight(ctx, node1.BlockchainClient, blocksToGenerate, blockWaitTime)
	require.NoError(t, err, "Node1 failed to reach expected height")

	err = helper.WaitForNodeBlockHeight(ctx, node2.BlockchainClient, blocksToGenerate, blockWaitTime)
	require.NoError(t, err, "Node2 failed to reach expected height")

	err = helper.WaitForNodeBlockHeight(ctx, node3.BlockchainClient, blocksToGenerate, blockWaitTime)
	require.NoError(t, err, "Node3 failed to reach expected height")

	// Get a specific block hash for testing
	resp, err := node1.CallRPC(node1.Ctx, "getblockhash", []any{5})
	require.NoError(t, err, "Failed to get block hash")

	var blockHashResp helper.GetBlockHashResponse
	err = json.Unmarshal([]byte(resp), &blockHashResp)
	require.NoError(t, err, "Failed to parse block hash response")

	testBlockHash := blockHashResp.Result
	t.Logf("Testing with block hash: %s", testBlockHash)

	// Test Case 1: getblock verbosity 0 (hex) consistency across nodes
	t.Run("getblock_hex_consistency", func(t *testing.T) {
		testGetBlockHexConsistency(t, []*daemon.TestDaemon{node1, node2, node3}, testBlockHash)
	})

	// Test Case 2: getblock verbosity 1 (JSON) consistency across nodes
	t.Run("getblock_json_consistency", func(t *testing.T) {
		testGetBlockJSONConsistency(t, []*daemon.TestDaemon{node1, node2, node3}, testBlockHash)
	})

	// Test Case 3: Block propagation validation
	t.Run("block_propagation_validation", func(t *testing.T) {
		testBlockPropagationValidation(t, []*daemon.TestDaemon{node1, node2, node3})
	})

	t.Log("BSV getblock multi-node test completed successfully")
}

// testGetBlockHexConsistency tests that getblock hex format is consistent across nodes
func testGetBlockHexConsistency(t *testing.T, nodes []*daemon.TestDaemon, blockHash string) {
	t.Log("Testing getblock hex consistency across nodes...")

	responses := make([]string, 0, 3)

	// Call getblock on all nodes
	for i, node := range nodes {
		resp, err := node.CallRPC(node.Ctx, "getblock", []any{blockHash, 0})
		require.NoError(t, err, "Failed to call getblock on node %d", i+1)

		var hexResp helper.GetBlockHexResponse
		err = json.Unmarshal([]byte(resp), &hexResp)
		require.NoError(t, err, "Failed to parse getblock hex response from node %d", i+1)

		responses = append(responses, hexResp.Result)
		t.Logf("Node %d hex length: %d characters", i+1, len(hexResp.Result))
	}

	// Verify all responses are identical (BSV ComparisonTestFramework behavior)
	baseResponse := responses[0]
	for i := 1; i < len(responses); i++ {
		require.Equal(t, baseResponse, responses[i],
			"getblock hex mismatch: node 1 vs node %d", i+1)
	}

	t.Log("✅ All nodes returned identical hex for getblock")
}

// testGetBlockJSONConsistency tests that getblock JSON format is consistent across nodes
func testGetBlockJSONConsistency(t *testing.T, nodes []*daemon.TestDaemon, blockHash string) {
	t.Log("Testing getblock JSON consistency across nodes...")

	responses := make([]map[string]interface{}, 0, 3)

	// Call getblock with verbosity 1 on all nodes
	for i, node := range nodes {
		resp, err := node.CallRPC(node.Ctx, "getblock", []any{blockHash, 1})
		require.NoError(t, err, "Failed to call getblock JSON on node %d", i+1)

		var jsonResp map[string]interface{}
		err = json.Unmarshal([]byte(resp), &jsonResp)
		require.NoError(t, err, "Failed to parse getblock JSON response from node %d", i+1)

		responses = append(responses, jsonResp)

		// Validate basic structure
		result, ok := jsonResp["result"].(map[string]interface{})
		require.True(t, ok, "Node %d should have result object", i+1)

		hash, ok := result["hash"].(string)
		require.True(t, ok, "Node %d should have hash field", i+1)
		require.Equal(t, blockHash, hash, "Node %d hash should match", i+1)

		t.Logf("Node %d JSON response validated", i+1)
	}

	// Compare key fields across nodes (like BSV ComparisonTestFramework)
	baseResult := responses[0]["result"].(map[string]interface{})
	for i := 1; i < len(responses); i++ {
		nodeResult := responses[i]["result"].(map[string]interface{})

		// Compare critical fields
		require.Equal(t, baseResult["hash"], nodeResult["hash"],
			"Hash mismatch: node 1 vs node %d", i+1)
		require.Equal(t, baseResult["height"], nodeResult["height"],
			"Height mismatch: node 1 vs node %d", i+1)
		require.Equal(t, baseResult["time"], nodeResult["time"],
			"Time mismatch: node 1 vs node %d", i+1)
	}

	t.Log("✅ All nodes returned consistent JSON for getblock")
}

// testBlockPropagationValidation tests that new blocks propagate correctly
func testBlockPropagationValidation(t *testing.T, nodes []*daemon.TestDaemon) {
	t.Log("Testing block propagation across P2P network...")

	// Generate a new block on node 2 (different from initial generation)
	t.Log("Generating new block on node 2...")
	_, err := nodes[1].CallRPC(nodes[1].Ctx, "generate", []any{1})
	require.NoError(t, err, "Failed to generate block on node 2")

	// Wait for propagation
	time.Sleep(3 * time.Second)

	// Get the new block hash from node 2
	resp, err := nodes[1].CallRPC(nodes[1].Ctx, "getbestblockhash", nil)
	require.NoError(t, err, "Failed to get best block hash from node 2")

	var bestHashResp helper.GetBestBlockHashResponse
	err = json.Unmarshal([]byte(resp), &bestHashResp)
	require.NoError(t, err, "Failed to parse best block hash response")

	newBlockHash := bestHashResp.Result
	t.Logf("New block hash: %s", newBlockHash)

	// Verify all nodes have the new block
	for i, node := range nodes {
		resp, err := node.CallRPC(node.Ctx, "getblock", []any{newBlockHash, 1})
		require.NoError(t, err, "Node %d should have the new block", i+1)

		var blockResp map[string]interface{}
		err = json.Unmarshal([]byte(resp), &blockResp)
		require.NoError(t, err, "Failed to parse block response from node %d", i+1)

		result := blockResp["result"].(map[string]interface{})
		hash := result["hash"].(string)
		require.Equal(t, newBlockHash, hash, "Node %d should have correct block hash", i+1)

		t.Logf("✅ Node %d has the new block", i+1)
	}

	t.Log("✅ Block propagation successful across all nodes")
}
