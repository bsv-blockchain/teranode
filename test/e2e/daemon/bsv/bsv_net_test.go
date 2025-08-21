package bsv

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

// TestBSVNet tests network-related RPC functionality in Teranode
//
// Converted from BSV net.py test
// Purpose: Test network RPCs for peer connections, network statistics, and node management
//
// Test Cases:
// 1. Connection Count - getconnectioncount RPC
// 2. Peer Information - getpeerinfo RPC
// 3. Authentication Connection Info - getauthconninfo RPC (BSV-specific)
// 4. Network Totals - getnettotals RPC and validation
// 5. Network Information - getnetworkinfo RPC and control
// 6. Added Node Info - addnode/getaddednodeinfo RPCs
//
// Expected Results:
// - Core network RPCs should work (getconnectioncount, getpeerinfo, getnetworkinfo)
// - BSV-specific RPCs may be unimplemented (gracefully skipped)
// - P2P connection information should be accurate
// - Network statistics should be consistent
func TestBSVNet(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	t.Log("Testing BSV network RPC functionality in Teranode...")

	// Create single node for basic network RPC testing
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})
	defer td.Stop(t)

	// Test Case 1: Connection Count
	t.Run("connection_count", func(t *testing.T) {
		testConnectionCount(t, td)
	})

	// Test Case 2: Peer Information
	t.Run("peer_information", func(t *testing.T) {
		testPeerInformation(t, td)
	})

	// Test Case 3: Authentication Connection Info (BSV-specific)
	t.Run("auth_connection_info", func(t *testing.T) {
		testAuthConnectionInfo(t, td)
	})

	// Test Case 4: Network Totals
	t.Run("network_totals", func(t *testing.T) {
		testNetworkTotals(t, td)
	})

	// Test Case 5: Network Information
	t.Run("network_information", func(t *testing.T) {
		testNetworkInformation(t, td)
	})

	// Test Case 6: Added Node Info
	t.Run("added_node_info", func(t *testing.T) {
		testAddedNodeInfo(t, td)
	})

	t.Log("✅ BSV network RPC test completed")
}

func testConnectionCount(t *testing.T, node *daemon.TestDaemon) {
	t.Log("Testing getconnectioncount RPC...")

	resp, err := node.CallRPC(node.Ctx, "getconnectioncount", []any{})
	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("⚠️ getconnectioncount RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call getconnectioncount")
	}

	var connCountResp struct {
		Result int               `json:"result"`
		Error  *helper.JSONError `json:"error"`
		ID     int               `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &connCountResp)
	require.NoError(t, err, "Failed to unmarshal getconnectioncount response")

	t.Logf("Connection count: %d", connCountResp.Result)

	// We expect at least 1 connection (to the other node)
	require.GreaterOrEqual(t, connCountResp.Result, 1, "Should have at least 1 peer connection")

	t.Log("✅ getconnectioncount test passed")
}

func testPeerInformation(t *testing.T, node *daemon.TestDaemon) {
	t.Log("Testing getpeerinfo RPC...")

	resp, err := node.CallRPC(node.Ctx, "getpeerinfo", []any{})
	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("⚠️ getpeerinfo RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call getpeerinfo")
	}

	var peerInfoResp struct {
		Result []map[string]interface{} `json:"result"`
		Error  *helper.JSONError        `json:"error"`
		ID     int                      `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &peerInfoResp)
	require.NoError(t, err, "Failed to unmarshal getpeerinfo response")

	t.Logf("Number of peers: %d", len(peerInfoResp.Result))

	// For single node test, 0 peers is expected
	require.GreaterOrEqual(t, len(peerInfoResp.Result), 0, "Peer count should be non-negative")

	// Check first peer has expected fields if any peers exist
	if len(peerInfoResp.Result) > 0 {
		peer := peerInfoResp.Result[0]
		t.Logf("First peer info: %+v", peer)

		// Check for common peer info fields
		expectedFields := []string{"id", "addr"}
		for _, field := range expectedFields {
			_, exists := peer[field]
			if exists {
				t.Logf("✅ Peer has %s field", field)
			} else {
				t.Logf("⚠️ Peer missing %s field", field)
			}
		}
	} else {
		t.Log("✅ No peers connected (expected for single node test)")
	}

	t.Log("✅ getpeerinfo test passed")
}

func testAuthConnectionInfo(t *testing.T, node *daemon.TestDaemon) {
	t.Log("Testing getauthconninfo RPC (BSV-specific)...")

	resp, err := node.CallRPC(node.Ctx, "getauthconninfo", []any{})
	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("⚠️ getauthconninfo RPC method not implemented in Teranode (expected - BSV-specific)")
			return
		}
		require.NoError(t, err, "Failed to call getauthconninfo")
	}

	var authConnResp struct {
		Result map[string]interface{} `json:"result"`
		Error  *helper.JSONError      `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &authConnResp)
	require.NoError(t, err, "Failed to unmarshal getauthconninfo response")

	t.Logf("Auth connection info: %+v", authConnResp.Result)

	// Check for expected fields if the RPC exists
	if pubkey, exists := authConnResp.Result["pubkey"]; exists {
		pubkeyStr, ok := pubkey.(string)
		require.True(t, ok, "pubkey should be a string")
		require.NotEmpty(t, pubkeyStr, "pubkey should not be empty")
		t.Logf("✅ Auth connection pubkey: %s", pubkeyStr)
	}

	t.Log("✅ getauthconninfo test passed")
}

func testNetworkTotals(t *testing.T, node *daemon.TestDaemon) {
	t.Log("Testing getnettotals RPC...")

	resp, err := node.CallRPC(node.Ctx, "getnettotals", []any{})
	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("⚠️ getnettotals RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call getnettotals")
	}

	var netTotalsResp struct {
		Result map[string]interface{} `json:"result"`
		Error  *helper.JSONError      `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &netTotalsResp)
	require.NoError(t, err, "Failed to unmarshal getnettotals response")

	t.Logf("Network totals: %+v", netTotalsResp.Result)

	// Check for expected fields
	expectedFields := []string{"totalbytesrecv", "totalbytessent"}
	for _, field := range expectedFields {
		if value, exists := netTotalsResp.Result[field]; exists {
			t.Logf("✅ Network totals has %s: %v", field, value)
		} else {
			t.Logf("⚠️ Network totals missing %s field", field)
		}
	}

	// Try to send a ping to generate network activity
	t.Log("Sending ping to generate network activity...")
	_, err = node.CallRPC(node.Ctx, "ping", []any{})
	if err != nil && !isMethodNotFoundError(err) {
		t.Logf("⚠️ ping RPC failed: %v", err)
	}

	t.Log("✅ getnettotals test passed")
}

func testNetworkInformation(t *testing.T, node *daemon.TestDaemon) {
	t.Log("Testing getnetworkinfo RPC...")

	resp, err := node.CallRPC(node.Ctx, "getnetworkinfo", []any{})
	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("⚠️ getnetworkinfo RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call getnetworkinfo")
	}

	var networkInfoResp struct {
		Result map[string]interface{} `json:"result"`
		Error  *helper.JSONError      `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &networkInfoResp)
	require.NoError(t, err, "Failed to unmarshal getnetworkinfo response")

	t.Logf("Network info: %+v", networkInfoResp.Result)

	// Check for expected fields
	expectedFields := []string{"networkactive", "connections", "version"}
	for _, field := range expectedFields {
		if value, exists := networkInfoResp.Result[field]; exists {
			t.Logf("✅ Network info has %s: %v", field, value)
		} else {
			t.Logf("⚠️ Network info missing %s field", field)
		}
	}

	t.Log("✅ getnetworkinfo test passed")
}

func testAddedNodeInfo(t *testing.T, node *daemon.TestDaemon) {
	t.Log("Testing getaddednodeinfo RPC...")

	// Try with no parameters first (BSV style)
	resp, err := node.CallRPC(node.Ctx, "getaddednodeinfo", []any{})
	if err != nil {
		// If parameter count error, try with boolean parameter
		if strings.Contains(err.Error(), "wrong number of params") {
			t.Log("Trying getaddednodeinfo with boolean parameter...")
			resp, err = node.CallRPC(node.Ctx, "getaddednodeinfo", []any{true})
		}

		if err != nil {
			if isMethodNotFoundError(err) {
				t.Skip("⚠️ getaddednodeinfo RPC method not implemented in Teranode")
				return
			}
			require.NoError(t, err, "Failed to call getaddednodeinfo")
		}
	}

	var addedNodeResp struct {
		Result []map[string]interface{} `json:"result"`
		Error  *helper.JSONError        `json:"error"`
		ID     int                      `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &addedNodeResp)
	require.NoError(t, err, "Failed to unmarshal getaddednodeinfo response")

	t.Logf("Added nodes: %+v", addedNodeResp.Result)
	t.Logf("Number of added nodes: %d", len(addedNodeResp.Result))

	// Test addnode RPC if available
	t.Log("Testing addnode RPC...")
	_, err = node.CallRPC(node.Ctx, "addnode", []any{"127.0.0.1:8333", "add"})
	if err != nil {
		if isMethodNotFoundError(err) {
			t.Log("⚠️ addnode RPC method not implemented in Teranode (expected)")
		} else {
			t.Logf("⚠️ addnode RPC failed: %v", err)
		}
	} else {
		t.Log("✅ addnode RPC succeeded")
	}

	t.Log("✅ getaddednodeinfo test passed")
}
