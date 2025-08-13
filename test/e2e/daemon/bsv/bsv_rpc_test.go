package bsv

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/stretchr/testify/require"
)

// TestBSVRPC tests basic BSV RPC functionality in Teranode
//
// Converted from BSV bsv-rpc.py test
// Purpose: Test basic RPC calls and node information retrieval
//
// Test Cases:
// 1. GetInfo RPC - Verify node information is returned correctly
// 2. GetInfo Fields - Validate expected fields are present
// 3. GetInfo Consistency - Verify repeated calls return consistent data
// 4. SetBlockMaxSize Handling - Test graceful handling of unimplemented RPC
//
// Note: This test is adapted for Teranode's RPC implementation which differs
// from BSV in some areas (e.g., setblockmaxsize is not implemented)
func TestBSVRPC(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	t.Log("Testing BSV RPC functionality in Teranode...")

	// Setup single node (like BSV test)
	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})
	defer td.Stop(t)

	// Test cases
	t.Run("getinfo_basic", func(t *testing.T) {
		testGetInfoBasic(t, ctx, td)
	})

	t.Run("getinfo_fields", func(t *testing.T) {
		testGetInfoFields(t, ctx, td)
	})

	t.Run("getinfo_consistency", func(t *testing.T) {
		testGetInfoConsistency(t, ctx, td)
	})

	t.Run("setblockmaxsize_handling", func(t *testing.T) {
		testSetBlockMaxSizeHandling(t, ctx, td)
	})

	t.Log("✅ BSV RPC test completed successfully")
}

func testGetInfoBasic(t *testing.T, ctx context.Context, td *daemon.TestDaemon) {
	t.Log("Testing basic getinfo RPC call...")

	// Call getinfo RPC
	resp, err := td.CallRPC(td.Ctx, "getinfo", []any{})
	require.NoError(t, err, "Failed to call getinfo")

	// Parse response
	var getInfoResp struct {
		Result map[string]interface{} `json:"result"`
		Error  interface{}            `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &getInfoResp)
	require.NoError(t, err, "Failed to unmarshal getinfo response")

	// Verify basic structure
	require.Nil(t, getInfoResp.Error, "getinfo should not return error")
	require.NotNil(t, getInfoResp.Result, "getinfo should return result")
	require.NotEmpty(t, getInfoResp.Result, "getinfo result should not be empty")

	t.Logf("✅ getinfo returned %d fields", len(getInfoResp.Result))
}

func testGetInfoFields(t *testing.T, ctx context.Context, td *daemon.TestDaemon) {
	t.Log("Testing getinfo field validation...")

	// Call getinfo RPC
	resp, err := td.CallRPC(td.Ctx, "getinfo", []any{})
	require.NoError(t, err, "Failed to call getinfo")

	// Parse response
	var getInfoResp struct {
		Result map[string]interface{} `json:"result"`
		Error  interface{}            `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &getInfoResp)
	require.NoError(t, err, "Failed to unmarshal getinfo response")

	result := getInfoResp.Result

	// Check for expected Teranode fields (based on handlers.go analysis)
	expectedFields := []string{
		"version",         // Node version
		"protocolversion", // Protocol version
		"blocks",          // Block height
		"connections",     // Peer connections
		"difficulty",      // Network difficulty
		"testnet",         // Testnet flag
	}

	for _, field := range expectedFields {
		value, exists := result[field]
		require.True(t, exists, "Field '%s' should exist in getinfo response", field)
		require.NotNil(t, value, "Field '%s' should not be nil", field)
		t.Logf("✅ Field '%s': %v", field, value)
	}

	// Verify specific field types
	if blocks, ok := result["blocks"]; ok {
		// Should be a number (int or float)
		switch v := blocks.(type) {
		case float64, int, int32, int64:
			require.True(t, true, "blocks field is numeric: %v", v)
		default:
			t.Errorf("blocks field should be numeric, got %T: %v", v, v)
		}
	}

	if version, ok := result["version"]; ok {
		// Should be a number
		switch v := version.(type) {
		case float64, int, int32, int64:
			require.True(t, true, "version field is numeric: %v", v)
		default:
			t.Errorf("version field should be numeric, got %T: %v", v, v)
		}
	}

	t.Log("✅ All expected fields validated")
}

func testGetInfoConsistency(t *testing.T, ctx context.Context, td *daemon.TestDaemon) {
	t.Log("Testing getinfo consistency across multiple calls...")

	// Make multiple getinfo calls
	var responses []map[string]interface{}

	for i := 0; i < 3; i++ {
		resp, err := td.CallRPC(td.Ctx, "getinfo", []any{})
		require.NoError(t, err, "Failed to call getinfo (attempt %d)", i+1)

		var getInfoResp struct {
			Result map[string]interface{} `json:"result"`
			Error  interface{}            `json:"error"`
			ID     int                    `json:"id"`
		}
		err = json.Unmarshal([]byte(resp), &getInfoResp)
		require.NoError(t, err, "Failed to unmarshal getinfo response (attempt %d)", i+1)

		responses = append(responses, getInfoResp.Result)
	}

	// Verify consistent fields across calls
	// Some fields like 'blocks' might change, but structural fields should be consistent
	consistentFields := []string{"version", "protocolversion", "testnet"}

	for _, field := range consistentFields {
		if len(responses) > 1 {
			firstValue := responses[0][field]
			for i := 1; i < len(responses); i++ {
				currentValue := responses[i][field]
				require.Equal(t, firstValue, currentValue,
					"Field '%s' should be consistent across calls: %v != %v",
					field, firstValue, currentValue)
			}
			t.Logf("✅ Field '%s' consistent across calls: %v", field, firstValue)
		}
	}

	t.Log("✅ getinfo consistency validated")
}

func testSetBlockMaxSizeHandling(t *testing.T, ctx context.Context, td *daemon.TestDaemon) {
	t.Log("Testing setblockmaxsize RPC handling...")

	// Try to call setblockmaxsize (should fail gracefully since it's not implemented)
	resp, err := td.CallRPC(td.Ctx, "setblockmaxsize", []any{1000000})

	if err != nil {
		// Expected: method not found or similar error
		t.Logf("✅ setblockmaxsize correctly not implemented: %v", err)

		// Verify it's a "method not found" type error
		errStr := err.Error()
		require.Contains(t, errStr, "Method not found", "Error should mention method not found")

		return
	}

	// If no error, log the response but don't fail the test
	// (in case Teranode implements this in the future)
	t.Logf("⚠️  setblockmaxsize unexpectedly succeeded: %s", resp)
	t.Log("Note: setblockmaxsize may have been implemented in this Teranode version")
}

// Helper function to check if error is method not found
func isRPCMethodNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return containsAny(errStr, []string{"method not found", "not implemented", "unimplemented", "unknown method"})
}

// Helper function to check if string contains any of the given substrings
func containsAny(s string, substrings []string) bool {
	for _, substr := range substrings {
		if len(s) >= len(substr) {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}
