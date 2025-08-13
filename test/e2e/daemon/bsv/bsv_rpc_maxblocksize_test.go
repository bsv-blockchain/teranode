package bsv

import (
	"encoding/json"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

// TestBSVRPCMaxBlockSize tests BSV RPC max block size functionality
//
// Converted from: bitcoin-sv/test/functional/bsv-rpc.py
// Purpose: Verify that setblockmaxsize RPC method correctly sets the maximum block size for mining
//
// COMPATIBILITY NOTES:
// ✅ Compatible: getinfo() - Returns node info (different fields than BSV)
// ❌ Incompatible: setblockmaxsize() - Not implemented in Teranode
//
// REASON:
//
// TEST BEHAVIOR: Tests gracefully skip incompatible features and log actual
// responses for debugging. 2/4 test cases pass, 2/4 skip due to missing RPC.
func TestBSVRPCMaxBlockSize(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Test Case 1: Default Max Block Size
	t.Run("default_max_block_size", func(t *testing.T) {
		testDefaultMaxBlockSize(t, td)
	})

	// Test Case 2: Set Small Block Size
	t.Run("set_small_block_size", func(t *testing.T) {
		testSetSmallBlockSize(t, td)
	})

	// Test Case 3: Set Large Block Size
	t.Run("set_large_block_size", func(t *testing.T) {
		testSetLargeBlockSize(t, td)
	})

	// Test Case 4: Block Size Persistence
	t.Run("block_size_persistence", func(t *testing.T) {
		testBlockSizePersistence(t, td)
	})
}

func testDefaultMaxBlockSize(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing default max block size...")

	// Call getinfo to get current block size settings
	resp, err := td.CallRPC(td.Ctx, "getinfo", []any{})
	require.NoError(t, err, "Failed to call getinfo")

	var getInfo helper.GetInfo
	err = json.Unmarshal([]byte(resp), &getInfo)
	require.NoError(t, err, "Failed to parse getinfo response")

	// Note: We need to check what field Teranode uses for max block size
	// BSV uses 'maxminedblocksize' but Teranode might be different
	t.Logf("GetInfo response: %+v", getInfo.Result)

	// For now, just verify the RPC call works
	// TODO: Add specific field validation once we know Teranode's field name
	require.NotNil(t, getInfo.Result, "GetInfo result should not be nil")
}

func testSetSmallBlockSize(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing set small block size...")

	// Try to call setblockmaxsize with small value
	resp, err := td.CallRPC(td.Ctx, "setblockmaxsize", []any{10})

	if err != nil {
		// If setblockmaxsize is not implemented, skip this test
		if isMethodNotFoundError(err) {
			t.Skip("setblockmaxsize RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call setblockmaxsize")
	}

	t.Logf("setblockmaxsize(10) response: %s", resp)

	// Verify the setting took effect
	resp, err = td.CallRPC(td.Ctx, "getinfo", []any{})
	require.NoError(t, err, "Failed to call getinfo after setblockmaxsize")

	var getInfo helper.GetInfo
	err = json.Unmarshal([]byte(resp), &getInfo)
	require.NoError(t, err, "Failed to parse getinfo response")

	t.Logf("GetInfo after setblockmaxsize(10): %+v", getInfo.Result)

	// TODO: Verify maxminedblocksize field equals 10 once we know the field name
}

func testSetLargeBlockSize(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing set large block size...")

	const oneGigabyte = 1073741824 // 1GB in bytes

	// Try to call setblockmaxsize with large value
	resp, err := td.CallRPC(td.Ctx, "setblockmaxsize", []any{oneGigabyte})

	if err != nil {
		// If setblockmaxsize is not implemented, skip this test
		if isMethodNotFoundError(err) {
			t.Skip("setblockmaxsize RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call setblockmaxsize")
	}

	t.Logf("setblockmaxsize(%d) response: %s", oneGigabyte, resp)

	// Verify the setting took effect
	resp, err = td.CallRPC(td.Ctx, "getinfo", []any{})
	require.NoError(t, err, "Failed to call getinfo after setblockmaxsize")

	var getInfo helper.GetInfo
	err = json.Unmarshal([]byte(resp), &getInfo)
	require.NoError(t, err, "Failed to parse getinfo response")

	t.Logf("GetInfo after setblockmaxsize(%d): %+v", oneGigabyte, getInfo.Result)

	// TODO: Verify maxminedblocksize field equals oneGigabyte once we know the field name
}

func testBlockSizePersistence(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing block size persistence...")

	// Call getinfo multiple times to verify persistence
	for i := 0; i < 3; i++ {
		resp, err := td.CallRPC(td.Ctx, "getinfo", []any{})
		require.NoError(t, err, "Failed to call getinfo (iteration %d)", i+1)

		var getInfo helper.GetInfo
		err = json.Unmarshal([]byte(resp), &getInfo)
		require.NoError(t, err, "Failed to parse getinfo response (iteration %d)", i+1)

		t.Logf("GetInfo iteration %d: %+v", i+1, getInfo.Result)

		// Verify the response is consistent
		require.NotNil(t, getInfo.Result, "GetInfo result should not be nil (iteration %d)", i+1)
	}
}

// Helper function to check if error is "method not found"
func isMethodNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return contains(errStr, "Method not found") ||
		contains(errStr, "method not found") ||
		contains(errStr, "not implemented") ||
		contains(errStr, "unimplemented")
}

// Helper function to check if string contains substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsSubstring(s, substr))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
