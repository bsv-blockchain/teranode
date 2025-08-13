package bsv

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

// TestUptime tests node uptime RPC functionality
//
// Converted from: bitcoin-sv/test/functional/uptime.py
// Purpose: Verify that the uptime RPC method returns the correct node uptime in seconds
//
// COMPATIBILITY NOTES:
// ✅ Compatible: uptime() - Returns node uptime (expected to work)
// ❌ Incompatible: setmocktime() - May not be implemented in Teranode
//
// REASON: Mock time functionality is primarily for BSV testing and may not
// exist in Teranode's architecture. Basic uptime reporting should be universal.
//
// TEST BEHAVIOR: Tests basic uptime functionality and gracefully skips mock time
// features if not available. 1/2 test cases expected to pass.
func TestUptime(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Test Case 1: Basic Uptime Functionality
	t.Run("basic_uptime", func(t *testing.T) {
		testBasicUptime(t, td)
	})

	// Test Case 2: Mock Time Functionality (may not be available)
	t.Run("mock_time_uptime", func(t *testing.T) {
		testMockTimeUptime(t, td)
	})
}

func testBasicUptime(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing basic uptime functionality...")

	// First uptime call
	resp, err := td.CallRPC(td.Ctx, "uptime", []any{})

	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("uptime RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call uptime")
	}

	var uptime1 helper.UptimeResponse
	err = json.Unmarshal([]byte(resp), &uptime1)
	require.NoError(t, err, "Failed to parse uptime response")

	// Validate first uptime reading
	require.GreaterOrEqual(t, uptime1.Result, int64(0), "Uptime should be non-negative")
	require.Less(t, uptime1.Result, int64(86400), "Uptime should be reasonable (less than 1 day for test)")

	t.Logf("First uptime reading: %d seconds", uptime1.Result)

	// Wait a short period
	time.Sleep(2 * time.Second)

	// Second uptime call
	resp, err = td.CallRPC(td.Ctx, "uptime", []any{})
	require.NoError(t, err, "Failed to call uptime second time")

	var uptime2 helper.UptimeResponse
	err = json.Unmarshal([]byte(resp), &uptime2)
	require.NoError(t, err, "Failed to parse second uptime response")

	// Validate second uptime reading
	require.GreaterOrEqual(t, uptime2.Result, uptime1.Result, "Second uptime should be >= first uptime")
	require.GreaterOrEqual(t, uptime2.Result-uptime1.Result, int64(1), "Uptime should have increased by at least 1 second")

	t.Logf("Second uptime reading: %d seconds (increased by %d)", uptime2.Result, uptime2.Result-uptime1.Result)
}

func testMockTimeUptime(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing mock time uptime functionality...")

	// Try to set mock time (may not be available in Teranode)
	currentTime := time.Now().Unix()
	mockTime := currentTime + 10 // 10 seconds in the future

	resp, err := td.CallRPC(td.Ctx, "setmocktime", []any{mockTime})

	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("setmocktime RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call setmocktime")
	}

	t.Logf("setmocktime response: %s", resp)

	// Now check uptime with mock time
	resp, err = td.CallRPC(td.Ctx, "uptime", []any{})
	require.NoError(t, err, "Failed to call uptime after setmocktime")

	var uptime helper.UptimeResponse
	err = json.Unmarshal([]byte(resp), &uptime)
	require.NoError(t, err, "Failed to parse uptime response after setmocktime")

	// With mock time, uptime should be at least 10 seconds
	require.GreaterOrEqual(t, uptime.Result, int64(10), "Uptime should be at least 10 seconds with mock time")

	t.Logf("Uptime with mock time: %d seconds", uptime.Result)
}
