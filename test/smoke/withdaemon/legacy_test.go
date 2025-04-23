//go:build test_docker_daemon || debug

package smoke

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/test/testcontainers"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

var (
	legacyTestLock sync.Mutex
)

const legacySyncURL = "http://localhost:18332"

func TestInitialSync(t *testing.T) {
	legacyTestLock.Lock()
	defer legacyTestLock.Unlock()

	err := os.RemoveAll("../../data")
	require.NoError(t, err)

	// Wait for directory removal to complete
	err = helper.WaitForDirRemoval("../../data", 2*time.Second)
	require.NoError(t, err)

	// Ensure the required Docker mount paths exist
	requiredDirs := []string{
		"../../data/aerospike1/logs",
		"../../data/aerospike2/logs",
		"../../data/aerospike3/logs",
	}
	for _, dir := range requiredDirs {
		err = os.MkdirAll(dir, 0755)
		require.NoError(t, err)
	}

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../../docker-compose-host-withLegacy.yml",
	})
	require.NoError(t, err)

	defer func() {
		err := tc.Compose.Down(t.Context())
		require.NoError(t, err)

		// Wait for all node ports to be free before continuing
		err = helper.WaitForPortsToBeAvailable(t.Context(), []int{18000, 28000, 38000}, 10*time.Second)
		require.NoError(t, err)
	}()

	tc.StopNode(t, "teranode-1")

	_, err = helper.CallRPC(legacySyncURL, "generate", []any{101})
	require.NoError(t, err, "Failed to generate blocks")

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		EnableLegacy:    true,
		SettingsContext: "docker.host.teranode1.legacy",
	})

	defer node1.Stop(t)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(node1.Ctx, node1.BlockchainClient, 101, 10*time.Second)
	require.NoError(t, err)

	resp, err := node1.CallRPC("getpeerinfo", []any{})

	require.NoError(t, err)

	var p2pResp helper.P2PRPCResponse

	err = json.Unmarshal([]byte(resp), &p2pResp)
	require.NoError(t, err)

	t.Logf("%s", resp)

	// Find and ban peers with port 18333
	var peersTo18333 []string

	for _, peer := range p2pResp.Result {
		// Handle both address formats - "ip:port" and "/ip4/ip/tcp/port"
		if strings.Contains(peer.Addr, ":18333") {
			peersTo18333 = append(peersTo18333, peer.Addr)
		}
	}

	require.Equal(t, 1, len(peersTo18333))
}

// Test CatchUpWithLegacy
// Start all nodes in legacy mode
// Generate 101 blocks on svnode
// Verify blockheight on all nodes
// Restart teranode-1
// Note they will sync
// Generate 100 block on svnode
// Teranode-1 crashes
func TestCatchUpWithLegacy(t *testing.T) {
	legacyTestLock.Lock()
	defer legacyTestLock.Unlock()

	err := os.RemoveAll("../../data")
	require.NoError(t, err)

	// Wait for directory removal to complete
	err = helper.WaitForDirRemoval("../../data", 2*time.Second)
	require.NoError(t, err)

	// Ensure the required Docker mount paths exist
	requiredDirs := []string{
		"../../data/aerospike1/logs",
		"../../data/aerospike2/logs",
		"../../data/aerospike3/logs",
	}
	for _, dir := range requiredDirs {
		err = os.MkdirAll(dir, 0755)
		require.NoError(t, err)
	}

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../../docker-compose-host-withLegacy.yml",
	})
	require.NoError(t, err)

	defer func() {
		err := tc.Compose.Down(t.Context())
		require.NoError(t, err)

		// Wait for all node ports to be free before continuing
		err = helper.WaitForPortsToBeAvailable(t.Context(), []int{18000, 28000, 38000}, 10*time.Second)
		require.NoError(t, err)
	}()

	tc.StopNode(t, "teranode-1")

	_, err = helper.CallRPC(legacySyncURL, "generate", []any{101})
	require.NoError(t, err, "Failed to generate blocks")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		EnableLegacy:    true,
		SettingsContext: "docker.host.teranode1.legacy",
	})

	defer td.Stop(t)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(td.Ctx, td.BlockchainClient, 101, 10*time.Second)
	// _, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	const extraBlocks = 100
	// generate 100 blocks on svnode
	_, err = helper.CallRPC(legacySyncURL, "generate", []any{extraBlocks})
	require.NoError(t, err)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(td.Ctx, td.BlockchainClient, 101+extraBlocks, 10*time.Second)
	require.NoError(t, err)
}
