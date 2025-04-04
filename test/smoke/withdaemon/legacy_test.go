//go:build test_docker_daemon || debug

package smoke

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/test/testcontainers"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

var (
// DEBUG DEBUG DEBUG
// blockWait = 30 * time.Second
)

const legacySyncURL = "http://localhost:18332"

func TestInitialSync(t *testing.T) {
	err := os.RemoveAll("../../data")
	require.NoError(t, err)

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../../docker-compose-host-withLegacy.yml",
	})
	require.NoError(t, err)

	tc.StopNode(t, "teranode-1")

	_, err = helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate blocks")
	time.Sleep(10 * time.Second)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		EnableP2P:        true,
		EnableValidator:  true,
		KillTeranode:     true,
		EnableLegacy:     true,
		SettingsOverride: settings.NewSettings("docker.host.teranode1.legacy"),
	})

	t.Cleanup(func() {
		td.Stop()
	})

	time.Sleep(10 * time.Second)

	// verify blockheight on node1
	_, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	resp, err := td.CallRPC("getpeerinfo", []interface{}{})

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
	err := os.RemoveAll("../../data")
	require.NoError(t, err)

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../../docker-compose-host-withLegacy.yml",
	})
	require.NoError(t, err)

	tc.StopNode(t, "teranode-1")

	_, err = helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate blocks")
	time.Sleep(10 * time.Second)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		EnableP2P:        true,
		EnableValidator:  true,
		KillTeranode:     true,
		EnableLegacy:     true,
		SettingsOverride: settings.NewSettings("docker.host.teranode1.legacy"),
	})

	t.Cleanup(func() {
		td.Stop()
	})

	time.Sleep(10 * time.Second)

	// verify blockheight on node1
	_, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// generate 100 blocks on svnode
	_, err = helper.CallRPC(legacySyncURL, "generate", []interface{}{100})
	require.NoError(t, err)

	// verify blockheight on node1
	_, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 201)
	require.NoError(t, err)
}
