//go:build test_docker_daemon || debug

package smoke

import (
	"encoding/hex"
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

	_, err = helper.CallRPC(legacySyncURL, "generate", []any{101})
	require.NoError(t, err, "Failed to generate blocks")

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(node1.Ctx, node1.BlockchainClient, 101, 30*time.Second)
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

func TestSendTxToLegacy(t *testing.T) {
	err := os.RemoveAll("../../data")
	require.NoError(t, err)

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../../docker-compose-host-withLegacy.yml",
	})
	require.NoError(t, err)

	node1 := tc.GetNodeClients(t, "docker.host.teranode1.legacy")

	// generate 10 blocks on svnode
	_, err = helper.CallRPC(legacySyncURL, "generatetoaddress", []any{100, "myL4TciLD59ESU9MmKH1rvfYb8QXhFHHN6"})
	require.NoError(t, err, "Failed to generate blocks")

	tc.StopNode(t, "teranode-1")
	tc.StartNode(t, "teranode-1")
	
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 100, 20*time.Second)
	require.NoError(t, err)

	tc.StopNode(t, "teranode-2")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		EnableP2P:        true,
		EnableValidator:  true,
		EnableLegacy:     true,
		SettingsContext:  "docker.host.teranode2",
	})

	t.Cleanup(func() {
		td.Stop(t)
	})

	// Wait for at least one peer with timeout
	timeout := time.After(30 * time.Second)
	
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for peers")
		case <-ticker.C:
			resp, err := td.CallRPC("getpeerinfo", []interface{}{})
			require.NoError(t, err)

			var p2pResp helper.P2PRPCResponse
			err = json.Unmarshal([]byte(resp), &p2pResp)
			require.NoError(t, err)

			t.Logf("peers: %s", resp)

			if len(p2pResp.Result) > 0 {
				t.Logf("Test succeeded, retrieved P2P peers information")
				goto CONTINUE
			}
			
			t.Logf("No peers yet, waiting...")
		}
	}
CONTINUE:

	time.Sleep(10 * time.Second)

	// verify blockheight on node1
	_, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 100)
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	coinbaseTx1 := block1.CoinbaseTx

	tx := td.CreateTransaction(t, coinbaseTx1)
	t.Logf("Sending New Transaction with RPC: %s\n", tx.TxIDChainHash())
	txBytes := hex.EncodeToString(tx.Bytes())

	// td.Stop()

	// tc.StartNode(t, "teranode-1")

	// time.Sleep(10 * time.Second)

	resp, err := helper.CallRPC(legacySyncURL, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err)
	t.Logf("Transaction sent with RPC: %s\n", resp)

	time.Sleep(10 * time.Second)

	td.VerifyInBlockAssembly(t, tx)
}
