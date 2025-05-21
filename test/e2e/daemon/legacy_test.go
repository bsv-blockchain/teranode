package smoke

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/test/testcontainers"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

var (
	legacyTestLock sync.Mutex
)

const (
	// RPC
	svNodeRPCPort    = 18332
	svNodeRPCPortStr = "18332"
	svNodeRPCHost    = "localhost:" + svNodeRPCPortStr
	svNodeRPCURL     = "http://" + svNodeRPCHost

	// SVNode P2P
	svNodePortStr = "18333"
	svNodeHost    = "localhost:" + svNodePortStr
)

func TestInitialSync(t *testing.T) {
	legacyTestLock.Lock()
	defer legacyTestLock.Unlock()

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		Path:        "../..",
		ComposeFile: "docker-compose-host.yml",
		Profiles:    []string{"svnode1"},
		// HealthServicePorts: []testcontainers.ServicePort{
		// 	{ServiceName: "teranode1", Port: 18000},
		// 	{ServiceName: "teranode2", Port: 28000},
		// 	{ServiceName: "teranode3", Port: 38000},
		// },
		ServicesToWaitFor: []testcontainers.ServicePort{
			{ServiceName: "svnode1", Port: svNodeRPCPort},
		},
	})
	require.NoError(t, err)

	defer tc.Cleanup(t)

	_, err = helper.CallRPC(svNodeRPCURL, "generate", []any{101})
	require.NoError(t, err, "Failed to generate blocks")

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Legacy.ConnectPeers = []string{svNodeHost}
			settings.P2P.StaticPeers = []string{}
		},
	})

	defer node1.Stop(t)

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
		if strings.Contains(peer.Addr, ":"+svNodePortStr) {
			peersTo18333 = append(peersTo18333, peer.Addr)
		}
	}

	require.Equal(t, 1, len(peersTo18333), "Expected 1 peer with port "+svNodePortStr)
}

// Test CatchUpWithLegacy
// Start all nodes in legacy mode
// Generate 101 blocks on svnode
// Verify blockheight on all nodes
// Restart teranode1
// Note they will sync
// Generate 100 block on svnode
// Teranode1 crashes
func TestCatchUpWithLegacy(t *testing.T) {
	legacyTestLock.Lock()
	defer legacyTestLock.Unlock()

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		Path:        "../..",
		ComposeFile: "docker-compose-host.yml",
		Profiles:    []string{"svnode1"},
		ServicesToWaitFor: []testcontainers.ServicePort{
			{ServiceName: "svnode1", Port: svNodeRPCPort},
		},
	})
	require.NoError(t, err)

	defer tc.Cleanup(t)

	_, err = helper.CallRPC(svNodeRPCURL, "generate", []any{101})
	require.NoError(t, err, "Failed to generate blocks")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Legacy.ConnectPeers = []string{svNodeHost}
			settings.P2P.StaticPeers = []string{}
		},
	})

	defer td.Stop(t)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(td.Ctx, td.BlockchainClient, 101, 30*time.Second)
	// _, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	const extraBlocks = 100
	// generate 100 blocks on svnode
	_, err = helper.CallRPC(svNodeRPCURL, "generate", []any{extraBlocks})
	require.NoError(t, err)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(td.Ctx, td.BlockchainClient, 101+extraBlocks, 30*time.Second)
	require.NoError(t, err)
}

func TestSendTxToLegacy(t *testing.T) {
	legacyTestLock.Lock()
	defer legacyTestLock.Unlock()

	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		Path:        "../..",
		ComposeFile: "docker-compose-host.yml",
		Profiles:    []string{"svnode1"},
		ServicesToWaitFor: []testcontainers.ServicePort{
			{ServiceName: "svnode1", Port: svNodeRPCPort},
		},
	})
	require.NoError(t, err)

	defer tc.Cleanup(t)

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Legacy.ConnectPeers = []string{svNodeHost}
			settings.P2P.StaticPeers = []string{}
		},
	})

	defer node1.Stop(t)

	_, err = node1.CallRPC("generate", []any{101})
	require.NoError(t, err)

	// Wait for at least one peer with timeout
	timeout := time.After(30 * time.Second)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for peers")
		case <-ticker.C:
			resp, err := node1.CallRPC("getpeerinfo", []interface{}{})
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

	// time.Sleep(10 * time.Second)

	// verify blockheight on node1
	_, err = node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 101)
	require.NoError(t, err)

	block1, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 1)
	require.NoError(t, err)

	coinbaseTx1 := block1.CoinbaseTx

	tx := node1.CreateTransaction(t, coinbaseTx1)
	t.Logf("Sending New Transaction with RPC: %s\n", tx.TxIDChainHash())
	txBytes := hex.EncodeToString(tx.Bytes())

	// td.Stop()

	// tc.StartNode(t, "teranode1")

	// time.Sleep(10 * time.Second)

	resp, err := helper.CallRPC(svNodeRPCURL, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err)
	t.Logf("Transaction sent with RPC: %s\n", resp)

	time.Sleep(10 * time.Second)

	node1.VerifyInBlockAssembly(t, tx)
}
