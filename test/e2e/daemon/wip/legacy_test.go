package smoke

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/testcontainers"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/tracing"
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
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Legacy.ConnectPeers = []string{svNodeHost}
			settings.P2P.StaticPeers = []string{}
		},
	})

	tracer := tracing.Tracer("testDaemon")

	ctx, _, endSpan := tracer.Start(
		context.Background(),
		"TestInitialSync",
		tracing.WithTag("node", "node1"),
	)

	defer func() {
		endSpan()
		node1.Stop(t)
	}()

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(ctx, node1.BlockchainClient, 101, 30*time.Second)
	require.NoError(t, err)

	resp, err := node1.CallRPC(ctx, "getpeerinfo", []any{})

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

func TestSVNodeCatchUpFromLegacy(t *testing.T) {
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

	// Important note: svnode won't accept blocks from a pruned node (teranode)
	// until it has at least 1 block on top of genesis
	_, err = helper.CallRPC(svNodeRPCURL, "generate", []any{1})
	require.NoError(t, err, "Failed to generate blocks")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Legacy.ConnectPeers = []string{svNodeHost}
			settings.P2P.StaticPeers = []string{}
		},
	})

	defer td.Stop(t)

	const (
		startingHeight = 1
		targetHeight   = 4
	)

	// we should have 1 block already
	// check blockheight on teranode
	err = helper.WaitForNodeBlockHeight(td.Ctx, td.BlockchainClient, startingHeight, 30*time.Second)
	require.NoError(t, err)

	// generate blocks with a slight delay between each block - sv-node complains otherwise
	GenerateBlocksAndSyncWithSVNode(t, td.Ctx, td, svNodeRPCURL, startingHeight, targetHeight, td.Logger)
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

	// need at least 1 block in sv-node before it will accept stuff from a pruned node (teranode)
	_, err = helper.CallRPC(svNodeRPCURL, "generate", []any{1})
	require.NoError(t, err, "Failed to generate blocks")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Legacy.ConnectPeers = []string{svNodeHost}
			settings.P2P.StaticPeers = []string{}
		},
	})

	defer td.Stop(t)

	// wait for teranode to catch up
	err = helper.WaitForNodeBlockHeight(td.Ctx, td.BlockchainClient, 1, 30*time.Second)
	require.NoError(t, err)

	// generate block 2 from teranode and wait for sv-node to catchup
	GenerateBlocksAndSyncWithSVNode(t, td.Ctx, td, svNodeRPCURL, 1, 2, td.Logger)

	// get to 102 blocks total, making coinbase tx in block 2 (generated by teranode) available to spend
	_, err = helper.CallRPC(svNodeRPCURL, "generate", []any{99})
	require.NoError(t, err, "Failed to generate blocks")

	// get block 2 from teranode
	block, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	coinbaseTx := block.CoinbaseTx

	// I get and error: unexpected error: currently only p2pkh supported
	tx := td.CreateTransaction(t, coinbaseTx)
	t.Logf("Sending New Transaction with RPC: %s\n", tx.TxIDChainHash())
	txBytes := hex.EncodeToString(tx.Bytes())

	// TODO
	// Currently get an error
	// PROCESSING (4): RPC returned error -> UNKNOWN (0): code: -26, message: 16: mandatory-script-verify-flag-failed (Script failed an OP_EQUALVERIFY operation)
	// 	            	- utils.callRPC() /Users/freemans/github/bsv-blockchain/teranode/test/utils/helper.go:133 [4] RPC returned error
	// 	            	- UNKNOWN (0): code: -26, message: 16: mandatory-script-verify-flag-failed (Script failed an OP_EQUALVERIFY operation)
	resp, err := helper.CallRPC(svNodeRPCURL, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err)
	t.Logf("Transaction sent with RPC: %s\n", resp)

	time.Sleep(1 * time.Second)

	td.VerifyInBlockAssembly(t, tx)
}

func GenerateBlocksAndSyncWithSVNode(t *testing.T, ctx context.Context, td *daemon.TestDaemon, svNodeRPCURL string, heightStart int, heightStop int, logger ulogger.Logger) {
	// Note: SV-NODE - if you generate a number of blocks in one go, svnode will not accept them
	// complaining that the block timestamps are too old
	// Maybe this is a docker time syncing issue?
	for i := heightStart + 1; i <= heightStop; i++ {
		// sv-node [msghand] ERROR: AcceptBlockHeader: Consensus::ContextualCheckBlockHeader: 209d29983b4fd300551689f465275a736745545d68be9850adeecfa25296456b, time-too-old, block's timestamp is too early (code 16)
		// sv node complains if blocks are generated too fast
		time.Sleep(500 * time.Millisecond)

		_, err := td.CallRPC(td.Ctx, "generate", []interface{}{1})
		require.NoError(t, err)
		err = waitForSvNode(t, svNodeRPCURL, "blocks", float64(i))
		require.NoError(t, err)
	}
}

func waitForSvNode(t *testing.T, svNodeRPCURL string, key string, value any) error {
	timeout := time.After(15 * time.Second)

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return errors.NewProcessingError("Timeout waiting for svnode to catch up")
		case <-ticker.C:
			resp, err := helper.CallRPC(svNodeRPCURL, "getblockchaininfo", []interface{}{})
			require.NoError(t, err)

			var blockchainInfo map[string]interface{}
			err = json.Unmarshal([]byte(resp), &blockchainInfo)
			require.NoError(t, err)

			t.Logf("blockchainInfo: %s", resp)

			resultMap, ok := blockchainInfo["result"].(map[string]interface{})
			if ok {
				v, ok := resultMap[key]
				if ok {
					if v == value {
						return nil
					}
				}
			}
		}
	}
}
