//go:build test_all || test_docker_daemon || debug

package smoke

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/test/testcontainers"
	"github.com/stretchr/testify/require"
)

var (
	// DEBUG DEBUG DEBUG
	blockWait = 30 * time.Second
)

func TestMoveUp(t *testing.T) {
	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../../docker-compose-host.yml",
	})
	require.NoError(t, err)

	node2 := tc.GetNodeClients(t, "docker.host.teranode2")
	node2.CallRPC(t, "generate", []interface{}{101})

	block101, err := node2.BlockchainClient.GetBlockByHeight(tc.Ctx, 101)
	require.NoError(t, err)

	tc.StopNode(t, "teranode-1")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		EnableP2P:        true,
		EnableValidator:  true,
		KillTeranode:     true,
		SettingsOverride: settings.NewSettings("docker.host.teranode1"),
	})

	t.Cleanup(func() {
		td.Stop()
	})

	// set run state
	err = td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	td.WaitForBlockHeight(t, block101, blockWait, true)

	// generate 1 block on node1
	_, err = td.CallRPC("generate", []interface{}{1})
	require.NoError(t, err)

	// verify blockheight on node1
	_, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
	_, err = node2.BlockchainClient.GetBlockByHeight(tc.Ctx, 102)
	require.NoError(t, err)

	// assert.Equal(t, block102Node1.Header.Hash(), block102Node2.Header.Hash())
}

func TestMoveDownMoveUp(t *testing.T) {
	// t.Skip("Test is disabled")
	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../docker-compose-host.yml",
	})
	// Add cleanup for test container at the start
	t.Cleanup(func() {
		if tc != nil {
			tc.Compose.Down(tc.Ctx)
		}
	})
	require.NoError(t, err)

	tc.StopNode(t, "teranode-1")

	node2 := tc.GetNodeClients(t, "docker.host.teranode2")
	_, err = node2.CallRPC(t, "generate", []interface{}{300})
	require.NoError(t, err)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		EnableP2P:        false,
		EnableValidator:  true,
		SettingsOverride: settings.NewSettings("docker.host.teranode1"),
	})

	blockgen, err := td.CallRPC("generate", []interface{}{200})
	t.Logf("blockgen: %s", blockgen)
	require.NoError(t, err)

	block200, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 200)
	t.Logf("block200: %s", block200.Header.Hash())
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block200, blockWait, true)

	td.Stop()
	td.ResetServiceManagerContext(t)

	td2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
		SettingsOverride:  settings.NewSettings("docker.host.teranode1"),
	})

	time.Sleep(10 * time.Second)

	// verify blockheight on node1
	_, err = td2.BlockchainClient.GetBlockByHeight(td2.Ctx, 300)
	require.NoError(t, err)

	// Add cleanup for td2
	t.Cleanup(func() {
		if td2 != nil {
			td2.Stop()
		}
	})
}

func TestTDRestart(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       false,
		EnableValidator: true,
	})

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	_, err = td.CallRPC("generate", []interface{}{1})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	td.Stop()

	// time.Sleep(10 * time.Second)

	td.ResetServiceManagerContext(t)

	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         false,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
	})

	td.WaitForBlockHeight(t, block1, blockWait, true)
}
