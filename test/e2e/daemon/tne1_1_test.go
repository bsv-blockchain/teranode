package smoke

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
)

func TestNode_DoNotVerifyTransactionsIfAlreadyVerified(t *testing.T) {
	ctx := context.Background()

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		EnableP2P:       true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Validator.UseLocalValidator = true
		},
	})

	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableValidator:   true,
		EnableP2P:         true,
		SkipRemoveDataDir: true,
		SettingsContext:   "docker.host.teranode2.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 28090
			settings.Validator.UseLocalValidator = true
		},
	})

	defer node2.Stop(t)

	node3 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableValidator:   true,
		EnableP2P:         true,
		SkipRemoveDataDir: true,
		SettingsContext:   "docker.host.teranode3.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 38090
			settings.Validator.UseLocalValidator = true
		},
	})

	defer node3.Stop(t)

	// set run state
	err := node1.BlockchainClient.Run(node1.Ctx, "test")
	require.NoError(t, err)

	// set run state
	err = node2.BlockchainClient.Run(node1.Ctx, "test")
	require.NoError(t, err)

	// set run state
	err = node3.BlockchainClient.Run(node1.Ctx, "test")
	require.NoError(t, err)

	// Generate blocks
	_, err = node1.CallRPC("generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	_, err = node2.CallRPC("generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	_, err = node3.CallRPC("generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	block1, errblock := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, errblock)

	block2, errblock := node2.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, errblock)

	block3, errblock := node3.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, errblock)

	var nodes = []*daemon.TestDaemon{node1, node2, node3}

	coinbases := []*bt.Tx{
		block1.CoinbaseTx,
		block2.CoinbaseTx,
		block3.CoinbaseTx,
	}

	for i, node := range nodes {
		_, hashes, err := node.CreateAndSendTxs(t, coinbases[i], i)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		node.Logger.Infof("Hashes: %v", hashes)

		_, err = node.CallRPC("generate", []interface{}{1})
		require.NoError(t, err, "Failed to mine blocks")

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	headerNode1, _, _ := node1.BlockchainClient.GetBestBlockHeader(ctx)
	headerNode2, _, _ := node2.BlockchainClient.GetBestBlockHeader(ctx)
	headerNode3, _, _ := node3.BlockchainClient.GetBestBlockHeader(ctx)

	require.Equal(t, headerNode1.Hash().String(), headerNode2.Hash().String())
	require.Equal(t, headerNode1.Hash().String(), headerNode3.Hash().String())
}
