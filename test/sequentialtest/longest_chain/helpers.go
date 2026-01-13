package longest_chain

import (
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

var (
	blockWait = 30 * time.Second
)

func setupLongestChainTest(t *testing.T, utxoStoreOverride string) (td *daemon.TestDaemon, block3 *model.Block) {
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		// EnableFullLogging: true,
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(tSettings *settings.Settings) {
			url, err := url.Parse(utxoStoreOverride)
			require.NoError(t, err)
			tSettings.UtxoStore.UtxoStore = url
			tSettings.ChainCfgParams.CoinbaseMaturity = 2
		},
	})

	// Set the FSM state to RUNNING...
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 3})
	require.NoError(t, err)

	block3, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block3, blockWait, true)

	return td, block3
}
