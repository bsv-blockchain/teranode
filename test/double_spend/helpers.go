//go:build test_sequentially || debug

package doublespendtest

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/settings"
	teranode_aerospike "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	aeroTest "github.com/bitcoin-sv/testcontainers-aerospike-go"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
)

func setupDoubleSpendTest(t *testing.T, utxoStoreOverride string) (td *daemon.TestDaemon, coinbaseTx1, txOriginal, txDoubleSpend *bt.Tx, block102 *model.Block, tx *bt.Tx) {
	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		SettingsContext: "dev.system.test",
		SettingsOverrideFunc: func(tSettings *settings.Settings) {
			url, err := url.Parse(utxoStoreOverride)
			require.NoError(t, err)
			tSettings.UtxoStore.UtxoStore = url
		},
	})

	// Set the FSM state to RUNNING...
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	coinbaseTx1 = block1.CoinbaseTx
	// t.Logf("Coinbase has %d outputs", len(coinbaseTx.Outputs))

	txOriginal = td.CreateTransaction(t, coinbaseTx1)
	txDoubleSpend = td.CreateTransaction(t, coinbaseTx1)

	err1 := td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal)
	require.NoError(t, err1)

	// td.Logger.SkipCancelOnFail(true)

	err2 := td.PropagationClient.ProcessTransaction(td.Ctx, txDoubleSpend)
	require.Error(t, err2) // This should fail as it is a double spend

	// td.Logger.SkipCancelOnFail(false)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block102, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	require.Equal(t, uint64(2), block102.TransactionCount)

	// Create another transaction from block2
	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, tx2)
	require.NoError(t, err)

	return td, coinbaseTx1, txOriginal, txDoubleSpend, block102, tx2
}

// TODO should be moved into a test helper package
func initAerospike() (string, func() error, error) {
	teranode_aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	if err != nil {
		return "", nil, err
	}

	cleanup := func() error {
		return container.Terminate(ctx)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return "", cleanup, err
	}

	port, err := container.ServicePort(ctx)
	if err != nil {
		return "", cleanup, err
	}

	// raw client to be able to do gets and cleanup
	client, aeroErr := uaerospike.NewClient(host, port)
	if aeroErr != nil {
		return "", cleanup, aeroErr
	}

	aerospikeContainerURL := fmt.Sprintf("aerospike://%s:%d/%s?set=%s&expiration=%s&externalStore=file://./data/externalStore", host, port, "test", "test", "10m")

	return aerospikeContainerURL, func() error {
		client.Close()
		return cleanup()
	}, nil
}
