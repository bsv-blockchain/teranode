package blockassembly

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

func initMockedServer(t *testing.T) (*blockassembly.BlockAssembly, context.CancelFunc, error) {
	blobStore, utxoStore, tSettings, blockchainClient, _, err := initStores(t)
	require.NoError(t, err)

	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024

	ctx, cancelCtx := context.WithCancel(context.Background())
	ba := blockassembly.New(ulogger.TestLogger{}, tSettings, blobStore, utxoStore, blobStore, blockchainClient)

	// Skip waiting for pending blocks in tests to avoid timeout
	ba.SetSkipWaitForPendingBlocks(true)

	err = ba.Init(ctx)
	require.NoError(t, err)

	readyCh := make(chan struct{}, 1)

	go func() {
		err = ba.Start(ctx, readyCh)
		if err != nil {
			panic(err)
		}
	}()

	<-readyCh

	return ba, cancelCtx, nil
}

func initStores(t *testing.T) (*memory.Memory, utxo.Store, *settings.Settings, blockchain.ClientI, *blockassembly.BlockAssembly, error) {
	blobStore := memory.New()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.BlockMaxSize = 1000000
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	tSettings.BlockAssembly.ResetWaitCount = 0
	tSettings.BlockAssembly.ResetWaitDuration = 0

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	tracing.SetupMockTracer()

	blockchainStoreURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)
	blockchainStore, err := blockchainstore.NewStore(logger, blockchainStoreURL, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	return blobStore, utxoStore, tSettings, blockchainClient, nil, nil
}
