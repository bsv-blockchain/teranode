package blockassembly

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func initMockedServer(t *testing.T) (*blockassembly.BlockAssembly, context.CancelFunc, error) {
	blobStore, utxoStore, tSettings, blockchainClient, _, err := initStores()
	require.NoError(t, err)

	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024

	ctx, cancelCtx := context.WithCancel(context.Background())
	ba := blockassembly.New(ulogger.TestLogger{}, tSettings, blobStore, utxoStore, blobStore, blockchainClient)

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

func initStores() (*memory.Memory, *utxostore.Memory, *settings.Settings, blockchain.ClientI, *blockassembly.BlockAssembly, error) {
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})

	tracing.SetupMockTracer()

	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.BlockMaxSize = 1000000
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	tSettings.BlockAssembly.ResetWaitCount = 0
	tSettings.BlockAssembly.ResetWaitDuration = 0

	blockchainStoreURL, _ := url.Parse("sqlitememory://")
	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, blockchainStoreURL, tSettings)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, nil)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	return blobStore, utxoStore, tSettings, blockchainClient, nil, nil
}
