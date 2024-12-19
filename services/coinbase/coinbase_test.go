package coinbase

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/settings"
	blockchainStore "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

// Test_HappyPath tests the happy path of the coinbase service by creating a new block and requesting funds
func Test_HappyPath(t *testing.T) {
	ctx := context.Background()

	connStr, teardown, err := SetupPostgresContainer()
	require.NoError(t, err)

	defer func() {
		err := teardown()
		require.NoError(t, err)
	}()

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	logger := ulogger.New("test-coinbase")

	tSettings := test.CreateBaseTestSettings()

	blockChainStore, err := blockchainStore.NewStore(logger, storeURL, tSettings.ChainCfgParams)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		t.Fatalf("Failed to create address: %v", err)
	}

	testSettings := settings.NewSettings()

	testSettings.Coinbase.WaitForPeers = false
	testSettings.Coinbase.TestMode = true

	// set coinbase maturity to 0 for this test to make the coinbase tx spendable immediately
	tSettings.ChainCfgParams.CoinbaseMaturity = 0

	coinbase, err := NewCoinbase(logger, testSettings, blockchainClient, blockChainStore)
	require.NoError(t, err)

	err = coinbase.createTables(ctx)
	require.NoError(t, err)

	genesisBlock, err := blockChainStore.GetBlockByHeight(ctx, 0)
	require.NoError(t, err)

	_, err = blockChainStore.GetBlockByHeight(ctx, 1)
	require.Error(t, err)

	block, err := GenerateTestBlock(2048, 1024, 500, genesisBlock.Hash())
	require.NoError(t, err)

	err = coinbase.storeBlock(ctx, block)
	require.NoError(t, err)

	block1, err := blockChainStore.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	require.NotEmpty(t, block1)

	coinbaseTx, err := coinbase.RequestFunds(ctx, address.AddressString, true)
	require.NoError(t, err)
	require.NotNil(t, coinbaseTx)
}
