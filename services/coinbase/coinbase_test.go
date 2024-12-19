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

	block, err := GenerateTestBlock(1, 2048, 1024, 500, genesisBlock.Hash())
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

func Test_CoinbaseTransactionUniqueness(t *testing.T) {
	ctx := context.Background()

	// Setup a fresh postgres container
	connStr, teardown, err := SetupPostgresContainer()
	require.NoError(t, err)

	defer func() {
		err := teardown()
		require.NoError(t, err)
	}()

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	logger := ulogger.New("test-coinbase-uniqueness")

	tSettings := test.CreateBaseTestSettings()

	blockChainStore, err := blockchainStore.NewStore(logger, storeURL, tSettings.ChainCfgParams)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, blockChainStore, nil, nil)
	require.NoError(t, err)

	testSettings := settings.NewSettings()
	testSettings.Coinbase.WaitForPeers = false
	// set coinbase maturity to 0 for this test to make the coinbase tx spendable immediately
	tSettings.ChainCfgParams.CoinbaseMaturity = 0
	testSettings.Coinbase.TestMode = true

	coinbase, err := NewCoinbase(logger, testSettings, blockchainClient, blockChainStore)
	require.NoError(t, err)

	err = coinbase.createTables(ctx)
	require.NoError(t, err)

	genesisBlock, err := blockChainStore.GetBlockByHeight(ctx, 0)
	require.NoError(t, err)

	numBlocks := 5

	var prevHash = genesisBlock.Hash()

	coinbaseTxIDs := make(map[string]bool)

	for i := 1; i <= numBlocks; i++ {
		// Pass the block height to ensure uniqueness in coinbase
		block, err := GenerateTestBlock(uint32(i), 2048, 1024, 500, prevHash) // #nosec G115 -- i is bounded by numBlocks
		require.NoError(t, err)

		err = coinbase.storeBlock(ctx, block)
		require.NoError(t, err)

		// Get stored block to verify
		storedBlock, err := blockChainStore.GetBlockByHeight(ctx, uint32(i)) // #nosec G115 -- i is bounded by numBlocks
		require.NoError(t, err)
		require.NotNil(t, storedBlock)

		// Extract coinbase txid and check uniqueness
		coinbaseTxID := block.CoinbaseTx.TxID()
		if _, exists := coinbaseTxIDs[coinbaseTxID]; exists {
			t.Fatalf("duplicate coinbase txid found for block height %d: %s", i, coinbaseTxID)
		}

		coinbaseTxIDs[coinbaseTxID] = true

		prevHash = block.Hash()
	}
}

func Test_MultipleParameterVariations(t *testing.T) {
	ctx := context.Background()

	// Setup a fresh postgres container
	connStr, teardown, err := SetupPostgresContainer()
	require.NoError(t, err)

	defer func() {
		err := teardown()
		require.NoError(t, err)
	}()

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	logger := ulogger.New("test-coinbase-params")
	tSettings := test.CreateBaseTestSettings()

	blockChainStore, err := blockchainStore.NewStore(logger, storeURL, tSettings.ChainCfgParams)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, blockChainStore, nil, nil)
	require.NoError(t, err)

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

	// Insert a few blocks with varying transaction sizes and spendable outputs
	var prevHash = genesisBlock.Hash()

	testCases := []struct {
		height                   uint32
		noOfTxs                  uint64
		subtreeSize              int
		numberOfSpendableOutputs uint64
	}{
		{1, 1024, 512, 200},
		{2, 2048, 1024, 500},
		{3, 4096, 2048, 1000},
	}

	for _, tc := range testCases {
		block, err := GenerateTestBlock(tc.height, tc.noOfTxs, tc.subtreeSize, tc.numberOfSpendableOutputs, prevHash)
		require.NoError(t, err)

		err = coinbase.storeBlock(ctx, block)
		require.NoError(t, err)

		storedBlock, err := blockChainStore.GetBlockByHeight(ctx, tc.height)
		require.NoError(t, err)
		require.NotNil(t, storedBlock)
		require.Equal(t, tc.noOfTxs, storedBlock.TransactionCount)

		prevHash = block.Hash()
	}

	// Verify coinbase balance increased after multiple parameter variations
	balance, err := coinbase.getBalance(ctx)
	require.NoError(t, err)
	require.NotNil(t, balance)
	require.Greater(t, balance.TotalSatoshis, uint64(0))
}

func Test_MalformedUTXOs(t *testing.T) {
	ctx := context.Background()

	// Setup postgres container
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

	testSettings := settings.NewSettings()
	testSettings.Coinbase.WaitForPeers = false
	testSettings.Coinbase.TestMode = true

	// set coinbase maturity to 0 for this test to make the coinbase tx spendable immediately
	tSettings.ChainCfgParams.CoinbaseMaturity = 0

	coinbase, err := NewCoinbase(logger, testSettings, blockchainClient, blockChainStore)
	require.NoError(t, err)

	// Configure 100% of UTXOs to be malformed
	coinbase.malformedConfig = &MalformedUTXOConfig{
		Percentage: 100,
		Type:       ZeroSatoshis,
	}

	err = coinbase.createTables(ctx)
	require.NoError(t, err)

	genesisBlock, err := blockChainStore.GetBlockByHeight(ctx, 0)
	require.NoError(t, err)

	// Generate and store a block with 500 spendable outputs
	// Each output will have 10_000_000 satoshis
	numberOfSpendableOutputs := uint64(500)

	block, err := GenerateTestBlock(1, 2048, 1024, numberOfSpendableOutputs, genesisBlock.Hash())
	require.NoError(t, err)

	err = coinbase.storeBlock(ctx, block)
	require.NoError(t, err)

	// Verify the block was stored
	block1, err := blockChainStore.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	require.NotEmpty(t, block1)

	// Verify coinbase balance
	balance, err := coinbase.getBalance(ctx)
	require.NoError(t, err)
	require.NotNil(t, balance)

	require.Equal(t, numberOfSpendableOutputs, balance.NumberOfUtxos)
	require.Equal(t, uint64(0), balance.TotalSatoshis)
}

func Test_SpendValidUTXOs(t *testing.T) {
	ctx := context.Background()

	// Setup postgres container
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
	require.NoError(t, err)

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

	// Generate and store a block with valid UTXOs
	block, err := GenerateTestBlock(1, 2048, 1024, 500, genesisBlock.Hash())
	require.NoError(t, err)

	err = coinbase.storeBlock(ctx, block)
	require.NoError(t, err)

	// Request funds from coinbase
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// Request funds and get transaction with UTXOs
	tx, err := coinbase.RequestFunds(ctx, address.AddressString, true)
	require.NoError(t, err)
	require.NotNil(t, tx)

	// Try to spend the UTXOs - this should succeed as they are valid
	err = TrySpendUTXOs(tx, privateKey)
	require.NoError(t, err, "should be able to spend valid UTXOs")
}

func Test_RequestFundsFromAllValidUTXOs(t *testing.T) {
	ctx := context.Background()

	// Setup postgres container
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

	// Generate and store a block with 500 spendable outputs
	block, err := GenerateTestBlock(1, 2048, 1024, 500, genesisBlock.Hash())
	require.NoError(t, err)

	err = coinbase.storeBlock(ctx, block)
	require.NoError(t, err)

	// Verify initial balance has 500 UTXOs
	balance, err := coinbase.getBalance(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(500), balance.NumberOfUtxos)

	// Request funds first time - should use oldest UTXO
	addr1 := "1DbzzfgL6BjhA8Zu8p4ydiZZqBEiNjFozL"
	tx1, err := coinbase.RequestFunds(ctx, addr1, true) // disableDistribute=true to not actually send to network
	require.NoError(t, err)
	require.NotNil(t, tx1)

	// Request funds second time - should use second oldest UTXO
	addr2 := "1H4zUpyFsL2xFmqTCcVHsXHBVpKzu7JQAP"
	tx2, err := coinbase.RequestFunds(ctx, addr2, true)
	require.NoError(t, err)
	require.NotNil(t, tx2)

	// Verify we got different transactions
	require.NotEqual(t, tx1.TxID(), tx2.TxID(), "Should get different transactions")

	// Verify each transaction has 100 outputs going to correct addresses
	require.Equal(t, 100, len(tx1.Outputs), "First tx should have 100 outputs")
	require.Equal(t, 100, len(tx2.Outputs), "Second tx should have 100 outputs")

	for _, out := range tx1.Outputs {
		addrs, err := out.LockingScript.Addresses()
		require.NoError(t, err)
		require.Equal(t, addr1, addrs[0])
	}

	for _, out := range tx2.Outputs {
		addrs, err := out.LockingScript.Addresses()
		require.NoError(t, err)
		require.Equal(t, addr2, addrs[0])
	}

	// Verify 498 UTXOs remain
	finalBalance, err := coinbase.getBalance(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(498), finalBalance.NumberOfUtxos)

	// Test spending one output from each transaction
	err = TrySpendUTXOs(tx1, coinbase.privateKey)
	require.NoError(t, err, "Should be able to spend output from first tx")

	err = TrySpendUTXOs(tx2, coinbase.privateKey)
	require.NoError(t, err, "Should be able to spend output from second tx")
}

func Test_RequestFundsMalformed(t *testing.T) {
	ctx := context.Background()

	// Setup postgres container
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
	require.NoError(t, err)

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

	// Define test cases
	testCases := []struct {
		name          string
		malformType   MalformationType
		expectSuccess bool
	}{
		{"ZeroSatoshis", ZeroSatoshis, true},
	}

	prevHash := genesisBlock.Hash()
	blockHeight := uint32(1)
	totalOutputs := 0
	spentOutputs := 0

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Configure 100% of UTXOs to be malformed with the current type
			coinbase.malformedConfig = &MalformedUTXOConfig{
				Percentage: 100,
				Type:       tc.malformType,
			}

			// Generate and store a block with malformed UTXOs
			block, err := GenerateTestBlock(blockHeight, 2048, 1024, 500, prevHash)
			require.NoError(t, err)

			err = coinbase.storeBlock(ctx, block)
			require.NoError(t, err)

			// Update prevHash to the current block's hash
			prevHash = block.Hash()

			// Increment block height for the next iteration
			blockHeight++
			totalOutputs += 500
			totalOutputs -= spentOutputs

			// Verify initial balance has 500 UTXOs
			balance, err := coinbase.getBalance(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(totalOutputs), balance.NumberOfUtxos) //nolint:gosec // G115: integer overflow conversion int -> uint32

			// Request funds
			address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
			require.NoError(t, err)

			tx, err := coinbase.RequestFunds(ctx, address.AddressString, true)
			require.NoError(t, err)
			require.NotNil(t, tx)

			// Try to spend outputs
			err = TrySpendUTXOs(tx, privateKey)
			if tc.expectSuccess {
				require.NoError(t, err, "Should be able to spend UTXOs")
			} else {
				require.Error(t, err, "Should not be able to spend malformed UTXOs")
			}

			spentOutputs++
		})
	}
}
