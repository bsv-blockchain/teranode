// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac00000000")

	txIDs = []string{
		"8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", // Coinbase
		"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
		"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
		"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
	}

	merkleRoot, _ = chainhash.NewHashFromStr("f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766")

	prevBlockHashStr = "000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250"
	bitsStr          = "1b04864c"
)

func TestOneTransaction(t *testing.T) {
	var err error

	tSettings := test.CreateBaseTestSettings()

	subtrees := make([]*util.Subtree, 1)

	subtrees[0], err = util.NewTree(1)
	require.NoError(t, err)

	err = subtrees[0].AddCoinbaseNode()
	require.NoError(t, err)

	// blockValidationService, err := New(ulogger.TestLogger{}, nil, nil, nil, nil)
	// require.NoError(t, err)

	// this now needs to be here since we do not have the full subtrees in the Block struct
	// which is used in the CheckMerkleRoot function
	coinbaseHash := coinbaseTx.TxIDChainHash()

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size()))

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	for i, subTree := range subtrees {
		rootHash := subTree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])
	}

	merkleRootHash := coinbaseTx.TxIDChainHash()

	block, err := model.NewBlock(
		&model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: merkleRootHash,
		},
		coinbaseTx,
		subtreeHashes,
		0, 0, 0, 0, tSettings)
	require.NoError(t, err)

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeBytes, _ := subtrees[0].Serialize()
	_ = subtreeStore.Set(ctx, subtrees[0].RootHash()[:], subtreeBytes, options.WithFileExtension(options.SubtreeFileExtension))

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, nil)
	require.NoError(t, err)

	// err = blockValidationService.CheckMerkleRoot(block)
	err = block.CheckMerkleRoot(ctx)
	assert.NoError(t, err)
}

func TestTwoTransactions(t *testing.T) {
	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff07044c86041b0147ffffffff0100f2052a01000000434104ad3b4c6ee28cb0c438c87b4efe1c36e1e54c10efc690f24c2c02446def863c50e9bf482647727b415aa81b45d0f7aa42c2cb445e4d08f18b49c027b58b6b4041ac00000000")
	coinbaseTxID, _ := chainhash.NewHashFromStr("de2c2e8628ab837ceff3de0217083d9d5feb71f758a5d083ada0b33a36e1b30e")
	txid1, _ := chainhash.NewHashFromStr("89878bfd69fba52876e5217faec126fc6a20b1845865d4038c12f03200793f48")
	expectedMerkleRoot, _ := chainhash.NewHashFromStr("7a059188283323a2ef0e02dd9f8ba1ac550f94646290d0a52a586e5426c956c5")

	assert.Equal(t, coinbaseTxID, coinbaseTx.TxIDChainHash())

	var err error

	tSettings := test.CreateBaseTestSettings()

	subtrees := make([]*util.Subtree, 1)

	subtrees[0], err = util.NewTree(1)
	require.NoError(t, err)

	empty := &chainhash.Hash{}
	err = subtrees[0].AddNode(*empty, 0, 0)
	require.NoError(t, err)

	err = subtrees[0].AddNode(*txid1, 0, 0)
	require.NoError(t, err)

	// blockValidationService, err := New(ulogger.TestLogger{}, nil, nil, nil, nil)
	// require.NoError(t, err)

	// this now needs to be here since we do not have the full subtrees in the Block struct
	// which is used in the CheckMerkleRoot function
	coinbaseHash := coinbaseTx.TxIDChainHash()

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size()))

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	for i, subTree := range subtrees {
		rootHash := subTree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])
	}

	expectedMerkleRootHash, _ := chainhash.NewHash(expectedMerkleRoot.CloneBytes())
	block, err := model.NewBlock(
		&model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: expectedMerkleRootHash,
		},
		coinbaseTx,
		subtreeHashes,
		0, 0, 0, 0, tSettings)
	assert.NoError(t, err)

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeBytes, _ := subtrees[0].Serialize()
	_ = subtreeStore.Set(ctx, subtrees[0].RootHash()[:], subtreeBytes, options.WithFileExtension(options.SubtreeFileExtension))

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, nil)
	require.NoError(t, err)

	// err = blockValidationService.CheckMerkleRoot(block)
	err = block.CheckMerkleRoot(ctx)
	assert.NoError(t, err)
}

func TestMerkleRoot(t *testing.T) {
	var err error

	tSettings := test.CreateBaseTestSettings()

	subtrees := make([]*util.Subtree, 2)

	subtrees[0], err = util.NewTreeByLeafCount(2) // height = 1
	require.NoError(t, err)
	subtrees[1], err = util.NewTreeByLeafCount(2) // height = 1
	require.NoError(t, err)

	err = subtrees[0].AddCoinbaseNode()
	require.NoError(t, err)

	hash1, err := chainhash.NewHashFromStr(txIDs[1])
	require.NoError(t, err)
	err = subtrees[0].AddNode(*hash1, 1, 0)
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr(txIDs[2])
	require.NoError(t, err)
	err = subtrees[1].AddNode(*hash2, 1, 0)
	require.NoError(t, err)

	hash3, err := chainhash.NewHashFromStr(txIDs[3])
	require.NoError(t, err)
	err = subtrees[1].AddNode(*hash3, 1, 0)
	require.NoError(t, err)

	assert.Equal(t, txIDs[0], coinbaseTx.TxID())

	prevBlockHash, err := chainhash.NewHashFromStr(prevBlockHashStr)
	if err != nil {
		t.Fail()
	}

	bits, err := hex.DecodeString(bitsStr)
	if err != nil {
		t.Fail()
	}

	// this now needs to be here since we do not have the full subtrees in the Block struct
	// which is used in the CheckMerkleRoot function
	coinbaseHash := coinbaseTx.TxIDChainHash()

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size()))

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	for i, subTree := range subtrees {
		rootHash := subTree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])

		subtreeBytes, _ := subTree.Serialize()
		_ = subtreeStore.Set(ctx, rootHash[:], subtreeBytes, options.WithFileExtension(options.SubtreeFileExtension))
	}

	nBits, _ := model.NewNBitFromSlice(bits)

	block, err := model.NewBlock(
		&model.BlockHeader{
			Version:        1,
			Timestamp:      1293623863,
			Nonce:          274148111,
			HashPrevBlock:  prevBlockHash,
			HashMerkleRoot: merkleRoot,
			Bits:           *nBits,
		},
		coinbaseTx,
		subtreeHashes,
		0, 0, 0, 0, tSettings)
	assert.NoError(t, err)

	// blockValidationService, err := New(ulogger.TestLogger{}, nil, nil, nil, nil)
	// require.NoError(t, err)

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, nil)
	require.NoError(t, err)

	// err = blockValidationService.CheckMerkleRoot(block)
	err = block.CheckMerkleRoot(ctx)
	assert.NoError(t, err)
}

func TestTtlCache(t *testing.T) {
	cache := ttlcache.New[chainhash.Hash, bool]()

	for _, txID := range txIDs {
		hash, _ := chainhash.NewHashFromStr(txID)
		cache.Set(*hash, true, 1*time.Second)
	}

	go cache.Start()

	assert.Equal(t, 4, cache.Len())
	time.Sleep(2 * time.Second)
	assert.Equal(t, 0, cache.Len())
}

func TestBlockHeadersN(t *testing.T) {
	var catchupBlockHeaders []*model.BlockHeader
	for i := 997; i >= 0; i-- {
		catchupBlockHeaders = append(catchupBlockHeaders, &model.BlockHeader{
			Version:        uint32(i), // nolint:gosec
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
		})
	}

	batchSize := 202
	batches := getBlockBatchGets(catchupBlockHeaders, batchSize)
	assert.Len(t, batches, 5)
	assert.Equal(t, 202, int(batches[0].size))
	assert.Equal(t, catchupBlockHeaders[201].String(), batches[0].hash.String())
	assert.Equal(t, 202, int(batches[1].size))
	assert.Equal(t, catchupBlockHeaders[403].String(), batches[1].hash.String())
	assert.Equal(t, 202, int(batches[2].size))
	assert.Equal(t, catchupBlockHeaders[605].String(), batches[2].hash.String())
	assert.Equal(t, 202, int(batches[3].size))
	assert.Equal(t, catchupBlockHeaders[807].String(), batches[3].hash.String())
	assert.Equal(t, 190, int(batches[4].size))
	assert.Equal(t, catchupBlockHeaders[997].String(), batches[4].hash.String())

	batchSize = 500
	batches = getBlockBatchGets(catchupBlockHeaders, batchSize)
	assert.Len(t, batches, 2)
	assert.Equal(t, 500, int(batches[0].size))
	assert.Equal(t, catchupBlockHeaders[499].String(), batches[0].hash.String())
	assert.Equal(t, 498, int(batches[1].size))
	assert.Equal(t, catchupBlockHeaders[997].String(), batches[1].hash.String())
}

func Test_Server_processBlockFound(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings()
	// regtest SubsidyReductionInterval is 150
	// so use mainnet params
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockHex := "010000000edfb8ccf30a17b7deae9c1f1a3dbbaeb1741ff5906192b921cbe7ece5ab380081caee50ec9ca9b5686bb6f71693a1c4284a269ab5f90d8662343a18e1a7200f52a83b66ffff00202601000001fdb1010001000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17033501002f6d322d75732fc1eaad86485d9cc712818b47ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a9141e7ee30c5c564b78533a44aae23bec1be188281d88ac00000000fd3501"
	blockBytes, err := hex.DecodeString(blockHex)
	require.NoError(t, err)

	block, err := model.NewBlockFromBytes(blockBytes, tSettings)
	require.NoError(t, err)

	blockchainStore := blockchain_store.NewMockStore()
	blockchainStore.BlockExists[*block.Header.HashPrevBlock] = true

	utxoStore := utxostore.New(ulogger.TestLogger{})

	txStore := memory.New()

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, utxoStore)
	require.NoError(t, err)

	kafkaConsumerClient := &kafka.KafkaConsumerGroup{}

	subtreeStore := memory.New()
	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(1)

	s := New(ulogger.TestLogger{}, tSettings, nil, txStore, utxoStore, nil, blockchainClient, kafkaConsumerClient)
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil)

	err = s.processBlockFound(context.Background(), block.Hash(), "legacy", block)
	require.NoError(t, err)
}

func TestServer_processBlockFoundChannel(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	if !tSettings.BlockValidation.UseCatchupWhenBehind {
		t.Skip("Skipping test as blockvalidation_useCatchupWhenBehind is false")
	}

	blockHex := "010000000edfb8ccf30a17b7deae9c1f1a3dbbaeb1741ff5906192b921cbe7ece5ab380081caee50ec9ca9b5686bb6f71693a1c4284a269ab5f90d8662343a18e1a7200f52a83b66ffff00202601000001fdb1010001000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17033501002f6d322d75732fc1eaad86485d9cc712818b47ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a9141e7ee30c5c564b78533a44aae23bec1be188281d88ac00000000fd3501"
	blockBytes, err := hex.DecodeString(blockHex)
	require.NoError(t, err)

	httpmock.Activate()
	httpmock.RegisterResponder(
		"GET",
		`=~^/block/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, blockBytes),
	)

	defer func() {
		httpmock.Deactivate()
	}()

	s := &Server{
		logger:       ulogger.TestLogger{},
		settings:     tSettings,
		catchupCh:    make(chan processBlockCatchup, 1),
		blockFoundCh: make(chan processBlockFound, 100),
		stats:        gocore.NewStat("test"),
	}

	blockFound := processBlockFound{
		hash:    &chainhash.Hash{},
		baseURL: "http://localhost:8080",
	}
	for i := 0; i < 10; i++ {
		s.blockFoundCh <- blockFound
	}

	err = s.processBlockFoundChannel(context.Background(), blockFound)
	require.NoError(t, err)

	// should have put something on the catchup channel
	assert.Len(t, s.catchupCh, 1)
}

func TestServer_catchup(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings()
	settings.BlockValidation.CatchupConcurrency = 1

	baseURL := "http://test.com"

	t.Run("catchup", func(t *testing.T) {
		// Setup
		mockBlockchainStore := blockchain_store.NewMockStore()
		mockBlockchainClient, err := blockchain.NewLocalClient(logger, mockBlockchainStore, nil, nil)
		require.NoError(t, err)

		utxoStore := utxostore.New(ulogger.TestLogger{})
		_ = utxoStore.SetBlockHeight(200)

		settings.BlockValidation.BloomFilterRetentionSize = uint32(0)

		server := &Server{
			logger:           logger,
			settings:         settings,
			blockchainClient: mockBlockchainClient,
			blockValidation:  NewBlockValidation(ctx, logger, settings, mockBlockchainClient, nil, nil, nil, nil),
			utxoStore:        utxoStore,
		}

		// Create a chain of test blocks
		blocks := createTestBlockChain(t, 200)
		lastBlock := blocks[len(blocks)-1]

		// Mark first 50 blocks as existing
		for i := 0; i < 50; i++ {
			_, _, err := mockBlockchainStore.StoreBlock(ctx, blocks[i], "")
			require.NoError(t, err)
		}

		headers := make([]byte, 0)
		blocksBytes := make([]byte, 0)

		for _, block := range blocks[1:] {
			headerBytes := block.Header.Bytes()
			headers = append(headerBytes, headers...)

			blockBytes, err := block.Bytes()
			require.NoError(t, err)

			blocksBytes = append(blocksBytes, blockBytes...)
		}

		// Setup HTTP mocks
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("%s/headers_to_common_ancestor/%s", baseURL, lastBlock.Header.HashPrevBlock.String()),
			httpmock.NewBytesResponder(200, headers))

		httpmock.RegisterResponder("GET",
			`=~^/blocks/.*\z`,
			httpmock.NewBytesResponder(200, blocksBytes))

		err = server.catchup(ctx, lastBlock, baseURL)
		require.NoError(t, err)
	})
}

func TestServer_catchupGetBlocks(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings()
	settings.BlockValidation.CatchupConcurrency = 1

	utxoStore := utxostore.New(ulogger.TestLogger{})
	_ = utxoStore.SetBlockHeight(110)

	baseURL := "http://test.com"

	t.Run("successful catchup with multiple blocks", func(t *testing.T) {
		// Setup
		mockBlockchainStore := blockchain_store.NewMockStore()
		mockBlockchainClient, err := blockchain.NewLocalClient(logger, mockBlockchainStore, nil, nil)
		require.NoError(t, err)

		settings.BlockValidation.BloomFilterRetentionSize = uint32(0)

		server := &Server{
			logger:           logger,
			settings:         settings,
			blockchainClient: mockBlockchainClient,
			blockValidation:  NewBlockValidation(ctx, logger, settings, mockBlockchainClient, nil, nil, nil, nil),
			utxoStore:        utxoStore,
		}

		// Create a chain of test blocks
		blocks := createTestBlockChain(t, 200)
		lastBlock := blocks[len(blocks)-1]

		// Mark first 50 blocks as existing
		for i := 0; i < 50; i++ {
			_, _, err := mockBlockchainStore.StoreBlock(ctx, blocks[i], "")
			require.NoError(t, err)
		}

		headers := make([]byte, 0)

		for _, block := range blocks[1:] {
			headerBytes := block.Header.Bytes()
			headers = append(headerBytes, headers...)
		}

		// Setup HTTP mocks
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("%s/headers_to_common_ancestor/%s", baseURL, lastBlock.Header.HashPrevBlock.String()),
			httpmock.NewBytesResponder(200, headers))

		// Execute
		catchupBlockHeaders, err := server.catchupGetBlocks(ctx, lastBlock, baseURL)
		require.NoError(t, err)

		// Assert
		assert.NotNil(t, catchupBlockHeaders)
		assert.Equal(t, 151, len(catchupBlockHeaders))
	})

	t.Run("catchup when target block already exists", func(t *testing.T) {
		// Setup
		mockBlockchainStore := blockchain_store.NewMockStore()
		mockBlockchainClient, err := blockchain.NewLocalClient(logger, mockBlockchainStore, nil, nil)
		require.NoError(t, err)

		settings.BlockValidation.BloomFilterRetentionSize = uint32(0)

		server := &Server{
			logger:           logger,
			settings:         settings,
			blockchainClient: mockBlockchainClient,
			blockValidation:  NewBlockValidation(ctx, logger, settings, mockBlockchainClient, nil, nil, nil, nil),
		}

		block := createTestBlock(t)

		// Pre-set block as existing
		err = server.blockValidation.SetBlockExists(block.Hash())
		require.NoError(t, err)

		// Execute
		catchupBlockHeaders, err := server.catchupGetBlocks(ctx, block, baseURL)
		require.NoError(t, err)

		// Assert
		assert.Nil(t, catchupBlockHeaders)
	})

	t.Run("error when getting block headers", func(t *testing.T) {
		// Setup
		mockBlockchainStore := blockchain_store.NewMockStore()
		mockBlockchainClient, err := blockchain.NewLocalClient(logger, mockBlockchainStore, nil, nil)
		require.NoError(t, err)

		settings.BlockValidation.BloomFilterRetentionSize = uint32(0)
		server := &Server{
			logger:           logger,
			settings:         settings,
			blockchainClient: mockBlockchainClient,
			blockValidation:  NewBlockValidation(ctx, logger, settings, mockBlockchainClient, nil, nil, nil, nil),
		}

		// Create a chain of test blocks
		blocks := createTestBlockChain(t, 3)
		lastBlock := blocks[len(blocks)-1]

		for _, block := range blocks[:len(blocks)-1] {
			_, _, err = mockBlockchainStore.StoreBlock(ctx, block, "")
			require.NoError(t, err)
		}

		// Setup HTTP mock with error
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("%s/headers_to_common_ancestor/%s", baseURL, lastBlock.Header.HashPrevBlock.String()),
			httpmock.NewErrorResponder(errors.New(errors.ERR_NETWORK_ERROR, "network error")))

		// Execute
		catchupBlockHeaders, err := server.catchupGetBlocks(ctx, lastBlock, baseURL)
		require.Error(t, err)

		// Assert
		assert.Contains(t, err.Error(), "network error")
		assert.Nil(t, catchupBlockHeaders)
	})
}

func Test_checkSecretMining(t *testing.T) {
	t.Run("secret mining 10 blocks", func(t *testing.T) {
		settings := test.CreateBaseTestSettings()
		settings.BlockValidation.SecretMiningThreshold = 10

		utxoStore := utxostore.New(ulogger.TestLogger{})
		_ = utxoStore.SetBlockHeight(110)

		blockchainClient := &blockchain.Mock{}

		server := New(ulogger.TestLogger{}, settings, nil, nil, utxoStore, nil, blockchainClient, nil)

		block := &model.Block{Height: 110}

		blockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

		secretMining, err := server.checkSecretMining(t.Context(), &chainhash.Hash{})
		require.NoError(t, err)
		assert.False(t, secretMining)

		block.Height = 120 // 10 blocks ahead
		blockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

		secretMining, err = server.checkSecretMining(t.Context(), &chainhash.Hash{})
		require.NoError(t, err)
		assert.False(t, secretMining)

		block.Height = 99 // 11 blocks old
		blockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

		secretMining, err = server.checkSecretMining(t.Context(), &chainhash.Hash{})
		require.NoError(t, err)
		assert.True(t, secretMining)
	})

	t.Run("secret mining from 0", func(t *testing.T) {
		settings := test.CreateBaseTestSettings()
		settings.BlockValidation.SecretMiningThreshold = 10

		utxoStore := utxostore.New(ulogger.TestLogger{})
		_ = utxoStore.SetBlockHeight(0)

		blockchainClient := &blockchain.Mock{}
		blockBytes, err := hex.DecodeString("0000002006226e46111a0b59caaf126043eb5bbf28c34f3a5e332a1fc7b2b73cf188910f1633819a69afbd7ce1f1a01c3b786fcbb023274f3b15172b24feadd4c80e6c6a8b491267ffff7f20040000000102000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
		require.NoError(t, err)

		server := New(ulogger.TestLogger{}, settings, nil, nil, utxoStore, nil, blockchainClient, nil)

		block, err := model.NewBlockFromBytes(blockBytes, nil)
		require.NoError(t, err)

		block.Height = 1 // same height as utxo store
		blockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

		secretMining, err := server.checkSecretMining(t.Context(), &chainhash.Hash{})
		require.NoError(t, err)
		assert.False(t, secretMining)
	})
}

// Helper functions

func createTestBlock(t *testing.T) *model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromSlice([]byte{0x1b, 0x04, 0x86, 0x4c})
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
		Bits:           *nBits,
		Nonce:          2083236893,
	}

	block := &model.Block{
		Header: header,
	}

	return block
}

func createTestBlockChain(t *testing.T, numBlocks int) []*model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromSlice([]byte{0x1b, 0x04, 0x86, 0x4c})
	require.NoError(t, err)

	blocks := make([]*model.Block, numBlocks)
	prevHash := &chainhash.Hash{}

	for i := 0; i < numBlocks; i++ {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
			Bits:           *nBits,
			Nonce:          uint32(2083236893 + i), // nolint:gosec
		}

		testCoinbaseTx := coinbaseTx.Clone()
		testCoinbaseTx.Outputs[0].Satoshis = 2500000000

		block := &model.Block{
			Header:     header,
			Height:     uint32(i), // nolint:gosec
			CoinbaseTx: testCoinbaseTx,
		}

		blocks[i] = block

		// Update prevHash for next block
		prevHash, err = chainhash.NewHash(header.Hash().CloneBytes())
		require.NoError(t, err)
	}

	return blocks
}
