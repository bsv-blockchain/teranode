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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// mockBlockValidationInterface is a mock implementation of the Interface interface
type mockBlockValidationInterface struct {
	mock.Mock
}

func (m *mockBlockValidationInterface) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (m *mockBlockValidationInterface) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseURL string, waitToComplete bool) error {
	args := m.Called(ctx, blockHash, baseURL, waitToComplete)
	return args.Error(0)
}

func (m *mockBlockValidationInterface) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32) error {
	args := m.Called(ctx, block, blockHeight)
	return args.Error(0)
}

func (m *mockBlockValidationInterface) ValidateBlock(ctx context.Context, block *model.Block) error {
	args := m.Called(ctx, block)
	return args.Error(0)
}

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

	subtrees := make([]*subtree.Subtree, 1)

	subtrees[0], err = subtree.NewTree(1)
	require.NoError(t, err)

	err = subtrees[0].AddCoinbaseNode()
	require.NoError(t, err)

	// blockValidationService, err := New(ulogger.TestLogger{}, nil, nil, nil, nil)
	// require.NoError(t, err)

	// this now needs to be here since we do not have the full subtrees in the Block struct
	// which is used in the CheckMerkleRoot function
	coinbaseHash := coinbaseTx.TxIDChainHash()

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size())) //nolint:gosec

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
	_ = subtreeStore.Set(ctx, subtrees[0].RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)

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

	subtrees := make([]*subtree.Subtree, 1)

	subtrees[0], err = subtree.NewTree(1)
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

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size())) // nolint:gosec

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
	_ = subtreeStore.Set(ctx, subtrees[0].RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)

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

	subtrees := make([]*subtree.Subtree, 2)

	subtrees[0], err = subtree.NewTreeByLeafCount(2) // height = 1
	require.NoError(t, err)
	subtrees[1], err = subtree.NewTreeByLeafCount(2) // height = 1
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

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size())) //nolint:gosec

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	for i, subTree := range subtrees {
		rootHash := subTree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])

		subtreeBytes, _ := subTree.Serialize()
		_ = subtreeStore.Set(ctx, rootHash[:], fileformat.FileTypeSubtree, subtreeBytes)
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

	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	if err != nil {
		panic(err)
	}

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	if err != nil {
		panic(err)
	}

	txStore := memory.New()

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, utxoStore)
	require.NoError(t, err)

	kafkaConsumerClient := &kafka.KafkaConsumerGroup{}

	subtreeStore := memory.New()
	tSettings.GlobalBlockHeightRetention = uint32(1)

	s := New(ulogger.TestLogger{}, tSettings, nil, txStore, utxoStore, nil, blockchainClient, kafkaConsumerClient, nil)
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
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockValidation.CatchupConcurrency = 1

	baseURL := "http://test.com"

	t.Run("catchup", func(t *testing.T) {
		// Setup
		mockBlockchainStore := blockchain_store.NewMockStore()
		mockBlockchainClient, err := blockchain.NewLocalClient(logger, mockBlockchainStore, nil, nil)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings()

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		if err != nil {
			panic(err)
		}

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		if err != nil {
			panic(err)
		}

		_ = utxoStore.SetBlockHeight(200)

		tSettings.GlobalBlockHeightRetention = uint32(0)

		server := &Server{
			logger:               logger,
			settings:             tSettings,
			blockchainClient:     mockBlockchainClient,
			blockValidation:      NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, nil, nil, nil, nil),
			utxoStore:            utxoStore,
			processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
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
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockValidation.CatchupConcurrency = 1

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_ = utxoStore.SetBlockHeight(110)

	baseURL := "http://test.com"

	t.Run("successful catchup with multiple blocks", func(t *testing.T) {
		// Setup
		mockBlockchainStore := blockchain_store.NewMockStore()
		mockBlockchainClient, err := blockchain.NewLocalClient(logger, mockBlockchainStore, nil, nil)
		require.NoError(t, err)

		tSettings.GlobalBlockHeightRetention = uint32(0)

		server := &Server{
			logger:               logger,
			settings:             tSettings,
			blockchainClient:     mockBlockchainClient,
			blockValidation:      NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, nil, nil, nil, nil),
			utxoStore:            utxoStore,
			processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
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
			fmt.Sprintf("%s/headers_to_common_ancestor/%s", baseURL, lastBlock.Hash().String()),
			httpmock.NewBytesResponder(200, headers))

		// Execute
		catchupBlockHeaders, err := server.catchupGetBlocks(ctx, lastBlock, baseURL)
		require.NoError(t, err)

		// Assert
		assert.NotNil(t, catchupBlockHeaders)
		assert.Equal(t, 150, len(catchupBlockHeaders))
	})

	t.Run("catchup when target block already exists", func(t *testing.T) {
		// Setup
		mockBlockchainStore := blockchain_store.NewMockStore()
		mockBlockchainClient, err := blockchain.NewLocalClient(logger, mockBlockchainStore, nil, nil)
		require.NoError(t, err)

		tSettings.GlobalBlockHeightRetention = uint32(0)

		server := &Server{
			logger:               logger,
			settings:             tSettings,
			blockchainClient:     mockBlockchainClient,
			blockValidation:      NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, nil, nil, nil, nil),
			processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
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

		tSettings.GlobalBlockHeightRetention = uint32(0)
		server := &Server{
			logger:               logger,
			settings:             tSettings,
			blockchainClient:     mockBlockchainClient,
			blockValidation:      NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, nil, nil, nil, nil),
			processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
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
			fmt.Sprintf("%s/headers_to_common_ancestor/%s", baseURL, lastBlock.Hash().String()),
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
		tSettings := test.CreateBaseTestSettings()
		tSettings.BlockValidation.SecretMiningThreshold = 10

		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		_ = utxoStore.SetBlockHeight(110)

		blockchainClient := &blockchain.Mock{}

		server := New(ulogger.TestLogger{}, tSettings, nil, nil, utxoStore, nil, blockchainClient, nil, nil)

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
		tSettings := test.CreateBaseTestSettings()
		tSettings.BlockValidation.SecretMiningThreshold = 10

		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		_ = utxoStore.SetBlockHeight(0)

		blockchainClient := &blockchain.Mock{}
		blockBytes, err := hex.DecodeString("0000002006226e46111a0b59caaf126043eb5bbf28c34f3a5e332a1fc7b2b73cf188910f1633819a69afbd7ce1f1a01c3b786fcbb023274f3b15172b24feadd4c80e6c6a8b491267ffff7f20040000000102000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
		require.NoError(t, err)

		server := New(ulogger.TestLogger{}, tSettings, nil, nil, utxoStore, nil, blockchainClient, nil, nil)

		block, err := model.NewBlockFromBytes(blockBytes, nil)
		require.NoError(t, err)

		block.Height = 1 // same height as utxo store
		blockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil).Once()

		secretMining, err := server.checkSecretMining(t.Context(), &chainhash.Hash{})
		require.NoError(t, err)
		assert.False(t, secretMining)
	})
}

func Test_checkSecretMining_blockchainClientError(t *testing.T) {
	t.Run("blockchain client returns error", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings()
		tSettings.BlockValidation.SecretMiningThreshold = 10

		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		_ = utxoStore.SetBlockHeight(100)

		blockchainClient := &blockchain.Mock{}
		errExpected := errors.New(errors.ERR_BLOCK_NOT_FOUND, "block not found")
		blockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(nil, errExpected).Once()

		server := New(ulogger.TestLogger{}, tSettings, nil, nil, utxoStore, nil, blockchainClient, nil, nil)

		secretMining, err := server.checkSecretMining(t.Context(), &chainhash.Hash{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "block not found")
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

	// Initialize with a proper genesis block hash
	prevHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	require.NoError(t, err)

	for i := 0; i < numBlocks; i++ {
		// Create a unique merkle root hash for each block
		merkleRoot := chainhash.Hash{}
		merkleRoot[0] = byte(i) // Make each merkle root unique
		merkleRoot[1] = byte(i >> 8)
		merkleRoot[2] = byte(i >> 16)
		merkleRoot[3] = byte(i >> 24)

		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: &merkleRoot,
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

		// Update prevHash for next block using the actual hash of this block
		prevHash = header.Hash()
	}

	return blocks
}
func TestServer_blockHandler_processBlockFound_happyPath(t *testing.T) {
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings()

	blocks := createTestBlockChain(t, 3)
	testBlock := blocks[2]
	hashStr := testBlock.Hash().String()
	url := "http://localhost:8080"

	blockFoundCh := make(chan processBlockFound, 10)

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return((chan *blockchain_api.Notification)(nil), nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("AddBlock", mock.Anything, testBlock, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, testBlock.Header.Hash()).Return(nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlock.Header}, []*model.BlockHeaderMeta{&model.BlockHeaderMeta{Height: 100}}, nil)
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil)

	bv := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, txMetaStore, subtreeValidationClient)

	server := &Server{
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		blockValidation: bv,
		blockFoundCh:    blockFoundCh,
		stats:           gocore.NewStat("test"),
	}

	kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
		Hash: hashStr,
		URL:  url,
	}

	msgBytes, err := proto.Marshal(kafkaMsg)
	require.NoError(t, err)

	msg := &kafka.KafkaMessage{
		ConsumerMessage: sarama.ConsumerMessage{
			Value: msgBytes,
		},
	}

	go func() {
		found := <-blockFoundCh
		hash, err := chainhash.NewHashFromStr(hashStr)
		require.NoError(t, err)
		assert.Equal(t, hash.String(), found.hash.String())
		assert.Equal(t, url, found.baseURL)
		assert.NotNil(t, found.errCh)
		found.errCh <- nil
	}()

	err = server.blockHandler(msg)
	assert.NoError(t, err)
}

func TestServer_blockFoundCh_triggersCatchupCh(t *testing.T) {
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockValidation.UseCatchupWhenBehind = true

	dummyBlock := createTestBlock(t)
	blockBytes, err := dummyBlock.Bytes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^http://peer[0-9]+/block/[a-f0-9]+$`, httpmock.NewBytesResponder(200, blockBytes))

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlock", mock.Anything, mock.Anything).Return(&model.Block{}, nil)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return((chan *blockchain_api.Notification)(nil), nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)

	blockFoundCh := make(chan processBlockFound, 1)
	catchupCh := make(chan processBlockCatchup, 1)

	baseServer := &Server{
		logger:               ulogger.TestLogger{},
		settings:             tSettings,
		blockFoundCh:         blockFoundCh,
		catchupCh:            catchupCh,
		stats:                gocore.NewStat("test"),
		blockValidation:      NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, txMetaStore, subtreeValidationClient),
		blockchainClient:     mockBlockchain,
		subtreeStore:         subtreeStore,
		txStore:              txStore,
		utxoStore:            txMetaStore,
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
	}

	err = baseServer.Init(context.Background())
	require.NoError(t, err)

	// Fill blockFoundCh to trigger the catchup path
	for i := 0; i < 1; i++ {
		blockFoundCh <- processBlockFound{
			hash:    &chainhash.Hash{},
			baseURL: fmt.Sprintf("http://peer%d", i),
			errCh:   make(chan error, 1),
		}
	}

	select {
	case got := <-catchupCh:
		assert.NotNil(t, got.block)
		assert.Equal(t, "http://peer0", got.baseURL)
	case <-time.After(time.Second):
		t.Fatal("processBlockFoundChannel did not put anything on catchupCh")
	}
}

func TestServer_blockFoundCh_triggersCatchupCh_BlockLocator(t *testing.T) {
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockValidation.UseCatchupWhenBehind = true

	blocks := createTestBlockChain(t, 10)
	block1 := blocks[0]
	block2 := blocks[1]
	block1Bytes, err := block1.Bytes()
	require.NoError(t, err)

	hashes := make([]*chainhash.Hash, len(blocks))

	for i, block := range blocks {
		hashes[i] = block.Hash()
	}

	for _, block := range blocks {
		blockBytes, err := block.Bytes()
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", `=~^http://peer[0-9]+/block/[a-f0-9]+$`, httpmock.NewBytesResponder(200, blockBytes))
	}

	httpmock.RegisterResponder(
		"GET",
		`=~^http://peer[0-9]+/headers_to_common_ancestor/[a-f0-9]+`,
		httpmock.NewBytesResponder(200, block1Bytes),
	)

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlock", mock.Anything, mock.Anything).Return(&model.Block{}, nil)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return((chan *blockchain_api.Notification)(nil), nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("CatchUpBlocks", mock.Anything).Return(nil)
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(block2.Header, &model.BlockHeaderMeta{Height: 2}, nil)
	mockBlockchain.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return(hashes[:1], nil)

	fsmState := blockchain_api.FSMStateType_CATCHINGBLOCKS
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
	mockBlockchain.On("Run", mock.Anything, mock.Anything).Return(nil)

	blockFoundCh := make(chan processBlockFound, 1)
	catchupCh := make(chan processBlockCatchup, 1)

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, txMetaStore, subtreeValidationClient)
	baseServer := &Server{
		logger:               ulogger.TestLogger{},
		settings:             tSettings,
		blockFoundCh:         blockFoundCh,
		catchupCh:            catchupCh,
		stats:                gocore.NewStat("test"),
		blockValidation:      blockValidation,
		blockchainClient:     mockBlockchain,
		subtreeStore:         subtreeStore,
		txStore:              txStore,
		utxoStore:            txMetaStore,
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
	}

	require.NoError(t, blockValidation.blockHashesCurrentlyValidated.Put(*block1.Hash()))
	require.NoError(t, blockValidation.blockHashesCurrentlyValidated.Put(*block2.Hash()))

	err = baseServer.Init(context.Background())
	require.NoError(t, err)

	// Fill blockFoundCh to trigger the catchup path
	for _, block := range blocks {
		blockFoundCh <- processBlockFound{
			hash:    block.Hash(),
			baseURL: "http://peer0",
			errCh:   make(chan error, 1),
		}
	}

	// there should be 4 catchups
	for i := 0; i < 10; i++ {
		select {
		case got := <-catchupCh:
			assert.NotNil(t, got.block)
			assert.Equal(t, "http://peer0", got.baseURL)
		case <-time.After(time.Second):
			t.Logf("processBlockFoundChannel did not put anything on catchupCh")
		}
	}
}

func TestServer_getBlocks_happyPath(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	server := &Server{settings: tSettings}

	// Create two test blocks and serialize them
	block1 := createTestBlock(t)
	block2 := createTestBlock(t)
	blockBytes1, err := block1.Bytes()
	require.NoError(t, err)
	blockBytes2, err := block2.Bytes()
	require.NoError(t, err)

	// Concatenate the bytes as getBlocks expects a stream of blocks
	allBlockBytes := append(blockBytes1, blockBytes2...) //nolint:gocritic

	// Mock the HTTP endpoint
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder(
		"GET",
		fmt.Sprintf("http://peer/blocks/%s?n=2", block1.Hash().String()),
		httpmock.NewBytesResponder(200, allBlockBytes),
	)

	// Call getBlocks
	gotBlocks, err := server.getBlocks(context.Background(), block1.Hash(), 2, "http://peer")
	require.NoError(t, err)
	require.Len(t, gotBlocks, 2)
	assert.Equal(t, block1.Hash().String(), gotBlocks[0].Hash().String())
	assert.Equal(t, block2.Hash().String(), gotBlocks[1].Hash().String())
}

func TestServer_getBlockHeaders_happyPath(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	// Create a mock blockchain client
	mockBlockchain := &blockchain.Mock{}

	// Prepare a best block header and meta
	bestBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}
	bestBlockMeta := &model.BlockHeaderMeta{Height: 100}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(bestBlockHeader, bestBlockMeta, nil)

	// Prepare block locator hashes
	locatorHashes := []*chainhash.Hash{&chainhash.Hash{}}
	mockBlockchain.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return(locatorHashes, nil)

	// Prepare a block header to be returned by the HTTP call
	blockHeader := &model.BlockHeader{
		Version:        2,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          1,
	}
	blockHeaderBytes := blockHeader.Bytes()

	// Mock the HTTP endpoint
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder(
		"GET",
		`=~^http://peer/headers_to_common_ancestor/.*`,
		httpmock.NewBytesResponder(200, blockHeaderBytes),
	)

	server := &Server{
		settings:         tSettings,
		blockchainClient: mockBlockchain,
	}

	// Call getBlockHeaders
	hash := chainhash.DoubleHashH([]byte("target"))
	gotHeaders, err := server.getBlockHeaders(context.Background(), &hash, 0, "http://peer")
	require.NoError(t, err)
	require.Len(t, gotHeaders, 1)
	assert.Equal(t, blockHeader.Version, gotHeaders[0].Version)
	assert.Equal(t, blockHeader.Nonce, gotHeaders[0].Nonce)
}

// testServer is a test-specific server type that allows overriding getBlock
type testServer struct {
	*Server
	blocks []*model.Block
}

func TestProcessBlockFoundChannelCatchup(t *testing.T) {
	initPrometheusMetrics()
	// Use the shared setup for proper in-memory stores and fixtures
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockValidation.UseCatchupWhenBehind = true

	// Create test blocks and hashes
	blocks := createTestBlockChain(t, 4)

	// Create mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchainClient.On("Subscribe", mock.Anything, mock.Anything).Return((chan *blockchain_api.Notification)(nil), nil)
	mockBlockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{blocks[0].Header}, []*model.BlockHeaderMeta{&model.BlockHeaderMeta{Height: 100}}, nil)
	mockBlockchainClient.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil)

	// Mock GetBestBlockHeader once for all test cases
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(blocks[0].Header, &model.BlockHeaderMeta{Height: 100}, nil).Once()

	// Mock HTTP responses for block requests
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock block responses for each peer
	for _, block := range blocks {
		blockBytes, err := block.Bytes()
		require.NoError(t, err)
		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("=~^http://peer1/block/%s", block.Hash().String()),
			httpmock.NewBytesResponder(200, blockBytes),
		)
		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("=~^http://peer2/block/%s", block.Hash().String()),
			httpmock.NewBytesResponder(200, blockBytes),
		)
	}

	// Create base server instance with real in-memory stores
	baseServer := &Server{
		logger:               ulogger.TestLogger{},
		settings:             tSettings,
		blockFoundCh:         make(chan processBlockFound, 10),
		catchupCh:            make(chan processBlockCatchup, 10),
		blockValidation:      NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, mockBlockchainClient, subtreeStore, txStore, txMetaStore, subtreeValidationClient),
		blockchainClient:     mockBlockchainClient,
		subtreeStore:         subtreeStore,
		txStore:              txStore,
		utxoStore:            txMetaStore,
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
		stats:                gocore.NewStat("test"),
	}

	// Create test server with blocks
	server := &testServer{
		Server: baseServer,
		blocks: blocks,
	}

	pbf1 := processBlockFound{hash: blocks[0].Hash(), baseURL: "http://peer1", errCh: make(chan error, 1)}
	pbf2 := processBlockFound{hash: blocks[1].Hash(), baseURL: "http://peer1", errCh: make(chan error, 1)}
	pbf3 := processBlockFound{hash: blocks[2].Hash(), baseURL: "http://peer2", errCh: make(chan error, 1)}
	pbf4 := processBlockFound{hash: blocks[3].Hash(), baseURL: "http://peer2", errCh: make(chan error, 1)}

	// Fill blockFoundCh with blocks
	server.blockFoundCh <- pbf1
	server.blockFoundCh <- pbf2
	server.blockFoundCh <- pbf3
	server.blockFoundCh <- pbf4

	ctx := context.Background()
	// Call processBlockFoundChannel with the first block
	err := server.processBlockFoundChannel(ctx, pbf1)
	require.NoError(t, err)

	// There should be 2 blocks in the catchup channel (latest per peer)
	require.Equal(t, 2, len(server.catchupCh))
	catchup1 := <-server.catchupCh
	catchup2 := <-server.catchupCh

	// Should be the latest block for each peer
	peerBlocks := map[string]*model.Block{"http://peer1": blocks[1], "http://peer2": blocks[3]}

	gotBlocks := map[string]bool{}

	for _, c := range []processBlockCatchup{catchup1, catchup2} {
		for peer, block := range peerBlocks {
			if c.baseURL == peer && c.block.Hash().IsEqual(block.Hash()) {
				gotBlocks[peer] = true
			}
		}
	}

	require.True(t, gotBlocks["http://peer1"])
	require.True(t, gotBlocks["http://peer2"])
}

func Test_HealthLiveness(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Create a minimal server with just enough setup for health checks
	server := &Server{
		logger:              logger,
		settings:            tSettings,
		kafkaConsumerClient: nil, // Tests may not set this
		blockchainClient:    nil,
		subtreeStore:        nil,
		txStore:             nil,
		utxoStore:           nil,
	}

	status, msg, err := server.Health(ctx, true)
	require.Equal(t, http.StatusOK, status)
	require.NoError(t, err)
	require.Equal(t, "OK", msg)
}

func Test_HealthReadiness(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Use actual in-memory stores that have proper health methods
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	// Create mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)
	// Mock FSM state check
	fsmState := blockchain_api.FSMStateType_RUNNING
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		kafkaConsumerClient: nil, // Tests may not set this
		blockchainClient:    mockBlockchainClient,
		subtreeStore:        subtreeStore,
		txStore:             txStore,
		utxoStore:           utxoStore,
	}

	status, msg, err := server.Health(ctx, false)
	require.Equal(t, http.StatusOK, status)
	require.NoError(t, err)

	// Parse and validate JSON response
	var jsonMsg map[string]interface{}
	err = json.Unmarshal([]byte(msg), &jsonMsg)
	require.NoError(t, err, "Message should be valid JSON")
	require.Contains(t, jsonMsg, "status", "JSON should contain 'status' field")
	require.Contains(t, jsonMsg, "dependencies", "JSON should contain 'dependencies' field")
}

func Test_HealthReadiness_UnhealthyDependency(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Use actual in-memory stores that have proper health methods
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	// Create mock dependencies with one unhealthy
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusServiceUnavailable, "Blockchain service unavailable", nil)
	// Mock FSM state check
	fsmState := blockchain_api.FSMStateType_RUNNING
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		kafkaConsumerClient: nil,
		blockchainClient:    mockBlockchainClient,
		subtreeStore:        subtreeStore,
		txStore:             txStore,
		utxoStore:           utxoStore,
	}

	status, msg, err := server.Health(ctx, false)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.NoError(t, err)
	require.Contains(t, msg, "Blockchain service unavailable")
}

func Test_HealthGRPC(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Use actual in-memory stores that have proper health methods
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	// Create mock dependencies
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)
	// Mock FSM state check
	fsmState := blockchain_api.FSMStateType_RUNNING
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		kafkaConsumerClient: nil,
		blockchainClient:    mockBlockchainClient,
		subtreeStore:        subtreeStore,
		txStore:             txStore,
		utxoStore:           utxoStore,
	}

	response, err := server.HealthGRPC(ctx, &blockvalidation_api.EmptyMessage{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.True(t, response.Ok)
	require.NotNil(t, response.Timestamp)

	// Validate details contain JSON
	var details map[string]interface{}
	err = json.Unmarshal([]byte(response.Details), &details)
	require.NoError(t, err)
	require.Contains(t, details, "status")
	require.Contains(t, details, "dependencies")
}

func Test_HealthGRPC_Unhealthy(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Use actual in-memory stores that have proper health methods
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	// Create mock dependencies with one unhealthy
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusServiceUnavailable, "Blockchain service unavailable", nil)
	// Mock FSM state check
	fsmState := blockchain_api.FSMStateType_RUNNING
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		kafkaConsumerClient: nil,
		blockchainClient:    mockBlockchainClient,
		subtreeStore:        subtreeStore,
		txStore:             txStore,
		utxoStore:           utxoStore,
	}

	response, err := server.HealthGRPC(ctx, &blockvalidation_api.EmptyMessage{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.False(t, response.Ok)
	require.NotNil(t, response.Timestamp)
	require.Contains(t, response.Details, "Blockchain service unavailable")
}

// Mock kafka consumer for testing
type mockKafkaConsumer struct {
	mock.Mock
}

func (m *mockKafkaConsumer) Start(ctx context.Context, consumerFn func(message *kafka.KafkaMessage) error, opts ...kafka.ConsumerOption) {
	m.Called(ctx, consumerFn, opts)
}

func (m *mockKafkaConsumer) BrokersURL() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *mockKafkaConsumer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func Test_Start(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Use actual in-memory stores
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	// Create mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(nil)

	// Create mock kafka consumer
	mockKafkaConsumer := &mockKafkaConsumer{}
	mockKafkaConsumer.On("Start", mock.Anything, mock.Anything, mock.Anything).Return()

	server := &Server{
		logger:               logger,
		settings:             tSettings,
		kafkaConsumerClient:  mockKafkaConsumer,
		blockchainClient:     mockBlockchainClient,
		subtreeStore:         subtreeStore,
		txStore:              txStore,
		utxoStore:            utxoStore,
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
	}

	// Create a context with quick timeout since Start() blocks on GRPC server
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	readyCh := make(chan struct{})
	err := server.Start(ctx, readyCh)

	// The error might be nil if the context cancels quickly before the GRPC server fully starts
	// or an error if port binding fails
	if err != nil {
		// If we get an error, it should be context related or port binding
		assert.Contains(t, err.Error(), "context")
	}
	mockBlockchainClient.AssertExpectations(t)
}

func Test_Start_FSMTransitionError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Create mock blockchain client that returns error
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(errors.New(errors.ERR_BLOCK_NOT_FOUND, "FSM not ready"))

	server := &Server{
		logger:           logger,
		settings:         tSettings,
		blockchainClient: mockBlockchainClient,
	}

	readyCh := make(chan struct{})
	err := server.Start(ctx, readyCh)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "FSM not ready")
	mockBlockchainClient.AssertExpectations(t)
}

func Test_Stop(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Create mock kafka consumer
	mockKafkaConsumer := &mockKafkaConsumer{}
	mockKafkaConsumer.On("Close").Return(nil)

	server := &Server{
		logger:               logger,
		settings:             tSettings,
		kafkaConsumerClient:  mockKafkaConsumer,
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
	}

	// Start the ttl cache so we can stop it
	go server.processSubtreeNotify.Start()
	time.Sleep(10 * time.Millisecond) // Give it time to start

	err := server.Stop(ctx)
	require.NoError(t, err)
	mockKafkaConsumer.AssertExpectations(t)
}

func Test_Stop_KafkaCloseError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Create mock kafka consumer that returns error
	mockKafkaConsumer := &mockKafkaConsumer{}
	mockKafkaConsumer.On("Close").Return(errors.New(errors.ERR_NETWORK_ERROR, "failed to close kafka"))

	server := &Server{
		logger:               logger,
		settings:             tSettings,
		kafkaConsumerClient:  mockKafkaConsumer,
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
	}

	// Start the ttl cache so we can stop it
	go server.processSubtreeNotify.Start()
	time.Sleep(10 * time.Millisecond) // Give it time to start

	err := server.Stop(ctx)
	require.NoError(t, err) // Stop doesn't return the kafka error
	mockKafkaConsumer.AssertExpectations(t)
}

func Test_BlockFound(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Create test hash
	hash := chainhash.HashH([]byte("test block"))
	hashBytes := hash.CloneBytes()

	t.Run("block already exists", func(t *testing.T) {
		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		}

		// Mark block as existing
		err := bv.SetBlockExists(&hash)
		require.NoError(t, err)

		server := &Server{
			logger:          logger,
			settings:        tSettings,
			blockValidation: bv,
			stats:           gocore.NewStat("test"),
		}

		req := &blockvalidation_api.BlockFoundRequest{
			Hash:    hashBytes,
			BaseUrl: "http://test.com",
		}

		resp, err := server.BlockFound(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("new block without wait", func(t *testing.T) {
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBlockExists", mock.Anything, &hash).Return(false, nil)

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
		}

		server := &Server{
			logger:          logger,
			settings:        tSettings,
			blockValidation: bv,
			blockFoundCh:    make(chan processBlockFound, 10),
			stats:           gocore.NewStat("test"),
		}

		req := &blockvalidation_api.BlockFoundRequest{
			Hash:           hashBytes,
			BaseUrl:        "http://test.com",
			WaitToComplete: false,
		}

		resp, err := server.BlockFound(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Give the goroutine time to add to the channel
		time.Sleep(10 * time.Millisecond)

		// Check that block was queued
		require.Equal(t, 1, len(server.blockFoundCh))
		blockFound := <-server.blockFoundCh
		require.Equal(t, hash.String(), blockFound.hash.String())
		require.Equal(t, "http://test.com", blockFound.baseURL)
		require.Nil(t, blockFound.errCh)
	})

	t.Run("new block with wait - success", func(t *testing.T) {
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBlockExists", mock.Anything, &hash).Return(false, nil)

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
		}

		server := &Server{
			logger:          logger,
			settings:        tSettings,
			blockValidation: bv,
			blockFoundCh:    make(chan processBlockFound, 10),
			stats:           gocore.NewStat("test"),
		}

		req := &blockvalidation_api.BlockFoundRequest{
			Hash:           hashBytes,
			BaseUrl:        "http://test.com",
			WaitToComplete: true,
		}

		// Process the block in a goroutine
		go func() {
			time.Sleep(10 * time.Millisecond) // Small delay
			blockFound := <-server.blockFoundCh
			require.NotNil(t, blockFound.errCh)
			blockFound.errCh <- nil // Signal success
		}()

		resp, err := server.BlockFound(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("new block with wait - error", func(t *testing.T) {
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("GetBlockExists", mock.Anything, &hash).Return(false, nil)

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
		}

		server := &Server{
			logger:          logger,
			settings:        tSettings,
			blockValidation: bv,
			blockFoundCh:    make(chan processBlockFound, 10),
			stats:           gocore.NewStat("test"),
		}

		req := &blockvalidation_api.BlockFoundRequest{
			Hash:           hashBytes,
			BaseUrl:        "http://test.com",
			WaitToComplete: true,
		}

		// Process the block in a goroutine
		go func() {
			time.Sleep(10 * time.Millisecond) // Small delay
			blockFound := <-server.blockFoundCh
			require.NotNil(t, blockFound.errCh)
			blockFound.errCh <- errors.New(errors.ERR_BLOCK_NOT_FOUND, "validation failed")
		}()

		resp, err := server.BlockFound(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "validation failed")
	})

	t.Run("invalid hash", func(t *testing.T) {
		server := &Server{
			logger:   logger,
			settings: tSettings,
			stats:    gocore.NewStat("test"),
		}

		req := &blockvalidation_api.BlockFoundRequest{
			Hash:    []byte("invalid"), // Too short to be a valid hash
			BaseUrl: "http://test.com",
		}

		resp, err := server.BlockFound(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "failed to create hash from bytes")
	})
}

func Test_ProcessBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Create a test block
	block := createTestBlock(t)
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	t.Run("success with height provided", func(t *testing.T) {
		// Use actual in-memory stores
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		mockBlockchainClient := &blockchain.Mock{}

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchainClient,
			subtreeStore:                  subtreeStore,
			txStore:                       txStore,
			utxoStore:                     utxoStore,
			recentBlocksBloomFilters:      txmap.NewSyncedMap[chainhash.Hash, *model.BlockBloomFilter](),
			blockBloomFiltersBeingCreated: txmap.NewSwissMap(0),
			stats:                         gocore.NewStat("test"),
		}

		server := &Server{
			logger:               logger,
			settings:             tSettings,
			blockValidation:      bv,
			blockchainClient:     mockBlockchainClient,
			stats:                gocore.NewStat("test"),
			processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
		}

		// Mock the blockchain client methods
		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

		req := &blockvalidation_api.ProcessBlockRequest{
			Block:  blockBytes,
			Height: 100,
		}

		resp, err := server.ProcessBlock(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with height from previous block", func(t *testing.T) {
		// Use actual in-memory stores
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		mockBlockchainClient := &blockchain.Mock{}

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchainClient,
			subtreeStore:                  subtreeStore,
			txStore:                       txStore,
			utxoStore:                     utxoStore,
			recentBlocksBloomFilters:      txmap.NewSyncedMap[chainhash.Hash, *model.BlockBloomFilter](),
			blockBloomFiltersBeingCreated: txmap.NewSwissMap(0),
			stats:                         gocore.NewStat("test"),
		}

		server := &Server{
			logger:               logger,
			settings:             tSettings,
			blockValidation:      bv,
			blockchainClient:     mockBlockchainClient,
			stats:                gocore.NewStat("test"),
			processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
		}

		// Mock getting previous block header
		prevBlockHeader := &model.BlockHeader{}
		prevBlockMeta := &model.BlockHeaderMeta{Height: 99}
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(prevBlockHeader, prevBlockMeta, nil)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

		req := &blockvalidation_api.ProcessBlockRequest{
			Block:  blockBytes,
			Height: 0, // No height provided, should fetch from previous block
		}

		resp, err := server.ProcessBlock(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("invalid block bytes", func(t *testing.T) {
		server := &Server{
			logger:   logger,
			settings: tSettings,
			stats:    gocore.NewStat("test"),
		}

		req := &blockvalidation_api.ProcessBlockRequest{
			Block:  []byte("invalid block"),
			Height: 100,
		}

		resp, err := server.ProcessBlock(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "failed to create block from bytes")
	})

	t.Run("invalid height after lookup", func(t *testing.T) {
		mockBlockchainClient := &blockchain.Mock{}

		server := &Server{
			logger:           logger,
			settings:         tSettings,
			blockchainClient: mockBlockchainClient,
			stats:            gocore.NewStat("test"),
		}

		// Mock getting previous block header returns error
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.New(errors.ERR_BLOCK_NOT_FOUND, "block not found"))

		req := &blockvalidation_api.ProcessBlockRequest{
			Block:  blockBytes,
			Height: 0,
		}

		resp, err := server.ProcessBlock(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "failed to get previous block header")
	})
}

func Test_ValidateBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	// Create a test block
	block := createTestBlock(t)
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		// Use actual in-memory stores
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		mockBlockchainClient := &blockchain.Mock{}

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchainClient,
			subtreeStore:                  subtreeStore,
			txStore:                       txStore,
			utxoStore:                     utxoStore,
			recentBlocksBloomFilters:      txmap.NewSyncedMap[chainhash.Hash, *model.BlockBloomFilter](),
			blockBloomFiltersBeingCreated: txmap.NewSwissMap(0),
			stats:                         gocore.NewStat("test"),
		}

		server := &Server{
			logger:           logger,
			settings:         tSettings,
			blockValidation:  bv,
			blockchainClient: mockBlockchainClient,
			stats:            gocore.NewStat("test"),
		}

		// Mock blockchain client calls
		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

		// Mock GetBestBlockHeader
		bestBlockHeader := &model.BlockHeader{}
		bestBlockMeta := &model.BlockHeaderMeta{Height: 100}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestBlockHeader, bestBlockMeta, nil)

		// Mock GetBlockHeaders - return empty list since there's no previous block
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)

		req := &blockvalidation_api.ValidateBlockRequest{
			Block:  blockBytes,
			Height: 100,
		}

		resp, err := server.ValidateBlock(ctx, req)
		require.Error(t, err) // ValidateBlock now returns error for invalid blocks
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "block is not valid")
	})

	t.Run("invalid block bytes", func(t *testing.T) {
		server := &Server{
			logger:   logger,
			settings: tSettings,
			stats:    gocore.NewStat("test"),
		}

		req := &blockvalidation_api.ValidateBlockRequest{
			Block:  []byte("invalid block"),
			Height: 100,
		}

		resp, err := server.ValidateBlock(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "failed to create block from bytes")
	})

	t.Run("validation failure", func(t *testing.T) {
		// Use actual in-memory stores
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		mockBlockchainClient := &blockchain.Mock{}

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchainClient,
			subtreeStore:                  subtreeStore,
			txStore:                       txStore,
			utxoStore:                     utxoStore,
			recentBlocksBloomFilters:      txmap.NewSyncedMap[chainhash.Hash, *model.BlockBloomFilter](),
			blockBloomFiltersBeingCreated: txmap.NewSwissMap(0),
			stats:                         gocore.NewStat("test"),
		}

		server := &Server{
			logger:           logger,
			settings:         tSettings,
			blockValidation:  bv,
			blockchainClient: mockBlockchainClient,
			stats:            gocore.NewStat("test"),
		}

		// Mock blockchain client to return error
		mockBlockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(nil, errors.New(errors.ERR_BLOCK_NOT_FOUND, "block not found"))
		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

		// Mock GetBlockHeaders
		blockHeaders := []*model.BlockHeader{block.Header}
		blockMetas := []*model.BlockHeaderMeta{{Height: 99}}
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(blockHeaders, blockMetas, nil)

		req := &blockvalidation_api.ValidateBlockRequest{
			Block:  blockBytes,
			Height: 100,
		}

		resp, err := server.ValidateBlock(ctx, req)
		require.Error(t, err) // ValidateBlock now returns error for bloom filter collection failures
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "failed to collect necessary bloom filters")
	})
}

func Test_consumerMessageHandler(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	hashStr := "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87"
	url := "http://test.com"

	t.Run("successful message handling", func(t *testing.T) {
		// Create mock blockchain client
		mockBlockchainClient := &blockchain.Mock{}
		hash, _ := chainhash.NewHashFromStr(hashStr)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, hash).Return(false, nil)

		// Create minimal BlockValidation
		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
			logger:                        logger,
		}

		server := &Server{
			logger:          logger,
			settings:        tSettings,
			blockFoundCh:    make(chan processBlockFound, 10),
			blockValidation: bv,
			stats:           gocore.NewStat("test"),
		}

		// Set up a mock for blockHandler
		kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
			Hash: hashStr,
			URL:  url,
		}
		msgBytes, err := proto.Marshal(kafkaMsg)
		require.NoError(t, err)

		msg := &kafka.KafkaMessage{
			ConsumerMessage: sarama.ConsumerMessage{
				Value: msgBytes,
			},
		}

		handler := server.consumerMessageHandler(ctx)

		// Process the message in a goroutine to handle the blockFoundCh
		go func() {
			blockFound := <-server.blockFoundCh
			require.NotNil(t, blockFound)
			if blockFound.errCh != nil {
				blockFound.errCh <- nil
			}
		}()

		err = handler(msg)
		require.NoError(t, err)
	})

	t.Run("recoverable error", func(t *testing.T) {
		// Create mock blockchain client
		mockBlockchainClient := &blockchain.Mock{}

		// Create minimal BlockValidation
		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
			logger:                        logger,
		}

		server := &Server{
			logger:          logger,
			settings:        tSettings,
			blockValidation: bv,
			stats:           gocore.NewStat("test"),
		}

		// Invalid message that will cause a parsing error
		msg := &kafka.KafkaMessage{
			ConsumerMessage: sarama.ConsumerMessage{
				Value: []byte("invalid protobuf"),
			},
		}

		handler := server.consumerMessageHandler(ctx)
		err := handler(msg)
		// blockHandler will return a non-recoverable error for invalid protobuf
		require.NoError(t, err) // Non-recoverable errors return nil to commit the message
	})

	t.Run("context cancellation", func(t *testing.T) {
		// Create mock blockchain client
		mockBlockchainClient := &blockchain.Mock{}
		hash, _ := chainhash.NewHashFromStr(hashStr)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, hash).Return(false, nil).Maybe()

		// Create minimal BlockValidation
		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
			logger:                        logger,
		}

		server := &Server{
			logger:          logger,
			settings:        tSettings,
			blockFoundCh:    make(chan processBlockFound, 10),
			blockValidation: bv,
			stats:           gocore.NewStat("test"),
		}

		// Create a cancellable context
		ctx, cancel := context.WithCancel(context.Background())

		kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
			Hash: hashStr,
			URL:  url,
		}
		msgBytes, err := proto.Marshal(kafkaMsg)
		require.NoError(t, err)

		msg := &kafka.KafkaMessage{
			ConsumerMessage: sarama.ConsumerMessage{
				Value: msgBytes,
			},
		}

		handler := server.consumerMessageHandler(ctx)

		// Cancel the context immediately
		cancel()

		err = handler(msg)
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
	})
}

func TestCatchup(t *testing.T) {
	initPrometheusMetrics()
	// Initialize stores
	txMetaStore, _, _, _, _, deferFunc := setup()
	defer deferFunc()

	// Configure test settings
	tSettings := test.CreateBaseTestSettings()
	tSettings.BlockValidation.SecretMiningThreshold = 100

	// Create test blocks
	blocks := createTestBlockChain(t, 150)
	blockUpTo := blocks[1]

	// Create mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}

	// Create a minimal BlockValidation instance without starting background goroutines
	bv := &BlockValidation{
		logger:                        ulogger.TestLogger{},
		settings:                      tSettings,
		blockchainClient:              mockBlockchainClient,
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		bloomFilterStats:              model.NewBloomStats(),
	}

	// Create server instance
	server := &Server{
		logger:               ulogger.TestLogger{},
		settings:             tSettings,
		blockFoundCh:         make(chan processBlockFound, 10),
		catchupCh:            make(chan processBlockCatchup, 10),
		blockValidation:      bv,
		blockchainClient:     mockBlockchainClient,
		utxoStore:            txMetaStore,
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
		stats:                gocore.NewStat("test"),
	}

	// Test cases
	t.Run("Empty Catchup Headers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Mock GetBlockExists to return true to simulate no catchup needed
		mockBlockchainClient.On("GetBlockExists", mock.Anything, blockUpTo.Hash()).Return(true, nil)

		err := server.catchup(ctx, blockUpTo, "test-peer")
		require.NoError(t, err)
	})

	t.Run("Secret Mining Check - Too Far Behind", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Set current block height to be above threshold
		require.NoError(t, server.utxoStore.SetBlockHeight(200))

		// Mock GetBlockExists to return false for all blocks except the first one
		for _, block := range blocks {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, block.Hash()).Return(false, nil)
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, blocks[0].Hash()).Return(true, nil)

		// Return all block headers
		blockHeaders := make([]*model.BlockHeader, len(blocks))

		for i, block := range blocks {
			blockHeaders[i] = block.Header
		}

		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(blockHeaders, []*model.BlockHeaderMeta{{Height: 100}}, nil)

		locatorHashes := []*chainhash.Hash{blocks[0].Hash()}
		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return(locatorHashes, nil)

		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		var headersBytes []byte

		for _, block := range blocks {
			headerBytes := block.Header.Bytes()
			headersBytes = append(headersBytes, headerBytes...)
		}

		httpmock.RegisterResponder(
			"GET",
			`=~^test-peer/headers_to_common_ancestor/.*`,
			httpmock.NewBytesResponder(200, headersBytes),
		)

		// Mock GetBlock for secret mining check to return a block far behind
		prevHash := blocks[len(blocks)-1].Hash()
		merkleRoot := blocks[len(blocks)-1].Hash()
		bits, _ := model.NewNBitFromString("1d00ffff")

		secretMiningBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
				Bits:           *bits,
				Nonce:          0,
			},
			Height: 1, // Far behind current height
		}
		mockBlockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(secretMiningBlock, nil)

		err := server.catchup(ctx, blockUpTo, "test-peer")
		require.NoError(t, err)
	})

	t.Run("Secret Mining Check - Too Far Behind", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Set current block height to be above threshold
		require.NoError(t, server.utxoStore.SetBlockHeight(200))

		for _, block := range blocks {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, block.Hash()).Return(false, nil)
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, blocks[0].Hash()).Return(true, nil)

		// Return all block headers
		blockHeaders := make([]*model.BlockHeader, len(blocks))
		for _, block := range blocks {
			blockHeaders = append(blockHeaders, block.Header)
		}

		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(blockHeaders, []*model.BlockHeaderMeta{{Height: 100}}, nil)

		// Mock GetBlockLocator
		locatorHashes := []*chainhash.Hash{blocks[0].Hash()}
		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return(locatorHashes, nil)

		// Mock HTTP responses for headers
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		var headersBytes []byte

		for _, block := range blocks {
			headerBytes := block.Header.Bytes()
			headersBytes = append(headersBytes, headerBytes...)
		}

		httpmock.RegisterResponder(
			"GET",
			`=~^test-peer/headers_to_common_ancestor/.*`,
			httpmock.NewBytesResponder(200, headersBytes),
		)

		// Mock GetBlock for secret mining check to return a block far behind
		prevHash := blocks[len(blocks)-1].Hash()
		merkleRoot := blocks[len(blocks)-1].Hash()
		bits, _ := model.NewNBitFromString("1d00ffff")
		secretMiningBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
				Bits:           *bits,
				Nonce:          0,
			},
			Height: 55, // Far behind current height
		}
		mockBlockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return(secretMiningBlock, nil)

		err := server.catchup(ctx, blockUpTo, "test-peer")
		require.NoError(t, err)
	})
}
