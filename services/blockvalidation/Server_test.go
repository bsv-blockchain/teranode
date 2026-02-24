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
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
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

func (m *mockBlockValidationInterface) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32, peerID, baseURL string) error {
	args := m.Called(ctx, block, blockHeight, peerID, baseURL)
	return args.Error(0)
}

func (m *mockBlockValidationInterface) ValidateBlock(ctx context.Context, block *model.Block, options *ValidateBlockOptions) error {
	args := m.Called(ctx, block, options)
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

func TestGetCatchupStatusPopulatesCurrentHeightWhenNotCatchingUp(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	suite.Server.isCatchingUp.Store(false)

	suite.MockBlockchain.
		On("GetBestBlockHeader", mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 123, ID: 1}, nil).
		Once()

	resp, err := suite.Server.GetCatchupStatus(suite.Ctx, &blockvalidation_api.EmptyMessage{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.False(t, resp.IsCatchingUp)
	assert.Equal(t, uint32(123), resp.CurrentHeight)
}

func TestOneTransaction(t *testing.T) {
	var err error

	tSettings := test.CreateBaseTestSettings(t)

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
		0, 0, 0, 0)
	require.NoError(t, err)

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeBytes, _ := subtrees[0].Serialize()
	_ = subtreeStore.Set(ctx, subtrees[0].RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
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

	tSettings := test.CreateBaseTestSettings(t)

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
		0, 0, 0, 0)
	assert.NoError(t, err)

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeBytes, _ := subtrees[0].Serialize()
	_ = subtreeStore.Set(ctx, subtrees[0].RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// err = blockValidationService.CheckMerkleRoot(block)
	err = block.CheckMerkleRoot(ctx)
	assert.NoError(t, err)
}

func TestMerkleRoot(t *testing.T) {
	var err error

	tSettings := test.CreateBaseTestSettings(t)

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
		0, 0, 0, 0)
	assert.NoError(t, err)

	// blockValidationService, err := New(ulogger.TestLogger{}, nil, nil, nil, nil)
	// require.NoError(t, err)

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// err = blockValidationService.CheckMerkleRoot(block)
	err = block.CheckMerkleRoot(ctx)
	assert.NoError(t, err)
}

func TestTtlCache(t *testing.T) {
	cache := ttlcache.New[chainhash.Hash, bool]()

	// Ensure cleanup happens
	defer func() {
		done := make(chan struct{})
		go func() {
			cache.Stop()
			close(done)
		}()
		select {
		case <-done:
			// Successfully stopped
		case <-time.After(100 * time.Millisecond):
			// Timeout - cache might not have been started yet
		}
	}()

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

	// TODO: The batch creation logic has been refactored to use a different approach
	// These tests need to be updated to match the new implementation
	/*
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
	*/
}

func Test_Server_processBlockFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	// regtest SubsidyReductionInterval is 150
	// so use mainnet params
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockHex := "010000000edfb8ccf30a17b7deae9c1f1a3dbbaeb1741ff5906192b921cbe7ece5ab380081caee50ec9ca9b5686bb6f71693a1c4284a269ab5f90d8662343a18e1a7200f52a83b66ffff00202601000001fdb1010001000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17033501002f6d322d75732fc1eaad86485d9cc712818b47ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a9141e7ee30c5c564b78533a44aae23bec1be188281d88ac00000000fd3501"
	blockBytes, err := hex.DecodeString(blockHex)
	require.NoError(t, err)

	block, err := model.NewBlockFromBytes(blockBytes)
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

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, utxoStore)
	require.NoError(t, err)

	kafkaConsumerClient := &kafka.KafkaConsumerGroup{}

	subtreeStore := memory.New()
	tSettings.GlobalBlockHeightRetention = uint32(1)

	s := New(ulogger.TestLogger{}, tSettings, nil, txStore, utxoStore, nil, blockchainClient, kafkaConsumerClient, nil, nil)
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, nil)

	err = s.processBlockFound(context.Background(), block.Hash(), "", "legacy", block)
	require.NoError(t, err)
}

func TestServer_processBlockFoundChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.UseCatchupWhenBehind = true
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockHex := "010000000edfb8ccf30a17b7deae9c1f1a3dbbaeb1741ff5906192b921cbe7ece5ab380081caee50ec9ca9b5686bb6f71693a1c4284a269ab5f90d8662343a18e1a7200f52a83b66ffff00202601000001fdb1010001000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17033501002f6d322d75732fc1eaad86485d9cc712818b47ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a9141e7ee30c5c564b78533a44aae23bec1be188281d88ac00000000fd3501"
	blockBytes, err := hex.DecodeString(blockHex)
	require.NoError(t, err)

	block, err := model.NewBlockFromBytes(blockBytes)
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	httpmock.RegisterResponder(
		"GET",
		`=~^/block/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, blockBytes),
	)

	defer func() {
		httpmock.Deactivate()
	}()

	blockchainStore := blockchain_store.NewMockStore()
	blockchainStore.BlockExists[*block.Header.HashPrevBlock] = true
	// Set up a best block so the blockchain client can subscribe
	blockchainStore.BestBlock = &model.Block{
		Header: block.Header,
		Height: 1,
	}

	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	txStore := memory.New()
	subtreeStore := memory.New()

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, utxoStore)
	require.NoError(t, err)

	kafkaConsumerClient := &kafka.KafkaConsumerGroup{}

	s := New(ulogger.TestLogger{}, tSettings, nil, txStore, utxoStore, nil, blockchainClient, kafkaConsumerClient, nil, nil)
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, nil)

	blockFound := processBlockFound{
		hash:    block.Hash(),
		baseURL: "http://localhost:8080",
	}
	for i := 0; i < 10; i++ {
		s.blockFoundCh <- blockFound
	}

	err = s.processBlockFoundChannel(ctx, blockFound)
	require.NoError(t, err)

	// should have put something on the catchup channel
	assert.Len(t, s.catchupCh, 1)
}

func TestServer_catchup(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupConcurrency = 1

	baseURL := "http://test.com"

	t.Run("catchup", func(t *testing.T) {
		// Create a sub-context for this test that gets cancelled when test ends
		testCtx, testCancel := context.WithCancel(ctx)
		defer testCancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.GlobalBlockHeightRetention = uint32(0)
		tSettings.ChainCfgParams.CoinbaseMaturity = 100
		tSettings.BlockValidation.SecretMiningThreshold = 100

		// Setup
		mockBlockchainStore := blockchain_store.NewMockStore()
		mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
		require.NoError(t, err)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		if err != nil {
			panic(err)
		}

		utxoStore, err := sql.New(testCtx, logger, tSettings, utxoStoreURL)
		if err != nil {
			panic(err)
		}

		_ = utxoStore.SetBlockHeight(100)

		subtreeStore := blobmemory.New()

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockchainClient:    mockBlockchainClient,
			blockValidation:     NewBlockValidation(testCtx, logger, tSettings, mockBlockchainClient, subtreeStore, nil, nil, nil, nil),
			utxoStore:           utxoStore,
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
			headerChainCache:    catchup.NewHeaderChainCache(logger),
			subtreeStore:        subtreeStore,
		}

		// Create a chain of test blocks
		blocks := testhelpers.CreateTestBlockChain(t, 100)

		// Set the Height field for each block
		for i, block := range blocks {
			block.Height = uint32(i)
		}
		lastBlock := blocks[len(blocks)-1]

		// Mark first 50 blocks as existing
		for i := 0; i < 50; i++ {
			_, _, err = mockBlockchainStore.StoreBlock(ctx, blocks[i], "test")
			require.NoError(t, err)
		}

		// Set the best block to the last stored block (block 49)
		mockBlockchainStore.BestBlock = blocks[49]

		// Build headers response - should include common ancestor (block 49) and new blocks (50-99)
		// Headers should be in order from oldest to newest
		headers := make([]byte, 0, 51*80) // 51 headers * 80 bytes each

		// Include the common ancestor (last existing block)
		commonAncestor := blocks[49]
		headers = append(headers, commonAncestor.Header.Bytes()...)

		// Then add all new blocks (50-99)
		for _, block := range blocks[50:] {
			headerBytes := block.Header.Bytes()
			headers = append(headers, headerBytes...)
		}

		// Setup HTTP mocks
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Register responder for headers_from_common_ancestor with regex to match any hash and query params
		httpmock.RegisterResponder("GET",
			`=~^http://test\.com/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				t.Logf("headers_from_common_ancestor request: %s", req.URL.String())
				t.Logf("Returning %d bytes of headers", len(headers))
				return httpmock.NewBytesResponse(200, headers), nil
			})

		// Register responder for blocks endpoint
		httpmock.RegisterResponder("GET",
			`=~^http://test\.com/blocks/.*`,
			func(req *http.Request) (*http.Response, error) {
				t.Logf("blocks request: %s", req.URL.String())

				// Parse the requested block hash from the URL
				parts := strings.Split(req.URL.Path, "/")
				if len(parts) < 3 {
					return httpmock.NewStringResponse(400, "Invalid URL"), nil
				}
				requestedHash := parts[2]

				// Find the starting block
				var startIdx = -1
				for i, block := range blocks {
					if block.Hash().String() == requestedHash {
						startIdx = i
						break
					}
				}

				if startIdx == -1 {
					t.Logf("Block not found: %s", requestedHash)
					return httpmock.NewStringResponse(404, "Block not found"), nil
				}

				// Parse the 'n' parameter to determine how many blocks to return
				n := 100
				if nParam := req.URL.Query().Get("n"); nParam != "" {
					if parsedN, err := strconv.Atoi(nParam); err == nil {
						n = parsedN
					}
				}

				t.Logf("Returning blocks from index %d, count %d", startIdx, n)

				// Collect the requested blocks in reverse order (newest first)
				// The server expects blocks to be returned newest-first
				var responseBytes []byte
				actualCount := 0

				// Start from the requested block and go backwards
				for i := startIdx; i >= 0 && actualCount < n; i-- {
					blockBytes, err := blocks[i].Bytes()
					require.NoError(t, err)
					responseBytes = append(responseBytes, blockBytes...)
					actualCount++
				}

				t.Logf("Actually returning %d blocks", actualCount)

				return httpmock.NewBytesResponse(200, responseBytes), nil
			})

		err = server.catchup(ctx, lastBlock, "test-peer-001", baseURL)
		require.NoError(t, err)
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

func createServerTestBlockChain(t *testing.T, numBlocks int) []*model.Block {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)

	blocks := testhelpers.CreateTestBlockChain(t, 3)
	testBlock := blocks[2]
	hashStr := testBlock.Hash().String()
	url := "http://localhost:8080"

	blockFoundCh := make(chan processBlockFound, 10)

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	subChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(subChan, nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("AddBlock", mock.Anything, testBlock, mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, testBlock.Hash()).Return([]chainhash.Hash{}, nil)
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlock.Header}, []*model.BlockHeaderMeta{{Height: 100}}, nil)
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)

	server := &Server{
		logger:              ulogger.TestLogger{},
		settings:            tSettings,
		blockValidation:     bv,
		blockFoundCh:        blockFoundCh,
		stats:               gocore.NewStat("test"),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
		Hash: hashStr,
		URL:  url,
	}

	go func() {
		found := <-blockFoundCh
		hash, err := chainhash.NewHashFromStr(hashStr)
		require.NoError(t, err)
		assert.Equal(t, hash.String(), found.hash.String())
		assert.Equal(t, url, found.baseURL)
		assert.Nil(t, found.errCh) // errCh should be nil to avoid blocking Kafka consumer
	}()

	err := server.blockHandler(kafkaMsg)
	assert.NoError(t, err)
}

func Test_HealthLiveness(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

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
	tSettings := test.CreateBaseTestSettings(t)
	// Clear the default addresses since we're not starting the servers in tests
	tSettings.BlockValidation.GRPCListenAddress = ""

	// Use actual in-memory stores that have proper health methods
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
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
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
	tSettings := test.CreateBaseTestSettings(t)

	// Use actual in-memory stores that have proper health methods
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
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
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
	tSettings := test.CreateBaseTestSettings(t)

	// Use actual in-memory stores that have proper health methods
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
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
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
	tSettings := test.CreateBaseTestSettings(t)

	// Use actual in-memory stores that have proper health methods
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
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
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	response, err := server.HealthGRPC(ctx, &blockvalidation_api.EmptyMessage{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.False(t, response.Ok)
	require.NotNil(t, response.Timestamp)
	require.Contains(t, response.Details, "Blockchain service unavailable")
}

// Mock kafka consumer for testing is now in mock.go

func (m *mockKafkaConsumer) PauseAll() {
	m.Called()
}

func (m *mockKafkaConsumer) ResumeAll() {
	m.Called()
}

func Test_Start(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Use actual in-memory stores
	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	// Create mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(nil)

	// Create mock kafka consumer
	mockKafkaConsumer := &mockKafkaConsumer{}
	mockKafkaConsumer.On("Start", mock.Anything, mock.Anything, mock.Anything).Return()

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		kafkaConsumerClient: mockKafkaConsumer,
		blockchainClient:    mockBlockchainClient,
		subtreeStore:        subtreeStore,
		txStore:             txStore,
		utxoStore:           utxoStore,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
		assert.True(t, strings.Contains(err.Error(), "context") || strings.Contains(err.Error(), "address already in use"),
			"Expected context cancellation or port binding error, got: %v", err)
	}
	mockBlockchainClient.AssertExpectations(t)
}

func Test_Start_FSMTransitionError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Clear the gRPC listen address to prevent port conflicts during testing
	tSettings.BlockValidation.GRPCListenAddress = ""

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
	tSettings := test.CreateBaseTestSettings(t)

	// Create mock kafka consumer
	mockKafkaConsumer := &mockKafkaConsumer{}
	mockKafkaConsumer.On("Close").Return(nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		kafkaConsumerClient: mockKafkaConsumer,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	// Start the ttl caches so we can stop them
	go server.processBlockNotify.Start()
	go server.catchupAlternatives.Start()
	time.Sleep(10 * time.Millisecond) // Give them time to start

	err := server.Stop(ctx)
	require.NoError(t, err)
	mockKafkaConsumer.AssertExpectations(t)
}

func Test_Stop_KafkaCloseError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Create mock kafka consumer that returns error
	mockKafkaConsumer := &mockKafkaConsumer{}
	mockKafkaConsumer.On("Close").Return(errors.New(errors.ERR_NETWORK_ERROR, "failed to close kafka"))

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		kafkaConsumerClient: mockKafkaConsumer,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	// Start the ttl caches so we can stop them
	go server.processBlockNotify.Start()
	go server.catchupAlternatives.Start()
	time.Sleep(10 * time.Millisecond) // Give them time to start

	err := server.Stop(ctx)
	require.NoError(t, err) // Stop doesn't return the kafka error
	mockKafkaConsumer.AssertExpectations(t)
}

func Test_BlockFound(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Create test hash
	hash := chainhash.HashH([]byte("test block"))
	hashBytes := hash.CloneBytes()

	t.Run("block already exists", func(t *testing.T) {
		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
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
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockValidation:     bv,
			blockFoundCh:        make(chan processBlockFound, 10),
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockValidation:     bv,
			blockFoundCh:        make(chan processBlockFound, 10),
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockValidation:     bv,
			blockFoundCh:        make(chan processBlockFound, 10),
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
	tSettings := test.CreateBaseTestSettings(t)

	// Create a test block
	block := createTestBlock(t)
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	t.Run("success with height provided", func(t *testing.T) {
		// Use actual in-memory stores
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		mockBlockchainClient := &blockchain.Mock{}

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchainClient,
			subtreeStore:                  subtreeStore,
			txStore:                       txStore,
			utxoStore:                     utxoStore,
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			stats:                         gocore.NewStat("test"),
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockValidation:     bv,
			blockchainClient:    mockBlockchainClient,
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		mockBlockchainClient := &blockchain.Mock{}

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchainClient,
			subtreeStore:                  subtreeStore,
			txStore:                       txStore,
			utxoStore:                     utxoStore,
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			stats:                         gocore.NewStat("test"),
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockValidation:     bv,
			blockchainClient:    mockBlockchainClient,
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
	tSettings := test.CreateBaseTestSettings(t)

	// Create a test block
	block := createTestBlock(t)
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		// Use actual in-memory stores
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		mockBlockchainClient := &blockchain.Mock{}

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchainClient,
			subtreeStore:                  subtreeStore,
			txStore:                       txStore,
			utxoStore:                     utxoStore,
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			stats:                         gocore.NewStat("test"),
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockValidation:     bv,
			blockchainClient:    mockBlockchainClient,
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		mockBlockchainClient := &blockchain.Mock{}

		bv := &BlockValidation{
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchainClient,
			subtreeStore:                  subtreeStore,
			txStore:                       txStore,
			utxoStore:                     utxoStore,
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			stats:                         gocore.NewStat("test"),
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockValidation:     bv,
			blockchainClient:    mockBlockchainClient,
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
		require.Contains(t, err.Error(), "block is not valid")
	})
}

func Test_consumerMessageHandler(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

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
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
			logger:                        logger,
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockFoundCh:        make(chan processBlockFound, 10),
			blockValidation:     bv,
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
			logger:                        logger,
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockValidation:     bv,
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			blockchainClient:              mockBlockchainClient,
			logger:                        logger,
		}

		server := &Server{
			logger:              logger,
			settings:            tSettings,
			blockFoundCh:        make(chan processBlockFound, 10),
			blockValidation:     bv,
			stats:               gocore.NewStat("test"),
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
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

// TestHealth_IncludesCatchupStatus tests that health check includes catchup metrics
func TestHealth_IncludesCatchupStatus(t *testing.T) {
	server, mockBlockchain, mockUTXO, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	// Mock the Health method for blockchain client
	ctx := context.Background()
	mockBlockchain.On("Health", ctx, false).Return(200, "OK", nil)

	// Return the RUNNING state for the FSM
	runningState := blockchain.FSMStateRUNNING
	mockBlockchain.On("GetFSMCurrentState", ctx).Return(&runningState, nil)

	// Mock the Health method for UTXO store if needed
	mockUTXO.On("Health", ctx, false).Return(200, "OK", nil)

	// Create mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}
	subChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchainClient.On("Subscribe", mock.Anything, mock.Anything).Return(subChan, nil)
	mockBlockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil)

	// Simulate some catchup operations with proper synchronization
	server.catchupAttempts.Store(10)
	server.catchupSuccesses.Store(8)

	// Use mutex for thread-safe access to non-atomic fields
	server.catchupStatsMu.Lock()
	server.lastCatchupTime = time.Now()
	server.lastCatchupResult = true
	server.catchupStatsMu.Unlock()

	server.isCatchingUp.Store(false)

	status, details, err := server.Health(ctx, false)

	require.NoError(t, err)
	assert.Equal(t, 200, status) // http.StatusOK
	assert.Contains(t, details, "CatchupStatus")
	assert.Contains(t, details, "active=false")
	assert.Contains(t, details, "attempts=10")
	assert.Contains(t, details, "successes=8")
	assert.Contains(t, details, "rate=0.80")
}

// TestCatchup_SuccessRateCalculation tests the success rate calculation logic
func TestCatchup_SuccessRateCalculation(t *testing.T) {
	testCases := []struct {
		name         string
		attempts     int64
		successes    int64
		expectedRate float64
	}{
		{"AllSuccess", 10, 10, 1.0},
		{"HalfSuccess", 10, 5, 0.5},
		{"NoSuccess", 10, 0, 0.0},
		{"NoAttempts", 0, 0, 0.0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server, _, _, cleanup := setupTestCatchupServer(t)
			defer cleanup()

			server.catchupAttempts.Store(tc.attempts)
			server.catchupSuccesses.Store(tc.successes)

			// Calculate rate as done in health check
			var successRate float64
			if tc.attempts > 0 {
				successRate = float64(tc.successes) / float64(tc.attempts)
			}

			assert.Equal(t, tc.expectedRate, successRate)
		})
	}
}
