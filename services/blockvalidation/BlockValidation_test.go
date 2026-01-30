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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	parentTx, _ = bt.NewTxFromString("010000000000000000ef01b835a938a206fda2e2b3faf124f7ac787a16900e0db9b229ced8c1f0927f94f1000000006b483045022100e565286097d8fe54ff0e16ac5b2cd323430dbcd18709ff623ec159e927d0434e02201896b2a3bede76b383e495c6a7b68d4bd1772418fafc98f0ba033a449e5a6bb74121039f271b930111fd7c818100ee1603d5c5094c68b3d15ad0a58f712e7d766225edffffffff80f0fa02000000001976a91448447c5d25d36e4dcac113b01cf9afc23b671e6e88ac0680969800000000001976a9144b23c629871f2b37d9cd47f183f0c87abf4bebe988aca0860100000000001976a91477833709542fdc936a2e1955e6745035d58a7d6e88ac204e0000000000001976a91476594b34e50a5eb61f40aee6bd1c3f2f2e3b63e488ac204e0000000000001976a9141bf90351db9af76d3def1aabf61af61e8354056788aca0060000000000001976a9145055f82f198d3d69e5b9eafb995c2289e661f26588ac8c2f6002000000001976a914308254c746057d189221c36418ba93337de33bc988ac00000000")
	// tx, _  = hex.DecodeString("0100000001ae73759b118b9c8d54a13ad4ccd5de662bbaa2175ee7f4b413f402affb831ed3000000006b483045022100a6af846212b0c611056a9a30c22f0eed3adc29f8c688e804509b113f322459220220708fb79c66d235d937e8348ac022b3cfa6b64fec8d35a749ea0b2293ad95da014121039f271b930111fd7c818100ee1603d5c5094c68b3d15ad0a58f712e7d766225edffffffff0550c30000000000001976a91448bea2d45f4f6175e47ccb717e4f5d19d8f68f3b88ac204e0000000000001976a91442859b9bada6461d08a0aab8a18105ef30457a8b88ac10270000000000001976a914d0e2122bdeed7b2235f670cdc832f518fb63db9f88ac0c040000000000001976a914d56f84ae869e4a743e929e31218b198f02ce67fe88ac8d0c0100000000001976a91444a8e7fb1a426e4c60597d9d3f534c677d4f858388ac00000000")
	tx1, _ = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	tx2    = newTx(1, parentTx.TxIDChainHash())
	tx3    = newTx(3, parentTx.TxIDChainHash())
	tx4    = newTx(4, parentTx.TxIDChainHash())

	hash1 = tx1.TxIDChainHash()
	hash2 = tx2.TxIDChainHash()
	hash3 = tx3.TxIDChainHash()
)

// SafeMockKafkaProducer is a thread-safe mock Kafka producer for testing
type SafeMockKafkaProducer struct {
	sync.Mutex
	mock.Mock
	publishCalled bool
}

func (m *SafeMockKafkaProducer) Publish(msg *kafka.Message) {
	m.Lock()
	defer m.Unlock()
	m.publishCalled = true
}

func (m *SafeMockKafkaProducer) Start(ctx context.Context, ch chan *kafka.Message) {
	m.Lock()
	defer m.Unlock()
	m.Called(ctx, ch)
}

func (m *SafeMockKafkaProducer) Stop() error {
	m.Lock()
	defer m.Unlock()
	m.Called()

	return nil
}

func (m *SafeMockKafkaProducer) IsPublishCalled() bool {
	m.Lock()
	defer m.Unlock()

	return m.publishCalled
}

func (m *SafeMockKafkaProducer) BrokersURL() []string {
	return []string{}
}

func newTx(random uint32, parentTxHash ...*chainhash.Hash) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = random
	tx.LockTime = 0

	if len(parentTxHash) > 0 {
		tx.Inputs = []*bt.Input{{
			PreviousTxSatoshis: uint64(random),
			PreviousTxOutIndex: random,
			UnlockingScript:    bscript.NewFromBytes([]byte{}),
		}}

		_ = tx.Inputs[0].PreviousTxIDAdd(parentTxHash[0])
	}

	return tx
}

type MockSubtreeValidationClient struct {
	server *subtreevalidation.Server
}

type trackingBlockchainClient struct {
	blockchain.ClientI
	invalidateOnce   sync.Once
	invalidateCalled chan struct{}
	notificationCh   chan *blockchain_api.Notification
	setMinedOnce     sync.Once
	setMinedCalled   chan struct{}
}

func newTrackingBlockchainClient(client blockchain.ClientI) *trackingBlockchainClient {
	return &trackingBlockchainClient{
		ClientI:          client,
		invalidateCalled: make(chan struct{}),
	}
}

func (t *trackingBlockchainClient) withNotificationChannel() *trackingBlockchainClient {
	if t.notificationCh == nil {
		t.notificationCh = make(chan *blockchain_api.Notification, 1)
	}
	return t
}

func (t *trackingBlockchainClient) subscriptionChannel() chan *blockchain_api.Notification {
	if t.notificationCh == nil {
		t.notificationCh = make(chan *blockchain_api.Notification, 1)
	}
	return t.notificationCh
}

func (t *trackingBlockchainClient) withSetMinedTracking() *trackingBlockchainClient {
	if t.setMinedCalled == nil {
		t.setMinedCalled = make(chan struct{})
	}
	return t
}

func (t *trackingBlockchainClient) InvalidateBlock(ctx context.Context, hash *chainhash.Hash) ([]chainhash.Hash, error) {
	hashes, err := t.ClientI.InvalidateBlock(ctx, hash)
	t.invalidateOnce.Do(func() {
		close(t.invalidateCalled)
	})
	return hashes, err
}

func (t *trackingBlockchainClient) Subscribe(ctx context.Context, tag string) (chan *blockchain_api.Notification, error) {
	if t.notificationCh != nil {
		return t.notificationCh, nil
	}
	return t.ClientI.Subscribe(ctx, tag)
}

func (t *trackingBlockchainClient) SetBlockMinedSet(ctx context.Context, hash *chainhash.Hash) error {
	if t.setMinedCalled != nil {
		t.setMinedOnce.Do(func() {
			close(t.setMinedCalled)
		})
	}
	return t.ClientI.SetBlockMinedSet(ctx, hash)
}

func (m *MockSubtreeValidationClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "MockValidator", nil
}

func (m *MockSubtreeValidationClient) CheckSubtreeFromBlock(ctx context.Context, subtreeHash chainhash.Hash, baseURL string, blockHeight uint32, blockHash, previousBlockHash *chainhash.Hash) error {
	request := subtreevalidation_api.CheckSubtreeFromBlockRequest{
		Hash:              subtreeHash.CloneBytes(),
		BaseUrl:           baseURL,
		BlockHeight:       blockHeight,
		BlockHash:         blockHash.CloneBytes(),
		PreviousBlockHash: previousBlockHash[:],
	}

	_, err := m.server.CheckSubtreeFromBlock(ctx, &request)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

func (m *MockSubtreeValidationClient) CheckBlockSubtrees(ctx context.Context, block *model.Block, peerID, baseURL string) error {
	blockBytes, err := block.Bytes()
	if err != nil {
		return errors.NewServiceError("failed to serialize block for subtree validation", err)
	}

	request := subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: baseURL,
		PeerId:  peerID,
	}

	_, err = m.server.CheckBlockSubtrees(ctx, &request)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// setup prepares a test environment with necessary components for block validation
// testing. It initializes and configures:
// - Transaction metadata store
// - Validator client with mock implementation
// - Subtree validation services
// - Transaction and block storage systems
// The function returns initialized components and a cleanup function to ensure
// proper test isolation.
func setup(t *testing.T) (utxostore.Store, subtreevalidation.Interface, blockchain.ClientI, blob.Store, blob.Store, func()) {
	// we only need the httpClient, utxoStore and validatorClient when blessing a transaction
	httpmock.ActivateNonDefault(util.HTTPClient())
	httpmock.RegisterResponder(
		"GET",
		`=~^/tx/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, tx1.ExtendedBytes()),
	)

	httpmock.RegisterResponder(
		"POST",
		`=~^/subtree/[a-fA-F0-9]{64}/txs`,
		httpmock.NewBytesResponder(200, tx1.ExtendedBytes()),
	)

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	if err != nil {
		panic(err)
	}

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	if err != nil {
		panic(err)
	}

	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	validatorClient := &validator.MockValidatorClient{UtxoStore: utxoStore}

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	if err != nil {
		panic(err)
	}

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	if err != nil {
		panic(err)
	}

	nilConsumer := &kafka.KafkaConsumerGroup{}

	subtreeValidationServer, err := subtreevalidation.New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, validatorClient, blockchainClient, nilConsumer, nilConsumer, nil)
	if err != nil {
		panic(err)
	}

	err = subtreeValidationServer.Init(context.Background())
	if err != nil {
		panic(err)
	}

	subtreeValidationClient := &MockSubtreeValidationClient{
		server: subtreeValidationServer,
	}

	return utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, func() {
		// Stop the subtreeValidationServer to clean up goroutines
		if err := subtreeValidationServer.Stop(context.Background()); err != nil {
			// Log the error but don't panic in cleanup
			t.Logf("Error stopping subtreeValidationServer: %v", err)
		}
		httpmock.DeactivateAndReset()
	}
}

// TestBlockValidationValidateBlockSmall verifies the validation of a small block
// with minimal transactions to ensure core validation functionality works correctly.
// This test demonstrates the basic block validation flow with a controlled set of
// test transactions and validates both the merkle root calculation and block structure.
func TestBlockValidationValidateBlockSmall(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	// Create a grandparent transaction for parentTx
	grandParentForParentTx := newTx(1000) // Create without parent
	_, err = utxoStore.Create(context.Background(), grandParentForParentTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}))
	require.NoError(t, err)

	// Add parentTx to UTXO store since tx1, tx2, tx3, tx4 all reference it as their parent
	// Use WithMinedBlockInfo to set BlockID to 0 (GenesisBlockID) so validation passes
	_, err = utxoStore.Create(context.Background(), parentTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}))
	require.NoError(t, err)

	// Create tx1 to reference parentTx instead of using the hex string version
	// This ensures the parent relationship is under our control
	// Use version 10 to differentiate from tx2 which uses version 1
	tx1New := newTx(10, parentTx.TxIDChainHash())
	_, err = utxoStore.Create(context.Background(), tx1New, 0)
	require.NoError(t, err)

	// Update hashes to use the new transactions
	hash1New := tx1New.TxIDChainHash()

	subtreeData := subtreepkg.NewSubtreeData(subtree)

	require.NoError(t, subtree.AddNode(*hash1New, 100, 0))
	require.NoError(t, subtree.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree.AddNode(*hash3, 100, 0))

	require.NoError(t, subtreeData.AddTx(tx1New, 1))

	_, err = utxoStore.Create(context.Background(), tx2, 0)
	require.NoError(t, err)

	require.NoError(t, subtreeData.AddTx(tx2, 2))

	_, err = utxoStore.Create(context.Background(), tx3, 0)
	require.NoError(t, err)

	require.NoError(t, subtreeData.AddTx(tx3, 3))

	_, err = utxoStore.Create(context.Background(), tx4, 0)
	require.NoError(t, err)

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree_data/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, subtreeDataBytes),
	)

	nBits, _ := model.NewNBitFromString("207fffff")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())
	// now create a subtree with the coinbase to calculate the merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint: gosec

	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	tSettings := test.CreateBaseTestSettings(t)

	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		//nolint:gosec
		Timestamp: uint32(time.Now().Unix()),
		Bits:      *nBits,
		Nonce:     0,
	}

	// mine to the target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, err := model.NewBlock(
		blockHeader,
		coinbase,
		subtreeHashes,            // should be the subtree with placeholder
		uint64(subtree.Length()), // nolint:gosec
		123123,
		0, 0)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.GlobalBlockHeightRetention = uint32(0)
	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	start := time.Now()

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000")
	require.NoError(t, err)

	t.Logf("Time taken: %s\n", time.Since(start))
}

// TestBlockValidationValidateBlock tests block validation at scale by processing
// a larger block with 1024 transactions. This test ensures the validation system
// can handle production-level transaction volumes while maintaining proper
// validation of all block components including merkle roots, transaction validity,
// and block structure.
func TestBlockValidationValidateBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	txCount := 1024
	// subtreeHashes := make([]*chainhash.Hash, 0)

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	subtree, err := subtreepkg.NewTreeByLeafCount(txCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)

	fees := 0

	for i := 0; i < txCount-1; i++ {
		// Create a parent transaction and add it to the UTXO store
		// This ensures that when we create child transactions, their parents exist
		// Use WithMinedBlockInfo to set BlockID to 0 (GenesisBlockID) so validation passes
		parentTx := newTx(uint32(i + 10000)) // Create parent without parent (no second param)
		_, err = utxoStore.Create(context.Background(), parentTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}))
		require.NoError(t, err)

		//nolint:gosec
		tx := newTx(uint32(i), parentTx.TxIDChainHash())

		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 100, 0))
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx))

		fees += 100

		_, err = utxoStore.Create(context.Background(), tx, 0)
		require.NoError(t, err)
	}

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	nBits, _ := model.NewNBitFromString("207fffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+uint64(fees)) // nolint: gosec

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes)
	require.NoError(t, err)

	// require.NoError(t, err)
	// t.Logf("subtree hash: %s", subtree.RootHash().String())

	// var merkleRootsubtreeHashes []*chainhash.Hash
	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())
	// now create a subtree with the coinbase to calculate the merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint: gosec

	// if len(subtreeHashes) == 1 {
	calculatedMerkleRootHash := replicatedSubtree.RootHash()
	// } else {
	// 	calculatedMerkleRootHash, err = calculateMerkleRoot(merkleRootsubtreeHashes)
	// 	require.NoError(t, err)
	// }

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		//nolint:gosec
		Timestamp: uint32(time.Now().Unix()),
		Bits:      *nBits,
		Nonce:     0,
	}

	// mine to the target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.CoinbaseMaturity = 0

	block, err := model.NewBlock(
		blockHeader,
		coinbase,
		subtreeHashes,            // should be the subtree with placeholder
		uint64(subtree.Length()), // nolint:gosec
		123123,
		1, 0)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.GlobalBlockHeightRetention = uint32(0)
	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	start := time.Now()

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000")
	require.NoError(t, err)

	t.Logf("Time taken: %s\n", time.Since(start))
}

// TestBlockValidationShouldNotAllowDuplicateCoinbasePlaceholder verifies that
// the validation system correctly enforces the Bitcoin protocol rule preventing
// duplicate coinbase placeholders within a block's transaction structure.
// This test ensures the integrity of block reward distribution.
func TestBlockValidationShouldNotAllowDuplicateCoinbasePlaceholder(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	nBits, _ := model.NewNBitFromString("207fffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	require.True(t, coinbase.IsCoinbase())

	_, err = utxoStore.Create(context.Background(), coinbase, 0)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	require.Error(t, subtree.AddCoinbaseNode())

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())
	// now create a subtree with the coinbase to calculate the merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint: gosec

	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		//nolint:gosec
		Timestamp: uint32(time.Now().Unix()),
		Bits:      *nBits,
		Nonce:     0,
	}

	// mine to the target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	tSettings := test.CreateBaseTestSettings(t)

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(subtree.Length()), // nolint: gosec
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes, // should be the subtree with placeholder
	}

	tSettings.GlobalBlockHeightRetention = uint32(0)
	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	start := time.Now()
	// err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000")
	err = blockValidation.ValidateBlock(context.Background(), block, "legacy")
	require.Error(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}

// TestBlockValidationShouldNotAllowDuplicateCoinbaseTx ensures that blocks
// containing multiple coinbase transactions are rejected. This validation is
// critical for maintaining the Bitcoin protocol's monetary policy by preventing
// multiple block rewards within a single block.
func TestBlockValidationShouldNotAllowDuplicateCoinbaseTx(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	nBits, _ := model.NewNBitFromString("207fffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	require.True(t, coinbase.IsCoinbase())

	_, err = utxoStore.Create(context.Background(), coinbase, 0)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*coinbase.TxIDChainHash(), 100, 0))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())
	// now create a subtree with the coinbase to calculate the merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint: gosec

	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		//nolint:gosec
		Timestamp: uint32(time.Now().Unix()),
		Bits:      *nBits,
		Nonce:     0,
	}

	// mine to the target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	tSettings := test.CreateBaseTestSettings(t)

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(subtree.Length()), // nolint: gosec
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes, // should be the subtree with placeholder
	}

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.GlobalBlockHeightRetention = uint32(0)
	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	start := time.Now()
	// err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000")
	err = blockValidation.ValidateBlock(context.Background(), block, "legacy")
	require.Error(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}

// TestInvalidBlockWithoutGenesisBlock verifies that blocks not properly
// connected to the genesis block are rejected. This test ensures the blockchain's
// fundamental requirement of an unbroken chain back to the genesis block,
// preventing any potential chain splits or invalid block sequences.
func TestInvalidBlockWithoutGenesisBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	// Create parent transaction for tx2, tx3, tx4 (they all reference parentTx)
	// Use WithMinedBlockInfo to set BlockID to 0 (GenesisBlockID) so validation passes
	_, err = utxoStore.Create(context.Background(), parentTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}))
	require.NoError(t, err)

	// Create tx1 with a parent reference to parentTx
	tx1Test := newTx(10, parentTx.TxIDChainHash())
	hash1Test := tx1Test.TxIDChainHash()

	require.NoError(t, subtree.AddNode(*hash1Test, 100, 0))
	require.NoError(t, subtree.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree.AddNode(*hash3, 100, 0))

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx1Test))
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx2))
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx3))

	_, err = utxoStore.Create(context.Background(), tx1Test, 0)
	require.NoError(t, err)

	_, err = utxoStore.Create(context.Background(), tx2, 0)
	require.NoError(t, err)

	_, err = utxoStore.Create(context.Background(), tx3, 0)
	require.NoError(t, err)

	_, err = utxoStore.Create(context.Background(), tx4, 0)
	require.NoError(t, err)

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil

	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())
	// now create a subtree with the coinbase to calculate the merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint: gosec

	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		//nolint:gosec
		Timestamp: uint32(time.Now().Unix()),
		Bits:      *nBits,
		Nonce:     0,
	}

	// mine to the target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	tSettings := test.CreateBaseTestSettings(t)
	// Reduce retry settings for faster test failure
	tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 2

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(subtree.Length()), // nolint: gosec
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes, // should be the subtree with placeholder
	}

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.GlobalBlockHeightRetention = uint32(0)
	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	start := time.Now()

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000")
	require.Error(t, err)

	t.Logf("Time taken: %s\n", time.Since(start))

	// With parent transaction validation fix, blocks now properly validate parent existence
	// This block should still be invalid (either SERVICE_ERROR, BLOCK_INVALID, or BLOCK_ERROR depending on which check fails first)
	require.True(t, errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrBlockInvalid) || errors.Is(err, errors.ErrBlockError),
		"Expected either SERVICE_ERROR (no genesis connection), BLOCK_INVALID, or BLOCK_ERROR, got: %v", err)
}

func TestInvalidChainWithoutGenesisBlock(t *testing.T) {
	// Given: A blockchain setup with a chain that does not connect to the Genesis block
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	// Ensure transactions are stored
	t.Logf("Storing transactions in utxoStore...")

	txns := []*bt.Tx{tx1, tx2, tx3, tx4}
	for i, tx := range txns {
		_, err := utxoStore.Create(context.Background(), tx, 0)
		require.NoError(t, err, "Failed to store tx%d: %v", i+1, tx.TxIDChainHash())
		t.Logf("Stored tx%d: %s", i+1, tx.TxIDChainHash())
	}

	// Build the chain starting from a block that does not connect to the Genesis block
	numBlocks := 5
	previousBlockHash, _ := chainhash.NewHashFromStr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")

	var blocks []*model.Block

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 2

	for i := 0; i < numBlocks; i++ {
		subtree, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, subtree.AddCoinbaseNode())

		require.NoError(t, subtree.AddNode(*hash1, 100, 0))
		require.NoError(t, subtree.AddNode(*hash2, 100, 0))
		require.NoError(t, subtree.AddNode(*hash3, 100, 0))

		nodeBytes, err := subtree.SerializeNodes()
		require.NoError(t, err)

		httpmock.RegisterResponder(
			"GET",
			`=~^/subtree/[a-z0-9]+\z`,
			httpmock.NewBytesResponder(200, nodeBytes),
		)

		nBits, _ := model.NewNBitFromString("2000ffff")

		coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
		require.NoError(t, err)

		coinbase.Outputs = nil

		_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes, options.WithAllowOverwrite(true))
		require.NoError(t, err)

		subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
		// now create a subtree with the coinbase to calculate the merkle root
		replicatedSubtree := subtree.Duplicate()
		replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint: gosec

		calculatedMerkleRootHash := replicatedSubtree.RootHash()

		blockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  previousBlockHash,
			HashMerkleRoot: calculatedMerkleRootHash,
			//nolint:gosec
			Timestamp: uint32(time.Now().Unix()),
			Bits:      *nBits,
			Nonce:     0,
		}

		// Mine to the target difficulty
		for {
			if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
				break
			}

			blockHeader.Nonce++
		}

		block, err := model.NewBlock(
			blockHeader,
			coinbase,
			subtreeHashes,            // should be the subtree with placeholder
			uint64(subtree.Length()), // nolint:gosec
			123123,
			0, 0)
		require.NoError(t, err)

		blocks = append(blocks, block)
		previousBlockHash = block.Header.Hash() // Update the previous block hash for the next block
	}

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.GlobalBlockHeightRetention = uint32(0)
	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// When: The last block in the chain is validated
	start := time.Now()
	err = blockValidation.ValidateBlock(context.Background(), blocks[len(blocks)-1], "http://localhost:8000")

	// Then: An error should be received because the chain does not connect to the Genesis block
	require.Error(t, err)
	t.Logf("Error received as expected: %s", err.Error())
	t.Logf("Time taken: %s\n", time.Since(start))
}

// TestBlockValidationMerkleTreeValidation implements the TNE-1.2 specification
// requirement that Teranode must accurately calculate and validate merkle trees
// for all blocks. This test verifies both successful validation of correct
// merkle roots and proper rejection of blocks with invalid merkle roots.
//
// The test covers:
// - Correct merkle root calculation from transaction set
// - Validation of valid merkle root values
// - Detection and rejection of invalid merkle roots
// - Proper handling of block header verification
func TestBlockValidationMerkleTreeValidation(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)

	txCount := 4 // We'll use 4 transactions to keep it simple
	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)

	defer deferFunc()

	// Create a subtree with our transactions
	subtree, err := subtreepkg.NewTreeByLeafCount(txCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)

	// Add transactions to the subtree
	fees := 0

	for i := 0; i < txCount-1; i++ {
		// Create a parent transaction and add it to the UTXO store
		// This ensures that when we create child transactions, their parents exist
		// Use WithMinedBlockInfo to set BlockID to 0 (GenesisBlockID) so validation passes
		parentTx := newTx(uint32(i + 10000)) // Create parent without parent (no second param)
		_, err = utxoStore.Create(context.Background(), parentTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}))
		require.NoError(t, err)

		tx := newTx(uint32(i), parentTx.TxIDChainHash()) //nolint:gosec

		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 100, 0))
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx))

		fees += 100

		_, err = utxoStore.Create(context.Background(), tx, 0)
		require.NoError(t, err)
	}

	// Create coinbase transaction
	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+uint64(fees)) //nolint:gosec

	// Store subtree
	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	// Mock the HTTP endpoint for the subtree
	httpmock.RegisterResponder(
		"GET",
		fmt.Sprintf("/subtree/%s", subtree.RootHash().String()),
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	// Create subtree hashes and calculate merkle root
	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	// Create block header with correct merkle root
	nBits, _ := model.NewNBitFromString("207fffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	//nolint:gosec
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *nBits,
		Nonce:          0,
	}

	// mine to target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	// Create the block with valid merkle root
	validBlock, err := model.NewBlock(
		blockHeader,
		coinbase,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		123123,
		101, 0)
	require.NoError(t, err)

	// Setup blockchain store and client
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create block validation instance
	tSettings.BlockValidation.OptimisticMining = false

	tSettings.GlobalBlockHeightRetention = uint32(0)
	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Test valid merkle root
	err = blockValidation.ValidateBlock(context.Background(), validBlock, "http://localhost:8000")
	require.NoError(t, err, "Block validation should succeed with valid merkle root")

	// Create block with invalid merkle root
	invalidBlockHeader := &model.BlockHeader{
		Version:        blockHeader.Version,
		HashPrevBlock:  blockHeader.HashPrevBlock,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      blockHeader.Timestamp,
		Bits:           blockHeader.Bits,
		Nonce:          blockHeader.Nonce,
	}

	invalidBlockHeader.HashMerkleRoot = coinbase.TxIDChainHash() // Invalid merkle root

	// Mine the invalid block to meet target difficulty
	for {
		if ok, _, _ := invalidBlockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		invalidBlockHeader.Nonce++
	}

	invalidBlock, err := model.NewBlock(
		invalidBlockHeader,
		coinbase,
		[]*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), //nolint:gosec
		123123,
		0, 0)
	require.NoError(t, err)

	// Test invalid merkle root
	err = blockValidation.ValidateBlock(context.Background(), invalidBlock, "http://localhost:8000")
	require.ErrorContains(t, err, "merkle root does not match")
}

// TestBlockValidationRequestMissingTransaction implements the TNE-2 specification
// requirement for handling missing transactions. When Teranode encounters an unknown
// transaction within a block, it must request and validate that transaction from
// the proposing node according to section 3.11 requirements.
//
// The test verifies:
// - Detection of missing transactions during block validation
// - Successful retrieval of missing transactions from peer nodes
// - Proper validation of retrieved transactions
// - Integration of validated transactions into the transaction store
// - Completion of block validation after resolving missing transactions
func TestBlockValidationRequestMissingTransaction(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tSettings := test.CreateBaseTestSettings(t)

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	// Create a chain of transactions
	txs := transactions.CreateTestTransactionChainWithCount(t, 5) // coinbase + 3 transactions
	tx0, tx1, tx2, tx3 := txs[0], txs[1], txs[2], txs[3]

	// Store all transactions except tx3 (which will be our missing transaction)
	for _, tx := range txs[:3] { // Store all except tx3
		_, err := utxoStore.Create(context.Background(), tx, 100)
		require.NoError(t, err)
	}

	blockIDsMap, err := utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{tx0.TxIDChainHash()}, utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 1,
		SubtreeIdx:  0,
	})
	require.NoError(t, err)
	require.Len(t, blockIDsMap, 1)
	require.Equal(t, []uint32{0}, blockIDsMap[*tx0.TxIDChainHash()])

	// Create a subtree with our transactions
	subtree, err := subtreepkg.NewTreeByLeafCount(4) // 4 transactions including coinbase
	require.NoError(t, err)

	// Add coinbase placeholder
	require.NoError(t, subtree.AddCoinbaseNode())

	// Add transactions to subtree
	for i, tx := range []*bt.Tx{tx1, tx2, tx3} {
		hash := tx.TxIDChainHash()
		require.NoError(t, subtree.AddNode(*hash, uint64(tx.Size()), uint64(i))) //nolint:gosec
	}

	require.True(t, subtree.IsComplete(), "Subtree should be complete")
	require.Equal(t, subtree.Length(), 4, "Subtree should have 4 transactions")

	// Calculate total fees for coinbase
	fees := uint64(0)
	for _, tx := range []*bt.Tx{tx1, tx2, tx3} {
		fees += tx.TotalInputSatoshis() - tx.TotalOutputSatoshis()
	}

	// Create block header
	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock := chaincfg.RegressionNetParams.GenesisBlock.BlockHash()

	// Calculate merkle root using coinbase and subtree
	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &hashPrevBlock,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	// Mine to target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	// Create the block
	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		func() uint64 {
			totalSize := int64(0)

			for _, tx := range []*bt.Tx{coinbaseTx, tx1, tx2, tx3} {
				size := tx.Size()
				if size < 0 {
					t.Fatal("negative transaction size")
				}

				totalSize += int64(size)
			}

			if totalSize < 0 {
				t.Fatal("negative total size")
			}

			return uint64(totalSize)
		}(),
		0, 0)

	require.NoError(t, err)

	// Setup blockchain store and client
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Mock HTTP endpoints
	httpmock.RegisterResponder(
		"GET",
		fmt.Sprintf("/subtree/%s", subtreeHashes[0].String()),
		func(req *http.Request) (*http.Response, error) {
			if subtree.Length() < 0 {
				return nil, errors.NewError("negative subtree length")
			}

			nodes := subtree.Nodes

			var responseBytes []byte
			for _, node := range nodes {
				responseBytes = append(responseBytes, node.Hash[:]...)
			}

			return httpmock.NewBytesResponse(200, responseBytes), nil
		},
	)

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(tx1, 1))
	require.NoError(t, subtreeData.AddTx(tx2, 2))
	require.NoError(t, subtreeData.AddTx(tx3, 3))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree_data/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, subtreeDataBytes),
	)

	httpmock.RegisterRegexpMatcherResponder(
		"POST",
		regexp.MustCompile("/subtree/[a-fA-F0-9]{64}/txs$"),
		httpmock.Matcher{},
		func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}

			numHashes := len(body) / 32

			var responseBytes []byte

			for i := 0; i < numHashes; i++ {
				hashBytes := body[i*32 : (i+1)*32]
				hash, err := chainhash.NewHash(hashBytes)
				require.NoError(t, err)

				var tx *bt.Tx

				switch hash.String() {
				case tx1.TxIDChainHash().String():
					tx = tx1
				case tx2.TxIDChainHash().String():
					tx = tx2
				case tx3.TxIDChainHash().String():
					tx = tx3
				default:
					return nil, errors.NewError("unexpected hash in request: " + hash.String())
				}

				responseBytes = append(responseBytes, tx.ExtendedBytes()...)
			}

			return httpmock.NewBytesResponse(200, responseBytes), nil
		},
	)

	// Create block validation instance with optimistic mining disabled
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.GlobalBlockHeightRetention = uint32(0)
	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Test block validation - it should request the missing transaction
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000")
	require.NoError(t, err, "Block validation should succeed after retrieving missing transaction")

	// Verify that the missing transaction was stored
	exists, err := utxoStore.Get(context.Background(), tx3.TxIDChainHash())
	require.NoError(t, err)
	require.NotEmpty(t, exists, "The missing transaction should have been stored after validation")
}

func TestBlockValidationExcessiveBlockSize(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tests := []struct {
		name               string
		excessiveBlockSize int
		blockSize          uint64
		expectError        bool
		errorMessage       string
	}{
		{
			name:               "Block size within limit",
			excessiveBlockSize: 1000000,
			blockSize:          999999,
			expectError:        false,
		},
		{
			name:               "Block size equals limit",
			excessiveBlockSize: 1000000,
			blockSize:          1000000,
			expectError:        false,
		},
		{
			name:               "Block size exceeds limit",
			excessiveBlockSize: 1000000,
			blockSize:          1000001,
			expectError:        true,
			errorMessage:       "block size 1000001 exceeds excessiveblocksize 1000000",
		},
		{
			name:               "Zero excessive block size (unlimited)",
			excessiveBlockSize: 0,
			blockSize:          5000000000,
			expectError:        false,
		},
		{
			name:               "Very large block within 4GB default",
			excessiveBlockSize: 4294967296,
			blockSize:          4294967295,
			expectError:        false,
		},
		{
			name:               "Block exceeds 4GB default",
			excessiveBlockSize: 4294967296,
			blockSize:          4294967297,
			expectError:        true,
			errorMessage:       "block size 4294967297 exceeds excessiveblocksize 4294967296",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
			defer cleanup()

			// Create test settings with specified excessive block size
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Policy.ExcessiveBlockSize = tt.excessiveBlockSize

			// Create blockchain store
			blockchainStoreURL, err := url.Parse("sqlitememory://")
			require.NoError(t, err)
			blockchainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, blockchainStoreURL, tSettings)
			require.NoError(t, err)

			// Create blockchain client
			blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
			require.NoError(t, err)

			tSettings.GlobalBlockHeightRetention = uint32(1)

			// Create block validator with 10 minute bloom filter expiration
			blockValidator := NewBlockValidation(
				ctx, // Use the context with cancel
				ulogger.TestLogger{},
				tSettings,
				blockchainClient,
				subtreeStore,
				txStore,
				utxoStore,
				nil,
				subtreeValidationClient,
			)

			// Create a valid block header
			nBits, _ := model.NewNBitFromString("2000ffff")
			hashPrevBlock := chaincfg.RegressionNetParams.GenesisHash
			merkleRoot := chainhash.Hash{}

			blockHeader := &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  hashPrevBlock,
				HashMerkleRoot: &merkleRoot,
				Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
				Bits:           *nBits,
				Nonce:          0,
			}

			// Create a test block with specified size and valid header
			block := &model.Block{
				Header:           blockHeader,
				SizeInBytes:      tt.blockSize,
				TransactionCount: 1,
				CoinbaseTx:       tx1,                 // Using tx1 from test setup
				Subtrees:         []*chainhash.Hash{}, // Initialize empty subtrees slice
			}
			// Set the settings to avoid nil pointer dereference

			// Validate the block
			err = blockValidator.ValidateBlock(ctx, block, "test")

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
			} else if err != nil {
				// For non-error cases, we only care about the excessive block size check
				// If there are other validation errors, we can ignore them
				require.NotContains(t, err.Error(), "exceeds excessiveblocksize")
			}
		})
	}
}

func Test_validateBlockSubtrees(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tSettings := test.CreateBaseTestSettings(t)

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}

	subtree1, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree1.AddCoinbaseNode())
	require.NoError(t, subtree1.AddNode(*hash1, 100, 0))

	subtree2, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree2.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree2.AddNode(*hash3, 100, 0))

	t.Run("smoke test", func(t *testing.T) {
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
		subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, nil, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

		block := &model.Block{
			Header:   blockHeader,
			Subtrees: make([]*chainhash.Hash, 0),
		}

		err = blockValidation.validateBlockSubtrees(t.Context(), block, "", "http://localhost:8000")
		require.NoError(t, err)
	})

	t.Run("processing in parallel", func(t *testing.T) {
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
		subtreeValidationClient.Mock.On("CheckSubtreeFromBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, nil, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

		block := &model.Block{
			Header:           blockHeader,
			SizeInBytes:      123,
			TransactionCount: 2,
			CoinbaseTx:       tx1,
			Subtrees: []*chainhash.Hash{
				subtree1.RootHash(),
				subtree2.RootHash(),
			},
		}

		require.NoError(t, blockValidation.validateBlockSubtrees(t.Context(), block, "", "http://localhost:8000"))
	})

	t.Run("fallback to series", func(t *testing.T) {
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
		// First call - for subtree1 - success
		subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Once().
			Run(func(args mock.Arguments) {
				// subtree was validated properly, let's add it to our subtree store so it doesn't get checked again
				subtreeBytes, err := subtree1.Serialize()
				require.NoError(t, err)

				require.NoError(t, subtreeStore.Set(context.Background(), subtree1.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))
			})

		blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, nil, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

		block := &model.Block{
			Header:           blockHeader,
			SizeInBytes:      123,
			TransactionCount: 2,
			CoinbaseTx:       tx1,
			Subtrees: []*chainhash.Hash{
				subtree1.RootHash(),
				subtree2.RootHash(),
			},
		}

		require.NoError(t, blockValidation.validateBlockSubtrees(t.Context(), block, "", "http://localhost:8000"))

		// check that the subtree validation was called 3 times
		assert.Len(t, subtreeValidationClient.Calls, 1)
	})
}

func TestBlockValidation_InvalidCoinbaseScriptLength(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)

	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create a valid block, then tamper with coinbase script length
	block := createValidBlock(t, tSettings, txMetaStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore)
	block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01}) // Too short

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.ErrorContains(t, err, "BLOCK_INVALID")
}

func createValidBlock(t *testing.T, tSettings *settings.Settings, txMetaStore utxostore.Store, subtreeValidationClient subtreevalidation.Interface, blockchainClient blockchain.ClientI, txStore blob.Store, subtreeStore blob.Store) *model.Block {
	// Create a private key and address for outputs
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// Create coinbase transaction
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From(
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xffffffff,
		"",
		0,
	)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	// Create a normal transaction spending the coinbase
	tx1 := bt.NewTx()

	err = tx1.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = tx1.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	require.NoError(t, err)
	err = tx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	// Store transactions in txMetaStore
	_, err = txMetaStore.Create(context.Background(), coinbaseTx, 0)
	require.NoError(t, err)
	_, err = txMetaStore.Create(context.Background(), tx1, 0)
	require.NoError(t, err)

	// Create a subtree with coinbase and tx1
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), uint64(tx1.Size()), 0)) //nolint:gosec

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Calculate merkle root with coinbase
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	// Mine to target difficulty
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()),             //nolint:gosec
		uint64(coinbaseTx.Size()+tx1.Size()), //nolint:gosec
		100, 0,
	)
	require.NoError(t, err)

	return block
}

func TestBlockValidation_DoubleSpendInBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create a valid block, then add two txs that spend the same UTXO
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Coinbase
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	// add the coinbase to the utxo store
	_, err = utxoStore.Create(t.Context(), coinbaseTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 1,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	// Two double-spend txs
	tx1 := bt.NewTx()
	tx1.Version = 1
	_ = tx1.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = tx1.AddP2PKHOutputFromAddress(address.AddressString, 25*100000000)
	_ = tx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	tx2 := bt.NewTx()
	tx2.Version = 2
	_ = tx2.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = tx2.AddP2PKHOutputFromAddress(address.AddressString, 25*100000000)
	_ = tx2.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	// Store in utxoStore
	_, err = utxoStore.Create(context.Background(), tx1, 2)
	require.NoError(t, err)

	// since this was a double spend it should be marked as conflicting
	_, err = utxoStore.Create(context.Background(), tx2, 2, utxostore.WithConflicting(true))
	require.NoError(t, err)

	err = utxoStore.SetBlockHeight(2)
	require.NoError(t, err)

	// Subtree with both double-spend txs
	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	subtreeData := subtreepkg.NewSubtreeData(subtree)

	_ = subtree.AddCoinbaseNode()
	_ = subtree.AddNode(*tx1.TxIDChainHash(), uint64(tx1.Size()), 0) //nolint:gosec
	_ = subtree.AddNode(*tx2.TxIDChainHash(), uint64(tx2.Size()), 1) //nolint:gosec

	require.NoError(t, subtreeData.AddTx(tx1, 1))
	require.NoError(t, subtreeData.AddTx(tx2, 2))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes)
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("207fffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(coinbaseTx.Size()+tx1.Size()+tx2.Size()), //nolint:gosec
		100, 0,
	)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost")
	require.Error(t, err)
	require.ErrorContains(t, err, "BLOCK_INVALID")
	require.ErrorContains(t, err, "has duplicate inputs")
}

func TestBlockValidation_InvalidTransactionChainOrdering(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)

	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Coinbase
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	// tx2 spends output of tx1, but tx2 is placed before tx1 in the block
	tx1 := bt.NewTx()
	_ = tx1.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = tx1.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	_ = tx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	tx2 := bt.NewTx()
	_ = tx2.FromUTXOs(&bt.UTXO{
		TxIDHash:      tx1.TxIDChainHash(),
		Vout:          0,
		LockingScript: tx1.Outputs[0].LockingScript,
		Satoshis:      tx1.Outputs[0].Satoshis,
	})
	_ = tx2.AddP2PKHOutputFromAddress(address.AddressString, 48*100000000)
	_ = tx2.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	// Store in txMetaStore
	_, _ = txMetaStore.Create(context.Background(), coinbaseTx, 0)
	_, _ = txMetaStore.Create(context.Background(), tx1, 0)
	_, _ = txMetaStore.Create(context.Background(), tx2, 0)

	// Subtree: coinbase, tx2, tx1 (wrong order: tx2 before tx1)
	subtree, _ := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), uint64(tx2.Size()), 0)) //nolint:gosec
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), uint64(tx1.Size()), 1)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx2))
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx1))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("207fffff")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(coinbaseTx.Size()+tx1.Size()+tx2.Size()), //nolint:gosec
		100, 0,
	)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.Error(t, err)
	require.ErrorContains(t, err, "BLOCK_INVALID")
	require.ErrorContains(t, err, "comes before parent transaction")
}

func TestBlockValidation_InvalidParentBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)

	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Coinbase
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	_, err = txMetaStore.Create(t.Context(), coinbaseTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 0,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	// Normal tx spending coinbase
	tx1 := bt.NewTx()
	_ = tx1.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = tx1.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	_ = tx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	// Store transactions
	_, _ = txMetaStore.Create(context.Background(), coinbaseTx, 0)
	_, _ = txMetaStore.Create(context.Background(), tx1, 0)

	// Subtree: coinbase, tx1
	subtree, _ := subtreepkg.NewTreeByLeafCount(2)
	_ = subtree.AddCoinbaseNode()
	_ = subtree.AddNode(*tx1.TxIDChainHash(), uint64(tx1.Size()), 0) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx1))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	// Use a random hash for HashPrevBlock (invalid parent)
	invalidPrevBlockHash := chainhash.DoubleHashH([]byte("invalid-parent-block"))

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &invalidPrevBlockHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()),             //nolint:gosec
		uint64(coinbaseTx.Size()+tx1.Size()), //nolint:gosec
		100, 0,
	)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.Error(t, err)
}

func Test_checkOldBlockIDs(t *testing.T) {
	initPrometheusMetrics()

	t.Run("empty map", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}
		blockValidation := &BlockValidation{
			blockchainClient: blockchainMock,
		}

		oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		blockchainMock.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{}, nil).Once()

		err := blockValidation.checkOldBlockIDs(t.Context(), oldBlockIDsMap, &model.Block{})
		require.NoError(t, err)
	})

	t.Run("empty parents", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}
		blockValidation := &BlockValidation{
			blockchainClient: blockchainMock,
		}

		oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		for i := uint32(0); i < 100; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("txHash_%d", i)))
			oldBlockIDsMap.Set(txHash, []uint32{})
		}

		blockchainMock.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{}, nil).Once()

		err := blockValidation.checkOldBlockIDs(t.Context(), oldBlockIDsMap, &model.Block{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "blockIDs is empty for txID")
	})

	t.Run("simple map", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}
		blockValidation := &BlockValidation{
			blockchainClient: blockchainMock,
		}

		oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		blockIDs := make([]uint32, 0, 100)

		for i := uint32(0); i < 100; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("txHash_%d", i)))
			oldBlockIDsMap.Set(txHash, []uint32{i})

			blockIDs = append(blockIDs, i)
		}

		blockchainMock.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil)
		blockchainMock.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return(blockIDs, nil).Once()

		err := blockValidation.checkOldBlockIDs(t.Context(), oldBlockIDsMap, &model.Block{})
		require.NoError(t, err)
	})

	t.Run("lookup and use cache", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}
		blockValidation := &BlockValidation{
			blockchainClient: blockchainMock,
		}

		oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		for i := uint32(0); i < 100; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("txHash_%d", i)))
			oldBlockIDsMap.Set(txHash, []uint32{1})
		}

		blockchainMock.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil).Once()
		blockchainMock.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{}, nil).Once()

		err := blockValidation.checkOldBlockIDs(t.Context(), oldBlockIDsMap, &model.Block{})
		require.NoError(t, err)
	})

	t.Run("not present", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}
		blockValidation := &BlockValidation{
			blockchainClient: blockchainMock,
		}

		oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		for i := uint32(0); i < 100; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("txHash_%d", i)))
			oldBlockIDsMap.Set(txHash, []uint32{1})
		}

		blockchainMock.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(false, nil).Once()
		blockchainMock.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{}, nil).Once()

		err := blockValidation.checkOldBlockIDs(t.Context(), oldBlockIDsMap, &model.Block{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "are not from current chain")
	})
}

func TestBlockValidation_ParentAndChildInSameBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Coinbase
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	_, err = txMetaStore.Create(t.Context(), coinbaseTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 0,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	// parentTx spends coinbase
	parentTx := bt.NewTx()
	_ = parentTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = parentTx.AddP2PKHOutputFromAddress(address.AddressString, 1e8)
	_ = parentTx.AddP2PKHOutputFromAddress(address.AddressString, 1e8)
	_ = parentTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	// childTx1 spends parentTx
	childTx1 := bt.NewTx()
	_ = childTx1.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          1,
		LockingScript: parentTx.Outputs[1].LockingScript,
		Satoshis:      parentTx.Outputs[1].Satoshis,
	})
	_ = childTx1.AddP2PKHOutputFromAddress(address.AddressString, parentTx.Outputs[1].Satoshis-1000)
	_ = childTx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	childTx2 := bt.NewTx()
	_ = childTx2.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	})
	_ = childTx2.AddP2PKHOutputFromAddress(address.AddressString, parentTx.Outputs[0].Satoshis-1000)
	_ = childTx2.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	_, err = txMetaStore.Create(context.Background(), parentTx, 101, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     101,
		BlockHeight: 101,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), childTx1, 101, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     101,
		BlockHeight: 101,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), childTx2, 101, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     101,
		BlockHeight: 101,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	// Subtree: coinbase, tx1, tx2 (correct order)
	subtree, _ := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*parentTx.TxIDChainHash(), uint64(parentTx.Size()), 0)) //nolint:gosec
	require.NoError(t, subtree.AddNode(*childTx1.TxIDChainHash(), uint64(childTx1.Size()), 1)) //nolint:gosec
	require.NoError(t, subtree.AddNode(*childTx2.TxIDChainHash(), uint64(childTx2.Size()), 2)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(parentTx))
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx1))
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx2))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("207fffff")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(coinbaseTx.Size()+parentTx.Size()+childTx1.Size()), //nolint:gosec
		100, 0,
	)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.NoError(t, err, "Block with parent and child tx in correct order should be valid")
}

// TestBlockValidation_TransactionChainInSameBlock validates a block containing a chain of transactions (parent -> child -> grandchild, etc.) all mined in the same block using the transaction chain utility.
func TestBlockValidation_TransactionChainInSameBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create a chain of transactions: coinbase -> tx1 -> tx2 -> ...
	chainLen := uint32(6)
	txs := transactions.CreateTestTransactionChainWithCount(t, chainLen)
	coinbaseTx := txs[0]

	// Store all transactions in the txMetaStore
	for i, tx := range txs {
		blockID := uint32(0)
		blockHeight := uint32(0)

		if i > 0 {
			blockID = 101
			blockHeight = 101
		}

		_, err := txMetaStore.Create(context.Background(), tx, blockID, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
			BlockID:     blockID,
			BlockHeight: blockHeight,
			SubtreeIdx:  0,
		}))
		require.NoError(t, err)
	}

	// Build the subtree for all non-coinbase txs (chain)
	subtree, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	for i, tx := range txs[1:] {
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), uint64(tx.Size()), uint64(i))) //nolint:gosec
	}

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	for _, tx := range txs[1:] {
		require.NoError(t, subtreeMeta.SetTxInpointsFromTx(tx))
	}

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("207fffff")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	totalSize := int64(coinbaseTx.Size()) //nolint:gosec

	for _, tx := range txs[1:] {
		size := tx.Size()
		if size < 0 {
			t.Fatal("negative transaction size")
		}

		totalSize += int64(size)
	}

	if totalSize < 0 {
		t.Fatal("negative total size")
	}

	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(totalSize),        //nolint:gosec
		100, 0,
	)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.NoError(t, err, "Block with transaction chain in correct order should be valid")
}

// TestBlockValidation_DuplicateTransactionInBlock ensures that a block containing the exact same transaction (same txid and content) more than once is rejected as invalid.
func TestBlockValidation_DuplicateTransactionInBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create coinbase and a normal transaction
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	// Normal tx spending coinbase
	normalTx := bt.NewTx()
	_ = normalTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = normalTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	_ = normalTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	// Store both in txMetaStore
	_, err = txMetaStore.Create(context.Background(), coinbaseTx, 0)
	require.NoError(t, err)
	_, err = txMetaStore.Create(context.Background(), normalTx, 0)
	require.NoError(t, err)

	// Build subtree with coinbase and the same normalTx twice
	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*normalTx.TxIDChainHash(), 1, uint64(normalTx.Size()))) //nolint:gosec
	require.NoError(t, subtree.AddNode(*normalTx.TxIDChainHash(), 1, uint64(normalTx.Size()))) //nolint:gosec
	require.NoError(t, subtree.AddNode(*normalTx.TxIDChainHash(), 1, uint64(normalTx.Size()))) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(normalTx))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("207fffff")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	totalSize := int64(coinbaseTx.Size()) + 2*int64(normalTx.Size())

	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(totalSize),        //nolint:gosec
		100, 0,
	)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.Error(t, err, "Block with duplicate transaction should be invalid")
	// Optionally check for a specific error message if the implementation provides one
	require.ErrorContains(t, err, "duplicate transaction")
}

func TestBlockValidation_RevalidateIsCalledOnHeaderError(t *testing.T) {
	// Setup minimal block and dependencies
	ctx := context.Background()

	// Create a mock blockchain client
	mockBlockchain := &blockchain.Mock{}
	// Mock GetNextWorkRequired for difficulty validation (in case it's called)
	defaultNBits6, _ := model.NewNBitFromString("2000ffff")
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(defaultNBits6, nil).Maybe()

	// Simulate GetBlockHeaders returns error
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewError("header fetch error"))
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// Other dependencies can be nil or no-op for this test
	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}

	// Use a custom revalidateBlockChan to observe the call
	revalidateChan := make(chan revalidateBlockData, 1)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              mockBlockchain,
		subtreeStore:                  subtreeStore,
		txStore:                       txStore,
		utxoStore:                     utxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		subtreeDeDuplicator:           NewDeDuplicator(0),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		setMinedChan:                  make(chan *chainhash.Hash, 1),
		revalidateBlockChan:           revalidateChan,
		stats:                         gocore.NewStat("blockvalidation"),
	}

	// Create a valid coinbase transaction
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From(
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xffffffff,
		"",
		0,
	)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))
	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
			Bits:           model.NBit{},
			Nonce:          0,
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		SizeInBytes:      uint64(coinbaseTx.Size()), //nolint:gosec
		Subtrees:         subtreeHashes,
	}

	// Call ValidateBlock (should trigger ReValidateBlock due to header error)
	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)
	t.Logf("ValidateBlock error: %v", err)

	// Check that the block was put on the revalidateBlockChan
	select {
	case data := <-revalidateChan:
		require.Equal(t, block, data.block)
		// Optionally check baseURL, retries, etc.
	case <-time.After(1 * time.Second):
		t.Fatal("ReValidateBlock was not called (block not put on revalidateBlockChan)")
	}
}

// Helper for revalidate block tests
func setupRevalidateBlockTest(t *testing.T) (*BlockValidation, *model.Block, *blockchain.Mock, *bt.Tx) {
	initPrometheusMetrics()

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}}, []*model.BlockHeaderMeta{{}}, nil)

	mockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return([]chainhash.Hash{}, nil)
	// Mock GetNextWorkRequired for difficulty validation
	defaultNBits, _ := model.NewNBitFromString("2000ffff")
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(defaultNBits, nil)
	// Mock GetBestBlockHeader for bloom filter pruning
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 200}, nil)

	tSettings := test.CreateBaseTestSettings(t)
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	bv := &BlockValidation{
		logger:                        ulogger.TestLogger{},
		settings:                      tSettings,
		blockchainClient:              mockBlockchain,
		subtreeStore:                  subtreeStore,
		txStore:                       txStore,
		utxoStore:                     txMetaStore,
		subtreeValidationClient:       subtreeValidationClient,
		subtreeDeDuplicator:           NewDeDuplicator(0),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		setMinedChan:                  make(chan *chainhash.Hash, 1),
		revalidateBlockChan:           make(chan revalidateBlockData, 1),
		stats:                         gocore.NewStat("blockvalidation"),
	}

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Coinbase
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	_, err := txMetaStore.Create(t.Context(), coinbaseTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 0,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	// parentTx spends coinbase
	parentTx := bt.NewTx()
	_ = parentTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = parentTx.AddP2PKHOutputFromAddress(address.AddressString, 1e8)
	_ = parentTx.AddP2PKHOutputFromAddress(address.AddressString, 1e8)
	_ = parentTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	// childTx1 spends parentTx
	childTx1 := bt.NewTx()
	_ = childTx1.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          1,
		LockingScript: parentTx.Outputs[1].LockingScript,
		Satoshis:      parentTx.Outputs[1].Satoshis,
	})
	_ = childTx1.AddP2PKHOutputFromAddress(address.AddressString, parentTx.Outputs[1].Satoshis-1000)
	_ = childTx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	childTx2 := bt.NewTx()
	_ = childTx2.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	})
	_ = childTx2.AddP2PKHOutputFromAddress(address.AddressString, parentTx.Outputs[0].Satoshis-1000)
	_ = childTx2.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	_, err = txMetaStore.Create(context.Background(), parentTx, 101, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     101,
		BlockHeight: 101,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), childTx1, 101, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     101,
		BlockHeight: 101,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), childTx2, 101, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     101,
		BlockHeight: 101,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	// Subtree: coinbase, tx1, tx2 (correct order)
	subtree, _ := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*parentTx.TxIDChainHash(), uint64(parentTx.Size()), 0)) //nolint:gosec
	require.NoError(t, subtree.AddNode(*childTx1.TxIDChainHash(), uint64(childTx1.Size()), 1)) //nolint:gosec
	require.NoError(t, subtree.AddNode(*childTx2.TxIDChainHash(), uint64(childTx2.Size()), 2)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(parentTx))
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx1))
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx2))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("2000ffff")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(coinbaseTx.Size()+parentTx.Size()+childTx1.Size()), //nolint:gosec
		100, 0,
	)

	// Update the GetBlockHeaders mock to return the actual block header
	// This ensures the bloom filter hash matches between storage and retrieval
	for _, call := range mockBlockchain.ExpectedCalls {
		if call.Method == "GetBlockHeaders" {
			call.ReturnArguments = mock.Arguments{[]*model.BlockHeader{blockHeader}, []*model.BlockHeaderMeta{{ID: 100}}, nil}
			break
		}
	}

	mockBlockchain.On("GetBlock", mock.Anything, mock.Anything).Return(block, nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{100}, nil)

	return bv, block, mockBlockchain, coinbaseTx
}

func TestBlockValidation_reValidateBlock_Success(t *testing.T) {
	bv, block, _, _ := setupRevalidateBlockTest(t)

	blockData := revalidateBlockData{
		block:   block,
		baseURL: "test",
	}

	err := bv.reValidateBlock(blockData)
	require.NoError(t, err)
}

func TestBlockValidation_reValidateBlock_InvalidBlock(t *testing.T) {
	bv, block, mockBlockchain, coinbaseTx := setupRevalidateBlockTest(t)

	// Make the block invalid by corrupting the coinbase output value to be too high
	// This will be caught by the block reward validation in checkBlockRewardAndFees
	originalSatoshis := coinbaseTx.Outputs[0].Satoshis
	coinbaseTx.Outputs[0].Satoshis = originalSatoshis * 1000 // Make it way too high

	// Step 2: Recalculate the merkle root since we modified the coinbase transaction
	// Get the subtree from the store
	subtreeBytes, err := bv.subtreeStore.Get(context.Background(), block.Subtrees[0][:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	subtreeReader := bytes.NewReader(subtreeBytes)
	subtree, err := subtreepkg.NewSubtreeFromReader(subtreeReader)
	require.NoError(t, err)

	// Recalculate merkle root with the modified coinbase transaction
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))
	newMerkleRoot := replicatedSubtree.RootHash()

	// Step 3: Update the block header with the new merkle root
	block.Header.HashMerkleRoot = newMerkleRoot

	// Step 3.1: Re-mine the block since changing merkle root changed the hash
	// Reset nonce and mine again to meet target difficulty
	block.Header.Nonce = 0
	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}
		block.Header.Nonce++
	}

	blockData := revalidateBlockData{
		block:   block,
		baseURL: "test",
	}

	err = bv.reValidateBlock(blockData)
	require.Error(t, err)

	invalidateBlockCallCount := 0
	for _, call := range mockBlockchain.Calls {
		if call.Method == "InvalidateBlock" {
			invalidateBlockCallCount++
		}
	}

	// The main purpose of this test is to verify that InvalidateBlock gets called when validation fails
	// We don't need to check the specific hash, just that it was called
	require.Greater(t, invalidateBlockCallCount, 0, "InvalidateBlock should have been called at least once")
}

func TestBlockValidation_RevalidateBlockChan_Retries(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tSettings := test.CreateBaseTestSettings(t)

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	// Create a dummy block
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	subtree, _ := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, subtree.AddCoinbaseNode())

	// Store the subtree in the subtreeStore to prevent nil dereference panics
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	// Mine the block to meet the difficulty target
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()),  //nolint:gosec
		uint64(coinbaseTx.Size()), //nolint:gosec
		100, 0,
	)

	// Use a mock blockchain client that always returns error for GetBlockHeaders
	mockBlockchain := &blockchain.Mock{}
	// Mock GetNextWorkRequired for difficulty validation
	defaultNBits2, _ := model.NewNBitFromString("2000ffff")
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(defaultNBits2, nil)

	var callCount int

	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(1)

	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewError("always fail")).Run(func(args mock.Arguments) {
		mu.Lock()
		defer mu.Unlock()

		callCount++

		if callCount == 4 {
			wg.Done()
		}
	})

	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	subChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(subChan, nil)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)

	bv.revalidateBlockChan <- revalidateBlockData{block: block}

	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("reValidateBlock was not called 4 times (initial + 3 retries)")
	}

	mu.Lock()
	require.Equal(t, 4, callCount, "reValidateBlock should be called 4 times (initial + 3 retries)")
	mu.Unlock()
}

func TestBlockValidation_OptimisticMining_InValidBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	subtree, _ := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, subtree.AddCoinbaseNode())
	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	nBits, _ := model.NewNBitFromString("207fffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, _ := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()),  //nolint:gosec
		uint64(coinbaseTx.Size()), //nolint:gosec
		100, 0,
	)

	invalidateBlockCalled := make(chan struct{})

	mockBlockchain := &blockchain.Mock{}
	// Mock GetNextWorkRequired for difficulty validation - return the same nBits as block
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil)
	mockBlockchain.On("AddBlock", mock.Anything, block, mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, block.Header.Hash()).Return([]chainhash.Hash{}, nil).Run(func(args mock.Arguments) {
		// Signal that InvalidateBlock was called
		close(invalidateBlockCalled)
	})
	mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).Return([]*model.Block{}, nil)
	subChan := make(chan *blockchain_api.Notification, 1)
	mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).Return(subChan, nil)
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil)
	mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	err = bv.ValidateBlock(ctx, block, "test", false)
	require.NoError(t, err)

	// Wait for the goroutine to call InvalidateBlock
	select {
	case <-invalidateBlockCalled:
		// Successfully received signal that InvalidateBlock was called
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateBlock should be called in background goroutine")
	}
}

// TestBlockValidation_SetMined_UpdatesTxMeta ensures that after block validation, calling setTxMined marks all block transactions as mined in the txMetaStore.
func TestBlockValidation_SetMined_UpdatesTxMeta(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Coinbase
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	// Child tx spending coinbase
	childTx := bt.NewTx()
	_ = childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = childTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	_ = childTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	// Store both in txMetaStore
	_, err = txMetaStore.Create(context.Background(), coinbaseTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 0,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)
	_, err = txMetaStore.Create(context.Background(), childTx, 0)
	require.NoError(t, err)

	// Build subtree with coinbase and normalTx
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), uint64(childTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("207fffff")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	totalSize := int64(coinbaseTx.Size()) + int64(childTx.Size()) //nolint:gosec

	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(totalSize),        //nolint:gosec
		1, 1,
	)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.NoError(t, err, "Block should be valid")

	err = blockValidation.setTxMinedStatus(context.Background(), block.Hash())
	require.NoError(t, err, "setTxMined should succeed")

	// Assert that both txs are marked as mined in the txMetaStore
	metaCoinbase, err := txMetaStore.Get(context.Background(), coinbaseTx.TxIDChainHash())
	require.NoError(t, err)
	metaChildTx, err := txMetaStore.Get(context.Background(), childTx.TxIDChainHash())
	require.NoError(t, err)

	// Both should have blockID and blockHeight set to the block's values
	require.NotEmpty(t, metaCoinbase.BlockIDs)
	require.NotEmpty(t, metaCoinbase.BlockHeights)
	require.NotEmpty(t, metaChildTx.BlockIDs)
	require.NotEmpty(t, metaChildTx.BlockHeights)

	require.Equal(t, metaChildTx.BlockIDs[0], uint32(0x1))
	require.Equal(t, metaChildTx.BlockHeights[0], uint32(0x1))
	require.Equal(t, metaChildTx.SubtreeIdxs[0], 0)
}

// TestBlockValidation_SetMinedChan_TriggersSetTxMined ensures that sending a block hash into setMinedChan triggers setTxMined in the background goroutine.
func TestBlockValidation_SetMinedChan_TriggersSetTxMined(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Use the real setup to get all dependencies
	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Create a valid block and validate it so it is in the stores
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	childTx := bt.NewTx()
	_ = childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = childTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	_ = childTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	_, err := utxoStore.Create(context.Background(), coinbaseTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 0,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), childTx, 0)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), uint64(childTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("207fffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	totalSize := int64(coinbaseTx.Size()) + int64(childTx.Size()) //nolint:gosec
	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(totalSize),        //nolint:gosec
		100, 0,
	)
	require.NoError(t, err)

	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.NoError(t, err, "Block should be valid")

	blockHash := block.Hash()
	blockValidation.setMinedChan <- blockHash

	// Wait for the goroutine to process
	time.Sleep(200 * time.Millisecond)

	// After success, block hash should be deleted from blockHashesCurrentlyValidated
	_, exists := blockValidation.blockHashesCurrentlyValidated.Get(*blockHash)
	require.False(t, exists, "block hash should be deleted from blockHashesCurrentlyValidated after success")
}

// TestBlockValidation_BlockchainSubscription_TriggersSetMined ensures that receiving a NotificationType_Block on the blockchainSubscription triggers setMined.
func TestBlockValidation_BlockchainSubscription_TriggersSetMined(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)

	tracker := newTrackingBlockchainClient(blockchainClient).withNotificationChannel()
	notificationCh := tracker.subscriptionChannel()

	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	childTx := bt.NewTx()
	_ = childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = childTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	_ = childTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	_, err := utxoStore.Create(context.Background(), coinbaseTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 0,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), childTx, 0)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), uint64(childTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("207fffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	totalSize := int64(coinbaseTx.Size()) + int64(childTx.Size()) //nolint:gosec
	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(totalSize),        //nolint:gosec
		100, 0,
	)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, tracker, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test")
	require.NoError(t, err, "Block should be valid")

	blockHash := block.Hash()

	// Simulate receiving a blockchain notification
	notificationCh <- &blockchain_api.Notification{
		Type: model.NotificationType_BlockSubtreesSet,
		Hash: blockHash[:],
	}

	time.Sleep(200 * time.Millisecond)

	_, exists := blockValidation.blockHashesCurrentlyValidated.Get(*blockHash)
	require.False(t, exists, "block hash should be deleted from blockHashesCurrentlyValidated after success")
}

func TestBlockValidation_InvalidBlock_PublishesToKafka(t *testing.T) {
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Duplicate Transaction Setup
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	normalTx := bt.NewTx()
	_ = normalTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	_ = normalTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000)
	_ = normalTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*normalTx.TxIDChainHash(), 1, uint64(normalTx.Size()))) // nolint: gosec
	require.NoError(t, subtree.AddNode(*normalTx.TxIDChainHash(), 1, uint64(normalTx.Size()))) // nolint: gosec
	require.NoError(t, subtree.AddNode(*normalTx.TxIDChainHash(), 1, uint64(normalTx.Size()))) // nolint: gosec

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)
	t.Logf("Stored subtree with hash: %s", subtree.RootHash().String())

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) // nolint: gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), // nolint: gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	totalSize := int64(coinbaseTx.Size()) + 2*int64(normalTx.Size())
	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), // nolint: gosec
		uint64(totalSize),        // nolint: gosec
		100, 0,
	)
	require.NoError(t, err)

	// Use our thread-safe mock
	mockKafka := &SafeMockKafkaProducer{}
	mockKafka.On("Publish", mock.MatchedBy(func(msg *kafka.Message) bool {
		return true
	})).Return(nil).Once()
	mockKafka.On("Start", mock.Anything, mock.Anything).Return()

	bv := &BlockValidation{
		logger:                        ulogger.TestLogger{},
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		subtreeStore:                  subtreeStore,
		txStore:                       txStore,
		utxoStore:                     txMetaStore,
		subtreeValidationClient:       subtreeValidationClient,
		subtreeDeDuplicator:           NewDeDuplicator(0),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		invalidBlockKafkaProducer:     mockKafka,
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		setMinedChan:                  make(chan *chainhash.Hash, 1),
		stats:                         gocore.NewStat("blockvalidation"),
	}

	err = bv.ValidateBlock(context.Background(), block, "test")
	t.Logf("ValidateBlock error type: %T, value: %v", err, err)
	require.Error(t, err)

	// Use the thread-safe method to check if Publish was called
	require.True(t, mockKafka.IsPublishCalled(), "Kafka Publish should be called for invalid block (duplicate transaction)")
}
