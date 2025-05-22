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
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	blobmemory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
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

func newTx(random uint32, parentTxHash ...*chainhash.Hash) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = random
	tx.LockTime = 0

	if len(parentTxHash) > 0 {
		tx.Inputs = []*bt.Input{{
			PreviousTxSatoshis: uint64(random),
			PreviousTxOutIndex: random,
		}}

		_ = tx.Inputs[0].PreviousTxIDAdd(parentTxHash[0])
	}

	return tx
}

type MockSubtreeValidationClient struct {
	server *subtreevalidation.Server
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

// setup prepares a test environment with necessary components for block validation
// testing. It initializes and configures:
// - Transaction metadata store
// - Validator client with mock implementation
// - Subtree validation services
// - Transaction and block storage systems
// The function returns initialized components and a cleanup function to ensure
// proper test isolation.
func setup() (utxostore.Store, subtreevalidation.Interface, blockchain.ClientI, blob.Store, blob.Store, func()) {
	// we only need the httpClient, utxoStore and validatorClient when blessing a transaction
	httpmock.Activate()
	httpmock.RegisterResponder(
		"GET",
		`=~^/tx/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, tx1.ExtendedBytes()),
	)

	httpmock.RegisterResponder(
		"POST",
		`=~^/txs`,
		httpmock.NewBytesResponder(200, tx1.ExtendedBytes()),
	)

	utxoStore := memory.New(ulogger.TestLogger{})
	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	validatorClient := &validator.MockValidatorClient{UtxoStore: utxoStore}

	tSettings := test.CreateBaseTestSettings()

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	if err != nil {
		panic(err)
	}

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	if err != nil {
		panic(err)
	}

	nilConsumer := &kafka.KafkaConsumerGroup{}

	subtreeValidationServer, err := subtreevalidation.New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, validatorClient, blockchainClient, nilConsumer, nilConsumer)
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
		httpmock.DeactivateAndReset()
	}
}

func createSpendingTx(t *testing.T, prevTx *bt.Tx, vout uint32, amount uint64, address *bscript.Address, privateKey *bec.PrivateKey) *bt.Tx {
	tx := bt.NewTx()
	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      prevTx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: prevTx.Outputs[vout].LockingScript,
		Satoshis:      prevTx.Outputs[vout].Satoshis,
	})
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromAddress(address.AddressString, amount)
	require.NoError(t, err)

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	return tx
}

// TestBlockValidationValidateBlockSmall verifies the validation of a small block
// with minimal transactions to ensure core validation functionality works correctly.
// This test demonstrates the basic block validation flow with a controlled set of
// test transactions and validates both the merkle root calculation and block structure.
func TestBlockValidationValidateBlockSmall(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	require.NoError(t, subtree.AddNode(*hash1, 100, 0))
	require.NoError(t, subtree.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree.AddNode(*hash3, 100, 0))

	_, err = utxoStore.Create(context.Background(), tx1, 0)
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

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())
	// now create a subtree with the coinbase to calculate the merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	tSettings := test.CreateBaseTestSettings()

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

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	block, err := model.NewBlock(
		blockHeader,
		coinbase,
		subtreeHashes,            // should be the subtree with placeholder
		uint64(subtree.Length()), // nolint:gosec
		123123,
		0, 0, tSettings)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(0)
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, subtreeValidationClient)
	start := time.Now()

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
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

	txCount := 1024
	// subtreeHashes := make([]*chainhash.Hash, 0)

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(txCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	fees := 0

	for i := 0; i < txCount-1; i++ {
		//nolint:gosec
		tx := newTx(uint32(i))

		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 100, 0))

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

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+uint64(fees))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
	require.NoError(t, err)

	// require.NoError(t, err)
	// t.Logf("subtree hash: %s", subtree.RootHash().String())

	// var merkleRootsubtreeHashes []*chainhash.Hash
	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())
	// now create a subtree with the coinbase to calculate the merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

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

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	tSettings := test.CreateBaseTestSettings()
	tSettings.ChainCfgParams.CoinbaseMaturity = 0

	block, err := model.NewBlock(
		blockHeader,
		coinbase,
		subtreeHashes,            // should be the subtree with placeholder
		uint64(subtree.Length()), // nolint:gosec
		123123,
		1, 0, tSettings)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(0)
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, subtreeValidationClient)
	start := time.Now()

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	require.NoError(t, err)

	t.Logf("Time taken: %s\n", time.Since(start))
}

// TestBlockValidationShouldNotAllowDuplicateCoinbasePlaceholder verifies that
// the validation system correctly enforces the Bitcoin protocol rule preventing
// duplicate coinbase placeholders within a block's transaction structure.
// This test ensures the integrity of block reward distribution.
func TestBlockValidationShouldNotAllowDuplicateCoinbasePlaceholder(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	require.True(t, coinbase.IsCoinbase())

	_, err = utxoStore.Create(context.Background(), coinbase, 0)
	require.NoError(t, err)

	subtree, err := util.NewTreeByLeafCount(4)
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
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

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

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes, // should be the subtree with placeholder
	}

	tSettings := test.CreateBaseTestSettings()

	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(0)
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, subtreeValidationClient)
	start := time.Now()
	// err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	err = blockValidation.ValidateBlock(context.Background(), block, "legacy", model.NewBloomStats())
	require.Error(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}

// TestBlockValidationShouldNotAllowDuplicateCoinbaseTx ensures that blocks
// containing multiple coinbase transactions are rejected. This validation is
// critical for maintaining the Bitcoin protocol's monetary policy by preventing
// multiple block rewards within a single block.
func TestBlockValidationShouldNotAllowDuplicateCoinbaseTx(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	require.True(t, coinbase.IsCoinbase())

	_, err = utxoStore.Create(context.Background(), coinbase, 0)
	require.NoError(t, err)

	subtree, err := util.NewTreeByLeafCount(4)
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
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

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

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes, // should be the subtree with placeholder
	}

	tSettings := test.CreateBaseTestSettings()

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(0)
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, subtreeValidationClient)
	start := time.Now()
	// err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	err = blockValidation.ValidateBlock(context.Background(), block, "legacy", model.NewBloomStats())
	require.Error(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}

// TestInvalidBlockWithoutGenesisBlock verifies that blocks not properly
// connected to the genesis block are rejected. This test ensures the blockchain's
// fundamental requirement of an unbroken chain back to the genesis block,
// preventing any potential chain splits or invalid block sequences.
func TestInvalidBlockWithoutGenesisBlock(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	require.NoError(t, subtree.AddNode(*hash1, 100, 0))
	require.NoError(t, subtree.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree.AddNode(*hash3, 100, 0))

	_, err = utxoStore.Create(context.Background(), tx1, 0)
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
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
	require.NoError(t, err)

	subtreeHashes := make([]*chainhash.Hash, 0)
	subtreeHashes = append(subtreeHashes, subtree.RootHash())
	// now create a subtree with the coinbase to calculate the merkle root
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

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

		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes, // should be the subtree with placeholder
	}

	tSettings := test.CreateBaseTestSettings()

	block.SetSettings(tSettings)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(0)
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, subtreeValidationClient)
	start := time.Now()

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	require.Error(t, err)

	t.Logf("Time taken: %s\n", time.Since(start))

	expectedErrorMessage := "SERVICE_ERROR (49)"
	require.Contains(t, err.Error(), expectedErrorMessage)
}

func TestInvalidChainWithoutGenesisBlock(t *testing.T) {
	// Given: A blockchain setup with a chain that does not connect to the Genesis block
	initPrometheusMetrics()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
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

	tSettings := test.CreateBaseTestSettings()

	for i := 0; i < numBlocks; i++ {
		subtree, err := util.NewTreeByLeafCount(4)
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
		err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
		// now create a subtree with the coinbase to calculate the merkle root
		replicatedSubtree := subtree.Duplicate()
		replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))

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

			if blockHeader.Nonce%1000000 == 0 {
				fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
			}
		}

		block, err := model.NewBlock(
			blockHeader,
			coinbase,
			subtreeHashes,            // should be the subtree with placeholder
			uint64(subtree.Length()), // nolint:gosec
			123123,
			0, 0, tSettings)
		require.NoError(t, err)

		blocks = append(blocks, block)
		previousBlockHash = block.Header.Hash() // Update the previous block hash for the next block
	}

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(0)
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, subtreeValidationClient)

	// When: The last block in the chain is validated
	start := time.Now()
	err = blockValidation.ValidateBlock(context.Background(), blocks[len(blocks)-1], "http://localhost:8000", model.NewBloomStats())

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

	tSettings := test.CreateBaseTestSettings()

	txCount := 4 // We'll use 4 transactions to keep it simple
	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()

	defer deferFunc()

	// Create a subtree with our transactions
	subtree, err := util.NewTreeByLeafCount(txCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	// Add transactions to the subtree
	fees := 0

	for i := 0; i < txCount-1; i++ {
		tx := newTx(uint32(i)) //nolint:gosec

		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 100, 0))

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
	subtreeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	// Mock the HTTP endpoint for the subtree
	httpmock.RegisterResponder(
		"GET",
		fmt.Sprintf("/subtree/%s", subtree.RootHash().String()),
		httpmock.NewBytesResponder(200, subtreeBytes),
	)

	subtreeBytes, err = subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
	require.NoError(t, err)

	// Create subtree hashes and calculate merkle root
	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	// Create block header with correct merkle root
	nBits, _ := model.NewNBitFromString("2000ffff")
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
		if blockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining Nonce: %d, hash: %s\n", blockHeader.Nonce, blockHeader.Hash().String())
		}
	}

	// Create the block with valid merkle root
	validBlock, err := model.NewBlock(
		blockHeader,
		coinbase,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		123123,
		101, 0, tSettings)
	require.NoError(t, err)

	// Setup blockchain store and client
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create block validation instance
	tSettings.BlockValidation.OptimisticMining = false

	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(0)
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, subtreeValidationClient)

	// Test valid merkle root
	err = blockValidation.ValidateBlock(context.Background(), validBlock, "http://localhost:8000", model.NewBloomStats())
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
		if invalidBlockHeader.Nonce%1000000 == 0 {
			fmt.Printf("mining invalid block Nonce: %d, hash: %s\n", invalidBlockHeader.Nonce, invalidBlockHeader.Hash().String())
		}
	}

	invalidBlock, err := model.NewBlock(
		invalidBlockHeader,
		coinbase,
		[]*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), //nolint:gosec
		123123,
		0, 0, tSettings)
	require.NoError(t, err)

	// Test invalid merkle root
	err = blockValidation.ValidateBlock(context.Background(), invalidBlock, "http://localhost:8000", model.NewBloomStats())
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

	tSettings := test.CreateBaseTestSettings()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	// Create a chain of transactions
	txs := transactions.CreateTestTransactionChainWithCount(t, 7) // coinbase + 4 transactions
	coinbaseTx, tx1, tx2, tx3, tx4 := txs[0], txs[1], txs[2], txs[3], txs[4]

	// Store all transactions except tx4 (which will be our missing transaction)
	for _, tx := range txs[:4] { // Store all except tx4
		_, err := utxoStore.Create(context.Background(), tx, 100)
		require.NoError(t, err)
	}

	// Create a subtree with our transactions
	subtree, err := util.NewTreeByLeafCount(4) // 4 transactions excluding coinbase
	require.NoError(t, err)

	// Add transactions to subtree
	for i, tx := range []*bt.Tx{tx1, tx2, tx3, tx4} {
		hash := tx.TxIDChainHash()
		require.NoError(t, subtree.AddNode(*hash, uint64(tx.Size()), uint64(i))) //nolint:gosec
	}

	require.True(t, subtree.IsComplete(), "Subtree should be complete")

	// Calculate total fees for coinbase
	fees := uint64(0)
	for _, tx := range []*bt.Tx{tx1, tx2, tx3, tx4} {
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

	require.Equal(t, subtree.Length(), 4, "Subtree should have 4 transactions")
	// Create the block
	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		func() uint64 {
			totalSize := int64(0)

			for _, tx := range []*bt.Tx{coinbaseTx, tx1, tx2, tx3, tx4} {
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
		0, 0, tSettings)

	require.NoError(t, err)

	// Setup blockchain store and client
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
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

	httpmock.RegisterRegexpMatcherResponder(
		"POST",
		regexp.MustCompile("/[a-fA-F0-9]{64}/txs$"),
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
				case tx4.TxIDChainHash().String():
					tx = tx4
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
	tSettings.BlockValidation.BloomFilterRetentionSize = uint32(0)
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, subtreeValidationClient)

	err = utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{tx1.TxIDChainHash()}, utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 1,
		SubtreeIdx:  0,
	})
	require.NoError(t, err)

	// Test block validation - it should request the missing transaction
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	require.NoError(t, err, "Block validation should succeed after retrieving missing transaction")

	// Verify that the missing transaction was stored
	exists, err := utxoStore.Get(context.Background(), tx4.TxIDChainHash())
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
			utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup()
			defer cleanup()

			// Create test settings with specified excessive block size
			tSettings := test.CreateBaseTestSettings()
			tSettings.Policy.ExcessiveBlockSize = tt.excessiveBlockSize

			// Create blockchain store
			blockchainStoreURL, err := url.Parse("sqlitememory://")
			require.NoError(t, err)
			blockchainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, blockchainStoreURL, tSettings)
			require.NoError(t, err)

			// Create blockchain client
			blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, nil)
			require.NoError(t, err)

			tSettings.BlockValidation.BloomFilterRetentionSize = uint32(1)

			// Create block validator with 10 minute bloom filter expiration
			blockValidator := NewBlockValidation(
				ctx, // Use the context with cancel
				ulogger.TestLogger{},
				tSettings,
				blockchainClient,
				subtreeStore,
				txStore,
				utxoStore,
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

			// Validate the block
			err = blockValidator.ValidateBlock(ctx, block, "test", nil)

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

	tSettings := test.CreateBaseTestSettings()

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}

	subtree1, err := util.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree1.AddCoinbaseNode())
	require.NoError(t, subtree1.AddNode(*hash1, 100, 0))

	subtree2, err := util.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree2.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree2.AddNode(*hash3, 100, 0))

	t.Run("smoke test", func(t *testing.T) {
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}

		blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, nil, subtreeStore, txStore, utxoStore, subtreeValidationClient)

		block := &model.Block{
			Header:   blockHeader,
			Subtrees: make([]*chainhash.Hash, 0),
		}

		err := blockValidation.validateBlockSubtrees(t.Context(), block, "http://localhost:8000")
		require.NoError(t, err)
	})

	t.Run("processing in parallel", func(t *testing.T) {
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
		subtreeValidationClient.Mock.On("CheckSubtreeFromBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, nil, subtreeStore, txStore, utxoStore, subtreeValidationClient)

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

		require.NoError(t, blockValidation.validateBlockSubtrees(t.Context(), block, "http://localhost:8000"))
	})

	t.Run("fallback to series", func(t *testing.T) {
		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
		// First call - for subtree1 - success
		subtreeValidationClient.Mock.On("CheckSubtreeFromBlock", mock.Anything, *subtree1.RootHash(), mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Once().
			Run(func(args mock.Arguments) {
				// subtree was validated properly, let's add it to our subtree store so it doesn't get checked again
				subtreeBytes, err := subtree1.Serialize()
				require.NoError(t, err)

				require.NoError(t, subtreeStore.Set(context.Background(), subtree1.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree")))
			})

		// Second call - for subtree2 - fail with ErrSubtreeError
		subtreeValidationClient.Mock.On("CheckSubtreeFromBlock", mock.Anything, *subtree2.RootHash(), mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.ErrSubtreeError).
			Once()

		// Third call - for subtree2 - success (retry in series)
		subtreeValidationClient.Mock.On("CheckSubtreeFromBlock", mock.Anything, *subtree2.RootHash(), mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).
			Once()

		blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, nil, subtreeStore, txStore, utxoStore, subtreeValidationClient)

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

		require.NoError(t, blockValidation.validateBlockSubtrees(t.Context(), block, "http://localhost:8000"))

		// check that the subtree validation was called 3 times
		assert.Len(t, subtreeValidationClient.Mock.Calls, 3)
	})
}

func TestBlockValidation_InvalidCoinbaseScriptLength(t *testing.T) {
	initPrometheusMetrics()

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()

	defer deferFunc()

	tSettings := test.CreateBaseTestSettings()
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create a valid block, then tamper with coinbase script length
	block := createValidBlock(t, tSettings, txMetaStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore)
	block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01}) // Too short

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test", model.NewBloomStats())
	require.ErrorContains(t, err, "BLOCK_INVALID")
}

func createValidBlock(t *testing.T, tSettings *settings.Settings, txMetaStore utxostore.Store, subtreeValidationClient subtreevalidation.Interface, blockchainClient blockchain.ClientI, txStore blob.Store, subtreeStore blob.Store) *model.Block {
	// Create a private key and address for outputs
	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// Create coinbase transaction
	coinbaseTx := bt.NewTx()
	err = coinbaseTx.From(
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xffffffff,
		"",
		0,
	)
	require.NoError(t, err)

	blockHeight := uint32(100)

	blockHeightBytes := make([]byte, 4)

	binary.LittleEndian.PutUint32(blockHeightBytes, blockHeight)

	arbitraryData := make([]byte, 0)

	arbitraryData = append(arbitraryData, 0x03)

	arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)

	arbitraryData = append(arbitraryData, []byte("/Test miner/")...)

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
	err = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)
	require.NoError(t, err)

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
	subtree, err := util.NewTreeByLeafCount(2)
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
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
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
		100, 0, tSettings,
	)
	require.NoError(t, err)

	return block
}

func TestBlockValidation_DoubleSpendInBlock(t *testing.T) {
	initPrometheusMetrics()

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings()
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create a valid block, then add two txs that spend the same UTXO
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Coinbase
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	// add the coinbase to the utxo store
	_, err = txMetaStore.Create(t.Context(), coinbaseTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:     0,
		BlockHeight: 0,
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

	// Store in txMetaStore
	_, _ = txMetaStore.Create(context.Background(), coinbaseTx, 0)
	_, _ = txMetaStore.Create(context.Background(), tx1, 0)
	// since this was a double spend it should be marked as conflicting
	_, _ = txMetaStore.Create(context.Background(), tx2, 0, utxostore.WithConflicting(true))

	// Subtree with both double-spend txs
	subtree, _ := util.NewTreeByLeafCount(4)
	_ = subtree.AddCoinbaseNode()
	_ = subtree.AddNode(*tx1.TxIDChainHash(), uint64(tx1.Size()), 0) //nolint:gosec
	_ = subtree.AddNode(*tx2.TxIDChainHash(), uint64(tx2.Size()), 1) //nolint:gosec

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtreeToCheck"))
	require.NoError(t, err)

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
		uint64(coinbaseTx.Size()+tx1.Size()+tx2.Size()), //nolint:gosec
		100, 0, tSettings,
	)

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, subtreeValidationClient)

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost", model.NewBloomStats())
	require.Error(t, err)
	require.ErrorContains(t, err, "BLOCK_INVALID")
	require.ErrorContains(t, err, "spends parent transaction")
}

func TestBlockValidation_InvalidTransactionChainOrdering(t *testing.T) {
	initPrometheusMetrics()

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()

	defer deferFunc()

	tSettings := test.CreateBaseTestSettings()
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, _ := bec.NewPrivateKey(bec.S256())
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
	subtree, _ := util.NewTreeByLeafCount(4)
	_ = subtree.AddCoinbaseNode()
	_ = subtree.AddNode(*tx2.TxIDChainHash(), uint64(tx2.Size()), 0) //nolint:gosec
	_ = subtree.AddNode(*tx1.TxIDChainHash(), uint64(tx1.Size()), 1) //nolint:gosec

	nodeBytes, _ := subtree.SerializeNodes()
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, _ := subtree.Serialize()

	_ = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
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
		uint64(coinbaseTx.Size()+tx1.Size()+tx2.Size()), //nolint:gosec
		100, 0, tSettings,
	)

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test", model.NewBloomStats())
	require.Error(t, err)
	require.ErrorContains(t, err, "BLOCK_INVALID")
	require.ErrorContains(t, err, "comes before parent transaction")
}

func TestBlockValidation_OrphanTransaction(t *testing.T) {
	initPrometheusMetrics()

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()

	defer deferFunc()

	tSettings := test.CreateBaseTestSettings()
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Coinbase
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	// Orphan tx: spends a non-existent UTXO (random hash)
	fakeHash := chainhash.DoubleHashH([]byte("nonexistent-utxo"))

	orphanTx := bt.NewTx()

	lockingScript, err := bscript.NewP2PKHFromAddress(address.AddressString)
	require.NoError(t, err)

	_ = orphanTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      &fakeHash,
		Vout:          0,
		LockingScript: lockingScript,
		Satoshis:      10 * 100000000,
	})
	_ = orphanTx.AddP2PKHOutputFromAddress(address.AddressString, 9*100000000)
	_ = orphanTx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})

	// Store only coinbase in txMetaStore (not orphanTx)
	_, _ = txMetaStore.Create(context.Background(), coinbaseTx, 0)

	// Subtree: coinbase, orphanTx
	subtree, _ := util.NewTreeByLeafCount(2)
	_ = subtree.AddCoinbaseNode()
	_ = subtree.AddNode(*orphanTx.TxIDChainHash(), uint64(orphanTx.Size()), 0) //nolint:gosec

	nodeBytes, _ := subtree.SerializeNodes()

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, _ := subtree.Serialize()
	_ = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
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
		uint64(coinbaseTx.Size()+orphanTx.Size()), //nolint:gosec
		100, 0, tSettings,
	)

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test", model.NewBloomStats())
	require.Error(t, err)
	require.ErrorContains(t, err, "BLOCK_INVALID")
	require.ErrorContains(t, err, "could not be found in tx txMetaStore")
}

func TestBlockValidation_InvalidParentBlock(t *testing.T) {
	initPrometheusMetrics()

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup()

	defer deferFunc()

	tSettings := test.CreateBaseTestSettings()
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	privateKey, _ := bec.NewPrivateKey(bec.S256())
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
	subtree, _ := util.NewTreeByLeafCount(2)
	_ = subtree.AddCoinbaseNode()
	_ = subtree.AddNode(*tx1.TxIDChainHash(), uint64(tx1.Size()), 0) //nolint:gosec

	nodeBytes, _ := subtree.SerializeNodes()

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, _ := subtree.Serialize()
	_ = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
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
		100, 0, tSettings,
	)

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, subtreeValidationClient)
	err = blockValidation.ValidateBlock(context.Background(), block, "test", model.NewBloomStats())
	t.Logf("err: %v", err)
	require.ErrorContains(t, err, "STORAGE_ERROR")
}

func Test_checkOldBlockIDs(t *testing.T) {
	initPrometheusMetrics()

	t.Run("empty map", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}
		blockValidation := &BlockValidation{
			blockchainClient: blockchainMock,
		}

		oldBlockIDsMap := util.NewSyncedMap[chainhash.Hash, []uint32]()

		blockchainMock.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{}, nil).Once()

		err := blockValidation.checkOldBlockIDs(t.Context(), oldBlockIDsMap, &model.Block{})
		require.NoError(t, err)
	})

	t.Run("empty parents", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}
		blockValidation := &BlockValidation{
			blockchainClient: blockchainMock,
		}

		oldBlockIDsMap := util.NewSyncedMap[chainhash.Hash, []uint32]()

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

		oldBlockIDsMap := util.NewSyncedMap[chainhash.Hash, []uint32]()

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

		oldBlockIDsMap := util.NewSyncedMap[chainhash.Hash, []uint32]()

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

		oldBlockIDsMap := util.NewSyncedMap[chainhash.Hash, []uint32]()

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

func Test_createAppendBloomFilter(t *testing.T) {
	logger := ulogger.TestLogger{}

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}

	// Create a test block with specified size and valid header
	block := &model.Block{
		Header:           blockHeader,
		SizeInBytes:      123,
		TransactionCount: 1,
		CoinbaseTx:       tx1,                 // Using tx1 from test setup
		Subtrees:         []*chainhash.Hash{}, // Initialize empty subtrees slice
	}

	t.Run("smoke test", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}

		blockValidation := &BlockValidation{
			logger:                        logger,
			blockchainClient:              blockchainMock,
			blockBloomFiltersBeingCreated: util.NewSwissMap(0),
			recentBlocksBloomFilters:      util.NewSyncedMap[chainhash.Hash, *model.BlockBloomFilter](),
			subtreeStore:                  blobmemory.New(),
		}

		blockchainMock.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{
			Height: 100,
		}, nil)

		err := blockValidation.createAppendBloomFilter(t.Context(), block)
		require.NoError(t, err)
	})

	t.Run("check the bloom filter", func(t *testing.T) {
		blockchainMock := &blockchain.Mock{}

		blockValidation := &BlockValidation{
			logger:                        logger,
			blockchainClient:              blockchainMock,
			blockBloomFiltersBeingCreated: util.NewSwissMap(0),
			recentBlocksBloomFilters:      util.NewSyncedMap[chainhash.Hash, *model.BlockBloomFilter](),
			subtreeStore:                  blobmemory.New(),
		}

		blockchainMock.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{
			Height: 100,
		}, nil)

		// create subtree with transactions
		subtree, err := util.NewTreeByLeafCount(4)
		require.NoError(t, err)

		txs := make([]chainhash.Hash, 0, 4)

		for i := uint64(0); i < 4; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("txHash_%d", i)))
			require.NoError(t, subtree.AddNode(txHash, i, i))

			txs = append(txs, txHash)
		}

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		err = blockValidation.subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		// clone the block
		blockClone := &model.Block{
			Header:           block.Header,
			CoinbaseTx:       block.CoinbaseTx,
			TransactionCount: block.TransactionCount,
			SizeInBytes:      block.SizeInBytes,
			Subtrees:         []*chainhash.Hash{subtree.RootHash()},
			SubtreeSlices:    []*util.Subtree{subtree},
			Height:           100,
		}

		err = blockValidation.createAppendBloomFilter(t.Context(), blockClone)
		require.NoError(t, err)

		// get the bloomfilter and check whether all the transactions are in there
		bloomFilter, ok := blockValidation.recentBlocksBloomFilters.Get(*blockClone.Hash())
		require.True(t, ok, "bloom filter should be present in the map")
		require.NotNil(t, bloomFilter)

		for _, txHash := range txs {
			// check whether the transaction is in the bloom filter
			n64 := binary.BigEndian.Uint64(txHash[:])
			assert.True(t, bloomFilter.Filter.Has(n64), "bloom filter should match the transaction: "+txHash.String())
		}

		// check for some random transaction
		randomTxHash := chainhash.HashH([]byte("randomTxHash"))
		n64 := binary.BigEndian.Uint64(randomTxHash[:])
		require.False(t, bloomFilter.Filter.Has(n64), "bloom filter should not match the random transaction")
	})
}
