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
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/stores/blob"
	blobmemory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxoStore "github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
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
	"github.com/stretchr/testify/require"
)

var (
	// tx, _  = hex.DecodeString("0100000001ae73759b118b9c8d54a13ad4ccd5de662bbaa2175ee7f4b413f402affb831ed3000000006b483045022100a6af846212b0c611056a9a30c22f0eed3adc29f8c688e804509b113f322459220220708fb79c66d235d937e8348ac022b3cfa6b64fec8d35a749ea0b2293ad95da014121039f271b930111fd7c818100ee1603d5c5094c68b3d15ad0a58f712e7d766225edffffffff0550c30000000000001976a91448bea2d45f4f6175e47ccb717e4f5d19d8f68f3b88ac204e0000000000001976a91442859b9bada6461d08a0aab8a18105ef30457a8b88ac10270000000000001976a914d0e2122bdeed7b2235f670cdc832f518fb63db9f88ac0c040000000000001976a914d56f84ae869e4a743e929e31218b198f02ce67fe88ac8d0c0100000000001976a91444a8e7fb1a426e4c60597d9d3f534c677d4f858388ac00000000")
	tx1, _ = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	tx2    = newTx(2)
	tx3    = newTx(3)
	tx4    = newTx(4)

	hash1 = tx1.TxIDChainHash()
	hash2 = tx2.TxIDChainHash()
	hash3 = tx3.TxIDChainHash()
)

func newTx(random uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = random

	return tx
}

type MockSubtreeValidationClient struct {
	server *subtreevalidation.Server
}

func (m *MockSubtreeValidationClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "MockValidator", nil
}

func (m *MockSubtreeValidationClient) CheckSubtreeFromBlock(ctx context.Context, subtreeHash chainhash.Hash, baseURL string, blockHeight uint32, blockHash *chainhash.Hash) error {
	request := subtreevalidation_api.CheckSubtreeFromBlockRequest{
		Hash:        subtreeHash.CloneBytes(),
		BaseUrl:     baseURL,
		BlockHeight: blockHeight,
		BlockHash:   blockHash.CloneBytes(),
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
func setup() (utxoStore.Store, *validator.MockValidatorClient, subtreevalidation.Interface, blob.Store, blob.Store, func()) {
	// we only need the httpClient, txMetaStore and validatorClient when blessing a transaction
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

	txMetaStore := memory.New(ulogger.TestLogger{})
	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	validatorClient := &validator.MockValidatorClient{TxMetaStore: txMetaStore}

	blockchainClient := &blockchain.LocalClient{}

	nilConsumer := &kafka.KafkaConsumerGroup{}
	tSettings := test.CreateBaseTestSettings()

	subtreeValidationServer, err := subtreevalidation.New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, txMetaStore, validatorClient, blockchainClient, nilConsumer, nilConsumer)
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

	return txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, func() {
		httpmock.DeactivateAndReset()
	}
}

// createTestTransactionChainWithCount generates a sequence of valid Bitcoin
// transactions for testing, starting with a coinbase transaction and creating
// a specified number of chained transactions. This utility function helps create
// realistic test scenarios with valid transaction chains.
//
// Parameters:
//   - t: Testing context for assertions
//   - count: Number of transactions to generate in the chain
//
// Returns a slice of transactions starting with the coinbase transaction.
func createTestTransactionChainWithCount(t *testing.T, count int) []*bt.Tx {
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

	// Create tx1 with multiple outputs for the chain
	tx1 := bt.NewTx()
	err = tx1.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	// Create enough outputs for the chain
	outputAmount := uint64(20 * 100000000)
	remainingAmount := coinbaseTx.Outputs[0].Satoshis

	for i := 0; i < count-1; i++ {
		//nolint:gosec
		if i == count-2 {
			// Last output gets remaining amount minus fees
			outputAmount = remainingAmount - 100000 // Leave some for fees
		}

		err = tx1.AddP2PKHOutputFromAddress(address.AddressString, outputAmount)

		require.NoError(t, err)

		remainingAmount -= outputAmount
	}

	err = tx1.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	// Create the chain of transactions
	result := []*bt.Tx{coinbaseTx, tx1}

	// nolint:gosec
	for i := uint32(0); i < uint32(count-2); i++ {
		tx := createSpendingTx(t, tx1, i, 15*100000000, address, privateKey)
		result = append(result, tx)
	}

	return result
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

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	require.NoError(t, subtree.AddNode(*hash1, 100, 0))
	require.NoError(t, subtree.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree.AddNode(*hash3, 100, 0))

	_, err = txMetaStore.Create(context.Background(), tx1, 0)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx2, 0)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx3, 0)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx4, 0)
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

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
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

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
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

		_, err = txMetaStore.Create(context.Background(), tx, 0)
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

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
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

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	require.True(t, coinbase.IsCoinbase())

	_, err = txMetaStore.Create(context.Background(), coinbase, 0)
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

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
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

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	require.True(t, coinbase.IsCoinbase())

	_, err = txMetaStore.Create(context.Background(), coinbase, 0)
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

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
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

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	require.NoError(t, subtree.AddNode(*hash1, 100, 0))
	require.NoError(t, subtree.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree.AddNode(*hash3, 100, 0))

	_, err = txMetaStore.Create(context.Background(), tx1, 0)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx2, 0)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx3, 0)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx4, 0)
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

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
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

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	// Ensure transactions are stored
	t.Logf("Storing transactions in txMetaStore...")

	txns := []*bt.Tx{tx1, tx2, tx3, tx4}
	for i, tx := range txns {
		_, err := txMetaStore.Create(context.Background(), tx, 0)
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

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))

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
	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()

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

		_, err = txMetaStore.Create(context.Background(), tx, 0)
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
		0, 0, tSettings)
	require.NoError(t, err)

	// Setup blockchain store and client
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Create block validation instance
	tSettings.BlockValidation.OptimisticMining = false
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))

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

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	// Create a chain of transactions
	txs := createTestTransactionChainWithCount(t, 5) // coinbase + 4 transactions
	coinbaseTx, tx1, tx2, tx3, tx4 := txs[0], txs[1], txs[2], txs[3], txs[4]

	// Store all transactions except tx4 (which will be our missing transaction)
	for _, tx := range txs[:4] { // Store all except tx4
		_, err := txMetaStore.Create(context.Background(), tx, 100)
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

	httpmock.RegisterResponder(
		"POST",
		"/txs",
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
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))

	// Test block validation - it should request the missing transaction
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	require.NoError(t, err, "Block validation should succeed after retrieving missing transaction")

	// Verify that the missing transaction was stored
	exists, err := txMetaStore.Get(context.Background(), tx4.TxIDChainHash())
	require.NoError(t, err)
	require.NotEmpty(t, exists, "The missing transaction should have been stored after validation")
}
