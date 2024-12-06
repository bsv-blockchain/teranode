package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blobmemory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxoStore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

const coinbaseHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"

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

func (m *MockSubtreeValidationClient) CheckSubtree(ctx context.Context, subtreeHash chainhash.Hash, baseURL string, blockHeight uint32, blockHash *chainhash.Hash) error {
	request := subtreevalidation_api.CheckSubtreeRequest{
		Hash:        subtreeHash[:],
		BaseUrl:     baseURL,
		BlockHeight: blockHeight,
		BlockHash:   blockHash[:],
	}
	_, err := m.server.CheckSubtree(ctx, &request)

	return err
}

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

	subtreeValidationClient := &MockSubtreeValidationClient{
		server: subtreeValidationServer,
	}

	return txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, func() {
		httpmock.DeactivateAndReset()
	}
}

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
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(coinbaseHex)
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
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"})
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
	start := time.Now()
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}

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

	coinbase, err := bt.NewTxFromString(coinbaseHex)
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

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(subtree.Length()),
		SizeInBytes:      123123,
		Subtrees:         subtreeHashes, // should be the subtree with placeholder
	}

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"})
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()

	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
	start := time.Now()
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}

func TestBlockValidationShouldNotAllowDuplicateCoinbasePlaceholder(t *testing.T) {
	initPrometheusMetrics()

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(coinbaseHex)
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
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"})
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
	start := time.Now()
	// err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	err = blockValidation.ValidateBlock(context.Background(), block, "legacy", model.NewBloomStats())
	require.Error(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}
func TestBlockValidationShouldNotAllowDuplicateCoinbaseTx(t *testing.T) {
	initPrometheusMetrics()

	txMetaStore, validatorClient, subtreeValidationClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	coinbase, err := bt.NewTxFromString(coinbaseHex)
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
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"})
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
	start := time.Now()
	// err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	err = blockValidation.ValidateBlock(context.Background(), block, "legacy", model.NewBloomStats())
	require.Error(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}

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

	coinbase, err := bt.NewTxFromString(coinbaseHex)
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
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"})
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))
	start := time.Now()
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000", model.NewBloomStats())
	require.Error(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))

	expectedErrorMessage := "Error: SERVICE_ERROR (error code: 49)"
	// Message [ValidateBlock][" + block.Header.Hash().String() + "] failed to store block, Wrapped err: Error: STORAGE_ERROR (error code: 59), Message: error storing block " + block.Header.Hash().String() + " as previous block " + hashPrevBlock.String() + " not found, Wrapped err: Error: UNKNOWN (error code: 0), Message: sql: no rows in result set"
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

		coinbase, err := bt.NewTxFromString(coinbaseHex)
		require.NoError(t, err)

		coinbase.Outputs = nil

		_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes, options.WithFileExtension("subtree"))
		require.NoError(t, err)

		subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

		// Create a replicated subtree to calculate the merkle root with the coinbase
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

		block := &model.Block{
			Header:           blockHeader,
			CoinbaseTx:       coinbase,
			TransactionCount: uint64(subtree.Length()),
			SizeInBytes:      123123,
			Subtrees:         subtreeHashes, // should be the subtree with placeholder
		}

		blocks = append(blocks, block)
		previousBlockHash = block.Header.Hash() // Update the previous block hash for the next block
	}

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"})
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore, nil, nil)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()
	blockValidation := NewBlockValidation(context.Background(), ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient, subtreeValidationClient, time.Duration(0))

	// When: The last block in the chain is validated
	start := time.Now()
	err = blockValidation.ValidateBlock(context.Background(), blocks[len(blocks)-1], "http://localhost:8000", model.NewBloomStats())

	// Then: An error should be received because the chain does not connect to the Genesis block
	require.Error(t, err)
	// expectedErrorMessage := "Error: SERVICE_ERROR (error code: 49), [ValidateBlock][" + blocks[len(blocks)-1].Header.Hash().String() + "] failed to store block: Error: STORAGE_ERROR (error code: 59), error storing block " + blocks[len(blocks)-1].Header.Hash().String() + " as previous block " + blocks[0].Header.HashPrevBlock.String() + " not found: 0: sql: no rows in result set"
	// require.Contains(t, err.Error(), expectedErrorMessage)

	t.Logf("Error received as expected: %s", err.Error())
	t.Logf("Time taken: %s\n", time.Since(start))
}
