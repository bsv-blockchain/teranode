package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blobmemory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jarcoal/httpmock"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

var (
	//tx, _  = hex.DecodeString("0100000001ae73759b118b9c8d54a13ad4ccd5de662bbaa2175ee7f4b413f402affb831ed3000000006b483045022100a6af846212b0c611056a9a30c22f0eed3adc29f8c688e804509b113f322459220220708fb79c66d235d937e8348ac022b3cfa6b64fec8d35a749ea0b2293ad95da014121039f271b930111fd7c818100ee1603d5c5094c68b3d15ad0a58f712e7d766225edffffffff0550c30000000000001976a91448bea2d45f4f6175e47ccb717e4f5d19d8f68f3b88ac204e0000000000001976a91442859b9bada6461d08a0aab8a18105ef30457a8b88ac10270000000000001976a914d0e2122bdeed7b2235f670cdc832f518fb63db9f88ac0c040000000000001976a914d56f84ae869e4a743e929e31218b198f02ce67fe88ac8d0c0100000000001976a91444a8e7fb1a426e4c60597d9d3f534c677d4f858388ac00000000")
	tx1, _ = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	tx2    = newTx(2)
	tx3    = newTx(3)
	tx4    = newTx(4)

	hash1 = tx1.TxIDChainHash()
	hash2 = tx2.TxIDChainHash()
	hash3 = tx3.TxIDChainHash()
	hash4 = tx4.TxIDChainHash()
)

func newTx(random uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = random
	return tx
}

func TestBlockValidationValidateSubtree(t *testing.T) {
	t.Run("validateSubtree - smoke test", func(t *testing.T) {
		initPrometheusMetrics()

		txMetaStore, validatorClient, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		subtree, err := util.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, subtree.AddNode(*hash1, 121, 0))
		require.NoError(t, subtree.AddNode(*hash2, 122, 0))
		require.NoError(t, subtree.AddNode(*hash3, 123, 0))
		require.NoError(t, subtree.AddNode(*hash4, 123, 0))

		_, err = txMetaStore.Create(context.Background(), tx1)
		require.NoError(t, err)

		_, err = txMetaStore.Create(context.Background(), tx2)
		require.NoError(t, err)

		_, err = txMetaStore.Create(context.Background(), tx3)
		require.NoError(t, err)

		_, err = txMetaStore.Create(context.Background(), tx4)
		require.NoError(t, err)

		t.Log(tx1.TxIDChainHash().String())
		t.Log(tx2.TxIDChainHash().String())
		t.Log(tx3.TxIDChainHash().String())
		t.Log(tx4.TxIDChainHash().String())

		nodeBytes, err := subtree.SerializeNodes()
		require.NoError(t, err)

		httpmock.RegisterResponder(
			"GET",
			`=~^/subtree/[a-z0-9]+\z`,
			httpmock.NewBytesResponder(200, nodeBytes),
		)

		blockValidation := NewBlockValidation(ulogger.TestLogger{}, nil, subtreeStore, txStore, txMetaStore, validatorClient)

		v := ValidateSubtree{
			SubtreeHash:   *subtree.RootHash(),
			BaseUrl:       "http://localhost:8000",
			SubtreeHashes: nil,
			AllowFailFast: false,
		}
		_, err = blockValidation.validateSubtree(context.Background(), v)
		require.NoError(t, err)
	})
}

// func TestBlockValidation_blessMissingTransaction(t *testing.T) {
// 	t.Run("blessMissingTransaction - smoke test", func(t *testing.T) {
// 		initPrometheusMetrics()

// 		txMetaStore, validatorClient, txStore, _, deferFunc := setup()
// 		defer deferFunc()

// 		blockValidation := NewBlockValidation(ulogger.TestLogger{}, nil, nil, txStore, txMetaStore, validatorClient)
// 		missingTx, err := blockValidation.getMissingTransaction(context.Background(), hash1, "http://localhost:8000")
// 		require.NoError(t, err)

// 		_, err = blockValidation.blessMissingTransaction(context.Background(), missingTx)
// 		require.NoError(t, err)
// 	})
// }

func setup() (*memory.Memory, *validator.MockValidatorClient, blob.Store, blob.Store, func()) {
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

	return txMetaStore, validatorClient, txStore, subtreeStore, func() {
		httpmock.DeactivateAndReset()
	}
}

func TestBlockValidationValidateBigSubtree(t *testing.T) {
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing of the current size in improved cache constants
	util.SkipVeryLongTests(t)
	initPrometheusMetrics()

	txMetaStore, validatorClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	blockValidation := NewBlockValidation(ulogger.TestLogger{}, nil, subtreeStore, txStore, txMetaStore, validatorClient)
	blockValidation.txMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, 2048)

	numberOfItems := 1_024 * 1_024

	subtree, err := util.NewTreeByLeafCount(numberOfItems)
	require.NoError(t, err)

	for i := 0; i < numberOfItems; i++ {
		tx := bt.NewTx()
		_ = tx.AddOpReturnOutput([]byte(fmt.Sprintf("tx%d", i)))

		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 1, 0))

		_, err := blockValidation.txMetaStore.Create(context.Background(), tx)
		require.NoError(t, err)
	}

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	// this calculation should not be in the test data, in the real world we would be getting this from the other miner
	rootHash := subtree.RootHash()

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	start := time.Now()

	v := ValidateSubtree{
		SubtreeHash:   *rootHash,
		BaseUrl:       "http://localhost:8000",
		SubtreeHashes: nil,
		AllowFailFast: false,
	}
	_, err = blockValidation.validateSubtree(context.Background(), v)
	require.NoError(t, err)

	t.Logf("Time taken: %s\n", time.Since(start))

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
}

func TestBlockValidation_validateBlock_small(t *testing.T) {

	initPrometheusMetrics()

	txMetaStore, validatorClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(model.CoinbasePlaceholder, 0, 0))

	require.NoError(t, subtree.AddNode(*hash1, 100, 0))
	require.NoError(t, subtree.AddNode(*hash2, 100, 0))
	require.NoError(t, subtree.AddNode(*hash3, 100, 0))

	_, err = txMetaStore.Create(context.Background(), tx1)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx2)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx3)
	require.NoError(t, err)

	_, err = txMetaStore.Create(context.Background(), tx4)
	require.NoError(t, err)

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	nBits := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)
	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes)
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
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           nBits,
		Nonce:          0,
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
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(ulogger.TestLogger{}, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient)
	start := time.Now()
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000")
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}
func TestBlockValidation_validateBlock(t *testing.T) {

	initPrometheusMetrics()
	txCount := 1024
	// subtreeHashes := make([]*chainhash.Hash, 0)

	txMetaStore, validatorClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(txCount)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(model.CoinbasePlaceholder, 0, 0))
	fees := 0
	for i := 0; i < txCount-1; i++ {
		tx := newTx(uint32(i))

		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 100, 0))
		fees += 100
		_, err = txMetaStore.Create(context.Background(), tx)
		require.NoError(t, err)
	}

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	nBits := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)
	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+uint64(fees))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	err = subtreeStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes)
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
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           nBits,
		Nonce:          0,
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
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockChainStore)
	require.NoError(t, err)

	blockValidation := NewBlockValidation(ulogger.TestLogger{}, blockchainClient, subtreeStore, txStore, txMetaStore, validatorClient)
	start := time.Now()
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost:8000")
	require.NoError(t, err)
	t.Logf("Time taken: %s\n", time.Since(start))
}

// copied from BigBlock_test
//func calculateMerkleRoot(hashes []*chainhash.Hash) (*chainhash.Hash, error) {
//	var calculatedMerkleRootHash *chainhash.Hash
//	if len(hashes) == 1 {
//		calculatedMerkleRootHash = hashes[0]
//	} else if len(hashes) > 0 {
//		// Create a new subtree with the hashes of the subtrees
//		st, err := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(hashes)))
//		if err != nil {
//			return nil, err
//		}
//		for _, hash := range hashes {
//			err := st.AddNode(*hash, 1, 0)
//			if err != nil {
//				return nil, err
//			}
//		}
//
//		calculatedMerkleRoot := st.RootHash()
//		calculatedMerkleRootHash, err = chainhash.NewHash(calculatedMerkleRoot[:])
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	return calculatedMerkleRootHash, nil
//}

func TestBlockValidationValidateSubtreeInternalWithMissingTx(t *testing.T) {
	initPrometheusMetrics()

	txMetaStore, validatorClient, txStore, subtreeStore, deferFunc := setup()
	defer deferFunc()

	subtree, err := util.NewTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*hash1, 121, 0))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	blockValidation := NewBlockValidation(ulogger.TestLogger{}, nil, subtreeStore, txStore, txMetaStore, validatorClient)

	// Create a mock context
	ctx := context.Background()

	// Create a mock ValidateSubtree struct
	v := ValidateSubtree{
		SubtreeHash:   *hash1,
		BaseUrl:       "http://localhost:8000",
		SubtreeHashes: nil,
		AllowFailFast: false,
	}

	// Call the validateSubtreeInternal method
	err = blockValidation.validateSubtreeInternal(ctx, v)
	require.NoError(t, err)

}
