package subtreevalidation

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blobmemory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jarcoal/httpmock"
	"github.com/libsv/go-bt/v2"
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

		blockValidation := New(ulogger.TestLogger{}, subtreeStore, txStore, txMetaStore, validatorClient)

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

// 		blockValidation := NewSubtreeValidation(ulogger.TestLogger{}, nil, nil, txStore, txMetaStore, validatorClient)
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

	blockValidation := New(ulogger.TestLogger{}, subtreeStore, txStore, txMetaStore, validatorClient)
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

	subtreeValidation := New(ulogger.TestLogger{}, subtreeStore, txStore, txMetaStore, validatorClient)

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
	err = subtreeValidation.validateSubtreeInternal(ctx, v)
	require.NoError(t, err)
}
