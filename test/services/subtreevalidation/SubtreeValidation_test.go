//go:build test_all || test_services || test_subtreevalidation || test_longlong

package subtreevalidation

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	stv "github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/stores/blob"
	blobmemory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/txmetacache"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/kafka" //nolint:gci
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_subtreevalidation ./test/...

var (
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

func TestBlockValidationValidateBigSubtree(t *testing.T) {
	stv.InitPrometheusMetrics()

	txMetaStore, validatorClient, txStore, subtreeStore, blockchainClient, deferFunc := setup()
	defer deferFunc()

	nilConsumer := &kafka.KafkaConsumerGroup{}
	tSettings := test.CreateBaseTestSettings()

	subtreeValidation, err := stv.New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, txMetaStore, validatorClient, blockchainClient, nilConsumer, nilConsumer)
	require.NoError(t, err)

	if utxoStore, err := txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore, txmetacache.Unallocated, 2048); err != nil {
		subtreeValidation.SetUutxoStore(utxoStore)
	} else {
		require.NoError(t, err)
	}

	numberOfItems := 1_024 * 1_024

	subtree, err := util.NewTreeByLeafCount(numberOfItems)
	require.NoError(t, err)

	for i := 0; i < numberOfItems; i++ {
		tx := bt.NewTx()
		_ = tx.AddOpReturnOutput([]byte(fmt.Sprintf("tx%d", i)))

		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 1, 0))

		_, err := subtreeValidation.GetUutxoStore().Create(context.Background(), tx, 0)
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

	v := stv.ValidateSubtree{
		SubtreeHash:   *rootHash,
		BaseURL:       "http://localhost:8000",
		TxHashes:      nil,
		AllowFailFast: false,
	}
	err = subtreeValidation.ValidateSubtreeInternal(context.Background(), v, chaincfg.GenesisActivationHeight, nil)
	require.NoError(t, err)

	t.Logf("Time taken: %s\n", time.Since(start))

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
}

func setup() (utxo.Store, *validator.MockValidatorClient, blob.Store, blob.Store, blockchain.ClientI, func()) {
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

	validatorClient := &validator.MockValidatorClient{TxMetaStore: utxoStore}

	blockchainClient := &blockchain.LocalClient{}

	return utxoStore, validatorClient, txStore, subtreeStore, blockchainClient, func() {
		httpmock.DeactivateAndReset()
	}
}
