package blockvalidation

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blobmemory "github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jarcoal/httpmock"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p"
	"github.com/stretchr/testify/require"
)

var (
	tx, _ = hex.DecodeString("0100000001ae73759b118b9c8d54a13ad4ccd5de662bbaa2175ee7f4b413f402affb831ed3000000006b483045022100a6af846212b0c611056a9a30c22f0eed3adc29f8c688e804509b113f322459220220708fb79c66d235d937e8348ac022b3cfa6b64fec8d35a749ea0b2293ad95da014121039f271b930111fd7c818100ee1603d5c5094c68b3d15ad0a58f712e7d766225edffffffff0550c30000000000001976a91448bea2d45f4f6175e47ccb717e4f5d19d8f68f3b88ac204e0000000000001976a91442859b9bada6461d08a0aab8a18105ef30457a8b88ac10270000000000001976a914d0e2122bdeed7b2235f670cdc832f518fb63db9f88ac0c040000000000001976a914d56f84ae869e4a743e929e31218b198f02ce67fe88ac8d0c0100000000001976a91444a8e7fb1a426e4c60597d9d3f534c677d4f858388ac00000000")
	tx1   = newTx(1)
	tx2   = newTx(2)
	tx3   = newTx(3)
	tx4   = newTx(4)

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

func TestBlockValidation_validateSubtree(t *testing.T) {
	t.Run("validateSubtree - smoke test", func(t *testing.T) {
		initPrometheusMetrics()

		txMetaStore, validatorClient, txStore, subtreeStore, deferFunc := setup()
		defer deferFunc()

		subtree := util.NewTreeByLeafCount(4)
		require.NoError(t, subtree.AddNode(hash1, 121, 0))
		require.NoError(t, subtree.AddNode(hash2, 122, 0))
		require.NoError(t, subtree.AddNode(hash3, 123, 0))
		require.NoError(t, subtree.AddNode(hash4, 124, 0))

		_, err := txMetaStore.Create(context.Background(), tx1)
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

		blockValidation := NewBlockValidation(p2p.TestLogger{}, nil, subtreeStore, txStore, txMetaStore, validatorClient)
		err = blockValidation.validateSubtree(context.Background(), subtree.RootHash(), "http://localhost:8000")
		require.NoError(t, err)
	})
}

func TestBlockValidation_blessMissingTransaction(t *testing.T) {
	t.Run("blessMissingTransaction - smoke test", func(t *testing.T) {
		initPrometheusMetrics()

		txMetaStore, validatorClient, txStore, _, deferFunc := setup()
		defer deferFunc()

		// add hash1 to txMetaStore
		_, _ = txMetaStore.Create(context.Background(), tx1)

		blockValidation := NewBlockValidation(p2p.TestLogger{}, nil, nil, txStore, txMetaStore, validatorClient)
		_, err := blockValidation.blessMissingTransaction(context.Background(), hash1, "http://localhost:8000")
		require.NoError(t, err)
	})
}

func setup() (*memory.Memory, *validator.MockValidatorClient, blob.Store, blob.Store, func()) {
	// we only need the httpClient, txMetaStore and validatorClient when blessing a transaction
	httpmock.Activate()
	httpmock.RegisterResponder(
		"GET",
		`=~^/tx/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, tx),
	)

	txMetaStore := memory.New()
	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	validatorClient := &validator.MockValidatorClient{}

	return txMetaStore, validatorClient, txStore, subtreeStore, func() {
		httpmock.DeactivateAndReset()
	}
}
