package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestPrunerReplayProtection: pruning a fully-spent child whose parent still
// has another output must leave deletedChildren on that parent, so
// SpendAndCreate cannot recreate the child as unmined.
func TestPrunerReplayProtection(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)

	require.NoError(t, store.SetBlockHeight(1000))

	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 1000, BlockHeight: 1000, OnLongestChain: true,
	})
	require.NoError(t, err)

	grandchild := bt.NewTx()
	require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
	require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
	_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 1001, BlockHeight: 1001, OnLongestChain: true,
	})
	require.NoError(t, err)

	astore.ResetPrunerServiceForTests()
	t.Cleanup(astore.ResetPrunerServiceForTests)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc)
	prunerSvc, ok := svc.(*apruner.Service)
	require.True(t, ok)
	n, err := prunerSvc.PruneWithPartitions(ctx, 1300, "replay-protection", 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	childKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), child.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	exists, err := client.Exists(nil, childKey)
	require.NoError(t, err)
	require.False(t, exists)

	_, _, err = store.SpendAndCreate(ctx, child, 1400)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	// ErrUtxoError is a broad class — pin the rejection to the deletedChildren
	// check in teranode.lua rather than any spend failure.
	require.Contains(t, err.Error(), "invalid spend")

	// The symptom in #1701 is the child being recreated as unmined with empty
	// blockIDs, so a rejected replay that still wrote the record must fail here.
	exists, err = client.Exists(nil, childKey)
	require.NoError(t, err)
	require.False(t, exists, "rejected replay must not recreate the pruned child")

	parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	parentRec, err := client.Get(nil, parentKey, fields.DeletedChildren.String())
	require.NoError(t, err)
	deletedChildren, ok := parentRec.Bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
	require.True(t, ok)
	_, marked := deletedChildren[child.TxID()]
	require.True(t, marked)
}
