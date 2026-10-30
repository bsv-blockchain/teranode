package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestPrunerWithdrawsMarkerFromHeldBackChild: C spends P:0 and Q:0 and is
// buried. Q's marker write fails, so C is held back and stays in the store. The
// marker that landed on P in the same cycle must be withdrawn. Left in place it
// poisons a live transaction: the spend path answers the marker ahead of every
// other check, so re-validating C's block (catch-up re-run, reorg) is refused
// as a pruned replay, and nothing compensates because C's record was not
// created by that attempt. This runs the store's real marking path (Lua or the
// native builder), not the pruner package's plain map write.
func TestPrunerWithdrawsMarkerFromHeldBackChild(t *testing.T) {
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = false
	s.Aerospike.EnableSpendFilterExpressions = true

	client, store, ctx, cleanup := initAerospike(t, s, ulogger.New("pruner-withdraw-test"))
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	mined := func(height uint32, txs ...*bt.Tx) {
		hashes := make([]*chainhash.Hash, 0, len(txs))
		for _, tx := range txs {
			hashes = append(hashes, tx.TxIDChainHash())
		}

		_, err := store.SetMinedMulti(ctx, hashes, utxo.MinedBlockInfo{BlockID: height, BlockHeight: height, OnLongestChain: true})
		require.NoError(t, err)
	}

	newParent := func(seed string) *bt.Tx {
		parent := bt.NewTx()
		require.NoError(t, parent.From(seed, 0, "51", 30000))
		// Output 1 stays unspent so the parent survives the prune cycle.
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)

		return parent
	}

	p := newParent("1111111111111111111111111111111111111111111111111111111111111111")
	q := newParent("2222222222222222222222222222222222222222222222222222222222222222")

	c := bt.NewTx()
	require.NoError(t, c.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	require.NoError(t, c.From(q.TxID(), 0, q.Outputs[0].LockingScript.String(), q.Outputs[0].Satoshis))
	require.NoError(t, c.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 7000))
	_, _, err := store.SpendAndCreate(ctx, c, 1000)
	require.NoError(t, err)
	mined(1000, p, q, c)

	d := bt.NewTx()
	require.NoError(t, d.From(c.TxID(), 0, c.Outputs[0].LockingScript.String(), c.Outputs[0].Satoshis))
	require.NoError(t, d.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 6000))
	_, _, err = store.SpendAndCreate(ctx, d, 1001)
	require.NoError(t, err)
	mined(1001, d)

	key := func(txID *chainhash.Hash) *aerospike.Key {
		k, keyErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySource(txID, 0, s.UtxoStore.UtxoBatchSize))
		require.NoError(t, keyErr)

		return k
	}

	astore.ResetPrunerServiceForTests()
	t.Cleanup(astore.ResetPrunerServiceForTests)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	require.NoError(t, client.Put(nil, key(q.TxIDChainHash()), aerospike.BinMap{fields.DeletedChildren.String(): "invalid-map"}))

	pruned, err := svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "withdraw-marker", 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), pruned, "C must be held back")

	exists, err := client.Exists(nil, key(c.TxIDChainHash()))
	require.NoError(t, err)
	require.True(t, exists, "the held-back child is retained")

	pRecord, err := client.Get(nil, key(p.TxIDChainHash()))
	require.NoError(t, err)

	if markers := pRecord.Bins[fields.DeletedChildren.String()]; markers != nil {
		require.NotContains(t, markers, c.TxID(), "P must not keep a marker for a child that was held back")
	}

	// The live child re-validates: its spends are idempotent, not a replay.
	require.NoError(t, client.Put(nil, key(q.TxIDChainHash()), aerospike.BinMap{fields.DeletedChildren.String(): nil}))

	_, err = store.Spend(ctx, c, 1200)
	require.NoError(t, err, "the held-back live child must still be able to re-spend its own inputs")
}
