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
	"github.com/stretchr/testify/require"
)

// TestMarkTransactionsOnLongestChain_RestampsDAH covers the repair path: a mined, fully spent tx is
// marked unmined (its DAH is kept), the pruner clears that DAH, and marking it on the longest chain
// again must stamp a DAH so the tx is still pruned.
func TestMarkTransactionsOnLongestChain_RestampsDAH(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = false

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)

	require.NoError(t, store.SetBlockHeight(1000))

	retention := s.GetUtxoStoreBlockHeightRetention()
	require.NotZero(t, retention)

	astore.ResetPrunerServiceForTests()
	t.Cleanup(astore.ResetPrunerServiceForTests)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	prunerSvc, ok := svc.(*apruner.Service)
	require.True(t, ok)

	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	_, err = store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parentHash[:])
	require.NoError(t, err)

	dahOf := func() (int, bool) {
		rec, gerr := client.Get(nil, key)
		require.NoError(t, gerr)

		v, has := rec.Bins[fields.DeleteAtHeight.String()].(int)

		return v, has
	}

	dah, has := dahOf()
	require.True(t, has, "mined and fully spent: DAH stamped")
	require.Equal(t, 1000+int(retention), dah)

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentHash}, false))

	_, has = dahOf()
	require.True(t, has, "marking unmined keeps the DAH")

	n, err := prunerSvc.PruneWithPartitions(ctx, uint32(dah), "unmined", 1)
	require.NoError(t, err)
	require.Zero(t, n)

	_, has = dahOf()
	require.False(t, has, "the pruner clears the DAH of the unmined tx")

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentHash}, true))

	dah, has = dahOf()
	require.True(t, has, "marking on the longest chain must re-stamp the DAH")
	require.Equal(t, 1000+int(retention), dah)

	n, err = prunerSvc.PruneWithPartitions(ctx, uint32(dah), "mined", 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	exists, err := client.Exists(nil, key)
	require.NoError(t, err)
	require.False(t, exists, "the re-mined tx is pruned at its new DAH")
}

// TestMarkTransactionsOnLongestChain_RestampsPageDAH checks that the DAH set on a large tx's master
// is propagated to its pagination records.
func TestMarkTransactionsOnLongestChain_RestampsPageDAH(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	s := test.CreateBaseTestSettings(t)

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)

	require.NoError(t, store.SetBlockHeight(1000))

	retention := s.GetUtxoStoreBlockHeightRetention()
	require.NotZero(t, retention)

	txID := chainhash.HashH([]byte("large"))

	masterKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID[:])
	require.NoError(t, err)

	pageKeySource := append(txID.CloneBytes(), 1, 0, 0, 0)
	pageKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), pageKeySource)
	require.NoError(t, err)

	// a mined, fully spent external tx with one pagination record, marked unmined and with its DAH
	// already cleared by the pruner
	require.NoError(t, client.Put(nil, masterKey, aerospike.BinMap{
		fields.TxID.String():           txID.CloneBytes(),
		fields.External.String():       true,
		fields.Conflicting.String():    false,
		fields.TotalExtraRecs.String(): 1,
		fields.SpentExtraRecs.String(): 1,
		fields.SpentUtxos.String():     1,
		fields.RecordUtxos.String():    1,
		fields.BlockIDs.String():       []interface{}{1},
		fields.UnminedSince.String():   900,
	}))
	require.NoError(t, client.Put(nil, pageKey, aerospike.BinMap{fields.TxID.String(): txID.CloneBytes()}))

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{txID}, true))

	for name, key := range map[string]*aerospike.Key{"master": masterKey, "page": pageKey} {
		rec, gerr := client.Get(nil, key)
		require.NoError(t, gerr)
		require.Equal(t, 1000+int(retention), rec.Bins[fields.DeleteAtHeight.String()], "%s DAH", name)
	}

	rec, err := client.Get(nil, masterKey)
	require.NoError(t, err)
	require.Nil(t, rec.Bins[fields.UnminedSince.String()])
}
