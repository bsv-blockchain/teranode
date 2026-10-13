package aerospike_test

import (
	"fmt"
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

// TestMarkTransactionsOnLongestChain_DAHRules pins the DAH that marking on the longest chain stamps,
// including the cases where it must stamp none and the ones where it must leave an existing one alone.
func TestMarkTransactionsOnLongestChain_DAHRules(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	s := test.CreateBaseTestSettings(t)

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)

	require.NoError(t, store.SetBlockHeight(1000))

	retention := int(s.GetUtxoStoreBlockHeightRetention())
	require.NotZero(t, retention)

	dahBin := fields.DeleteAtHeight.String()

	tests := []struct {
		name  string
		bins  aerospike.BinMap
		want  int // 0: no DAH
		start int // existing DAH, 0: none
	}{
		{name: "mined above the store height uses the mined height", bins: aerospike.BinMap{fields.BlockHeights.String(): []interface{}{1100}}, want: 1100 + retention},
		{name: "mined below the store height uses the store height", bins: aerospike.BinMap{fields.BlockHeights.String(): []interface{}{900}}, want: 1000 + retention},
		{name: "highest block height wins", bins: aerospike.BinMap{fields.BlockHeights.String(): []interface{}{900, 1200, 1100}}, want: 1200 + retention},
		{name: "existing higher DAH is never lowered", bins: aerospike.BinMap{fields.BlockHeights.String(): []interface{}{900}}, start: 5000, want: 5000},
		{name: "existing lower DAH is raised", bins: aerospike.BinMap{fields.BlockHeights.String(): []interface{}{900}}, start: 1002, want: 1000 + retention},
		{name: "unspent output gets no DAH", bins: aerospike.BinMap{fields.SpentUtxos.String(): 0}},
		{name: "unspent output loses a stale DAH", bins: aerospike.BinMap{fields.SpentUtxos.String(): 0}, start: 1002},
		{name: "preserved gets no DAH", bins: aerospike.BinMap{fields.PreserveUntil.String(): 5000}},
		{name: "preserved keeps its DAH", bins: aerospike.BinMap{fields.PreserveUntil.String(): 5000}, start: 1002, want: 1002},
		{name: "empty blockIDs gets no DAH", bins: aerospike.BinMap{fields.BlockIDs.String(): []interface{}{}}},
		{name: "conflicting keeps an existing DAH", bins: aerospike.BinMap{fields.Conflicting.String(): true}, start: 1002, want: 1002},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txID := chainhash.HashH([]byte(fmt.Sprintf("dah-rules-%d", i)))

			key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID[:])
			require.NoError(t, err)

			bins := aerospike.BinMap{
				fields.TxID.String():           txID.CloneBytes(),
				fields.Conflicting.String():    false,
				fields.TotalExtraRecs.String(): 0,
				fields.SpentUtxos.String():     1,
				fields.RecordUtxos.String():    1,
				fields.BlockIDs.String():       []interface{}{1},
				fields.BlockHeights.String():   []interface{}{900},
				fields.UnminedSince.String():   50,
			}
			for k, v := range tt.bins {
				bins[k] = v
			}

			if tt.start > 0 {
				bins[dahBin] = tt.start
			}

			require.NoError(t, client.Put(nil, key, bins))
			require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{txID}, true))

			rec, err := client.Get(nil, key)
			require.NoError(t, err)

			got, has := rec.Bins[dahBin].(int)
			if tt.want == 0 {
				require.False(t, has, "no DAH expected, got %d", got)
			} else {
				require.True(t, has)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

// TestMarkTransactionsOnLongestChain_PageFollowUp checks the pagination records follow the master:
// a cleared master DAH clears the pages, and an unchanged master DAH writes no pages.
func TestMarkTransactionsOnLongestChain_PageFollowUp(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	s := test.CreateBaseTestSettings(t)

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)

	require.NoError(t, store.SetBlockHeight(1000))

	retention := int(s.GetUtxoStoreBlockHeightRetention())
	dahBin := fields.DeleteAtHeight.String()

	setup := func(name string, masterDAH, pageDAH int, spent int) (chainhash.Hash, *aerospike.Key, *aerospike.Key) {
		txID := chainhash.HashH([]byte(name))

		masterKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID[:])
		require.NoError(t, err)

		pageKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), append(txID.CloneBytes(), 1, 0, 0, 0))
		require.NoError(t, err)

		bins := aerospike.BinMap{
			fields.TxID.String():           txID.CloneBytes(),
			fields.External.String():       true,
			fields.Conflicting.String():    false,
			fields.TotalExtraRecs.String(): 1,
			fields.SpentExtraRecs.String(): 1,
			fields.SpentUtxos.String():     spent,
			fields.RecordUtxos.String():    1,
			fields.BlockIDs.String():       []interface{}{1},
			fields.BlockHeights.String():   []interface{}{900},
			fields.UnminedSince.String():   50,
		}
		if masterDAH > 0 {
			bins[dahBin] = masterDAH
		}

		require.NoError(t, client.Put(nil, masterKey, bins))
		require.NoError(t, client.Put(nil, pageKey, aerospike.BinMap{fields.TxID.String(): txID.CloneBytes(), dahBin: pageDAH}))

		return txID, masterKey, pageKey
	}

	t.Run("cleared master DAH clears the page", func(t *testing.T) {
		txID, masterKey, pageKey := setup("stale", 1002, 1002, 0) // unspent: ineligible, stale DAH

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{txID}, true))

		rec, err := client.Get(nil, masterKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[dahBin])

		rec, err = client.Get(nil, pageKey)
		require.NoError(t, err)
		require.Nil(t, rec.Bins[dahBin], "page DAH must follow the master")
	})

	t.Run("unchanged master DAH writes no pages", func(t *testing.T) {
		txID, masterKey, pageKey := setup("unchanged", 1000+retention, 777, 1)

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{txID}, true))

		rec, err := client.Get(nil, masterKey)
		require.NoError(t, err)
		require.Equal(t, 1000+retention, rec.Bins[dahBin])

		rec, err = client.Get(nil, pageKey)
		require.NoError(t, err)
		require.Equal(t, 777, rec.Bins[dahBin], "page must be untouched when the master DAH did not change")
	})
}

// TestSetDAHForChildRecordsMulti_MissingPage checks a pagination record that is gone is not an error
// and is not created as an empty record.
func TestSetDAHForChildRecordsMulti_MissingPage(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	s := test.CreateBaseTestSettings(t)

	client, store, _, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)

	txID := chainhash.HashH([]byte("missing-page"))

	pageKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), append(txID.CloneBytes(), 1, 0, 0, 0))
	require.NoError(t, err)

	require.NoError(t, store.SetDAHForChildRecordsMulti([]struct {
		TxID           *chainhash.Hash
		ChildCount     int
		DeleteAtHeight uint32
	}{{TxID: &txID, ChildCount: 1, DeleteAtHeight: 1234}}))

	exists, err := client.Exists(nil, pageKey)
	require.NoError(t, err)
	require.False(t, exists, "a missing page must not be created")
}
