package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// txWithOutputs builds a transaction with numOutputs outputs that the store
// accepts; its only input spends output 0 of the zero hash.
func txWithOutputs(t *testing.T, numOutputs int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0,
		"76a914000000000000000000000000000000000000000088ac", 1000))

	for range numOutputs {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	return tx
}

// TestReadExtraRecordCounts checks that only a transaction with extra records
// comes back, with its count, and that a missing master is skipped quietly.
func TestReadExtraRecordCounts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	store, _ := setupStoreForLockTest(t)
	store.utxoBatchSize = 4

	single := txWithOutputs(t, 3)
	paginated := txWithOutputs(t, 10)

	_, err := store.Create(ctx, single, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, paginated, 100)
	require.NoError(t, err)

	missing := chainhash.Hash{0xde, 0xad}

	items, err := store.readExtraRecordCounts([]chainhash.Hash{missing, *single.TxIDChainHash(), *paginated.TxIDChainHash()})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, *paginated.TxIDChainHash(), *items[0].txID)
	require.Equal(t, 2, items[0].childCount, "10 outputs at batch size 4 must give two extra records")
}

// TestExtraRecordItemsReportsReadFailures checks the per-record handling of the
// extra record count read without a server: a KEY_NOT_FOUND master is skipped,
// while a read error, or a record returned with neither data nor an error, is
// reported by txid, and the healthy transaction still comes back.
func TestExtraRecordItemsReportsReadFailures(t *testing.T) {
	readPolicy := aerospike.NewBatchReadPolicy()

	newRead := func(t *testing.T, id *chainhash.Hash) *aerospike.BatchRead {
		t.Helper()

		key, err := aerospike.NewKey("test", "test", id[:])
		require.NoError(t, err)

		return aerospike.NewBatchRead(readPolicy, key, []string{fields.TotalExtraRecs.String()})
	}

	healthy := &chainhash.Hash{0x01}
	missing := &chainhash.Hash{0x02}
	timedOut := &chainhash.Hash{0x03}
	empty := &chainhash.Hash{0x04}
	ids := []*chainhash.Hash{healthy, missing, timedOut, empty}

	records := make([]aerospike.BatchRecordIfc, 0, len(ids))
	for _, id := range ids {
		records = append(records, newRead(t, id))
	}

	records[0].BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{fields.TotalExtraRecs.String(): 3}}
	records[1].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}
	records[2].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}

	items, err := extraRecordItems(records, ids)
	require.Error(t, err)
	require.ErrorContains(t, err, timedOut.String())
	require.ErrorContains(t, err, empty.String())
	require.NotContains(t, err.Error(), missing.String())

	require.Len(t, items, 1)
	require.Equal(t, healthy, items[0].txID)
	require.Equal(t, 3, items[0].childCount)
}

// TestApplyLockClearWorkClearsHealthyTxWhenASiblingReadFails checks the
// FILTERED_OUT follow-up when one transaction's count read fails inside an
// otherwise healthy batch: the failure reaches the caller with its txid, and
// the other transaction's extra records still lose `creating`.
func TestApplyLockClearWorkClearsHealthyTxWhenASiblingReadFails(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	store, client := setupStoreForLockTest(t)
	store.utxoBatchSize = 4

	healthy := txWithOutputs(t, 10)
	failing := txWithOutputs(t, 9)

	for _, tx := range []*bt.Tx{healthy, failing} {
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)
	}

	healthyHash := healthy.TxIDChainHash()

	extraKey := func(idx uint32) *aerospike.Key {
		key, err := aerospike.NewKey(store.namespace, store.setName, uaerospike.CalculateKeySourceInternal(healthyHash, idx))
		require.NoError(t, err)

		return key
	}

	for idx := uint32(1); idx <= 2; idx++ {
		require.NoError(t, client.PutBins(nil, extraKey(idx), aerospike.NewBin(fields.Creating.String(), true)))
	}

	failingKey, keyErr := aerospike.NewKey(store.namespace, store.setName, failing.TxIDChainHash()[:])
	require.NoError(t, keyErr)

	// Fail only the count read of the second transaction, after a real batch.
	store.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		if err := client.BatchOperate(policy, records); err != nil {
			return err
		}

		for _, r := range records {
			if _, isRead := r.(*aerospike.BatchRead); isRead && string(r.BatchRec().Key.Digest()) == string(failingKey.Digest()) {
				r.BatchRec().Record = nil
				r.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
			}
		}

		return nil
	}

	t.Cleanup(func() { store.batchOperateFn = nil })

	err := store.applyLockClearWork(ctx, lockClearWork{fullUnlock: []chainhash.Hash{*healthyHash, *failing.TxIDChainHash()}})
	require.ErrorContains(t, err, "readExtraRecordCounts")
	require.ErrorContains(t, err, failing.TxIDChainHash().String())

	for idx := uint32(1); idx <= 2; idx++ {
		rec, getErr := client.Get(nil, extraKey(idx))
		require.NoError(t, getErr)

		_, hasCreating := rec.Bins[fields.Creating.String()]
		require.Falsef(t, hasCreating, "extra record %d of the healthy transaction must lose creating", idx)
	}
}

// TestApplyLockClearWorkReportsEveryFailure checks that a failed read of the
// extra record count, a failed clear and a failed unlock all reach the caller.
func TestApplyLockClearWorkReportsEveryFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	store, _ := setupStoreForLockTest(t)

	txID := chainhash.Hash{0x01}
	other := chainhash.Hash{0x02}

	// Every Aerospike call fails once the client is closed.
	store.client.Close()

	t.Run("clear fails alone", func(t *testing.T) {
		err := store.applyLockClearWork(context.Background(), lockClearWork{
			items: []lockClearItem{{txID: &txID, childCount: 1}},
		})
		require.ErrorContains(t, err, "clearLockedOnRecordsMulti")
	})

	t.Run("read, clear and unlock all fail", func(t *testing.T) {
		err := store.applyLockClearWork(context.Background(), lockClearWork{
			items:      []lockClearItem{{txID: &txID, childCount: 1}},
			fullUnlock: []chainhash.Hash{other},
		})
		require.ErrorContains(t, err, "readExtraRecordCounts")
		require.ErrorContains(t, err, "clearLockedOnRecordsMulti")
		require.ErrorContains(t, err, "setLocked")
	})
}
