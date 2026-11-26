package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// txWithOutputs builds a transaction with numOutputs outputs that the store
// accepts; its only input spends output 0 of the zero hash.
func txWithOutputs(t *testing.T, numOutputs int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = NextUniqueLockTime() // a txid of its own: the server is shared
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0,
		"76a914000000000000000000000000000000000000000088ac", 1000))

	for range numOutputs {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	return tx
}

// failRecordsAfterBatch runs every cleanup batch for real, then fails with a
// TIMEOUT each record whose key is in keys and that match selects. The flag it
// returns reports whether such a record was sent.
func failRecordsAfterBatch(t *testing.T, store *Store, client *uaerospike.Client, keys []*aerospike.Key, match func(aerospike.BatchRecordIfc) bool) *bool {
	t.Helper()

	seen := false

	store.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		if err := client.BatchOperate(policy, records); err != nil {
			return err
		}

		for _, r := range records {
			if !match(r) {
				continue
			}

			for _, key := range keys {
				if string(r.BatchRec().Key.Digest()) == string(key.Digest()) {
					seen = true
					r.BatchRec().Record = nil
					r.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
				}
			}
		}

		return nil
	}

	t.Cleanup(func() { store.batchOperateFn = nil })

	return &seen
}

// recordKey is the key of record idx of txHash.
func recordKey(t *testing.T, store *Store, txHash *chainhash.Hash, idx uint32) *aerospike.Key {
	t.Helper()

	key, err := aerospike.NewKey(store.namespace, store.setName, uaerospike.CalculateKeySourceInternal(txHash, idx))
	require.NoError(t, err)

	return key
}

// hasCreating reports whether record idx of txHash has a `creating` bin.
func hasCreating(t *testing.T, client *uaerospike.Client, store *Store, txHash *chainhash.Hash, idx uint32) bool {
	t.Helper()

	rec, err := client.Get(nil, recordKey(t, store, txHash, idx))
	require.NoError(t, err)

	_, ok := rec.Bins[fields.Creating.String()]

	return ok
}

func isBatchWrite(r aerospike.BatchRecordIfc) bool {
	_, ok := r.(*aerospike.BatchWrite)
	return ok
}

// TestReadExtraRecordCounts checks that a transaction comes back only with extra
// records or a `creating` master, and that a missing master is skipped quietly.
func TestReadExtraRecordCounts(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	store, client := setupStoreForLockTest(t)
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
	require.False(t, items[0].master, "a master without creating needs no clear")

	require.NoError(t, client.PutBins(nil, recordKey(t, store, single.TxIDChainHash(), 0), aerospike.NewBin(fields.Creating.String(), true)))

	items, err = store.readExtraRecordCounts([]chainhash.Hash{*single.TxIDChainHash()})
	require.NoError(t, err)
	require.Equal(t, []lockClearItem{{txID: single.TxIDChainHash(), master: true}}, items)
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

// TestApplyLockClearWorkClearsHealthyTxWhenASiblingReadFails: a failed count
// read reaches the caller with its txid and keeps `creating` on that master,
// while the healthy transaction in the same batch still loses it.
func TestApplyLockClearWorkClearsHealthyTxWhenASiblingReadFails(t *testing.T) {
	t.Parallel()

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

	for idx := uint32(0); idx <= 2; idx++ {
		require.NoError(t, client.PutBins(nil, recordKey(t, store, healthyHash, idx), aerospike.NewBin(fields.Creating.String(), true)))
	}

	require.NoError(t, client.PutBins(nil, recordKey(t, store, failing.TxIDChainHash(), 0), aerospike.NewBin(fields.Creating.String(), true)))

	// Fail only the count read of the second transaction.
	failRecordsAfterBatch(t, store, client, []*aerospike.Key{recordKey(t, store, failing.TxIDChainHash(), 0)}, func(r aerospike.BatchRecordIfc) bool {
		_, isRead := r.(*aerospike.BatchRead)
		return isRead
	})

	err := store.applyLockClearWork(ctx, lockClearWork{fullUnlock: []chainhash.Hash{*healthyHash, *failing.TxIDChainHash()}})
	require.ErrorContains(t, err, "readExtraRecordCounts")
	require.ErrorContains(t, err, failing.TxIDChainHash().String())

	for idx := uint32(0); idx <= 2; idx++ {
		require.Falsef(t, hasCreating(t, client, store, healthyHash, idx), "record %d of the healthy transaction must lose creating", idx)
	}

	require.True(t, hasCreating(t, client, store, failing.TxIDChainHash(), 0), "a master whose read failed must keep creating")
}

// TestApplyLockClearWorkReportsFailedClears checks the FILTERED_OUT follow-up
// when clears fail: each failure names its transaction and record, and a failed
// extra record keeps `creating` on its master.
func TestApplyLockClearWorkReportsFailedClears(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	store, client := setupStoreForLockTest(t)
	store.utxoBatchSize = 4

	healthy := txWithOutputs(t, 2)
	paginated := txWithOutputs(t, 10)
	single := txWithOutputs(t, 2)

	for _, tx := range []*bt.Tx{healthy, paginated, single} {
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)
	}

	healthyHash := healthy.TxIDChainHash()
	paginatedHash := paginated.TxIDChainHash()
	singleHash := single.TxIDChainHash()

	for _, key := range []*aerospike.Key{recordKey(t, store, healthyHash, 0), recordKey(t, store, paginatedHash, 0), recordKey(t, store, singleHash, 0)} {
		require.NoError(t, client.PutBins(nil, key, aerospike.NewBin(fields.Creating.String(), true)))
	}

	// Fail the clear of the paginated transaction's first extra record and of the
	// single-record transaction's master.
	clearAttempted := failRecordsAfterBatch(t, store, client,
		[]*aerospike.Key{recordKey(t, store, paginatedHash, 1), recordKey(t, store, singleHash, 0)}, isBatchWrite)

	// The healthy transaction comes first, so a failure named after the wrong item shows.
	err := store.applyLockClearWork(ctx, lockClearWork{fullUnlock: []chainhash.Hash{*healthyHash, *paginatedHash, *singleHash}})
	require.True(t, *clearAttempted)
	require.ErrorIs(t, err, errors.ErrStorageError)
	require.ErrorContains(t, err, paginatedHash.String()+"] failed to clear record 1")
	require.ErrorContains(t, err, singleHash.String()+"] failed to clear record 0")

	require.True(t, hasCreating(t, client, store, paginatedHash, 0), "a failed extra record must keep creating on its master")
	require.False(t, hasCreating(t, client, store, paginatedHash, 2), "the other extra record is still cleared")
	require.False(t, hasCreating(t, client, store, healthyHash, 0), "a healthy master in the same batch is cleared")
	require.NotContains(t, err.Error(), healthyHash.String())
}

// TestClearLockedOnRecordsMultiMasterItem pins the master write: it deletes only
// `creating`, never creates or needlessly rewrites a master, and waits for every
// extra record of its transaction.
func TestClearLockedOnRecordsMultiMasterItem(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	store, client := setupStoreForLockTest(t)
	store.utxoBatchSize = 4

	exists := func(t *testing.T, key *aerospike.Key) bool {
		ok, err := client.Exists(nil, key)
		require.NoError(t, err)

		return ok
	}

	create := func(t *testing.T, outputs int, masterCreating bool) *chainhash.Hash {
		tx := txWithOutputs(t, outputs)
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)

		if masterCreating {
			require.NoError(t, client.PutBins(nil, recordKey(t, store, tx.TxIDChainHash(), 0),
				aerospike.NewBin(fields.Creating.String(), true), aerospike.NewBin(fields.Locked.String(), true)))
		}

		return tx.TxIDChainHash()
	}

	t.Run("missing master stays missing", func(t *testing.T) {
		txHash := txWithOutputs(t, 2).TxIDChainHash()

		require.NoError(t, store.clearLockedOnRecordsMulti([]lockClearItem{{txID: txHash, master: true}}))
		require.False(t, exists(t, recordKey(t, store, txHash, 0)), "the clear must not create a master")
	})

	t.Run("master without creating is not rewritten", func(t *testing.T) {
		txHash := create(t, 2, false)

		before, err := client.Get(nil, recordKey(t, store, txHash, 0))
		require.NoError(t, err)

		require.NoError(t, store.clearLockedOnRecordsMulti([]lockClearItem{{txID: txHash, master: true}}))

		after, err := client.Get(nil, recordKey(t, store, txHash, 0))
		require.NoError(t, err)
		require.Equal(t, before.Generation, after.Generation, "a master without creating must not be written")
	})

	t.Run("master with creating loses only creating", func(t *testing.T) {
		txHash := create(t, 2, true)

		require.NoError(t, store.clearLockedOnRecordsMulti([]lockClearItem{{txID: txHash, master: true}}))
		require.False(t, hasCreating(t, client, store, txHash, 0), "the master must lose creating")

		rec, err := client.Get(nil, recordKey(t, store, txHash, 0))
		require.NoError(t, err)
		require.Equal(t, true, rec.Bins[fields.Locked.String()], "the master's locked is left to SetLocked")
	})

	t.Run("item without master leaves the master alone", func(t *testing.T) {
		txHash := create(t, 10, true)

		require.NoError(t, store.clearLockedOnRecordsMulti([]lockClearItem{{txID: txHash, childCount: 2}}))
		require.True(t, hasCreating(t, client, store, txHash, 0), "only an item that sets master clears the master")
		require.False(t, hasCreating(t, client, store, txHash, 2), "its extra records are cleared")
	})

	t.Run("missing extra record keeps creating on the master", func(t *testing.T) {
		txHash := create(t, 10, true)

		_, err := client.Delete(nil, recordKey(t, store, txHash, 2))
		require.NoError(t, err)

		require.NoError(t, store.clearLockedOnRecordsMulti([]lockClearItem{{txID: txHash, childCount: 2, master: true}}))
		require.True(t, hasCreating(t, client, store, txHash, 0), "an incomplete transaction must keep creating on its master")
		require.False(t, exists(t, recordKey(t, store, txHash, 2)), "the missing extra record stays missing")
	})
}

// TestApplyLockClearWorkReportsEveryFailure checks that a failed read of the
// extra record count, a failed clear and a failed unlock all reach the caller.
func TestApplyLockClearWorkReportsEveryFailure(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	store, _ := setupStoreForLockTestOwnServer(t)

	txID := chainhash.Hash{0x01}
	other := chainhash.Hash{0x02}

	// Every Aerospike call fails once the client is closed.
	store.client.Close()

	t.Run("clear fails alone", func(t *testing.T) {
		err := store.applyLockClearWork(context.Background(), lockClearWork{
			items: []lockClearItem{{txID: &txID, childCount: 1}},
		})
		require.ErrorContains(t, err, "[clearLockedOnRecordsMulti] failed to clear locked flag")
	})

	t.Run("master clear fails alone", func(t *testing.T) {
		err := store.applyLockClearWork(context.Background(), lockClearWork{
			items: []lockClearItem{{txID: &txID, master: true}},
		})
		require.ErrorIs(t, err, errors.ErrStorageError)
		require.ErrorContains(t, err, "[clearLockedOnRecordsMulti] failed to clear creating on 1 master records")
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
