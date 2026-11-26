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

// failRecordAfterBatch runs every cleanup batch for real, then fails each record
// of txHash's master that match selects with a TIMEOUT. The flag it returns
// reports whether such a record was sent.
func failRecordAfterBatch(t *testing.T, store *Store, client *uaerospike.Client, txHash *chainhash.Hash, match func(aerospike.BatchRecordIfc) bool) *bool {
	t.Helper()

	key, err := aerospike.NewKey(store.namespace, store.setName, txHash[:])
	require.NoError(t, err)

	seen := false

	store.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		if err := client.BatchOperate(policy, records); err != nil {
			return err
		}

		for _, r := range records {
			if match(r) && string(r.BatchRec().Key.Digest()) == string(key.Digest()) {
				seen = true
				r.BatchRec().Record = nil
				r.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
			}
		}

		return nil
	}

	t.Cleanup(func() { store.batchOperateFn = nil })

	return &seen
}

// TestReadExtraRecordCounts checks that only a transaction with extra records
// comes back, with its count, and that a missing master is skipped quietly.
func TestReadExtraRecordCounts(t *testing.T) {
	t.Parallel()

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

	extraKey := func(idx uint32) *aerospike.Key {
		key, err := aerospike.NewKey(store.namespace, store.setName, uaerospike.CalculateKeySourceInternal(healthyHash, idx))
		require.NoError(t, err)

		return key
	}

	for idx := uint32(1); idx <= 2; idx++ {
		require.NoError(t, client.PutBins(nil, extraKey(idx), aerospike.NewBin(fields.Creating.String(), true)))
	}

	// Fail only the count read of the second transaction.
	failRecordAfterBatch(t, store, client, failing.TxIDChainHash(), func(r aerospike.BatchRecordIfc) bool {
		_, isRead := r.(*aerospike.BatchRead)
		return isRead
	})

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

// TestApplyLockClearWorkReportsAFailedMasterClear checks the FILTERED_OUT
// follow-up of a single-record transaction: the master's clear is attempted, and
// its failure reaches the caller as a storage error naming the record.
func TestApplyLockClearWorkReportsAFailedMasterClear(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	store, client := setupStoreForLockTest(t)

	tx := txWithOutputs(t, 2)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Fail only the master's clear write.
	clearAttempted := failRecordAfterBatch(t, store, client, txHash, func(r aerospike.BatchRecordIfc) bool {
		_, isWrite := r.(*aerospike.BatchWrite)
		return isWrite
	})

	err = store.applyLockClearWork(ctx, lockClearWork{fullUnlock: []chainhash.Hash{*txHash}})
	require.True(t, *clearAttempted, "the master of a FILTERED_OUT transaction must be cleared")
	require.ErrorIs(t, err, errors.ErrStorageError)
	require.ErrorContains(t, err, "clearLockedOnRecordsMulti")
	require.ErrorContains(t, err, txHash.String()+"] failed to clear record 0")
}

// TestClearLockedOnRecordsMultiMasterItem pins the master write of a FILTERED_OUT
// follow-up: it deletes only `creating`, leaves a missing master missing and
// does not rewrite a master that has no `creating` bin.
func TestClearLockedOnRecordsMultiMasterItem(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	store, client := setupStoreForLockTest(t)

	masterKey := func(txHash *chainhash.Hash) *aerospike.Key {
		key, err := aerospike.NewKey(store.namespace, store.setName, txHash[:])
		require.NoError(t, err)

		return key
	}

	t.Run("missing master stays missing", func(t *testing.T) {
		txHash := txWithOutputs(t, 2).TxIDChainHash()

		require.NoError(t, store.clearLockedOnRecordsMulti([]lockClearItem{{txID: txHash, master: true}}))

		exists, err := client.Exists(nil, masterKey(txHash))
		require.NoError(t, err)
		require.False(t, exists, "the clear must not create a master")
	})

	t.Run("master without creating is not rewritten", func(t *testing.T) {
		tx := txWithOutputs(t, 2)
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)

		txHash := tx.TxIDChainHash()

		before, err := client.Get(nil, masterKey(txHash))
		require.NoError(t, err)

		require.NoError(t, store.clearLockedOnRecordsMulti([]lockClearItem{{txID: txHash, master: true}}))

		after, err := client.Get(nil, masterKey(txHash))
		require.NoError(t, err)
		require.Equal(t, before.Generation, after.Generation, "a master without creating must not be written")
	})

	t.Run("master with creating loses only creating", func(t *testing.T) {
		tx := txWithOutputs(t, 2)
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)

		txHash := tx.TxIDChainHash()
		require.NoError(t, client.PutBins(nil, masterKey(txHash),
			aerospike.NewBin(fields.Creating.String(), true), aerospike.NewBin(fields.Locked.String(), true)))

		require.NoError(t, store.clearLockedOnRecordsMulti([]lockClearItem{{txID: txHash, master: true}}))

		rec, err := client.Get(nil, masterKey(txHash))
		require.NoError(t, err)

		_, hasCreating := rec.Bins[fields.Creating.String()]
		require.False(t, hasCreating, "the master must lose creating")
		require.Equal(t, true, rec.Bins[fields.Locked.String()], "the master's locked is left to SetLocked")
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
