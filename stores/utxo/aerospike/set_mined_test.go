package aerospike

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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

// TestSetMinedMultiKeepsTheStorageCodeOfAFailedCleanup checks that a cleanup
// failure after a successful mined update leaves SetMinedMulti as a StorageError,
// so legacy sync keeps the block for a retry instead of rejecting it.
func TestSetMinedMultiKeepsTheStorageCodeOfAFailedCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	for _, tc := range []struct {
		name        string
		expressions bool
		sibling     bool // a never-created hash in the batch, so the mined update fails too
		cleanup     string
		// fail is the cleanup's batch seam; nil fails every cleanup batch whole.
		fail func(client *uaerospike.Client) func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error
		// recordTimeouts is how many per-record timeouts the error must carry; 0 skips the check.
		recordTimeouts int
	}{
		{name: "lua: the clear of the extra records fails", cleanup: "clearLockedOnRecordsMulti"},
		{name: "expressions: the extra record count read of a FILTERED_OUT tx fails", expressions: true, cleanup: "readExtraRecordCounts"},
		{name: "lua: the clear fails beside a failed sibling", sibling: true, cleanup: "clearLockedOnRecordsMulti"},
		{name: "expressions: the count read fails beside a failed sibling", expressions: true, sibling: true, cleanup: "readExtraRecordCounts"},
		{name: "lua: each record of the clear fails on its own", cleanup: "clearLockedOnRecordsMulti", fail: failClearRecords, recordTimeouts: 2},
		{name: "expressions: only the unlock of a FILTERED_OUT tx fails", expressions: true, cleanup: "applyLockClearWork", fail: failAfterReadAndClear},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Aerospike.EnableSetMinedFilterExpressions = tc.expressions

			store, client := newSharedTestStore(t, logger, tSettings, "&externalStore=file://./data/external&block_retention=100")
			store.utxoBatchSize = 4

			tx := txWithOutputs(t, 10)
			_, err := store.Create(ctx, tx, 100, utxo.WithLocked(true))
			require.NoError(t, err)

			hashes := []*chainhash.Hash{tx.TxIDChainHash()}
			info := utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}

			if tc.expressions {
				// The first mine stores the blockID, so the next write is FILTERED_OUT.
				_, err = store.SetMinedMulti(ctx, hashes, info)
				require.NoError(t, err)
			}

			missing := &chainhash.Hash{0xDE, 0xAD, 0xBE, 0xEF}
			if tc.sibling {
				hashes = append(hashes, missing)
			}

			// The mined update calls the client directly. The cleanup batches go through this seam, and so
			// does the pre-flight read of the creating state, which is let through: only the cleanup fails.
			fail := errorOperate()
			if tc.fail != nil {
				fail = tc.fail(client)
			}

			store.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				if isCreatingStateRead(records) {
					return client.BatchOperate(policy, records)
				}

				return fail(policy, records)
			}

			t.Cleanup(func() { store.batchOperateFn = nil })

			_, err = store.SetMinedMulti(ctx, hashes, info)
			require.ErrorIs(t, err, errors.ErrStorageError)
			require.ErrorContains(t, err, tc.cleanup)

			if tc.sibling {
				require.ErrorContains(t, err, missing.String(), "the sibling's failure must survive the cleanup's")
			}

			if tc.recordTimeouts > 0 {
				require.Equal(t, tc.recordTimeouts, strings.Count(err.Error(), "ResultCode: TIMEOUT"), "every failed record must reach the caller: %v", err)
			}

			// Quick validation's wrap, which legacy sync's park reads as retry-later.
			err = errors.NewProcessingError("[createAndSpendUTXOsForBatch] failed to update mined info",
				utxo.SetMinedMultiChunked(ctx, logger, store, hashes, info, 10, 1))
			require.True(t, errors.IsTransientLocalError(err), "got %v", err)
		})
	}
}

// isCreatingStateRead reports whether a batch is the pre-flight's read of the creating state.
func isCreatingStateRead(records []aerospike.BatchRecordIfc) bool {
	for _, record := range records {
		read, ok := record.(*aerospike.BatchRead)
		if !ok || !slices.Equal(read.BinNames, creatingReadBins) {
			return false
		}
	}

	return len(records) > 0
}

// failClearRecords runs each batch for real, then marks every written record
// as timed out, so the batch succeeds and each record fails on its own.
func failClearRecords(client *uaerospike.Client) func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
	return func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		if err := client.BatchOperate(policy, records); err != nil {
			return err
		}

		for _, r := range records {
			if _, isWrite := r.(*aerospike.BatchWrite); isWrite {
				r.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
			}
		}

		return nil
	}
}

// failAfterReadAndClear lets the first two cleanup batches of a FILTERED_OUT tx
// run for real (the count read, then the clear) and fails every later one, which
// is SetLocked's unlock.
func failAfterReadAndClear(client *uaerospike.Client) func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
	var calls atomic.Int32

	return func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		if calls.Add(1) <= 2 {
			return client.BatchOperate(policy, records)
		}

		return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
	}
}

// TestSetMinedMultiKeepsTheStorageCodeOfAFailedCreatingRead checks that a failed
// pre-flight read of the creating state leaves SetMinedMulti as a StorageError,
// so legacy sync keeps the block for a retry instead of rejecting it. Nothing is
// marked mined before the read succeeds, and the retry completes the call.
func TestSetMinedMultiKeepsTheStorageCodeOfAFailedCreatingRead(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	for _, tc := range []struct {
		name        string
		expressions bool
		// fail is what the seam does to the read of the creating state.
		fail func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error
	}{
		{name: "lua: the batch of the read fails", fail: errorOperate},
		{name: "lua: every record of the read fails", fail: timeoutEveryRecord},
		{name: "expressions: the batch of the read fails", expressions: true, fail: errorOperate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Aerospike.EnableSetMinedFilterExpressions = tc.expressions

			store, client := newSharedTestStore(t, logger, tSettings, "&externalStore=file://./data/external&block_retention=100")

			tx := txWithOutputs(t, 2)
			_, err := store.Create(ctx, tx, 100)
			require.NoError(t, err)

			hashes := []*chainhash.Hash{tx.TxIDChainHash()}
			info := utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}

			fail := tc.fail()

			store.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				if isCreatingStateRead(records) {
					return fail(policy, records)
				}

				return client.BatchOperate(policy, records)
			}

			t.Cleanup(func() { store.batchOperateFn = nil })

			_, err = store.SetMinedMulti(ctx, hashes, info)
			require.ErrorIs(t, err, errors.ErrStorageError)
			require.ErrorContains(t, err, "read of the creating state")

			// Quick validation's wrap, which legacy sync's park reads as retry-later.
			err = errors.NewProcessingError("[createAndSpendUTXOsForBatch] failed to update mined info",
				utxo.SetMinedMultiChunked(ctx, logger, store, hashes, info, 10, 1))
			require.True(t, errors.IsTransientLocalError(err), "got %v", err)

			got, err := store.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
			require.NoError(t, err)
			require.Empty(t, got.BlockIDs, "the pre-flight fails closed: nothing is marked mined while the creating state is not read")

			store.batchOperateFn = nil

			_, err = store.SetMinedMulti(ctx, hashes, info)
			require.NoError(t, err, "the retry completes once the store answers")

			got, err = store.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
			require.NoError(t, err)
			require.Equal(t, []uint32{7}, got.BlockIDs)
		})
	}
}

// TestProcessBatchResultsForSetMinedKeepsTheStorageCodeOfAFailedIncrement checks
// that a failed spent-extra-records increment after a successful mine reaches
// the caller as a StorageError, so legacy sync keeps the block for a retry.
func TestProcessBatchResultsForSetMinedKeepsTheStorageCodeOfAFailedIncrement(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error
	}{
		{name: "the whole batch fails", fail: errorOperate},
		{name: "every record of a successful batch fails", fail: timeoutEveryRecord},
	} {
		t.Run(tc.name, func(t *testing.T) {
			processBatchResultsWithFailedFollowUps(t, tc.fail())
		})
	}
}

func processBatchResultsWithFailedFollowUps(t *testing.T, fail func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error) {
	t.Helper()

	s := newTestStoreForSetMinedExpressions(t)
	s.batchOperateFn = fail

	hash := &chainhash.Hash{0xCC}
	rec := newFakeBatchWrite(t, hash)
	rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
		LuaSuccess.String(): map[interface{}]interface{}{
			"status": "OK",
			"signal": string(LuaSignalAllSpent),
		},
	}}

	// A second tx whose child DAH unset fails too: both follow-up errors must reach the caller.
	unsetHash := &chainhash.Hash{0xCD}
	unsetRec := newFakeBatchWrite(t, unsetHash)
	unsetRec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
		LuaSuccess.String(): map[interface{}]interface{}{
			"status":     "OK",
			"signal":     string(LuaSignalDAHUnset),
			"childCount": 1,
		},
	}}

	_, _, err := s.processBatchResultsForSetMined(context.Background(), []aerospike.BatchRecordIfc{rec, unsetRec},
		[]*chainhash.Hash{hash, unsetHash}, utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100})
	require.ErrorIs(t, err, errors.ErrStorageError)
	require.ErrorContains(t, err, "IncrementSpentRecordsMulti")
	require.ErrorContains(t, err, "SetDAHForChildRecordsMulti")
}

// timeoutEveryRecord reports the batch as sent and every record in it as timed out.
func timeoutEveryRecord() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
	return func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, r := range records {
			r.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		}

		return nil
	}
}
