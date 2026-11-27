package aerospike

import (
	"context"
	"fmt"
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

// timedOut is how a clear error lists records that failed with TIMEOUT.
func timedOut(idxs ...uint32) string {
	parts := make([]string, len(idxs))
	for i, idx := range idxs {
		parts[i] = fmt.Sprintf("%d (ResultCode: TIMEOUT)", idx)
	}

	return strings.Join(parts, ", ")
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

// TestApplyLockClearWorkReportsFailures checks the FILTERED_OUT follow-up when a
// read or a clear fails: each failure names its transaction, the failing
// transaction's master keeps `creating`, and healthy transactions still lose it.
func TestApplyLockClearWorkReportsFailures(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	type rec struct {
		tx  int    // index into outputs and hashes
		idx uint32 // record index, 0 is the master
	}

	type named struct {
		tx   int
		text string // expected after the txid in the error
	}

	isBatchRead := func(r aerospike.BatchRecordIfc) bool {
		_, ok := r.(*aerospike.BatchRead)
		return ok
	}

	for _, tc := range []struct {
		name     string
		outputs  []int // one transaction per entry, at batch size 4; the first is healthy
		creating []rec
		fail     []rec
		match    func(aerospike.BatchRecordIfc) bool
		wantErr  []named
		keep     []rec
		lose     []rec
	}{
		{
			name:     "count read fails",
			outputs:  []int{10, 9},
			creating: []rec{{0, 0}, {0, 1}, {0, 2}, {1, 0}},
			fail:     []rec{{1, 0}},
			match:    isBatchRead,
			wantErr:  []named{{1, "] failed to read the extra record count"}},
			keep:     []rec{{1, 0}},
			lose:     []rec{{0, 0}, {0, 1}, {0, 2}},
		},
		{
			name:     "extra record and master clears fail",
			outputs:  []int{2, 10, 2},
			creating: []rec{{0, 0}, {1, 0}, {1, 1}, {1, 2}, {2, 0}},
			fail:     []rec{{1, 1}, {2, 0}},
			match:    isBatchWrite,
			wantErr:  []named{{1, " records " + timedOut(1) + ";"}, {2, " records " + timedOut(0)}},
			keep:     []rec{{1, 0}},
			lose:     []rec{{0, 0}, {1, 2}},
		},
		{
			// Twelve failed clears behind a failed read: every record is still named.
			name:     "read failure and twelve failed clears",
			outputs:  []int{2, 25, 25, 9},
			creating: []rec{{0, 0}, {1, 0}, {2, 0}, {3, 0}},
			fail:     []rec{{3, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}, {1, 6}, {2, 1}, {2, 2}, {2, 3}, {2, 4}, {2, 5}, {2, 6}},
			match:    func(aerospike.BatchRecordIfc) bool { return true },
			wantErr:  []named{{3, "] failed to read the extra record count"}, {1, " records " + timedOut(1, 2, 3, 4, 5, 6)}, {2, " records " + timedOut(1, 2, 3, 4, 5, 6)}},
			keep:     []rec{{1, 0}, {2, 0}, {3, 0}},
			lose:     []rec{{0, 0}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, client := setupStoreForLockTest(t)
			store.utxoBatchSize = 4

			hashes := make([]*chainhash.Hash, len(tc.outputs))
			fullUnlock := make([]chainhash.Hash, len(tc.outputs))

			for i, outputs := range tc.outputs {
				tx := txWithOutputs(t, outputs)
				_, err := store.Create(ctx, tx, 100)
				require.NoError(t, err)

				hashes[i] = tx.TxIDChainHash()
				fullUnlock[i] = *hashes[i]
			}

			for _, r := range tc.creating {
				require.NoError(t, client.PutBins(nil, recordKey(t, store, hashes[r.tx], r.idx), aerospike.NewBin(fields.Creating.String(), true)))
			}

			failKeys := make([]*aerospike.Key, 0, len(tc.fail))
			for _, r := range tc.fail {
				failKeys = append(failKeys, recordKey(t, store, hashes[r.tx], r.idx))
			}

			attempted := failRecordsAfterBatch(t, store, client, failKeys, tc.match)

			// The healthy transaction comes first, so a failure named after the wrong item shows.
			err := store.applyLockClearWork(ctx, lockClearWork{fullUnlock: fullUnlock})
			require.True(t, *attempted)
			require.ErrorIs(t, err, errors.ErrStorageError)
			require.NotContains(t, err.Error(), hashes[0].String())

			for _, w := range tc.wantErr {
				require.ErrorContains(t, err, hashes[w.tx].String()+w.text)
			}

			for _, r := range tc.keep {
				require.Truef(t, hasCreating(t, client, store, hashes[r.tx], r.idx), "record %d of tx %d must keep creating", r.idx, r.tx)
			}

			for _, r := range tc.lose {
				require.Falsef(t, hasCreating(t, client, store, hashes[r.tx], r.idx), "record %d of tx %d must lose creating", r.idx, r.tx)
			}
		})
	}
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

		for idx := uint32(1); idx <= 2; idx++ {
			require.NoError(t, client.PutBins(nil, recordKey(t, store, txHash, idx), aerospike.NewBin(fields.Creating.String(), true)))
		}

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

			// The mined update calls the client directly; only the cleanup batches go through this seam.
			store.batchOperateFn = errorOperate()
			if tc.fail != nil {
				store.batchOperateFn = tc.fail(client)
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
