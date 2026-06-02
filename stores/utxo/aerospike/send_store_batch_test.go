package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newTestStoreForSendStoreBatch builds the minimum Store fields sendStoreBatch
// touches. It deliberately leaves s.client nil — every test installs its own
// batchOperateFn so BatchOperate is never called through the real client.
func newTestStoreForSendStoreBatch(t *testing.T) *Store {
	t.Helper()

	InitPrometheusMetrics()

	tSettings := &settings.Settings{}
	tSettings.Aerospike.UseDefaultPolicies = true
	tSettings.UtxoStore.UtxoBatchSize = 20_000

	return &Store{
		ctx:           context.Background(),
		namespace:     "test-ns",
		setName:       "test-set",
		logger:        ulogger.TestLogger{},
		settings:      tSettings,
		utxoBatchSize: tSettings.UtxoStore.UtxoBatchSize,
	}
}

// txWithSingleOutput builds a partial transaction (no inputs, one output) so
// sendStoreBatch's GetBinsToStore takes the fee-zero path without needing a
// real parent UTXO.
func txWithSingleOutput(t *testing.T) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	return tx
}

// drainOne reads at most one value from ch within timeout; returns ok=true if a
// value arrived. Used so tests can detect the *absence* of a notification too.
func drainOne(ch chan error, timeout time.Duration) (error, bool) {
	select {
	case v := <-ch:
		return v, true
	case <-time.After(timeout):
		return nil, false
	}
}

// TestSendStoreBatch_TopLevelNonKeyExistsError_NoDuplicateNotification verifies
// that when BatchOperate returns a non-KEY_EXISTS top-level error, each item's
// done channel receives exactly ONE notification (the error). The bug was that
// after notifying all items, control fell through to the per-record loop which
// then called SafeSend(nil) for any record whose per-record Err was unset —
// producing a spurious success notification on top of the real error.
func TestSendStoreBatch_TopLevelNonKeyExistsError_NoDuplicateNotification(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	// Mock BatchOperate to return a top-level network error WITHOUT touching
	// per-record Err fields. This is the exact shape that triggered the bug.
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		return aerospike.ErrNetwork
	}

	// Build 3 items, each with a BUFFERED done channel (capacity 2) so a
	// spurious second send is observable instead of being dropped by SafeSend's
	// closed-channel recovery.
	batch := make([]*BatchStoreItem, 3)
	for i := range batch {
		tx := txWithSingleOutput(t)
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, make(chan error, 2))
	}

	s.sendStoreBatch(batch)

	for i, item := range batch {
		first, ok := drainOne(item.done, 500*time.Millisecond)
		require.True(t, ok, "item %d: expected one notification, got none", i)
		require.Error(t, first, "item %d: expected error notification, got nil", i)

		_, gotSecond := drainOne(item.done, 50*time.Millisecond)
		require.False(t, gotSecond, "item %d: got a duplicate notification — the per-record loop fell through after the top-level error path", i)
	}
}

// TestSendStoreBatch_TopLevelKeyExistsError_EachItemGetsOwnTxHash verifies that
// when a multi-item batch hits a top-level KEY_EXISTS_ERROR, each item's error
// message references its OWN txHash, not batch[0]'s. The bug was the message
// hardcoded "batch[0].txHash" even though the batcher routinely groups dozens
// of items.
func TestSendStoreBatch_TopLevelKeyExistsError_EachItemGetsOwnTxHash(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		// Synthesize a top-level KEY_EXISTS_ERROR (no const error exists for this code).
		return &aerospike.AerospikeError{ResultCode: types.KEY_EXISTS_ERROR}
	}

	batch := make([]*BatchStoreItem, 3)
	for i := range batch {
		tx := txWithSingleOutput(t)
		// Make each tx distinct so its hash is unique. PayToAddress with a
		// different satoshi value flips the hash.
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(100+i)))
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, make(chan error, 2))
	}

	s.sendStoreBatch(batch)

	for i, item := range batch {
		err, ok := drainOne(item.done, 500*time.Millisecond)
		require.True(t, ok, "item %d: expected a notification", i)
		require.Error(t, err, "item %d: expected an error", i)
		require.Contains(t, err.Error(), item.txHash.String(), "item %d: error message must reference its own txHash, not batch[0]", i)
	}
}

// TestSendStoreBatch_PerRecordConstError_StillNotifies verifies that a
// per-record error of a concrete type other than *AerospikeError still results
// in a notification on the done channel. The bug was the production-code type
// assertion `err.(*aerospike.AerospikeError)` returned ok=false for the const
// error sentinels the client exposes (aerospike.ErrTimeout, ErrNetwork, etc.),
// and the loop body's fallback SafeSend was nested inside the `if ok` branch.
// Result: the caller hung on <-errCh forever.
func TestSendStoreBatch_PerRecordConstError_StillNotifies(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		// aerospike.ErrTimeout is a *constAerospikeError, which implements
		// aerospike.Error but does NOT satisfy the `err.(*AerospikeError)`
		// type assertion in production code.
		for _, rec := range records {
			rec.BatchRec().Err = aerospike.ErrTimeout
		}
		return nil // top-level OK; the bug is exercised only in the per-record loop
	}

	tx := txWithSingleOutput(t)
	batch := []*BatchStoreItem{
		NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, make(chan error, 2)),
	}

	s.sendStoreBatch(batch)

	err, ok := drainOne(batch[0].done, 500*time.Millisecond)
	require.True(t, ok, "item must be notified even when per-record error is a const aerospike.Error, not *AerospikeError")
	require.Error(t, err, "notification must be an error, not nil success")
}

// TestSendStoreBatch_KeyNotFoundOnRealRecord_NotifiesError verifies that a
// per-record KEY_NOT_FOUND_ERROR on a record that was NOT a NOOP placeholder
// produces a notification rather than being silently skipped. The bug was the
// KEY_NOT_FOUND_ERROR branch assumed every such code meant NOOP and called
// `continue` without sending — a Create() caller hung if the assumption was
// ever wrong (e.g. an unusual Aerospike state, a future client change).
func TestSendStoreBatch_KeyNotFoundOnRealRecord_NotifiesError(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		// Set KEY_NOT_FOUND_ERROR on every record. With this Store's batch
		// construction every record IS a real BatchWrite (small single-batch
		// tx, no goroutine offload), so KEY_NOT_FOUND_ERROR is a real failure
		// that must be surfaced.
		notFound := &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}
		for _, rec := range records {
			rec.BatchRec().Err = notFound
		}
		return nil
	}

	tx := txWithSingleOutput(t)
	batch := []*BatchStoreItem{
		NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, make(chan error, 2)),
	}

	s.sendStoreBatch(batch)

	err, ok := drainOne(batch[0].done, 500*time.Millisecond)
	require.True(t, ok, "KEY_NOT_FOUND on a real (non-NOOP) BatchWrite must notify the caller")
	require.Error(t, err, "notification must be an error, not nil success")
}

// TestSendStoreBatch_AllSuccess_SingleSuccessNotificationPerItem verifies the
// happy path: BatchOperate returns no error AND no per-record errors → each
// item receives exactly ONE notification of nil (success). This guards against
// regressions where the success path accidentally double-sends or skips.
func TestSendStoreBatch_AllSuccess_SingleSuccessNotificationPerItem(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		return nil
	}

	batch := make([]*BatchStoreItem, 3)
	for i := range batch {
		tx := txWithSingleOutput(t)
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, make(chan error, 2))
	}

	s.sendStoreBatch(batch)

	for i, item := range batch {
		first, ok := drainOne(item.done, 500*time.Millisecond)
		require.True(t, ok, "item %d: expected success notification", i)
		require.NoError(t, first, "item %d: expected nil success, got error", i)

		_, gotSecond := drainOne(item.done, 50*time.Millisecond)
		require.False(t, gotSecond, "item %d: success path must send exactly one notification", i)
	}
}

// TestSendStoreBatch_MixedPerRecordResults verifies that when a batch contains
// a mix of (success, KEY_EXISTS_ERROR, const-error), each item gets exactly the
// right notification. Tests the per-record loop's classification across all
// surfaces simultaneously.
func TestSendStoreBatch_MixedPerRecordResults(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		// idx 0: success (no err)
		// idx 1: KEY_EXISTS_ERROR
		// idx 2: const error (ErrTimeout — fails *AerospikeError type assertion)
		records[1].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.KEY_EXISTS_ERROR}
		records[2].BatchRec().Err = aerospike.ErrTimeout
		return nil
	}

	batch := make([]*BatchStoreItem, 3)
	for i := range batch {
		tx := txWithSingleOutput(t)
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, make(chan error, 2))
	}

	s.sendStoreBatch(batch)

	// idx 0: success
	err0, ok := drainOne(batch[0].done, 500*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err0, "item 0 must succeed")

	// idx 1: TxExistsError
	err1, ok := drainOne(batch[1].done, 500*time.Millisecond)
	require.True(t, ok)
	require.Error(t, err1, "item 1 must report an error")
	require.True(t, errors.Is(err1, errors.ErrTxExists), "item 1 must be TxExistsError, got %v", err1)

	// idx 2: StorageError fallback (const error path)
	err2, ok := drainOne(batch[2].done, 500*time.Millisecond)
	require.True(t, ok, "item 2 must notify even for const aerospike error")
	require.Error(t, err2, "item 2 must report an error")

	// And no duplicates anywhere.
	for i, item := range batch {
		_, gotSecond := drainOne(item.done, 50*time.Millisecond)
		require.False(t, gotSecond, "item %d: must receive exactly one notification", i)
	}
}

// TestSendStoreBatch_TopLevelError_DoesNotDuplicateNotifyPreSendItems checks
// that when a SETUP error (e.g. NewKey failure simulated below isn't directly
// reachable, so we use GetBinsToStore failure via a transaction with zero
// outputs) notifies an item, and then BatchOperate ALSO returns a top-level
// error, the pre-notified item does not receive a SECOND notification from
// the top-level error loop.
//
// The bug being guarded: previously the top-level error loop sent to every
// item unconditionally, which would duplicate-send to items the setup loop
// had already notified.
func TestSendStoreBatch_TopLevelError_DoesNotDuplicateNotifyPreSendItems(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		// Top-level non-KEY_EXISTS error AFTER the setup loop already errored on
		// item 0 (zero-output tx → GetBinsToStore returns "tx has no outputs").
		return aerospike.ErrNetwork
	}

	// Item 0: zero outputs → GetBinsToStore fails in the setup loop → ProcessingError
	// already sent to item 0.done, NOOP record placed in batchRecords[0].
	zeroOutputsTx := bt.NewTx()
	// Item 1: normal single-output tx → real BatchWrite, will hit top-level error.
	tx1 := txWithSingleOutput(t)
	batch := []*BatchStoreItem{
		NewBatchStoreItem(zeroOutputsTx.TxIDChainHash(), false, zeroOutputsTx, 100, nil, 0, make(chan error, 2)),
		NewBatchStoreItem(tx1.TxIDChainHash(), false, tx1, 100, nil, 0, make(chan error, 2)),
	}

	s.sendStoreBatch(batch)

	// Item 0 received its ProcessingError from the setup loop; the top-level
	// error path MUST NOT send a second notification.
	err0, ok := drainOne(batch[0].done, 500*time.Millisecond)
	require.True(t, ok)
	require.Error(t, err0, "item 0 must get its setup-loop error")
	_, gotSecond := drainOne(batch[0].done, 50*time.Millisecond)
	require.False(t, gotSecond, "item 0: pre-notified items must not be re-notified by the top-level error path")

	// Item 1 was a real BatchWrite — it should get the top-level error.
	err1, ok := drainOne(batch[1].done, 500*time.Millisecond)
	require.True(t, ok)
	require.Error(t, err1, "item 1 must get the top-level network error")
	_, gotSecond1 := drainOne(batch[1].done, 50*time.Millisecond)
	require.False(t, gotSecond1, "item 1: must receive exactly one notification")
}
