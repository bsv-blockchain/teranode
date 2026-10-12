package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
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
	// GetBinsToStore -> ShouldStoreOutputAsUTXO reads the Genesis activation
	// height; a realistic Store always has ChainCfgParams populated.
	chainParams := chaincfg.RegressionNetParams
	tSettings.ChainCfgParams = &chainParams

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

// TestSendStoreBatch_TopLevelNonKeyExistsError verifies that when BatchOperate
// returns a non-KEY_EXISTS top-level error, every item completes exactly once
// (the group reaches zero, so none is orphaned) and each carries the error in
// its result slot. The bug this guards: after notifying all items, control fell
// through to the per-record loop which then completed items a second time with
// nil — a spurious success. Exactly-once is now structural (complete is
// CAS-guarded), so the check reduces to "group completes AND every result is an
// error".
func TestSendStoreBatch_TopLevelNonKeyExistsError(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	// Mock BatchOperate to return a top-level network error WITHOUT touching
	// per-record Err fields. This is the exact shape that triggered the bug.
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		return aerospike.ErrNetwork
	}

	const n = 3

	group := completion.NewGroup(n)
	batch := make([]*BatchStoreItem, n)
	for i := range batch {
		tx := txWithSingleOutput(t)
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, group)
	}

	s.sendStoreBatch(batch)

	requireGroupCompleted(t, group, 2*time.Second)

	for i, item := range batch {
		require.Error(t, item.result, "item %d: expected error result, got nil", i)
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

	const n = 3

	group := completion.NewGroup(n)
	batch := make([]*BatchStoreItem, n)
	for i := range batch {
		tx := txWithSingleOutput(t)
		// Make each tx distinct so its hash is unique. PayToAddress with a
		// different satoshi value flips the hash.
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(100+i)))
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, group)
	}

	s.sendStoreBatch(batch)

	requireGroupCompleted(t, group, 2*time.Second)

	for i, item := range batch {
		require.Error(t, item.result, "item %d: expected an error", i)
		require.Contains(t, item.result.Error(), item.txHash.String(), "item %d: error message must reference its own txHash, not batch[0]", i)
	}
}

// TestSendStoreBatch_PerRecordConstError_StillNotifies verifies that a
// per-record error of a concrete type other than *AerospikeError still results
// in a completion. The bug was the production-code type assertion
// `err.(*aerospike.AerospikeError)` returned ok=false for the const error
// sentinels the client exposes (aerospike.ErrTimeout, ErrNetwork, etc.), and
// the loop body's fallback completion was nested inside the `if ok` branch.
// Result: the caller hung on group.Wait forever.
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

	group := completion.NewGroup(1)
	tx := txWithSingleOutput(t)
	batch := []*BatchStoreItem{
		NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, group),
	}

	s.sendStoreBatch(batch)

	requireGroupCompleted(t, group, 2*time.Second)
	require.Error(t, batch[0].result, "item must complete with an error even when per-record error is a const aerospike.Error, not *AerospikeError")
}

// TestSendStoreBatch_KeyNotFoundOnRealRecord_NotifiesError verifies that a
// per-record KEY_NOT_FOUND_ERROR on a record that was NOT a NOOP placeholder
// produces an error completion rather than being silently skipped. The bug was
// the KEY_NOT_FOUND_ERROR branch assumed every such code meant NOOP and called
// `continue` without completing — a Create() caller hung if the assumption was
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

	group := completion.NewGroup(1)
	tx := txWithSingleOutput(t)
	batch := []*BatchStoreItem{
		NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, group),
	}

	s.sendStoreBatch(batch)

	requireGroupCompleted(t, group, 2*time.Second)
	require.Error(t, batch[0].result, "KEY_NOT_FOUND on a real (non-NOOP) BatchWrite must complete the caller with an error")
}

// TestSendStoreBatch_AllSuccess verifies the happy path: BatchOperate returns
// no error AND no per-record errors → every item completes exactly once with a
// nil result. Exactly-once is structural (a second complete would over-decrement
// the group and panic in Done); requireGroupCompleted confirms none orphaned.
func TestSendStoreBatch_AllSuccess(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		return nil
	}

	const n = 3

	group := completion.NewGroup(n)
	batch := make([]*BatchStoreItem, n)
	for i := range batch {
		tx := txWithSingleOutput(t)
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, group)
	}

	require.NotPanics(t, func() { s.sendStoreBatch(batch) }, "a double completion would panic in Group.Done")

	requireGroupCompleted(t, group, 2*time.Second)

	for i, item := range batch {
		require.NoError(t, item.result, "item %d: expected nil success result, got error", i)
	}
}

// TestSendStoreBatch_MixedPerRecordResults verifies that when a batch contains
// a mix of (success, KEY_EXISTS_ERROR, const-error), each item gets exactly the
// right result. Tests the per-record loop's classification across all surfaces
// simultaneously.
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

	const n = 3

	group := completion.NewGroup(n)
	batch := make([]*BatchStoreItem, n)
	for i := range batch {
		tx := txWithSingleOutput(t)
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, group)
	}

	require.NotPanics(t, func() { s.sendStoreBatch(batch) }, "a double completion would panic in Group.Done")

	requireGroupCompleted(t, group, 2*time.Second)

	// idx 0: success
	require.NoError(t, batch[0].result, "item 0 must succeed")

	// idx 1: TxExistsError
	require.Error(t, batch[1].result, "item 1 must report an error")
	require.True(t, errors.Is(batch[1].result, errors.ErrTxExists), "item 1 must be TxExistsError, got %v", batch[1].result)

	// idx 2: StorageError fallback (const error path)
	require.Error(t, batch[2].result, "item 2 must report an error even for a const aerospike error")
}

// TestSendStoreBatch_TopLevelError_DoesNotDoubleCompletePreCompletedItems checks
// that when a SETUP error (a zero-output tx → GetBinsToStore fails) completes an
// item, and then BatchOperate ALSO returns a top-level error, the pre-completed
// item is not completed a second time by the top-level error loop.
//
// The bug being guarded: previously the top-level error loop sent to every item
// unconditionally, which would duplicate-notify items the setup loop had already
// handled. Under the group model a second completion would over-decrement the
// group and panic in Done — so NotPanics + a completing group proves the
// resultHandledElsewhere skip still holds.
func TestSendStoreBatch_TopLevelError_DoesNotDoubleCompletePreCompletedItems(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		// Top-level non-KEY_EXISTS error AFTER the setup loop already errored on
		// item 0 (zero-output tx → GetBinsToStore returns "tx has no outputs").
		return aerospike.ErrNetwork
	}

	group := completion.NewGroup(2)

	// Item 0: zero outputs → GetBinsToStore fails in the setup loop → ProcessingError
	// completes item 0, NOOP record placed in batchRecords[0].
	zeroOutputsTx := bt.NewTx()
	// Item 1: normal single-output tx → real BatchWrite, will hit top-level error.
	tx1 := txWithSingleOutput(t)
	batch := []*BatchStoreItem{
		NewBatchStoreItem(zeroOutputsTx.TxIDChainHash(), false, zeroOutputsTx, 100, nil, 0, group),
		NewBatchStoreItem(tx1.TxIDChainHash(), false, tx1, 100, nil, 0, group),
	}

	require.NotPanics(t, func() { s.sendStoreBatch(batch) }, "a double completion of the pre-completed item would panic in Group.Done")

	requireGroupCompleted(t, group, 2*time.Second)

	// Item 0 got its setup-loop error; the top-level error path must not have
	// completed it again (guaranteed by the group not over-decrementing).
	require.Error(t, batch[0].result, "item 0 must get its setup-loop error")

	// Item 1 was a real BatchWrite — it should get the top-level error.
	require.Error(t, batch[1].result, "item 1 must get the top-level network error")
}

// TestSendStoreBatch_PanicCompletesGroup is the group-completion equivalent of
// the old leak test: a panic inside sendStoreBatch (here from a panicking
// BatchOperate) must complete every waiting item via the deferred sweep, so the
// group reaches zero and no caller is orphaned on group.Wait.
func TestSendStoreBatch_PanicCompletesGroup(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		panic("simulated batch panic")
	}

	const n = 3

	group := completion.NewGroup(n)
	batch := make([]*BatchStoreItem, n)
	for i := range batch {
		tx := txWithSingleOutput(t)
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, group)
	}

	require.NotPanics(t, func() { s.sendStoreBatch(batch) })

	requireGroupCompleted(t, group, 2*time.Second)

	for i, item := range batch {
		require.Error(t, item.result, "item %d: panic sweep must complete each item with an error", i)
	}
}
