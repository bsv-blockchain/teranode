package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newErrTestDiskTxMap(t *testing.T) *DiskTxMap {
	t.Helper()

	m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
	require.NoError(t, err)

	return m
}

// A payload write that fails must be reported, not dropped: the index would
// otherwise claim the entry exists while its inpoints never reached disk.
func TestDiskTxMap_WriteErrorIsReported(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	failDiskTxMapLogs(m, 1, nil)

	inp := subtreepkg.TxInpoints{}
	m.Set(batchTestHash(1), &inp)
	require.NoError(t, m.Flush())

	require.Error(t, m.TakeErr())
	require.NoError(t, m.TakeErr(), "an error is reported once")
}

// alwaysFailWrites fails every payload write of m from now on.
const alwaysFailWrites = 1 << 30

// A failed read must not pass for "not found".
func TestDiskTxMap_ReadErrorIsReported(t *testing.T) {
	m := newErrTestDiskTxMap(t)

	inp := subtreepkg.TxInpoints{}
	m.Set(batchTestHash(1), &inp)
	require.NoError(t, m.Flush())
	require.NoError(t, m.TakeErr())

	failDiskTxMapLogs(m, 0, errors.NewStorageError("read failed"))

	_, ok := m.Get(batchTestHash(1))
	require.False(t, ok)
	require.Error(t, m.TakeErr())
}

// AddDirectly is a load path like AddNodesDirectly and gets the same
// end-of-operation boundary: a pending map error fails an otherwise
// successful call.
func TestAddDirectly_ReportsDiskTxMapErrorOnce(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	node := subtreepkg.Node{Hash: batchTestHash(1), Fee: 1, SizeInBytes: 100}
	inp := subtreepkg.TxInpoints{}

	err := stp.AddDirectly(&node, &inp, true)
	require.ErrorIs(t, err, boom)

	node2 := subtreepkg.Node{Hash: batchTestHash(2), Fee: 1, SizeInBytes: 100}
	require.NoError(t, stp.AddDirectly(&node2, &inp, true))
}

// AddDirectlyReportOnly is used for reset's postProcess reload, which runs
// after reset has already committed (header at the target tip, maps
// cleared): a pending map error must be logged and counted, not fail an
// otherwise-successful call, unlike AddDirectly.
func TestAddDirectlyReportOnly_LogsInsteadOfFailing(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("AddDirectlyReportOnly"))

	node := subtreepkg.Node{Hash: batchTestHash(1), Fee: 1, SizeInBytes: 100}
	inp := subtreepkg.TxInpoints{}

	require.NoError(t, stp.AddDirectlyReportOnly(&node, &inp, true), "a pending map error must not fail an otherwise-successful report-only add")

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("AddDirectlyReportOnly"))
	require.Equal(t, before+1, after, "the pending error must still be logged and counted")
	require.NoError(t, stp.diskTxMapErr(), "drained by the report-only path, not left pending")
}

// AddNodesDirectlyReportOnly is the batch counterpart.
func TestAddNodesDirectlyReportOnly_LogsInsteadOfFailing(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("AddNodesDirectlyReportOnly"))

	txs := makeUnminedBatches(10, 10)[0]
	require.NoError(t, stp.AddNodesDirectlyReportOnly(txs, true), "a pending map error must not fail an otherwise-successful report-only add")

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("AddNodesDirectlyReportOnly"))
	require.Equal(t, before+1, after, "the pending error must still be logged and counted")
	require.NoError(t, stp.diskTxMapErr(), "drained by the report-only path, not left pending")
}

// Clear's old-generation Close runs after the rotation has already succeeded
// (every replacement store was created); the data is fine either way, so this
// is a benign cleanup failure (a leaked Badger directory), not an operation-
// failing error like a rotation failure. Clear callers (resetSubtreeState,
// swapCurrentTxMapBack, clearCurrentTxMapShadow, reset) must not have a
// moveForward, reorg or reset fail on it - only log and count it.
//
// A real Close-failure trigger wasn't reproducible here: BadgerTempStore.Close
// tolerates a double-close (the underlying badger.DB.Close returns nil the
// second time), so this exercises recordCloseWarn/TakeCloseWarn directly
// rather than via an actual failing Close call.
func TestDiskTxMap_CloseWarn_IsSeparateFromOperationFailingErr(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	closeWarn := errors.NewStorageError("disk tx map: closing previous generation on disk 0", errors.NewProcessingError("permission denied"))
	m.recordCloseWarn(closeWarn)

	require.NoError(t, m.TakeErr(), "a Close warning must never surface as an operation-failing error")
	require.ErrorIs(t, m.TakeCloseWarn(), closeWarn, "but it must still be retrievable for the caller to log and count")
	require.NoError(t, m.TakeCloseWarn(), "reported once")
}

// SubtreeProcessor.reportDiskTxMapCloseWarn is what Clear callers use to
// report m's warning: it must log and count, not touch the operation-failing
// diskTxMapErr() path at all.
func TestReportDiskTxMapCloseWarn_LogsAndCountsWithoutFailingTheOperation(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	closeWarn := errors.NewStorageError("disk tx map: closing previous generation on disk 0", errors.NewProcessingError("permission denied"))
	stp.diskTxMap.recordCloseWarn(closeWarn)

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("test_clear"))

	stp.reportDiskTxMapCloseWarn(stp.diskTxMap, "test_clear")

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("test_clear"))
	require.Equal(t, before+1, after, "the warning must be logged and counted")
	require.NoError(t, stp.diskTxMapErr(), "and must never have been recorded as an operation-failing error")
}

// A healthy map reports nothing.
func TestDiskTxMap_NoErrorWhenHealthy(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	inp := subtreepkg.TxInpoints{}
	m.Set(batchTestHash(1), &inp)
	_, ok := m.Get(batchTestHash(1))
	require.True(t, ok)
	m.Delete(batchTestHash(1))

	require.NoError(t, m.TakeErr())
}

// A map error fails the AddNodesDirectly call it happened in, and the
// processor keeps working: the next call succeeds.
func TestAddNodesDirectly_ReportsDiskTxMapErrorOnce(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	first := makeUnminedBatches(100, 100)[0]
	err := stp.AddNodesDirectly(first, true)
	require.ErrorIs(t, err, boom)

	second := makeUnminedBatches(100, 100)[0]
	for i := range second {
		second[i].Node.Hash[31] = 0x42
	}

	require.NoError(t, stp.AddNodesDirectly(second, true))
}

// A map error during an internal step (here a subtree completing inside
// addNode, via UpdateSubtreeIndexBatch) must not abort that step: the subtree
// still completes, and the error stays pending for the operation boundary to
// report.
func TestProcessCompleteSubtree_DefersDiskTxMapError(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 4, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	txs := makeUnminedBatches(3, 3)[0] // with the coinbase placeholder, fills a 4-slot subtree

	for _, tx := range txs {
		require.NoError(t, stp.addNode(*tx.Node, tx.TxInpoints, true))
	}

	require.Len(t, stp.chainedSubtrees, 1, "the subtree completed")
	require.NoError(t, stp.diskTxMapErr(), "no error yet: every write above succeeded")
}

// runHandlerWithRecover is a pure panic-recovery wrapper: it returns exactly
// fn's error and does not itself inspect disk tx map errors. Handlers that
// commit state before returning (moveForwardBlock, reorgBlocks, reset) would
// otherwise have a map error observed here — after fn already succeeded and
// committed — misattributed to them and returned as a failure, desyncing
// callers that treat an error as "not applied" from state that already
// reflects it. See TestMoveForwardBlock_DiskTxMapErrorBeforeCommitFailsAndRollsBack
// and friends for the handlers' own boundary checks.
func TestRunHandlerWithRecover_DoesNotReportDiskTxMapError(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	boom := errors.NewStorageError("badger read failed")

	ran := false
	err := stp.runHandlerWithRecover("test", func() error {
		ran = true
		stp.diskTxMap.recordErr(boom)

		return nil
	})

	require.True(t, ran)
	require.NoError(t, err, "runHandlerWithRecover must return exactly fn's error, not inspect the map")
	require.ErrorIs(t, stp.diskTxMapErr(), boom, "the error is still pending for the handler itself to drain")
}

// A failed operation must not leave its own disk tx map errors pending: the
// next operation to check would have the earlier failure's storage cause
// misattributed to it. joinDiskTxMapErrOnFailure folds the map error into the
// operation's own error and drains it.
func TestJoinDiskTxMapErrOnFailure_DrainsOnFailureOnly(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	boom := errors.NewStorageError("badger write failed")

	// Success: left untouched, for the caller's own commit-point check.
	stp.diskTxMap.recordErr(boom)
	var okErr error
	stp.joinDiskTxMapErrOnFailure(&okErr)
	require.NoError(t, okErr)
	require.ErrorIs(t, stp.diskTxMapErr(), boom, "success leaves the error pending for the caller to check/report")

	// Failure: joined and drained.
	stp.diskTxMap.recordErr(boom)
	var failErr error = errors.NewProcessingError("unrelated failure")
	stp.joinDiskTxMapErrOnFailure(&failErr)
	require.ErrorIs(t, failErr, boom, "the map error must be joined into the operation's own failure")
	require.NoError(t, stp.diskTxMapErr(), "drained: must not leak to whatever runs next")
}

// A failed load-path operation (AddNodesDirectly, AddDirectly) drains and
// joins its own map error the same way; a successful one still fails (the map
// may be partially populated, and a restart reloads it).
func TestFailOrJoinDiskTxMapErrForLoad_JoinsOnFailureFailsOnSuccess(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	boom := errors.NewStorageError("badger write failed")

	stp.diskTxMap.recordErr(boom)
	var failErr error = errors.NewProcessingError("unrelated failure")
	stp.failOrJoinDiskTxMapErrForLoad("test", &failErr)
	require.ErrorIs(t, failErr, boom)
	require.NoError(t, stp.diskTxMapErr())

	stp.diskTxMap.recordErr(boom)
	var okErr error
	stp.failOrJoinDiskTxMapErrForLoad("test", &okErr)
	require.ErrorIs(t, okErr, boom, "a pending error must fail an otherwise-successful load")
	require.NoError(t, stp.diskTxMapErr())
}

// moveForwardBlock's own failure (here the "you must pass in a block" guard)
// must fold in a pending disk tx map error rather than leave it for whatever
// call succeeds next.
func TestMoveForwardBlock_FailureDrainsPendingDiskTxMapErr(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	_, _, err := stp.moveForwardBlock(context.Background(), nil, false, map[chainhash.Hash]struct{}{}, false, true)
	require.Error(t, err)
	require.ErrorIs(t, err, boom, "the pending map error must be folded into this call's own failure")

	require.NoError(t, stp.diskTxMapErr(), "must not leak to the next call")
}

// GetWithErr is for callers that run concurrently with processor operations
// (the async subtree storer): a read error it causes must not become pending
// on the map, where it would be misattributed to whatever operation next
// checks the map's pending error, instead of handled by the caller itself.
func TestDiskTxMap_GetWithErr_DoesNotRecordOnMap(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	inp := subtreepkg.TxInpoints{}
	m.Set(batchTestHash(1), &inp)
	require.NoError(t, m.Flush())
	require.NoError(t, m.TakeErr())

	failDiskTxMapLogs(m, 0, errors.NewStorageError("read failed"))

	_, found, err := m.GetWithErr(batchTestHash(1))
	require.False(t, found)
	require.Error(t, err, "the caller must see the read error")

	require.NoError(t, m.TakeErr(), "GetWithErr must not record the error on the map")
}

// A retired disk tx map is being discarded either way - there is nothing left
// to roll back to - so its recorded error and any Close failure must be
// logged and counted, not fed back into the surviving map's recordErr, which
// would let it resurface later misattributed to whatever operation next
// checks the map's pending error.
func TestCloseRetiredDiskTxMaps_ReportsRetiredMapError(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

	retired, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
	require.NoError(t, err)

	boom := errors.NewStorageError("badger write failed")
	retired.recordErr(boom)

	stp.diskTxMapRetired = []*DiskTxMap{retired}

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("closeRetiredDiskTxMaps"))

	stp.closeRetiredDiskTxMaps()

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("closeRetiredDiskTxMaps"))
	require.Equal(t, before+1, after, "the retired map's error must be logged and counted")

	require.Empty(t, stp.diskTxMapRetired)
	require.NoError(t, retired.TakeErr(), "the retired map's own error was drained when it was closed, not left for reuse")
}

// Stop's pending-error drain must go through the same counted path as every
// other boundary, not a bare logger.Errorf call.
// A post-commit disk tx map error is not unique to moveForwardBlock: any
// operation that reports rather than fails (removeTx here; dequeue,
// reorgBlocks and reset's own reload share the same drainAndLogDiskTxMapErr/
// reportOrJoinDiskTxMapErr helpers) leaves the same phantom risk and must
// request a reset too.
func TestRemoveTx_PostCommitErrorRequestsReset(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	stp.Start(context.Background())

	require.False(t, stp.TakeResetRequested(), "precondition: no reset pending")

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("removeTx"))

	require.NoError(t, stp.Remove(context.Background(), batchTestHash(1)))

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("removeTx")) == before+1
	}, time.Second, time.Millisecond, "removeTx must log and count the pending error")

	require.True(t, stp.TakeResetRequested(), "a post-commit storage error observed during removeTx must request a reset")
}

func TestStop_DrainsPendingDiskTxMapErrThroughCountedPath(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("stop"))

	cleanup() // calls stp.Stop

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("stop"))
	require.Equal(t, before+1, after, "Stop must log and count a pending error, not just log it")
}

// AddDirectly's own boundary check cannot see a write failure for a payload
// still sitting in a log segment's buffer: nothing wrote it yet, so
// diskTxMapErr() has nothing to observe. FlushDiskTxMapForLoad exists for
// exactly this - a caller flushes once, after the whole load, and only then
// checks. Fails a REAL write via the failingLogFile seam, not recordErr, so a
// regression that goes back to a naked diskTxMapErr() check is caught.
func TestFlushDiskTxMapForLoad_CatchesTrailingUnflushedWriteFailure(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	failDiskTxMapLogs(stp.diskTxMap, alwaysFailWrites, nil)

	node := subtreepkg.Node{Hash: batchTestHash(1), Fee: 1, SizeInBytes: 100}
	inp := subtreepkg.TxInpoints{}

	// A single write stays well below logBufferSize, so AddDirectly's own
	// deferred check has nothing to see yet.
	require.NoError(t, stp.AddDirectly(&node, &inp, true), "the write itself is unflushed, not yet failing")
	require.NoError(t, stp.diskTxMapErr(), "nothing flushed yet, so nothing pending")

	err := stp.FlushDiskTxMapForLoad("test", false)
	require.Error(t, err, "the startup load must fail on the trailing flush failure")
	require.NoError(t, stp.diskTxMapErr(), "drained, not left pending")
}

// The same trailing flush failure must only be logged and counted for
// reset's report-only reload.
func TestFlushDiskTxMapForLoad_ReportsTrailingUnflushedWriteFailureOnReload(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	failDiskTxMapLogs(stp.diskTxMap, alwaysFailWrites, nil)

	node := subtreepkg.Node{Hash: batchTestHash(1), Fee: 1, SizeInBytes: 100}
	inp := subtreepkg.TxInpoints{}

	require.NoError(t, stp.AddDirectlyReportOnly(&node, &inp, true))

	before := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("test_reload"))

	err := stp.FlushDiskTxMapForLoad("test_reload", true)
	require.NoError(t, err, "the reload must not fail on a post-commit flush error")

	after := testutil.ToFloat64(prometheusSubtreeProcessorDiskTxMapErrors.WithLabelValues("test_reload"))
	require.Equal(t, before+1, after, "the trailing flush failure must still be logged and counted")
}

// DrainQueue's own writes (dequeueDuringBlockMovement adding a queued tx to
// the current map) land after the caller's own end-of-load
// FlushDiskTxMapForLoad call (BlockAssembler's startup Start() and reset's
// postProcess both call DrainQueue after that flush already ran). A second
// FlushDiskTxMapForLoad call after DrainQueue is what catches a trailing
// unflushed write failure from DrainQueue itself; without it, this surfaces
// later as a spurious "not found in currentTxMap" failure on an unrelated
// block instead.
func TestDrainQueue_TrailingUnflushedWriteFailure_CaughtByFlushDiskTxMapForLoad(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	drainAt := enqueueAt.Add(1 * time.Millisecond)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: drainAt}

	queuedTxHash := chainhash.HashH([]byte("queued-tx-drainqueue-flush"))
	stp.queue.enqueueBatch(
		[]subtreepkg.Node{{Hash: queuedTxHash, Fee: 1, SizeInBytes: 220}},
		[]*subtreepkg.TxInpoints{{}},
	)
	require.Equal(t, int64(1), stp.queue.length(), "precondition: 1 batch enqueued")

	failDiskTxMapLogs(stp.diskTxMap, alwaysFailWrites, nil)

	stp.DrainQueue(map[chainhash.Hash]struct{}{chainhash.HashH([]byte("unrelated-drop")): {}})
	require.Equal(t, int64(0), stp.queue.length(), "precondition: DrainQueue actually drained the queue")

	err := stp.FlushDiskTxMapForLoad("test_startup", false)
	require.Error(t, err, "a trailing unflushed DrainQueue write failure must be caught, not missed until some later, unrelated call")
}

// Errors recorded on the inactive half of the double buffer are reported too.
func TestDiskTxMapErr_IncludesShadow(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	require.NotNil(t, stp.diskTxMapShadow)

	boom := errors.NewStorageError("shadow rotate failed")
	stp.diskTxMapShadow.recordErr(boom)

	require.ErrorIs(t, stp.diskTxMapErr(), boom)
	require.NoError(t, stp.diskTxMapErr())
}

// Draining a pending map error requests a reset wherever it happens: the
// drain can't tell whether the error came from the draining operation's own
// writes or from an earlier write that never reached disk, whose phantom only
// a reset cures. Nothing pending requests nothing.
func TestDiskTxMapErr_RequestsReset(t *testing.T) {
	stp, cleanup := newAddNodesBenchProcessor(t, 64, WithTxMapDirs([]string{t.TempDir()}))
	defer cleanup()

	require.NoError(t, stp.diskTxMapErr())
	require.False(t, stp.TakeResetRequested())

	boom := errors.NewStorageError("badger write failed")
	stp.diskTxMap.recordErr(boom)

	require.ErrorIs(t, stp.diskTxMapErr(), boom)
	require.True(t, stp.TakeResetRequested())
}
