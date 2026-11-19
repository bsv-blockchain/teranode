package blockvalidation

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// childPanicValue is the value the tests below panic with. It holds a word that each of
// IsNetworkError, IsMaliciousResponseError and IsBlockCorrupt matches, so an error that carried
// the panic value in its text would be misread by every one of them.
const childPanicValue = "injected panic: http timeout, corrupt and malicious"

// panickingSubtreeStore panics on Get or Exists for one (key, file type) pair and forwards every
// other call. It counts the panics, so a test can show its panic was really reached.
type panickingSubtreeStore struct {
	blob.Store

	key      chainhash.Hash
	fileType fileformat.FileType
	onGet    bool
	onExists bool

	// failKey, when set, makes Exists on failKey (same file type) return failErr at once.
	// panicAfter delays the panic, so that ordinary failure is recorded first.
	failKey    *chainhash.Hash
	failErr    error
	panicAfter time.Duration

	panics atomic.Int32
}

func (s *panickingSubtreeStore) hit(key []byte, fileType fileformat.FileType) bool {
	return fileType == s.fileType && string(key) == string(s.key[:])
}

func (s *panickingSubtreeStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) ([]byte, error) {
	if s.onGet && s.hit(key, fileType) {
		s.panics.Add(1)
		panic(childPanicValue)
	}

	return s.Store.Get(ctx, key, fileType, opts...)
}

func (s *panickingSubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (bool, error) {
	if s.failKey != nil && fileType == s.fileType && string(key) == string(s.failKey[:]) {
		return false, s.failErr
	}

	if s.onExists && s.hit(key, fileType) {
		time.Sleep(s.panicAfter)
		s.panics.Add(1)
		panic(childPanicValue)
	}

	return s.Store.Exists(ctx, key, fileType, opts...)
}

// requireChildPanicError checks that err is the error recoverChildPanic produces: a local panic
// that no peer may be charged for, and that no text classifier reads as anything else.
func requireChildPanicError(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	require.True(t, isLocalPanic(err), "the error must carry the local-panic marker, got %v", err)
	require.True(t, isLocalCatchupFault(err), "catchup must read the error as a local fault, got %v", err)
	require.False(t, errors.IsNetworkError(err), "%v", err)
	require.False(t, errors.IsMaliciousResponseError(err), "%v", err)
	require.False(t, errors.IsBlockCorrupt(err), "%v", err)
	require.False(t, errors.IsContextError(err), "%v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "%v", err)
	require.NotContains(t, err.Error(), "injected panic", "the panic value must stay out of the error text")
}

// TestRecoverChildPanic_ErrgroupWaitReturnsLocalPanic is the case the helper exists for: a
// panic in an errgroup goroutine reaches the parent as an error from Wait, and the process lives.
// The panic value holds words that the text classifiers match, and none of them may match the
// error. The panic is counted under the path given.
func TestRecoverChildPanic_ErrgroupWaitReturnsLocalPanic(t *testing.T) {
	initPrometheusMetrics()

	before := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathCatchupFetch))

	var g errgroup.Group

	g.Go(func() (err error) {
		defer recoverChildPanic(ulogger.TestLogger{}, recoveredPanicPathCatchupFetch, &err, nil, "test site %d", 7)

		panic(childPanicValue)
	})

	err := g.Wait()
	requireChildPanicError(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "the error must stay ERR_PROCESSING, got %v", err)

	// processBlockFound and fetchSubtreeDataForBlock wrap the error in a ServiceError. The
	// marker must survive that.
	requireChildPanicError(t, errors.NewServiceError("failed block validation", err))

	require.Equal(t, before+1, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathCatchupFetch)))
}

// TestRecoverChildPanic_NilRetErrHandsTheErrorToOnPanic covers the sites whose group must not
// see an error: a nil retErr leaves the goroutine's return value alone, and onPanic gets the
// error instead.
func TestRecoverChildPanic_NilRetErrHandsTheErrorToOnPanic(t *testing.T) {
	initPrometheusMetrics()

	var (
		g       errgroup.Group
		handed  error
		onCalls int
	)

	g.Go(func() error {
		defer recoverChildPanic(ulogger.TestLogger{}, recoveredPanicPathBlockValidation, nil, func(err error) {
			onCalls++
			handed = err
		}, "test site")

		panic(childPanicValue)
	})

	require.NoError(t, g.Wait(), "with a nil retErr the group must not see an error")
	require.Equal(t, 1, onCalls)
	requireChildPanicError(t, handed)
}

// TestRecoverChildPanic_NoPanicLeavesEverythingAlone: on a normal return the helper sets
// nothing, calls nothing and counts nothing, so a goroutine's own error passes through.
func TestRecoverChildPanic_NoPanicLeavesEverythingAlone(t *testing.T) {
	initPrometheusMetrics()

	before := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathService))
	own := errors.NewStorageError("the goroutine's own error")

	var g errgroup.Group

	g.Go(func() (err error) {
		defer recoverChildPanic(ulogger.TestLogger{}, recoveredPanicPathService, &err, func(error) {
			t.Error("onPanic must not run without a panic")
		}, "test site")

		return own
	})

	require.Same(t, own, g.Wait())
	require.Equal(t, before, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathService)))
}

// TestRecoverChildPanic_NilLogger: the helper runs inside a recover, so it must not panic itself.
// Several BlockValidation literals in tests leave the logger unset.
func TestRecoverChildPanic_NilLogger(t *testing.T) {
	var g errgroup.Group

	g.Go(func() (err error) {
		defer recoverChildPanic(nil, recoveredPanicPathService, &err, nil, "test site")

		panic(childPanicValue)
	})

	requireChildPanicError(t, g.Wait())
}

// TestBindSubtreeBodyToHeader_ReadPanicIsALocalPanicNotCorrupt pins the binding pass. A panic in
// one subtree read used to leave that root slot unset with no verdict recorded. The chunk error
// only skipped the checks, the roots were composed with the empty slot, and the honest block was
// called corrupt: a bug in this node scored against the peer. The panic must come back as a
// local panic instead.
func TestBindSubtreeBodyToHeader_ReadPanicIsALocalPanicNotCorrupt(t *testing.T) {
	h := newPreBindHarness(t, nil)

	f := h.forgedBodies(0x3b, 0x20) // no forged index: an honest body

	store := &panickingSubtreeStore{Store: h.subtreeStore, key: *f.roots[2], fileType: fileformat.FileTypeSubtreeToCheck, onExists: true}
	h.bv.subtreeStore = store
	h.subtreeStore = store

	err := h.bv.bindSubtreeBodyToHeader(h.ctx, f.block)

	require.Positive(t, store.panics.Load(), "setup: the read of subtree 2 must reach the panic")
	requireChildPanicError(t, err)
}

// TestSubtreeReadVerdicts_PanicSurvivesAnOrdinaryFailure: several reads fail and one of them
// panicked. Whichever was recorded first, the error the verdicts fold into must carry the
// local-panic marker, or the caller scores a bug in this node against the peer. errors.Is
// matches on the code, and so the panic is identified only by its
// marker type, never by its code.
func TestSubtreeReadVerdicts_PanicSurvivesAnOrdinaryFailure(t *testing.T) {
	ordinary := errors.NewStorageError("ordinary read failure")
	mismatch := markSubtreeKeyMismatch(errors.NewProcessingError("subtree key mismatch"), subtreeBlobRef{hash: chainhash.Hash{0x71}, fileType: fileformat.FileTypeSubtreeToCheck})

	cases := map[string][]error{
		"ordinary first":            {ordinary, newChildPanicError()},
		"panic first":               {newChildPanicError(), ordinary},
		"mismatch, ordinary, panic": {mismatch, ordinary, newChildPanicError()},
	}

	for name, recorded := range cases {
		t.Run(name, func(t *testing.T) {
			var v subtreeReadVerdicts

			for _, err := range recorded {
				v.record(err)
			}

			require.True(t, v.failed())
			require.True(t, isLocalPanic(v.err()), "the panic marker must survive, got %v", v.err())
		})
	}
}

// TestBindSubtreeBodyToHeader_PanicAfterAnOrdinaryFailureIsALocalPanic drives the binding pass
// with two failing reads: subtree 1 fails at once with an ordinary storage error, and subtree 2
// panics after it. The panic must still decide the result.
func TestBindSubtreeBodyToHeader_PanicAfterAnOrdinaryFailureIsALocalPanic(t *testing.T) {
	h := newPreBindHarness(t, nil)

	f := h.forgedBodies(0x3d, 0x40)

	store := &panickingSubtreeStore{
		Store:      h.subtreeStore,
		key:        *f.roots[2],
		fileType:   fileformat.FileTypeSubtreeToCheck,
		onExists:   true,
		failKey:    f.roots[1],
		failErr:    errors.NewStorageError("ordinary read failure"),
		panicAfter: 200 * time.Millisecond,
	}
	h.bv.subtreeStore = store
	h.subtreeStore = store

	err := h.bv.bindSubtreeBodyToHeader(h.ctx, f.block)

	require.Positive(t, store.panics.Load(), "setup: the read of subtree 2 must reach the panic")
	requireChildPanicError(t, err)
}

// TestPrefetchSubtreeBatch_ReaderPanicIsALocalPanic pins the batch reader. A reader that panics
// never sends its result, so the collector used to block until the group closed the channel and
// then return "channel closed", an error with no marker that catchup scored against the peer.
func TestPrefetchSubtreeBatch_ReaderPanicIsALocalPanic(t *testing.T) {
	h := newPreBindHarness(t, nil)

	f := h.forgedBodies(0x3c, 0x30)

	store := &panickingSubtreeStore{Store: h.subtreeStore, key: *f.roots[1], fileType: fileformat.FileTypeSubtreeToCheck, onExists: true}
	h.bv.subtreeStore = store
	h.subtreeStore = store

	batch, err := h.bv.prefetchSubtreeBatch(h.ctx, f.block, 0, len(f.roots), false)

	require.Positive(t, store.panics.Load(), "setup: the read of subtree 1 must reach the panic")
	require.Nil(t, batch)
	requireChildPanicError(t, err)
	require.NotContains(t, err.Error(), "channel")
}

// TestSubtreeToCheckPresentBefore_ProbePanicFailsSafe: a probe that panics must record the hash
// as present, like a probe that errors, so the corrupt-branch cleanup can only fail to delete a
// file, never delete one that was there before.
func TestSubtreeToCheckPresentBefore_ProbePanicFailsSafe(t *testing.T) {
	ctx := context.Background()

	panicking := chainhash.Hash{0x51}
	absent := chainhash.Hash{0x52}

	store := &panickingSubtreeStore{Store: blobmemory.New(), key: panicking, fileType: fileformat.FileTypeSubtreeToCheck, onExists: true}

	bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
	presentBefore := bv.subtreeToCheckPresentBefore(ctx, cleanupTestBlock(t, &panicking, &absent))

	require.Positive(t, store.panics.Load(), "setup: the probe must reach the panic")
	require.Contains(t, presentBefore, panicking, "a panicked probe must fail safe to present")
	require.NotContains(t, presentBefore, absent)
}

// TestFetchBlocksConcurrently_SetupPanicClosesValidateBlocksChan: validateBlocksOnChannel ranges
// over validateBlocksChan, and a range does not see a cancelled context. If
// fetchBlocksConcurrently panics before orderedDelivery starts, the channel must still close,
// or the recovered catchup waits for ever holding the catchup lock. A Server with no settings
// panics on the first settings read in the setup.
func TestFetchBlocksConcurrently_SetupPanicClosesValidateBlocksChan(t *testing.T) {
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}}

	u := &Server{logger: ulogger.TestLogger{}}
	catchupCtx := &CatchupContext{blockUpTo: block, blockHeaders: []*model.BlockHeader{block.Header}}

	validateBlocksChan := make(chan blockForValidation, 1)

	var g errgroup.Group

	g.Go(func() (err error) {
		defer recoverChildPanic(u.logger, recoveredPanicPathCatchupFetch, &err, nil, "test fetchBlocksConcurrently")

		return u.fetchBlocksConcurrently(context.Background(), catchupCtx, validateBlocksChan, nil)
	})

	requireChildPanicError(t, g.Wait())

	select {
	case _, open := <-validateBlocksChan:
		require.False(t, open, "validateBlocksChan must be closed after a panic in the setup")
	case <-time.After(5 * time.Second):
		t.Fatal("validateBlocksChan was left open after a panic in the setup")
	}
}

// TestProcessBlockFound_UnifiedRouteChildPanicIsRetryLaterAndCounted drives the legacy unified
// route, where processBlockFound returns quickValidateBlock's error to netsync. netsync's
// parkCommitFailure reads an ERR_PROCESSING error it does not know as a rejection: it deletes the
// parked record, blames the peer and marks the hash failed. A panic in a quick-validation
// goroutine must come back as a ServiceError, the retry-later row, with nothing applied, and be
// counted toward the per-peer panic cap.
func TestProcessBlockFound_UnifiedRouteChildPanicIsRetryLaterAndCounted(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_child_panic")

	block, parent, child := unifiedRouteSpendingBlock(t, us, 0x61)

	store := &panickingSubtreeStore{Store: us.subtreeStore, key: *block.Subtrees[0], fileType: fileformat.FileTypeSubtreeToCheck, onExists: true}
	us.s.blockValidation.subtreeStore = store

	before := testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathBlockValidation))

	err := us.s.processBlockFound(context.Background(), block.Hash(), "peer-1", "legacy", true, block)

	require.Positive(t, store.panics.Load(), "setup: quick validation must reach the panic")
	requireChildPanicError(t, err)
	require.True(t, errors.IsTransientLocalError(err), "netsync must read the error as retry later, got %v", err)
	require.True(t, strings.Contains(err.Error(), "retrying later"), "%v", err)

	requireNothingApplied(t, us, block, parent, child)

	require.Positive(t, testutil.ToFloat64(prometheusRecoveredPanics.WithLabelValues(recoveredPanicPathBlockValidation))-before)

	us.s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 1
	require.True(t, us.s.panicAttemptsExhausted(block.Hash(), "peer-1"), "the child panic must count toward the peer's panic cap")

	// The next delivery from the same peer stops at the cap gate. netsync reads that answer
	// too, so it must also be the retry-later row: a bare ERR_PROCESSING error would make
	// parkCommitFailure delete the parked record, blame the peer and mark the hash failed.
	panicsBefore := store.panics.Load()

	err = us.s.processBlockFound(context.Background(), block.Hash(), "peer-1", "legacy", true, block)

	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "the capped legacy delivery must read as retry later, got %v", err)
	require.Equal(t, panicsBefore, store.panics.Load(), "the cap gate must stop the delivery before validation runs")

	requireNothingApplied(t, us, block, parent, child)
}

// TestTryQuickValidation_ChildPanicAbortsTheAttempt drives catchup's quick-validation step with a
// subtree read that panics. The panic comes back from quickValidateBlockAsync as an error, and
// tryQuickValidation must return that error and stop the attempt. Falling through to normal
// validation would drop the error and its marker, so releaseCatchupLock would never reach its
// local_panic case.
func TestTryQuickValidation_ChildPanicAbortsTheAttempt(t *testing.T) {
	h := newPreBindHarness(t, nil)

	f := h.forgedBodies(0x3e, 0x50) // no forged index: an honest body

	store := &panickingSubtreeStore{Store: h.subtreeStore, key: *f.roots[2], fileType: fileformat.FileTypeSubtreeToCheck, onExists: true}
	h.bv.subtreeStore = store
	h.subtreeStore = store

	server, catchupCtx := h.newAbortServer()
	catchupCtx.blockUpTo = f.block

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	tryNormal, err := server.tryQuickValidation(h.ctx, f.block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	require.Positive(t, store.panics.Load(), "setup: quick validation must reach the panic")
	require.False(t, tryNormal, "a local panic must stop the attempt, not fall through to normal validation")
	requireChildPanicError(t, err)
	require.Empty(t, catchupCtx.corruptBlockHash, "a local panic must not be recorded as a corrupt block")
	require.Empty(t, catchupCtx.incompleteBlockHash)

	// End state of the cycle: releaseCatchupLock, handed the error as validateBlocksOnChannel
	// returns it, records the attempt as local_panic.
	catchupCtx.startTime = time.Now()
	require.NoError(t, server.acquireCatchupLock(catchupCtx))
	server.releaseCatchupLock(catchupCtx, &err)

	require.NotNil(t, server.previousCatchupAttempt)
	require.Equal(t, "local_panic", server.previousCatchupAttempt.ErrorType)
}

// TestApplyTxsWithRetry_PanicWinsOverAnOrdinaryHardFailure: one apply panics and another fails
// with an ordinary hard error. Whichever finishes first, the error the wave returns must carry
// the local-panic marker. The legacy route keys on that marker to count the panic and to answer
// retry later; without it netsync reads the ordinary error as a judgement on the block.
func TestApplyTxsWithRetry_PanicWinsOverAnOrdinaryHardFailure(t *testing.T) {
	cases := map[string]struct {
		panicDelay, failDelay time.Duration
	}{
		"panic first":    {panicDelay: 0, failDelay: 200 * time.Millisecond},
		"ordinary first": {panicDelay: 200 * time.Millisecond, failDelay: 0},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			spy := &spendRetrySpyStore{failuresLeft: map[chainhash.Hash]int{}, failErr: map[chainhash.Hash]error{}}
			u, block, txs := newSpendRetryHarness(t, spy)

			items := make([]txApply, len(txs))
			for i, tx := range txs {
				items[i] = txApply{tx: tx}
			}

			panicking := *txs[0].TxIDChainHash()
			failing := *txs[1].TxIDChainHash()

			var panics atomic.Int32

			err := u.applyTxsWithRetry(context.Background(), block, "test", items, len(items),
				func(_ context.Context, item txApply) error {
					switch *item.tx.TxIDChainHash() {
					case panicking:
						time.Sleep(tc.panicDelay)
						panics.Add(1)
						panic(childPanicValue)
					case failing:
						time.Sleep(tc.failDelay)

						return errors.NewProcessingError("ordinary hard failure")
					}

					return nil
				})

			require.Equal(t, int32(1), panics.Load(), "setup: the apply must reach the panic")
			requireChildPanicError(t, err)
			require.NotContains(t, err.Error(), "ordinary hard failure")
		})
	}
}
