package netsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// testDispatcher builds a dispatcher with an injected tail so no peer, store or
// gRPC client is needed. Each test also replaces bd.parkedRun, so nothing here
// ever reaches HandleBlockDirect or HandleConvertedBlock.
func testDispatcher(t *testing.T, depth int) (*blockDispatcher, *tailRecorder) {
	t.Helper()

	s := test.CreateBaseTestSettings(t)
	s.BlockValidation.QuickWindowBlocks = depth
	s.BlockValidation.QuickValidateSkipUtxoLock = true
	s.BlockValidation.MaxBlocksBehindBlockAssembly = 20

	sm := &SyncManager{logger: ulogger.TestLogger{}, settings: s, ctx: context.Background()}
	bd := newBlockDispatcher(sm)
	rec := &tailRecorder{}
	bd.parkedTail = rec.tail

	return bd, rec
}

// tailRecorder stands in for the dispatcher's own parkedTail and records the
// order the tails ran in, with the error each one was handed.
type tailRecorder struct {
	mu      sync.Mutex
	heights []uint32
	errs    []error
}

func (r *tailRecorder) tail(d *blockDispatch, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.heights = append(r.heights, d.height)
	r.errs = append(r.errs, err)

	return err
}

func (r *tailRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.heights)
}

// dispatchAt builds the dispatch a drain would build for a parked block at
// height, with no resolved parent — see blockDispatcher.dispatch's own guard
// for why that must always be true of a parked dispatch.
func dispatchAt(height uint32, bytes int64) *blockDispatch {
	h := chainhash.HashH([]byte{byte(height)})

	return &blockDispatch{
		parked:   &parkedBlock{hash: h},
		height:   height,
		windowed: true,
		bytes:    bytes,
	}
}

// drainCompletions pumps the completions channel the way the consumer goroutine
// does, until want tails have run. want == 0 means "pump briefly and expect
// nothing", which is how a test proves a tail is still blocked behind an
// unsettled frontier head.
func (bd *blockDispatcher) drainCompletions(t *testing.T, rec *tailRecorder, want int) {
	t.Helper()

	if want == 0 {
		quiet := time.After(50 * time.Millisecond)

		for {
			select {
			case c := <-bd.completions:
				bd.complete(c)
			case <-quiet:
				return
			}
		}
	}

	deadline := time.After(2 * time.Second)

	for rec.count() < want {
		select {
		case c := <-bd.completions:
			bd.complete(c)
		case <-deadline:
			t.Fatalf("timed out waiting for %d tails, got %d", want, rec.count())
		}
	}
}

func TestDispatcher_TailsRunInDispatchOrderWhenWorkersFinishOutOfOrder(t *testing.T) {
	bd, rec := testDispatcher(t, 3)

	release := map[uint32]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}
	bd.parkedRun = func(_ context.Context, d *blockDispatch) error { <-release[d.height]; return nil }

	for h := uint32(1); h <= 3; h++ {
		d := dispatchAt(h, 1000)
		require.True(t, bd.canDispatch(d))
		bd.dispatch(d)
	}

	close(release[3])
	close(release[2])
	bd.drainCompletions(t, rec, 0)
	require.Equal(t, 0, rec.count(), "no tail before the head completed")

	close(release[1])
	bd.drainCompletions(t, rec, 3)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, []uint32{1, 2, 3}, rec.heights)
}

func TestDispatcher_HeadFailureAbortsSuccessorsWithServiceErrorsAndNoBackoff(t *testing.T) {
	bd, rec := testDispatcher(t, 3)

	release := map[uint32]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}
	bd.parkedRun = func(_ context.Context, d *blockDispatch) error {
		<-release[d.height]

		if d.height == 1 {
			return errors.NewProcessingError("block 1 broke")
		}

		return nil
	}

	dispatches := make([]*blockDispatch, 0, 3)

	for h := uint32(1); h <= 3; h++ {
		d := dispatchAt(h, 1000)
		dispatches = append(dispatches, d)
		bd.dispatch(d)
	}

	close(release[1])
	close(release[2])
	close(release[3])
	bd.drainCompletions(t, rec, 3)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, []uint32{1, 2, 3}, rec.heights)
	require.False(t, errors.IsTransientLocalError(rec.errs[0]), "the head keeps its own error class")
	require.True(t, errors.IsTransientLocalError(rec.errs[1]))
	require.True(t, errors.IsTransientLocalError(rec.errs[2]))
	require.False(t, dispatches[0].aborted, "the head is at fault, so its tail still records a failure backoff")
	require.True(t, dispatches[1].aborted, "an aborted successor records no failure backoff")
	require.True(t, dispatches[2].aborted)
	require.True(t, bd.frontierEmpty())
}

func TestDispatcher_CapacityAndBudgetGateAdmission(t *testing.T) {
	bd, rec := testDispatcher(t, 2)
	// Two 1000-byte blocks fit (each charged four times its wire size); a third is
	// held out by the depth, not the budget.
	bd.budget = 12_000

	block := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error { <-block; return nil }

	require.True(t, bd.canDispatch(dispatchAt(1, 1000)))
	bd.dispatch(dispatchAt(1, 1000))
	require.True(t, bd.canDispatch(dispatchAt(2, 1000)))
	bd.dispatch(dispatchAt(2, 1000))
	require.False(t, bd.canDispatch(dispatchAt(3, 1000)), "depth 2 reached")

	close(block)
	bd.drainCompletions(t, rec, 2)
	require.True(t, bd.frontierEmpty())

	// An over-budget block is admitted only into an empty frontier, and once it is
	// in flight nothing joins it.
	over := dispatchAt(4, 20_000)
	require.True(t, bd.canDispatch(over), "over budget but the window is empty")
	bd.dispatch(over)
	require.False(t, bd.canDispatch(dispatchAt(5, 1000)), "the budget is already overdrawn")
}

func TestDispatcher_NonWindowBlockWaitsForAnEmptyFrontier(t *testing.T) {
	bd, rec := testDispatcher(t, 3)
	block := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error { <-block; return nil }

	bd.dispatch(dispatchAt(1, 1000))

	serial := dispatchAt(2, 1000)
	serial.windowed = false
	require.False(t, bd.canDispatch(serial))

	close(block)
	bd.drainCompletions(t, rec, 1)
	require.True(t, bd.canDispatch(serial))
}

// With the window route off, a drained block carries height zero and is never
// marked windowed (see drainStep). That has to change nothing: an unwindowed
// block is still admitted only into an empty frontier, and the budget it
// charges and releases still nets back to where it started rather than
// drifting.
func TestDispatcher_UnmeasuredNonWindowBlockChargesNothingAndStillSerialises(t *testing.T) {
	bd, rec := testDispatcher(t, 3)
	block := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error { <-block; return nil }

	require.Zero(t, bd.inflight)

	first := dispatchAt(1, 0)
	first.windowed = false
	require.True(t, bd.canDispatch(first))
	bd.dispatch(first)
	require.Zero(t, bd.inflight, "an unmeasured block charges nothing")

	second := dispatchAt(2, 0)
	second.windowed = false
	require.False(t, bd.canDispatch(second), "a zero size must not let a second block join the frontier")

	close(block)
	bd.drainCompletions(t, rec, 1)

	require.True(t, bd.frontierEmpty())
	require.Zero(t, bd.inflight, "the release nets the charge back to zero")
	require.True(t, bd.canDispatch(second))
}

func TestDispatcher_CheckpointBlockIsABarrier(t *testing.T) {
	bd, rec := testDispatcher(t, 3)
	block := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error { <-block; return nil }

	cp := dispatchAt(1, 1000)
	cp.isCheckpoint = true
	bd.dispatch(cp)
	require.False(t, bd.canDispatch(dispatchAt(2, 1000)), "nothing dispatches while a checkpoint block is in flight")

	close(block)
	bd.drainCompletions(t, rec, 1)
	require.True(t, bd.canDispatch(dispatchAt(2, 1000)))
}

func TestDispatcher_ContextErrorFromAWorkerIsSubstitutedWithAServiceError(t *testing.T) {
	bd, rec := testDispatcher(t, 2)
	bd.parkedRun = func(context.Context, *blockDispatch) error { return context.Canceled }

	bd.dispatch(dispatchAt(1, 1000))
	bd.drainCompletions(t, rec, 1)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Error(t, rec.errs[0], "a cancelled block is never reported as accepted")
	require.True(t, errors.IsTransientLocalError(rec.errs[0]))
	require.False(t, errors.IsContextError(rec.errs[0]), "the tail's context branch must not swallow it as an accepted block")
}

// TestDispatcher_InFlight covers the frontier lookup the drain uses to decide
// whether a hash already has a worker running for it: any hash in the frontier
// counts as in flight, whether it is the tail or further back.
//
// This used to also cover parentFor, the head's own lookup for which frontier
// tail a queued block's parent was — deleted along with the decoded-block
// consumer (handleBlockMsgHead) that called it: a parked dispatch never
// resolves a parent of its own any more (blockDispatcher.dispatch refuses one),
// so there is nothing left to pin there.
func TestDispatcher_InFlight(t *testing.T) {
	bd, rec := testDispatcher(t, 3)
	block := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error { <-block; return nil }

	first := dispatchAt(1, 1000)
	second := dispatchAt(2, 1000)
	bd.dispatch(first)
	bd.dispatch(second)

	require.True(t, bd.inFlight(first.parked.hash))
	require.True(t, bd.inFlight(second.parked.hash))
	require.False(t, bd.inFlight(chainhash.HashH([]byte("absent"))))

	close(block)
	bd.drainCompletions(t, rec, 2)
	require.False(t, bd.inFlight(first.parked.hash))
	require.False(t, bd.inFlight(second.parked.hash))
}
