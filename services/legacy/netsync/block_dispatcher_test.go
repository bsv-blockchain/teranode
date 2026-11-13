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
func testDispatcher(t *testing.T) (*blockDispatcher, *tailRecorder) {
	t.Helper()

	s := test.CreateBaseTestSettings(t)

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
func dispatchAt(height uint32) *blockDispatch {
	h := chainhash.HashH([]byte{byte(height)})

	return &blockDispatch{
		parked: &parkedBlock{hash: h},
		height: height,
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

// TestDispatcher_OneBlockAtATime pins what replaced the quick window: a second block
// is never admitted while one is in flight, and is admitted as soon as the first one's
// tail has run. Two blocks in flight at once is the window this branch removed.
func TestDispatcher_OneBlockAtATime(t *testing.T) {
	bd, rec := testDispatcher(t)
	block := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error { <-block; return nil }

	require.True(t, bd.canDispatch(dispatchAt(1)))
	bd.dispatch(dispatchAt(1))
	require.False(t, bd.canDispatch(dispatchAt(2)), "nothing joins a block in flight")

	close(block)
	bd.drainCompletions(t, rec, 1)
	require.True(t, bd.frontierEmpty())
	require.True(t, bd.canDispatch(dispatchAt(2)))
}

// TestDispatcher_AFailedBlockKeepsItsOwnErrorClass: with one block in flight there is no
// successor to abort, and the failed block's tail still records its own error, not a
// substituted local fault.
func TestDispatcher_AFailedBlockKeepsItsOwnErrorClass(t *testing.T) {
	bd, rec := testDispatcher(t)
	bd.parkedRun = func(context.Context, *blockDispatch) error { return errors.NewProcessingError("block 1 broke") }

	d := dispatchAt(1)
	bd.dispatch(d)
	bd.drainCompletions(t, rec, 1)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Error(t, rec.errs[0])
	require.False(t, errors.IsTransientLocalError(rec.errs[0]), "the block keeps its own error class")
	require.False(t, d.aborted, "the block is at fault, so its tail still records a failure backoff")
}

func TestDispatcher_CheckpointBlockIsABarrier(t *testing.T) {
	bd, rec := testDispatcher(t)
	block := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error { <-block; return nil }

	cp := dispatchAt(1)
	cp.isCheckpoint = true
	bd.dispatch(cp)
	require.False(t, bd.canDispatch(dispatchAt(2)), "nothing dispatches while a checkpoint block is in flight")

	close(block)
	bd.drainCompletions(t, rec, 1)
	require.True(t, bd.canDispatch(dispatchAt(2)))
}

func TestDispatcher_ContextErrorFromAWorkerIsSubstitutedWithAServiceError(t *testing.T) {
	bd, rec := testDispatcher(t)
	bd.parkedRun = func(context.Context, *blockDispatch) error { return context.Canceled }

	bd.dispatch(dispatchAt(1))
	bd.drainCompletions(t, rec, 1)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Error(t, rec.errs[0], "a cancelled block is never reported as accepted")
	require.True(t, errors.IsTransientLocalError(rec.errs[0]))
	require.False(t, errors.IsContextError(rec.errs[0]), "the tail's context branch must not swallow it as an accepted block")
}

// TestDispatcher_InFlight covers the frontier lookup the drain uses to decide
// whether a hash already has a worker running for it: any hash in the frontier
// counts as in flight.
//
// This used to also cover parentFor, the head's own lookup for which frontier
// tail a queued block's parent was — deleted along with the decoded-block
// consumer (handleBlockMsgHead) that called it: a parked dispatch never
// resolves a parent of its own any more (blockDispatcher.dispatch refuses one),
// so there is nothing left to pin there.
func TestDispatcher_InFlight(t *testing.T) {
	bd, rec := testDispatcher(t)
	block := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error { <-block; return nil }

	first := dispatchAt(1)
	bd.dispatch(first)

	require.True(t, bd.inFlight(first.parked.hash))
	require.False(t, bd.inFlight(chainhash.HashH([]byte("absent"))))

	close(block)
	bd.drainCompletions(t, rec, 1)
	require.False(t, bd.inFlight(first.parked.hash))
}

// TestDispatcher_InFlightUntilItsTailHasRun pins that a block stays in flight while its
// tail runs. The tail is what deletes a committed block's record, so a block that left
// the frontier before its tail looked, to the wanted-range pass in that window, like a
// record on disk that nothing owned: the pass adopted it into the park, and the park's
// sweep then read it back after its files had gone. Mainnet logged that 82 times in four
// hours on 2026-10-06, as a parked block that "could not be read back".
//
// The tail checks in flight itself because that is where the wanted-range pass runs:
// a committed block's tail tops up block requests (fetchHeaderBlocks), which is what
// reaches unownedBlocks.
func TestDispatcher_InFlightUntilItsTailHasRun(t *testing.T) {
	bd, rec := testDispatcher(t)
	bd.parkedRun = func(context.Context, *blockDispatch) error { return nil }

	var duringTail bool

	bd.parkedTail = func(d *blockDispatch, err error) error {
		duringTail = bd.inFlight(d.parked.hash)

		return rec.tail(d, err)
	}

	first := dispatchAt(1)
	bd.dispatch(first)
	bd.drainCompletions(t, rec, 1)

	require.True(t, duringTail, "a block whose tail is still deleting its record is in flight")
	require.False(t, bd.inFlight(first.parked.hash), "and is not once the tail has run")
}
