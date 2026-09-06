package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// chainOf returns n consecutive blocks whose headers link, heights 1..n, with distinct hashes.
func chainOf(t *testing.T, n int) []*model.Block {
	t.Helper()

	blocks := make([]*model.Block, 0, n)
	prev := chainhash.Hash{}

	for i := 1; i <= n; i++ {
		// prevHash is a fresh variable each iteration: HashPrevBlock is a pointer, and a
		// single reused variable across iterations would leave every header aliasing
		// whatever value the loop last wrote, corrupting all but the final block's parent
		// link once the loop has finished.
		prevHash := prev
		h := &model.BlockHeader{Version: 1, HashPrevBlock: &prevHash, HashMerkleRoot: &chainhash.Hash{}, Timestamp: uint32(i), Bits: model.NBit{}, Nonce: uint32(i)}
		b := &model.Block{Header: h, Height: uint32(i)}
		blocks = append(blocks, b)
		prev = *b.Hash()
	}

	return blocks
}

type recordingCommitter struct {
	mu     sync.Mutex
	order  []uint32
	failAt uint32 // height whose commit returns an error; 0 = never
}

func (r *recordingCommitter) commit(_ context.Context, e *windowEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.failAt != 0 && e.Height() == r.failAt {
		return errors.NewProcessingError("commit failed at %d", e.Height())
	}

	r.order = append(r.order, e.Height())

	return nil
}

func newTestWindow(t *testing.T, depth int, c *recordingCommitter) (*quickWindow, context.CancelFunc) {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	w := newQuickWindow(ulogger.TestLogger{}, depth, 64, c.commit)
	w.Start(ctx)

	return w, cancel
}

func TestQuickWindow_CommitsInHeightOrderWhenStoreWorkCompletesOutOfOrder(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 3, c)
	defer cancel()

	blocks := chainOf(t, 3)
	ctx := context.Background()

	e1, dup, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	require.False(t, dup)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)
	e3, _, err := w.Admit(ctx, blocks[2])
	require.NoError(t, err)

	// Store work finishes 3, 2, 1.
	e3.StoreDone()
	e2.StoreDone()

	// Nothing may commit while the head is pending.
	select {
	case <-time.After(50 * time.Millisecond):
	case <-e3.committed:
		t.Fatal("block 3 committed before block 1")
	}

	e1.StoreDone()

	for _, e := range []*windowEntry{e1, e2, e3} {
		require.NoError(t, e.WaitCommitted(ctx))
		e.Leave()
	}

	c.mu.Lock()
	require.Equal(t, []uint32{1, 2, 3}, c.order)
	c.mu.Unlock()
}

func TestQuickWindow_AdmitRefusesABlockWhoseParentIsNotTheTail(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 3, c)
	defer cancel()

	blocks := chainOf(t, 3)
	ctx := context.Background()

	_, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)

	// blocks[2]'s parent is blocks[1], which is not in flight.
	_, _, err = w.Admit(ctx, blocks[2])
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "a refused admission must be a local fault, got %v", err)
}

func TestQuickWindow_AdmitBlocksAtDepthAndResumesWhenAnEntryLeaves(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 1, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)

	admitted := make(chan struct{})
	go func() {
		defer close(admitted)
		_, _, err := w.Admit(ctx, blocks[1])
		require.NoError(t, err)
	}()

	select {
	case <-admitted:
		t.Fatal("second block admitted while the window was full at depth 1")
	case <-time.After(50 * time.Millisecond):
	}

	e1.StoreDone()
	require.NoError(t, e1.WaitCommitted(ctx))
	e1.Leave()

	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("second block was not admitted after the first left")
	}
}

func TestQuickWindow_DuplicateAdmitReturnsTheLiveEntry(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 1)
	ctx := context.Background()

	e1, dup, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	require.False(t, dup)

	e1again, dup, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	require.True(t, dup)
	require.Same(t, e1, e1again)
}

func TestQuickWindow_GateReleasesWaiterOnlyWhenClosed(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	parent := chainhash.HashH([]byte("parent"))
	g, err := e1.RegisterBatch([]chainhash.Hash{parent})
	require.NoError(t, err)
	e1.RegistrationComplete()

	require.NoError(t, e2.WaitPredecessorsRegistered(ctx))
	require.Same(t, g, w.GateFor(e2, &parent), "block 2 must see block 1's open gate")
	require.Nil(t, w.GateFor(e1, &parent), "an entry never waits on its own gate")
	require.True(t, w.Registered(nil, &parent))

	var released atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		require.NoError(t, g.Wait(ctx))
		released.Store(true)
	}()

	select {
	case <-done:
		t.Fatal("waiter released before the gate closed")
	case <-time.After(50 * time.Millisecond):
	}

	g.Close()
	<-done
	require.True(t, released.Load())
	require.Nil(t, w.GateFor(e2, &parent), "a closed gate is no longer open")
	require.True(t, w.Registered(nil, &parent), "the retained set keeps the id until the entry leaves")
}

func TestQuickWindow_RegisterBatchRefusesATxidHeldByAnotherLiveEntry(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	txid := chainhash.HashH([]byte("shared"))
	_, err = e1.RegisterBatch([]chainhash.Hash{txid})
	require.NoError(t, err)
	_, err = e2.RegisterBatch([]chainhash.Hash{txid})
	require.Error(t, err)
}

func TestQuickWindow_HeadFailureAbortsSuccessorsWithAServiceErrorAndFailsTheirGates(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 3, c)
	defer cancel()

	blocks := chainOf(t, 3)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)
	e3, _, err := w.Admit(ctx, blocks[2])
	require.NoError(t, err)

	g2, err := e2.RegisterBatch([]chainhash.Hash{chainhash.HashH([]byte("b2"))})
	require.NoError(t, err)

	e3.StoreDone() // block 3 finished its store work before block 1 failed

	head := errors.NewProcessingError("spend hard-failed")
	e1.Fail(head)

	err = e1.WaitCommitted(ctx)
	require.ErrorIs(t, err, head, "the head returns its own error with its own class")

	err = e2.WaitCommitted(ctx)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "an aborted successor returns a service error, got %v", err)
	require.Error(t, e3.WaitCommitted(ctx))
	require.Error(t, g2.Wait(ctx), "a failed entry's gates release waiters with an error")
	require.Error(t, e2.Context().Err(), "an aborted entry's context is cancelled")

	c.mu.Lock()
	require.Empty(t, c.order, "nothing commits after the head failed")
	c.mu.Unlock()

	e1.Leave()
	e2.Leave()
	e3.Leave()

	// The window is empty again and admits fresh.
	_, _, err = w.Admit(ctx, blocks[0])
	require.NoError(t, err)
}

func TestQuickWindow_CommitFailureFailsTheEntryAndAbortsSuccessors(t *testing.T) {
	c := &recordingCommitter{failAt: 1}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	e1.StoreDone()
	e2.StoreDone()

	err = e1.WaitCommitted(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed at 1", "the head's error must wrap the committer's own error")

	err = e2.WaitCommitted(ctx)
	require.True(t, errors.IsTransientLocalError(err))

	c.mu.Lock()
	require.Empty(t, c.order, "nothing commits after the head's own commit call failed")
	c.mu.Unlock()
}

func TestQuickWindow_IDAssignedChainOrdersSuccessors(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	require.NoError(t, e1.WaitPredecessorIDAssigned(ctx), "the first entry has no predecessor and never waits")

	waited := make(chan error, 1)
	go func() { waited <- e2.WaitPredecessorIDAssigned(ctx) }()

	select {
	case <-waited:
		t.Fatal("block 2 proceeded before block 1 assigned its id")
	case <-time.After(50 * time.Millisecond):
	}

	e1.IDAssigned()
	require.NoError(t, <-waited)
}

func TestQuickWindow_AwaitParentReturnsWhenTheParentIsAdmitted(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 1)
	ctx := context.Background()

	require.Nil(t, w.AwaitParent(ctx, blocks[0].Hash(), 30*time.Millisecond))

	found := make(chan *windowEntry, 1)
	go func() { found <- w.AwaitParent(ctx, blocks[0].Hash(), 2*time.Second) }()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	require.Same(t, e1, <-found)
}

// Leaving an entry before it has ever reached StoreDone is a caller giving up early instead of
// calling Fail. The window must fail-close it (and everything behind it) rather than silently
// erase it from the ordered chain, or the committer would wedge waiting on channels nothing
// would ever close again.
func TestQuickWindow_LeaveBeforeCommitAbortsSuccessorsAndFreesTheWindow(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 2)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	// e1 never calls StoreDone; its owner bails out and leaves directly.
	e1.Leave()

	err = e2.WaitCommitted(ctx)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "an aborted successor returns a service error, got %v", err)
	e2.Leave()

	c.mu.Lock()
	require.Empty(t, c.order, "nothing commits once the head left before committing")
	c.mu.Unlock()

	// The window is empty again and admits fresh.
	_, _, err = w.Admit(ctx, blocks[0])
	require.NoError(t, err)
}

// failLocked cancels the entry's context before closing committed, so a caller waiting on
// e.Context() races a select between the two: with no non-blocking re-check, Go picks between
// two simultaneously-ready channels at random, and roughly half the calls would see a bare
// context.Canceled instead of the recorded service error. 200 iterations makes that failure
// mode from the old code (were it still present) all but certain to show up.
func TestQuickWindow_WaitCommittedNeverReturnsBareContextCanceledAfterFail(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 1, c)
	defer cancel()

	blocks := chainOf(t, 1)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)

	// A service error, not a processing error: WaitCommitted returns the head's own recorded
	// error verbatim, so the error must itself already classify as a transient local fault for
	// the assertions below to be meaningful.
	e1.Fail(errors.NewServiceError("boom"))

	for i := 0; i < 200; i++ {
		err := e1.WaitCommitted(e1.Context())
		require.Error(t, err)
		require.True(t, errors.IsTransientLocalError(err), "iteration %d: got %v", i, err)
		require.False(t, errors.Is(err, context.Canceled), "iteration %d: got a bare context.Canceled", i)
	}
}

// Leave releases the retained id, not just the ordered-chain slot: RegisterBatch's duplicate
// check must stop seeing a txid the moment its owner has left.
func TestQuickWindow_LeaveClearsRegistered(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 1)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)

	txid := chainhash.HashH([]byte("gone-after-leave"))
	_, err = e1.RegisterBatch([]chainhash.Hash{txid})
	require.NoError(t, err)
	require.True(t, w.Registered(nil, &txid))

	e1.StoreDone()
	require.NoError(t, e1.WaitCommitted(ctx))
	e1.Leave()

	require.False(t, w.Registered(nil, &txid), "Leave must release the retained id")
}

// A head failing concurrently with a fourth block's Admit blocked at depth must resolve
// cleanly: the abort frees the ordered-chain slots the blocked Admit is waiting on, nothing
// commits, and there is no deadlock between the committer, the failing goroutine and the
// blocked admitter.
func TestQuickWindow_ConcurrentHeadFailureAndBlockedAdmitResolveWithoutDeadlock(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 3, c)
	defer cancel()

	blocks := chainOf(t, 4)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)
	e3, _, err := w.Admit(ctx, blocks[2])
	require.NoError(t, err)

	// admitResult carries the goroutine's outcome back to the test goroutine: require must
	// never be called from a non-test goroutine, since t.FailNow only unwinds that goroutine,
	// not the test.
	type admitResult struct {
		e   *windowEntry
		err error
	}

	admitted := make(chan admitResult, 1)
	go func() {
		e4, _, err := w.Admit(ctx, blocks[3])
		admitted <- admitResult{e: e4, err: err}
	}()

	select {
	case <-admitted:
		t.Fatal("fourth block admitted while the window was full at depth 3")
	case <-time.After(50 * time.Millisecond):
	}

	go e1.Fail(errors.NewProcessingError("boom"))

	require.Error(t, e1.WaitCommitted(ctx))
	require.Error(t, e2.WaitCommitted(ctx))
	require.Error(t, e3.WaitCommitted(ctx))

	var res admitResult
	select {
	case res = <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("fourth block was not admitted after the head failed")
	}
	require.NoError(t, res.err)
	require.NotNil(t, res.e)
	e4 := res.e

	c.mu.Lock()
	require.Empty(t, c.order, "nothing committed once the head failed")
	c.mu.Unlock()

	e1.Leave()
	e2.Leave()
	e3.Leave()
	e4.Leave()
}

// Leave on an entry still mid-commit (violating the normal "Leave only after the pipeline
// returns" contract) must not race the committer's own read of e.Block(), and must not panic.
// leave clears the parent links that make e unreachable from the window, but must never nil
// e.block itself while a commit callback holding the same *windowEntry could still be reading
// it.
func TestQuickWindow_LeaveDuringCommitDoesNotRaceOrPanic(t *testing.T) {
	initPrometheusMetrics()

	entered := make(chan struct{})
	release := make(chan struct{})
	commitDone := make(chan struct{})

	var sawNilBlock atomic.Bool
	var readHeight atomic.Uint32

	commit := func(_ context.Context, e *windowEntry) error {
		close(entered)
		<-release

		// Read the block the same way a real commit callback does, concurrently with the
		// Leave call below.
		b := e.Block()
		if b == nil {
			sawNilBlock.Store(true)
		} else {
			readHeight.Store(b.Height)
		}

		// e1.committed can already be closed by the concurrent Leave by this point (it aborts
		// the still-queued entry before this call returns), so WaitCommitted alone gives the
		// test no happens-before edge to this goroutine's stores above. Close commitDone to
		// give it one.
		close(commitDone)

		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newQuickWindow(ulogger.TestLogger{}, 1, 64, commit)
	w.Start(ctx)

	blocks := chainOf(t, 1)
	admitCtx := context.Background()

	e1, _, err := w.Admit(admitCtx, blocks[0])
	require.NoError(t, err)

	e1.StoreDone()
	<-entered // commit is now blocked mid-call, holding e1's block

	e1.Leave()
	close(release)

	select {
	case <-commitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("commit callback never returned")
	}

	// Whichever outcome wins the race between the concurrent Leave and the in-flight commit,
	// WaitCommitted must return without hanging.
	_ = e1.WaitCommitted(admitCtx)

	require.False(t, sawNilBlock.Load(), "commit must never observe a nil block")
	require.Equal(t, uint32(1), readHeight.Load())
}

// Once the committer stops, no entry still in the window may be left parked on WaitCommitted
// forever, and no further Admit may succeed and hand back an entry nothing will ever resolve.
func TestQuickWindow_ShutdownAbortsInFlightAndRefusesFurtherAdmits(t *testing.T) {
	c := &recordingCommitter{}
	w, cancel := newTestWindow(t, 2, c)
	defer cancel()

	blocks := chainOf(t, 3)
	ctx := context.Background()

	e1, _, err := w.Admit(ctx, blocks[0])
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, blocks[1])
	require.NoError(t, err)

	cancel()

	err = e1.WaitCommitted(ctx)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "got %v", err)

	err = e2.WaitCommitted(ctx)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "got %v", err)

	_, _, err = w.Admit(ctx, blocks[2])
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "got %v", err)
}
