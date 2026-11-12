package propagation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// waitBoundHarness drives ProcessTransaction's batch hand-off precisely: the
// dispatcher signals that it has the item and then parks on a gate until the
// test releases it, so the submitter is reliably blocked in group.Wait.
//
// c.batcher is a concrete *batcher.Batcher[batchItem], not an interface, so the
// dispatch-fn closure supplied to batcher.New IS the dispatcher.
type waitBoundHarness struct {
	client    *Client
	entered   chan struct{}
	released  chan struct{}
	finished  chan struct{} // one signal per completed dispatch-fn run
	completed atomic.Int32

	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newWaitBoundHarness(t *testing.T) *waitBoundHarness {
	t.Helper()

	h := &waitBoundHarness{
		entered:  make(chan struct{}),
		released: make(chan struct{}),
		// Buffered well beyond the dispatch runs any test here performs, so the
		// dispatcher never blocks on a signal a test does not drain.
		finished: make(chan struct{}, 8),
	}

	h.client = &Client{
		logger:    ulogger.TestLogger{},
		settings:  &settings.Settings{},
		batchSize: 1,
	}

	h.client.batcher = batcher.New(1, 10*time.Millisecond, func(batch []*batchItem) {
		h.enterOnce.Do(func() { close(h.entered) })
		<-h.released

		for _, it := range batch {
			it.complete(nil)
			h.completed.Add(1)
		}

		// Signalled AFTER the counter. it.complete releases the waiting caller
		// from inside the loop, i.e. BEFORE the increment lands, so a test that
		// read h.completed straight after ProcessTransaction returned could
		// legitimately observe zero. requireDispatchFinished is that edge.
		h.finished <- struct{}{}
	}, true)

	t.Cleanup(func() {
		h.release()
		h.client.batcher.Close()
	})

	return h
}

// release opens the dispatcher's gate. Idempotent, so a test may release
// explicitly and the cleanup may release again.
func (h *waitBoundHarness) release() {
	h.releaseOnce.Do(func() { close(h.released) })
}

// requireEntered blocks until the dispatcher holds the item, which is the
// precondition that makes an early return a genuine abandonment rather than a
// no-op on an item that was never dispatched.
func (h *waitBoundHarness) requireEntered(t *testing.T) {
	t.Helper()

	select {
	case <-h.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the dispatcher never received the item")
	}
}

// requireDispatchFinished waits for one dispatch-fn run to finish in full. The
// completion group releases the caller from inside it.complete(), before the
// dispatcher has finished its own bookkeeping, so this is the edge a test must
// synchronise on before reading h.completed.
func (h *waitBoundHarness) requireDispatchFinished(t *testing.T) {
	t.Helper()

	select {
	case <-h.finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the dispatcher did not finish its run")
	}
}

func (h *waitBoundHarness) processAsync(ctx context.Context) <-chan error {
	out := make(chan error, 1)

	go func() { out <- h.client.ProcessTransaction(ctx, bt.NewTx()) }()

	return out
}

func requireProcessOutcome(t *testing.T, out <-chan error) error {
	t.Helper()

	select {
	case err := <-out:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("ProcessTransaction did not return; the batch hand-off wait is unbounded")

		return nil
	}
}

// TestProcessTransaction_AbandonsOnCallerCancel is the late-completion
// functional test: the caller's context is cancelled while the dispatcher is
// parked, the caller returns, and only THEN does the dispatcher complete the
// already-abandoned item.
//
// This test does NOT prove that the abort path leaves item.result unread.
// Releasing the gate only after observing the caller's return establishes a
// happens-before edge, so an erroneous abort-path read would be ordered before
// the dispatcher's write and the race detector would stay silent. The race
// guard is TestProcessTransaction_AbortPathDoesNotReadResultSlot.
func TestProcessTransaction_AbandonsOnCallerCancel(t *testing.T) {
	h := newWaitBoundHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := h.processAsync(ctx)
	h.requireEntered(t)

	cancel()

	err := requireProcessOutcome(t, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "abandoned", "the error must identify the hand-off as abandoned")
	require.False(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrThresholdExceeded),
		"an abandoned hand-off must not be mistakable for a queue-full shed, or the caller unwinds a transaction that may still be in flight")

	// The dispatcher now finishes the abandoned item — the late result write that
	// has no reader. A double Group.Done would panic here.
	h.release()
	h.requireDispatchFinished(t)

	require.GreaterOrEqual(t, h.completed.Load(), int32(1),
		"the dispatcher must be able to finish the abandoned item")

	// The client is still usable: a fresh hand-off on an uncancelled context
	// waits to completion and succeeds, exactly as batch mode always did.
	require.NoError(t, h.client.ProcessTransaction(context.Background(), bt.NewTx()),
		"abandoning a wait must not leave the batcher or the completion group broken")
}

// TestProcessTransaction_AbortPathDoesNotReadResultSlot is the race regression
// test for the abandonment contract: cancellation and the dispatcher's result
// publication proceed concurrently with NO ordering imposed by the test, so an
// abort-path read of item.result is genuinely unsynchronised against the
// dispatcher's write and -race reports it.
func TestProcessTransaction_AbortPathDoesNotReadResultSlot(t *testing.T) {
	const iterations = 200

	for i := 0; i < iterations; i++ {
		entered := make(chan struct{})
		release := make(chan struct{})

		client := &Client{
			logger:    ulogger.TestLogger{},
			settings:  &settings.Settings{},
			batchSize: 1,
		}

		client.batcher = batcher.New(1, 10*time.Millisecond, func(b []*batchItem) {
			close(entered)
			<-release

			for _, it := range b {
				it.complete(errors.NewProcessingError("late")) // writes item.result
			}
		}, true)

		ctx, cancel := context.WithCancel(context.Background())

		out := make(chan error, 1)
		go func() { out <- client.ProcessTransaction(ctx, bt.NewTx()) }()

		select {
		case <-entered: // the item is on the batcher and NOT yet completed
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatalf("iteration %d: the dispatcher never received the item", i)
		}

		start := make(chan struct{})
		go func() { <-start; cancel() }()
		go func() { <-start; close(release) }()
		close(start) // cancel and publication race, unordered

		select {
		case <-out:
		case <-time.After(10 * time.Second):
			t.Fatalf("iteration %d: ProcessTransaction did not return", i)
		}

		client.batcher.Close()
		cancel()
	}
}

// TestProcessTransaction_AbandonsOnBackstopTimeout pins the finite backstop
// itself. Without it, reverting the timeout argument to 0 would still pass every
// other test here: the context is deliberately NON-cancellable, so only the
// timer arm can release the caller.
func TestProcessTransaction_AbandonsOnBackstopTimeout(t *testing.T) {
	old := batchHandoffTimeout
	batchHandoffTimeout = 50 * time.Millisecond

	t.Cleanup(func() { batchHandoffTimeout = old })

	h := newWaitBoundHarness(t)

	out := h.processAsync(context.Background())
	h.requireEntered(t)

	err := requireProcessOutcome(t, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "abandoned")
	require.True(t, errors.Is(err, errors.ErrServiceError), "the backstop must abandon with a service error")
	require.False(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrThresholdExceeded),
		"a backstop abandonment must not be mistakable for a queue-full shed")

	// The timer arm must abandon, not corrupt: the later completion is still
	// permitted, and a double Group.Done would panic.
	h.release()
	h.requireDispatchFinished(t)

	require.GreaterOrEqual(t, h.completed.Load(), int32(1),
		"the dispatcher must be able to finish the item the backstop abandoned")
}

// TestProcessTransaction_ReturnsResultWhenDispatcherCompletes pins that the
// early return did not break the success path: once Wait returns nil the result
// slot is read as before.
func TestProcessTransaction_ReturnsResultWhenDispatcherCompletes(t *testing.T) {
	h := newWaitBoundHarness(t)
	h.release()

	require.NoError(t, h.client.ProcessTransaction(context.Background(), bt.NewTx()))

	// Synchronise on the dispatcher before reading its counter: the caller is
	// released by it.complete(), which runs BEFORE the increment.
	h.requireDispatchFinished(t)
	require.Equal(t, int32(1), h.completed.Load())
}
