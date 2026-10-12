package validator

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

const waitBoundTestFee = 4242

// waitBoundMetaBytes is the serialised metadata a successful dispatch hands
// back, so the happy-path test exercises the real post-Wait decode.
func waitBoundMetaBytes(t *testing.T) []byte {
	t.Helper()

	b, err := (&utxometa.Data{Fee: waitBoundTestFee, SizeInBytes: 100}).Bytes()
	require.NoError(t, err)

	return b
}

// waitBoundHarness drives ValidateWithOptions' batch hand-off precisely: the
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

	metaBytes := waitBoundMetaBytes(t)

	h.client = &Client{
		logger:    ulogger.TestLogger{},
		batchSize: 1,
	}

	h.client.batcher = batcher.New(1, 10*time.Millisecond, func(batch []*batchItem) {
		h.enterOnce.Do(func() { close(h.entered) })
		<-h.released

		for _, it := range batch {
			it.complete(validateBatchResponse{metaData: metaBytes})
			h.completed.Add(1)
		}

		// Signalled AFTER the counter. it.complete releases the waiting caller
		// from inside the loop, i.e. BEFORE the increment lands, so a test that
		// read h.completed straight after ValidateWithOptions returned could
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

type validateOutcome struct {
	data *utxometa.Data
	err  error
}

func (h *waitBoundHarness) validateAsync(ctx context.Context) <-chan validateOutcome {
	out := make(chan validateOutcome, 1)

	go func() {
		data, err := h.client.ValidateWithOptions(ctx, bt.NewTx(), 1, NewDefaultOptions())
		out <- validateOutcome{data: data, err: err}
	}()

	return out
}

func requireValidateOutcome(t *testing.T, out <-chan validateOutcome) validateOutcome {
	t.Helper()

	select {
	case o := <-out:
		return o
	case <-time.After(10 * time.Second):
		t.Fatal("ValidateWithOptions did not return; the batch hand-off wait is unbounded")

		return validateOutcome{}
	}
}

// TestValidateWithOptions_AbandonsOnCallerCancel is the late-completion
// functional test: the caller's context is cancelled while the dispatcher is
// parked, the caller returns, and only THEN does the dispatcher complete the
// already-abandoned item.
//
// This test does NOT prove that the abort path leaves item.result unread.
// Releasing the gate only after observing the caller's return establishes a
// happens-before edge, so an erroneous abort-path read would be ordered before
// the dispatcher's write and the race detector would stay silent. The race
// guard is TestValidateWithOptions_AbortPathDoesNotReadResultSlot.
func TestValidateWithOptions_AbandonsOnCallerCancel(t *testing.T) {
	h := newWaitBoundHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := h.validateAsync(ctx)
	h.requireEntered(t)

	cancel()

	o := requireValidateOutcome(t, out)
	require.Error(t, o.err)
	require.Nil(t, o.data)
	require.Contains(t, o.err.Error(), "abandoned", "the error must identify the hand-off as abandoned")
	require.False(t, errors.Is(errors.UnwrapGRPC(o.err), errors.ErrThresholdExceeded),
		"an abandoned hand-off must not be mistakable for a queue-full shed, or the caller unwinds a transaction that may still be in flight")

	// The dispatcher now finishes the abandoned item — the late result write that
	// has no reader. A double Group.Done would panic here.
	h.release()
	h.requireDispatchFinished(t)

	require.GreaterOrEqual(t, h.completed.Load(), int32(1),
		"the dispatcher must be able to finish the abandoned item")

	// The client is still usable: a fresh hand-off on an uncancelled context
	// waits to completion and succeeds, exactly as batch mode always did.
	data, err := h.client.ValidateWithOptions(context.Background(), bt.NewTx(), 1, NewDefaultOptions())
	require.NoError(t, err, "abandoning a wait must not leave the batcher or the completion group broken")
	require.Equal(t, uint64(waitBoundTestFee), data.Fee)
}

// TestValidateWithOptions_AbortPathDoesNotReadResultSlot is the race regression
// test for the abandonment contract: cancellation and the dispatcher's result
// publication proceed concurrently with NO ordering imposed by the test, so an
// abort-path read of item.result — a struct value, so a read copies fields the
// dispatcher may be writing — is genuinely unsynchronised and -race reports it.
func TestValidateWithOptions_AbortPathDoesNotReadResultSlot(t *testing.T) {
	const iterations = 200

	for i := 0; i < iterations; i++ {
		entered := make(chan struct{})
		release := make(chan struct{})

		client := &Client{
			logger:    ulogger.TestLogger{},
			batchSize: 1,
		}

		client.batcher = batcher.New(1, 10*time.Millisecond, func(b []*batchItem) {
			close(entered)
			<-release

			for _, it := range b {
				it.complete(validateBatchResponse{err: errors.NewProcessingError("late")}) // writes item.result
			}
		}, true)

		ctx, cancel := context.WithCancel(context.Background())

		out := make(chan struct{}, 1)

		go func() {
			_, _ = client.ValidateWithOptions(ctx, bt.NewTx(), 1, NewDefaultOptions())
			out <- struct{}{}
		}()

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
			t.Fatalf("iteration %d: ValidateWithOptions did not return", i)
		}

		client.batcher.Close()
		cancel()
	}
}

// TestValidateWithOptions_AbandonsOnBackstopTimeout pins the finite backstop
// itself. Without it, reverting the timeout argument to 0 would still pass every
// other test here: the context is deliberately NON-cancellable, so only the
// timer arm can release the caller.
func TestValidateWithOptions_AbandonsOnBackstopTimeout(t *testing.T) {
	old := batchHandoffTimeout
	batchHandoffTimeout = 50 * time.Millisecond

	t.Cleanup(func() { batchHandoffTimeout = old })

	h := newWaitBoundHarness(t)

	out := h.validateAsync(context.Background())
	h.requireEntered(t)

	o := requireValidateOutcome(t, out)
	require.Error(t, o.err)
	require.Nil(t, o.data)
	require.Contains(t, o.err.Error(), "abandoned")
	require.True(t, errors.Is(o.err, errors.ErrServiceError), "the backstop must abandon with a service error")
	require.False(t, errors.Is(errors.UnwrapGRPC(o.err), errors.ErrThresholdExceeded),
		"a backstop abandonment must not be mistakable for a queue-full shed")

	// The timer arm must abandon, not corrupt: the later completion is still
	// permitted, and a double Group.Done would panic.
	h.release()
	h.requireDispatchFinished(t)

	require.GreaterOrEqual(t, h.completed.Load(), int32(1),
		"the dispatcher must be able to finish the item the backstop abandoned")
}

// TestValidateWithOptions_ReturnsResultWhenDispatcherCompletes pins that the
// early return did not break the success path: once Wait returns nil the result
// slot is read and decoded as before.
func TestValidateWithOptions_ReturnsResultWhenDispatcherCompletes(t *testing.T) {
	h := newWaitBoundHarness(t)
	h.release()

	data, err := h.client.ValidateWithOptions(context.Background(), bt.NewTx(), 1, NewDefaultOptions())
	require.NoError(t, err)
	require.Equal(t, uint64(waitBoundTestFee), data.Fee)

	// Synchronise on the dispatcher before reading its counter: the caller is
	// released by it.complete(), which runs BEFORE the increment.
	h.requireDispatchFinished(t)
	require.Equal(t, int32(1), h.completed.Load())
}
