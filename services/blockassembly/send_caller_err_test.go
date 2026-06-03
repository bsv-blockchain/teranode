package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestSendCallerErr_AbandonsOnCancel pins the fix for the block-assembly
// shutdown deadlock: the subtree storage worker delivers its result on the
// caller's unbuffered ErrChan, but the caller (SubtreeProcessor) abandons the
// matching receive on its own context cancellation. A bare send then blocked
// the worker forever, which hung runNewSubtreeListener's wg.Wait() and
// deadlocked shutdown. sendCallerErr must instead release on ctx cancel.
func TestSendCallerErr_AbandonsOnCancel(t *testing.T) {
	errChan := make(chan error) // unbuffered, deliberately no reader

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sendCallerErr(ctx, errChan, errors.NewProcessingError("boom"))
		close(done)
	}()

	// With no reader it must be blocked on the send.
	select {
	case <-done:
		t.Fatal("sendCallerErr returned before cancel; expected it to block on the send")
	case <-time.After(50 * time.Millisecond):
	}

	// Cancelling the context must release it — this is the fix.
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sendCallerErr blocked after ctx cancel — would hang block-assembly shutdown")
	}
}

// TestSendCallerErr_DeliversToReader pins the happy path: with a reader present,
// the result is delivered.
func TestSendCallerErr_DeliversToReader(t *testing.T) {
	errChan := make(chan error)
	want := errors.NewProcessingError("delivered")

	go sendCallerErr(context.Background(), errChan, want)

	select {
	case got := <-errChan:
		require.Equal(t, want, got)
	case <-time.After(2 * time.Second):
		t.Fatal("error was not delivered to the waiting reader")
	}
}

// TestSendCallerErr_NilChannelNoop pins that a nil ErrChan is a no-op — the
// caller may not supply one.
func TestSendCallerErr_NilChannelNoop(t *testing.T) {
	require.NotPanics(t, func() {
		sendCallerErr(context.Background(), nil, nil)
	})
}
