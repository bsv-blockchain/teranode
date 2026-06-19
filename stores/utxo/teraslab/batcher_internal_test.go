package teraslab

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMergeBatchContextsCancelsWhenAllItemsCancel verifies that the merged
// context fires only after every item's context has been cancelled, so a
// single canceled caller cannot prematurely abort a batch RPC.
func TestMergeBatchContextsCancelsWhenAllItemsCancel(t *testing.T) {
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())

	merged, release := mergeBatchContexts([]context.Context{ctxA, ctxB})
	defer release()

	// Cancelling only one parent must NOT cancel the merged ctx.
	cancelA()
	select {
	case <-merged.Done():
		t.Fatal("merged ctx canceled after only one parent canceled")
	case <-time.After(50 * time.Millisecond):
	}

	// Cancelling the second parent must cancel the merged ctx.
	cancelB()
	select {
	case <-merged.Done():
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("merged ctx did not cancel after all parents canceled")
	}

	require.Error(t, merged.Err())
}

// TestMergeBatchContextsReleaseStopsWatchers verifies that calling the release
// func returned by mergeBatchContexts cancels the merged ctx and unblocks any
// watcher goroutines, even if no parent context ever cancels.
func TestMergeBatchContextsReleaseStopsWatchers(t *testing.T) {
	parent := context.Background()
	merged, release := mergeBatchContexts([]context.Context{parent, parent})

	release()

	select {
	case <-merged.Done():
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("merged ctx did not cancel after release()")
	}
}

// TestMergeBatchContextsEmptyBatch ensures the empty-input path still produces
// a usable, releasable context (used when a flusher is invoked with no items).
func TestMergeBatchContextsEmptyBatch(t *testing.T) {
	merged, release := mergeBatchContexts(nil)

	// Should not be canceled yet.
	select {
	case <-merged.Done():
		t.Fatal("empty merged ctx fired immediately")
	default:
	}

	release()

	select {
	case <-merged.Done():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("merged ctx did not cancel after release()")
	}
}

// TestMergeBatchContextsNilContextTreatedAsNeverCancels verifies that a nil
// per-item context is handled gracefully (waits for release rather than
// panicking). This should not happen in production but defends against future
// callers forgetting to set ctx on a batch item.
func TestMergeBatchContextsNilContextTreatedAsNeverCancels(t *testing.T) {
	merged, release := mergeBatchContexts([]context.Context{nil})

	select {
	case <-merged.Done():
		t.Fatal("merged ctx fired despite nil parent never canceling")
	case <-time.After(50 * time.Millisecond):
	}

	release()

	select {
	case <-merged.Done():
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("merged ctx did not cancel after release()")
	}

	assert.Error(t, merged.Err())
}
