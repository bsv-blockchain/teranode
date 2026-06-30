package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWaitForLockedResult_BoundedWaitWhenBatcherWedged verifies the #1172 leak
// guard still holds on the SetLocked path after the per-op timer was moved to the
// pooled acquireBatchTimer/releaseBatchTimer helpers: when the locked batcher
// never signals and the caller's context has no deadline (as in legacy sync /
// validation), waitForLockedResult returns after batcherWait instead of parking
// forever. This exercises the pooled timer end-to-end (acquire -> timer.C ->
// release) on a callsite that otherwise had no wedge-to-release test.
func TestWaitForLockedResult_BoundedWaitWhenBatcherWedged(t *testing.T) {
	s := &Store{batcherWait: 150 * time.Millisecond}

	// errCh is never signalled (the batcher is wedged) and ctx has no deadline, so
	// only the pooled timer can release the caller.
	errCh := make(chan error, 1)

	start := time.Now()

	err := s.waitForLockedResult(context.Background(), errCh)

	require.Error(t, err)
	require.Contains(t, err.Error(), "did not complete within")
	require.Less(t, time.Since(start), time.Second, "waitForLockedResult should return at ~batcherWait, not block")
}

// TestWaitForLockedResult_ReturnsResultBeforeTimeout verifies the normal path:
// when the batcher signals, the pooled timer is stopped/released and the caller
// gets the result rather than a spurious timeout.
func TestWaitForLockedResult_ReturnsResultBeforeTimeout(t *testing.T) {
	s := &Store{batcherWait: time.Hour}

	errCh := make(chan error, 1)
	errCh <- nil // batcher signals success immediately

	err := s.waitForLockedResult(context.Background(), errCh)
	require.NoError(t, err)
}
