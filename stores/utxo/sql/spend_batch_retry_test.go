package sql

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// These pin retrySpendBatch, the lock-retry loop around one spend batch: an
// attempt that reports a lock error is run again after a backoff, up to three
// retries, every item gets exactly one answer, and a cancelled store context
// ends the wait instead of sleeping it out.

func newRetryBatch(n int) []*batchSpend {
	batch := make([]*batchSpend, n)
	for i := range batch {
		batch[i] = &batchSpend{errCh: make(chan error, 1)}
	}

	return batch
}

// lockedThen returns a fake attempt that reports a lock error for the first
// locked calls and answers every item with nil after that.
func lockedThen(locked int, calls *int) func([]*batchSpend) bool {
	return func(batch []*batchSpend) bool {
		*calls++
		if *calls <= locked {
			return true
		}

		for _, item := range batch {
			item.errCh <- nil
		}

		return false
	}
}

func requireOneAnswerEach(t *testing.T, batch []*batchSpend) []error {
	t.Helper()

	errs := make([]error, len(batch))
	for i, item := range batch {
		select {
		case errs[i] = <-item.errCh:
		default:
			t.Fatalf("item %d was not answered", i)
		}

		select {
		case err := <-item.errCh:
			t.Fatalf("item %d was answered twice, second answer %v", i, err)
		default:
		}
	}

	return errs
}

func TestRetrySpendBatch_SucceedsAfterLockErrors(t *testing.T) {
	s := &Store{ctx: context.Background(), logger: ulogger.TestLogger{}}
	batch := newRetryBatch(2)

	calls := 0
	s.retrySpendBatch(batch, lockedThen(2, &calls))

	require.Equal(t, 3, calls, "two lock errors, then the attempt that succeeds")

	for i, err := range requireOneAnswerEach(t, batch) {
		require.NoError(t, err, "item %d", i)
	}
}

func TestRetrySpendBatch_GivesUpAfterThreeRetries(t *testing.T) {
	s := &Store{ctx: context.Background(), logger: ulogger.TestLogger{}}
	batch := newRetryBatch(2)

	calls := 0
	start := time.Now()
	s.retrySpendBatch(batch, lockedThen(100, &calls))
	elapsed := time.Since(start)

	require.Equal(t, 4, calls, "the first attempt and three retries")
	// 100+200+400ms between attempts, and no sleep after the last one.
	require.GreaterOrEqual(t, elapsed, 700*time.Millisecond)
	require.Less(t, elapsed, 1400*time.Millisecond, "slept after the final attempt, which adds 800ms")

	for i, err := range requireOneAnswerEach(t, batch) {
		require.ErrorIs(t, err, errors.ErrStorageError, "item %d", i)
		require.Contains(t, err.Error(), "deadlock persisted after 3 retries", "item %d", i)
	}
}

func TestRetrySpendBatch_CancelledContextEndsTheWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &Store{ctx: ctx, logger: ulogger.TestLogger{}}
	batch := newRetryBatch(2)

	calls := 0
	start := time.Now()
	s.retrySpendBatch(batch, lockedThen(100, &calls))

	require.Equal(t, 1, calls, "no attempt after the context is cancelled")
	require.Less(t, time.Since(start), 100*time.Millisecond, "waited out the backoff")

	for i, err := range requireOneAnswerEach(t, batch) {
		require.ErrorIs(t, err, errors.ErrStorageError, "item %d", i)
		require.ErrorIs(t, err, context.Canceled, "item %d", i)
	}
}
