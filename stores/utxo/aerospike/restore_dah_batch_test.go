package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/require"
)

func restoreItems(t *testing.T, n int) []dahRestore {
	t.Helper()

	items := make([]dahRestore, n)
	for i := range items {
		key, err := aerospike.NewKey("test-ns", "test-set", []byte{byte(i), 1, 2})
		require.NoError(t, err)

		items[i] = dahRestore{
			ID:     string(rune('a' + i)),
			Key:    key,
			Bins:   expiredPreservationRestoreBins(190, nil),
			Filter: dahUnchangedExpression(nil),
		}
	}

	return items
}

// TestRestoreDAHBatch pins the shared master-restore helper: one BatchOperate per attempt for the
// whole set, only the failed records retried, per-record "someone else owns it" results skipped,
// and a backoff that honours ctx.
func TestRestoreDAHBatch(t *testing.T) {
	prev := childDAHRetryBackoff
	childDAHRetryBackoff = time.Millisecond

	t.Cleanup(func() { childDAHRetryBackoff = prev })

	t.Run("N items are one call per attempt", func(t *testing.T) {
		s := newTestStoreForGet(t)

		var sizes []int
		s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
			sizes = append(sizes, len(recs))
			return nil
		}

		require.NoError(t, s.restoreDAHBatch(context.Background(), restoreItems(t, 5)))
		require.Equal(t, []int{5}, sizes, "a healthy restore must be a single BatchOperate for all items")
	})

	t.Run("only the failed records are retried, as one batch", func(t *testing.T) {
		s := newTestStoreForGet(t)

		var sizes []int
		s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
			sizes = append(sizes, len(recs))
			if len(sizes) == 1 {
				recs[1].BatchRec().Err = aerospike.ErrNetwork
				recs[3].BatchRec().Err = aerospike.ErrNetwork
			}

			return nil
		}

		require.NoError(t, s.restoreDAHBatch(context.Background(), restoreItems(t, 5)))
		require.Equal(t, []int{5, 2}, sizes)
	})

	t.Run("FILTERED_OUT and KEY_NOT_FOUND mean another writer owns it: skipped, no retry, no error", func(t *testing.T) {
		s := newTestStoreForGet(t)

		calls := 0
		s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
			calls++
			recs[0].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.FILTERED_OUT}
			recs[1].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}

			return nil
		}

		require.NoError(t, s.restoreDAHBatch(context.Background(), restoreItems(t, 3)))
		require.Equal(t, 1, calls)
	})

	t.Run("a persistent failure is a restore failed error after one retry", func(t *testing.T) {
		s := newTestStoreForGet(t)

		calls := 0
		s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
			calls++
			return aerospike.ErrNetwork
		}

		err := s.restoreDAHBatch(context.Background(), restoreItems(t, 3))
		require.Error(t, err)
		require.Contains(t, err.Error(), "restore failed")
		require.Equal(t, 2, calls)
	})

	t.Run("a cancelled ctx stops the retry", func(t *testing.T) {
		childDAHRetryBackoff = time.Hour // would hang if the sleep ignored ctx

		defer func() { childDAHRetryBackoff = time.Millisecond }()

		s := newTestStoreForGet(t)

		ctx, cancel := context.WithCancel(context.Background())

		calls := 0
		s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
			calls++
			cancel()

			return aerospike.ErrNetwork
		}

		done := make(chan error, 1)
		go func() { done <- s.restoreDAHBatch(ctx, restoreItems(t, 2)) }()

		select {
		case err := <-done:
			require.Error(t, err)
			require.Equal(t, 1, calls, "no second attempt after cancellation")
		case <-time.After(5 * time.Second):
			t.Fatal("the backoff did not honour ctx cancellation")
		}
	})
}
