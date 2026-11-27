package lane

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryCounts reads the two retry counters of service.
func retryCounts(service string) (retried, exhausted float64) {
	initRowsMetrics()

	return testutil.ToFloat64(prometheusLaneBitmapsRetried.WithLabelValues(service)),
		testutil.ToFloat64(prometheusLaneBitmapsExhausted.WithLabelValues(service))
}

// fastRetry is a retry whose bound and backoffs suit a test.
func fastRetry(bound time.Duration) *BitmapsWriteRetry {
	return &BitmapsWriteRetry{Bound: bound, FirstBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}
}

// failingWrite fails with err for its first n calls, then succeeds; n < 0 fails always.
func failingWrite(n int, err error) (func() error, *atomic.Int32) {
	calls := &atomic.Int32{}

	return func() error {
		if c := calls.Add(1); n < 0 || int(c) <= n {
			return err
		}

		return nil
	}, calls
}

// A write that fails while the store is unavailable is written once it is back; one that fails
// past the bound gives up and is counted; one that cannot pass on a retry is tried once.
func TestBitmapsWriteRetryDo(t *testing.T) {
	storeDown := errors.NewStorageError("failed to schedule blob deletion", errors.NewStorageError("connection refused"))

	for _, tc := range []struct {
		name          string
		failures      int
		err           error
		wantErr       bool
		wantCalls     int32
		wantRetried   float64
		wantExhausted float64
	}{
		{"written first time", 0, storeDown, false, 1, 0, 0},
		{"fails three times then is written", 3, storeDown, false, 4, 1, 0},
		{"fails past the bound", -1, storeDown, true, -1, 1, 1},
		{"cannot be encoded", -1, errors.NewProcessingError("encode"), true, 1, 0, 0},
		{"a file already there", -1, errors.ErrBlobAlreadyExists, true, 1, 0, 0},
		{"no deletion scheduler", -1, errors.NewConfigurationError("scheduler not configured"), true, 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := "test-do-" + tc.name
			write, calls := failingWrite(tc.failures, tc.err)

			start := time.Now()
			err := fastRetry(50*time.Millisecond).Do(t.Context(), service, write)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.wantCalls > 0 {
				require.Equal(t, tc.wantCalls, calls.Load())
			} else {
				require.Greater(t, calls.Load(), int32(2), "it retried until the bound")
				require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond, "it gave up only once the bound ran out")
			}

			retried, exhausted := retryCounts(service)
			require.Equal(t, tc.wantRetried, retried)
			require.Equal(t, tc.wantExhausted, exhausted)
		})
	}
}

// A context that ends during the backoff stops the retry at once, and a write that fails with the
// context already ended is not retried; neither counts as exhausted.
func TestBitmapsWriteRetryStopsWithItsContext(t *testing.T) {
	storeDown := errors.NewStorageError("store down")

	t.Run("ended during the backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		write, calls := failingWrite(-1, storeDown)

		time.AfterFunc(50*time.Millisecond, cancel)

		start := time.Now()
		err := (&BitmapsWriteRetry{Bound: time.Minute, FirstBackoff: 30 * time.Second}).Do(ctx, "test-ctx-backoff", write)
		require.True(t, errors.Is(err, errors.ErrContextCanceled), "%v", err)
		require.Less(t, time.Since(start), 5*time.Second, "it did not wait out the backoff")
		require.Equal(t, int32(1), calls.Load())

		_, exhausted := retryCounts("test-ctx-backoff")
		require.Zero(t, exhausted)
	})

	t.Run("ended before the write", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		write, calls := failingWrite(-1, storeDown)

		require.Error(t, fastRetry(time.Minute).Do(ctx, "test-ctx-before", write))
		require.Equal(t, int32(1), calls.Load())

		retried, exhausted := retryCounts("test-ctx-before")
		require.Zero(t, retried)
		require.Zero(t, exhausted)
	})
}

// The bound is per outage: once one write has used it up, the next failing write is not retried
// and is counted at once, until a write succeeds or no write has failed for a whole bound.
func TestBitmapsWriteRetryBoundIsPerOutage(t *testing.T) {
	const service = "test-per-outage"

	storeDown := errors.NewStorageError("store down")
	r := fastRetry(40 * time.Millisecond)

	write, _ := failingWrite(-1, storeDown)
	require.Error(t, r.Do(t.Context(), service, write), "the first write uses up the bound")

	start := time.Now()
	write, calls := failingWrite(-1, storeDown)
	require.Error(t, r.Do(t.Context(), service, write))
	require.Equal(t, int32(1), calls.Load(), "a write in the same outage is not retried")
	require.Less(t, time.Since(start), 40*time.Millisecond)

	_, exhausted := retryCounts(service)
	require.Equal(t, float64(2), exhausted)

	// a write succeeds: the outage is over, and the next failure is retried again
	require.NoError(t, r.Do(t.Context(), service, func() error { return nil }))

	write, calls = failingWrite(2, storeDown)
	require.NoError(t, r.Do(t.Context(), service, write))
	require.Equal(t, int32(3), calls.Load())

	// a failure a whole bound after the last one starts a new outage
	write, _ = failingWrite(-1, storeDown)
	require.Error(t, r.Do(t.Context(), service, write))
	time.Sleep(50 * time.Millisecond)

	write, calls = failingWrite(1, storeDown)
	require.NoError(t, r.Do(t.Context(), service, write))
	require.Equal(t, int32(2), calls.Load())
}

// RetryableBitmapsWriteError retries what can pass on a retry, including the shapes a database or
// blockchain service outage reaches the writer in through the file store and gRPC.
func TestRetryableBitmapsWriteError(t *testing.T) {
	live := t.Context()

	ended, cancel := context.WithCancel(t.Context())
	cancel()

	// the blockchain store's error, sent over gRPC and decoded by the client, wrapped by the file store
	overGRPC := errors.NewStorageError("failed to schedule blob deletion",
		errors.UnwrapGRPC(errors.WrapGRPC(errors.NewStorageError("failed to schedule deletion", errors.NewStorageError("failed to schedule blob deletion")))))

	// the blockchain service unreachable: a status with no details decodes to a generic error
	unreachable := errors.NewStorageError("failed to schedule blob deletion", errors.UnwrapGRPC(status.Error(codes.Unavailable, "connection refused")))

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"nil", live, nil, false},
		{"storage error", live, errors.NewStorageError("disk"), true},
		{"over gRPC", live, overGRPC, true},
		{"blockchain service unreachable", live, unreachable, true},
		{"service unavailable", live, errors.NewServiceUnavailableError("down"), true},
		{"network timeout", live, errors.NewNetworkTimeoutError("slow"), true},
		{"an attempt's deadline", live, errors.NewStorageError("scheduler", context.DeadlineExceeded), true},
		{"a context ended", ended, errors.NewStorageError("disk"), false},
		{"cancelled", live, errors.NewStorageError("key lock", context.Canceled), false},
		{"a file already there", live, errors.ErrBlobAlreadyExists, false},
		{"configuration", live, errors.NewConfigurationError("no scheduler"), false},
		{"invalid argument", live, errors.NewInvalidArgumentError("bad key"), false},
		{"processing", live, errors.NewProcessingError("encode"), false},
		{"unknown", live, errors.NewTxInvalidError("not a store error"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RetryableBitmapsWriteError(tc.ctx, tc.err))
		})
	}
}
