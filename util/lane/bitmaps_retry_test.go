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
func failingWrite(n int, err error) (func(context.Context) error, *atomic.Int32) {
	calls := &atomic.Int32{}

	return func(context.Context) error {
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
		{"no deletion scheduler", -1, errors.NewStorageError("failed to get file name", errors.NewConfigurationError("scheduler not configured")), true, 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := "test-do-" + tc.name
			write, calls := failingWrite(tc.failures, tc.err)
			retriedBefore, exhaustedBefore := retryCounts(service)

			// a bound of seconds where the write must succeed, so a stalled runner cannot end it
			bound := 5 * time.Second
			if tc.wantExhausted > 0 {
				bound = 200 * time.Millisecond
			}

			start := time.Now()
			err := fastRetry(bound).Do(t.Context(), service, write)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.wantCalls > 0 {
				require.Equal(t, tc.wantCalls, calls.Load())
			} else {
				require.Greater(t, calls.Load(), int32(1), "it retried until the bound")
				require.GreaterOrEqual(t, time.Since(start), bound, "it gave up only once the bound ran out")
			}

			retried, exhausted := retryCounts(service)
			require.Equal(t, retriedBefore+tc.wantRetried, retried)
			require.Equal(t, exhaustedBefore+tc.wantExhausted, exhausted)
		})
	}
}

// No attempt starts once the bound has run out: a backoff clamped to the time left ends the retry
// instead of starting a last attempt at the bound.
func TestBitmapsWriteRetryStartsNoAttemptAfterTheBound(t *testing.T) {
	const service = "test-no-attempt-after-bound"

	_, exhaustedBefore := retryCounts(service)

	write, calls := failingWrite(-1, errors.NewStorageError("store down"))

	// attempts at 0 and 600ms; the next backoff would end past the 1s bound, so it is the last
	r := &BitmapsWriteRetry{Bound: time.Second, FirstBackoff: 600 * time.Millisecond, MaxBackoff: 600 * time.Millisecond}
	require.Error(t, r.Do(t.Context(), service, write))
	require.Equal(t, int32(2), calls.Load())

	_, exhausted := retryCounts(service)
	require.Equal(t, exhaustedBefore+1, exhausted)
}

// The first attempt gets the caller's context, and each retry one that ends at the bound.
func TestBitmapsWriteRetryGivesEachRetryTheBoundAsItsDeadline(t *testing.T) {
	var deadlines []time.Time

	write := func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			deadline = time.Time{}
		}

		deadlines = append(deadlines, deadline)

		if len(deadlines) < 3 {
			return errors.NewStorageError("store down")
		}

		return nil
	}

	start := time.Now()
	require.NoError(t, fastRetry(10*time.Second).Do(t.Context(), "test-attempt-deadline", write))

	require.Len(t, deadlines, 3)
	require.True(t, deadlines[0].IsZero(), "the first attempt has the caller's context")
	require.WithinRange(t, deadlines[1], start.Add(10*time.Second), time.Now().Add(10*time.Second))
	require.Equal(t, deadlines[1], deadlines[2], "every retry ends at the same bound")
}

// A success that started before a failure does not end that failure's outage, however late it is
// recorded; one that started after it does.
func TestBitmapsWriteRetryAnOlderSuccessKeepsANewerOutage(t *testing.T) {
	r := fastRetry(time.Minute)

	now := time.Now()
	began := r.failed(now)

	r.succeeded(now.Add(-time.Second)) // started before the failure
	require.Equal(t, began, r.failed(now.Add(time.Second)), "the outage goes on")

	r.succeeded(now.Add(2 * time.Second))
	require.Equal(t, now.Add(3*time.Second), r.failed(now.Add(3*time.Second)), "a newer success ends it")

	// an older failure noted after a newer one does not move the last failure back
	r.failed(now.Add(5 * time.Second))
	r.failed(now.Add(4 * time.Second))
	r.succeeded(now.Add(4500 * time.Millisecond)) // started between the two failures
	require.Equal(t, now.Add(3*time.Second), r.failed(now.Add(6*time.Second)), "the outage goes on")
}

// A write's retries end at most a bound after its own first failure, even when that failure is
// noted after a newer outage has begun.
func TestBitmapsWriteRetryDeadlineIsNeverPastAWritesOwnBound(t *testing.T) {
	r := fastRetry(time.Minute)

	now := time.Now()
	require.Equal(t, now.Add(time.Minute), r.deadline(now))

	later := now.Add(5 * time.Minute) // a newer outage, noted first
	require.Equal(t, later.Add(time.Minute), r.deadline(later))

	require.Equal(t, now.Add(time.Minute), r.deadline(now), "the older failure keeps its own bound")
}

// Through Do: a write whose first failure joins an outage noted as beginning later still gives up
// a bound after its own first failure.
func TestBitmapsWriteRetryDoClampsItsDeadline(t *testing.T) {
	const service = "test-do-clamp"

	r := fastRetry(200 * time.Millisecond)
	r.failed(time.Now().Add(time.Hour)) // a newer outage, noted first

	_, exhaustedBefore := retryCounts(service)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	write, _ := failingWrite(-1, errors.NewStorageError("store down"))

	start := time.Now()
	require.Error(t, r.Do(ctx, service, write))
	require.Less(t, time.Since(start), 10*time.Second, "it gave up at its own bound")
	require.NoError(t, ctx.Err())

	_, exhausted := retryCounts(service)
	require.Equal(t, exhaustedBefore+1, exhausted)
}

// Through Do: a write in flight while another write fails past the bound, succeeding only then,
// does not give the next failing write a fresh bound.
func TestBitmapsWriteRetryASuccessInFlightDuringAnOutageDoesNotEndIt(t *testing.T) {
	const service = "test-success-in-flight"

	r := fastRetry(200 * time.Millisecond)
	storeDown := errors.NewStorageError("store down")

	release := make(chan struct{})
	inFlight := make(chan struct{})
	done := make(chan error)

	go func() {
		done <- r.Do(t.Context(), service, func(context.Context) error {
			close(inFlight)
			<-release

			return nil
		})
	}()

	<-inFlight

	write, _ := failingWrite(-1, storeDown)
	require.Error(t, r.Do(t.Context(), service, write), "a write fails past the bound while the other is in flight")

	close(release)
	require.NoError(t, <-done)

	write, calls := failingWrite(-1, storeDown)
	require.Error(t, r.Do(t.Context(), service, write))
	require.Equal(t, int32(1), calls.Load(), "the outage went on, so the next failing write is not retried")
}

// With the defaults, a write is retried for a minute, with waits doubling from 250ms to a 4s cap.
func TestBitmapsWriteRetryDefaults(t *testing.T) {
	var r BitmapsWriteRetry

	require.Equal(t, time.Minute, r.bound())

	waits := []time.Duration{r.firstBackoff()}
	for len(waits) < 7 {
		waits = append(waits, r.nextBackoff(waits[len(waits)-1]))
	}

	require.Equal(t, []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second, 4 * time.Second}, waits)
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
	r := fastRetry(500 * time.Millisecond)
	_, exhaustedBefore := retryCounts(service)

	write, _ := failingWrite(-1, storeDown)
	require.Error(t, r.Do(t.Context(), service, write), "the first write uses up the bound")

	write, calls := failingWrite(-1, storeDown)
	require.Error(t, r.Do(t.Context(), service, write))
	require.Equal(t, int32(1), calls.Load(), "a write in the same outage is not retried")

	_, exhausted := retryCounts(service)
	require.Equal(t, exhaustedBefore+2, exhausted)

	// a write succeeds: the outage is over, and the next failure is retried again
	require.NoError(t, r.Do(t.Context(), service, func(context.Context) error { return nil }))

	write, calls = failingWrite(2, storeDown)
	require.NoError(t, r.Do(t.Context(), service, write))
	require.Equal(t, int32(3), calls.Load())

	// a failure a whole bound after the last one starts a new outage
	write, _ = failingWrite(-1, storeDown)
	require.Error(t, r.Do(t.Context(), service, write))
	time.Sleep(600 * time.Millisecond)

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
		{"configuration wrapped by the file store", live, errors.NewStorageError("failed to get file name", errors.NewConfigurationError("no scheduler")), false},
		{"invalid argument", live, errors.NewInvalidArgumentError("bad key"), false},
		{"invalid argument wrapped by the file store", live, errors.NewStorageError("failed to get file name", errors.NewInvalidArgumentError("bad key")), false},
		{"processing", live, errors.NewProcessingError("encode"), false},
		{"unknown", live, errors.NewTxInvalidError("not a store error"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RetryableBitmapsWriteError(tc.ctx, tc.err))
		})
	}
}
