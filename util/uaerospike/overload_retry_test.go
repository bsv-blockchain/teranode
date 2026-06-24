package uaerospike

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/require"
)

func newOverloadTestClient(maxElapsed, baseBackoff, maxBackoff time.Duration) *Client {
	return &Client{
		stats: NewClientStats(),
		overloadRetry: overloadRetryConfig{
			maxElapsed:  maxElapsed,
			baseBackoff: baseBackoff,
			maxBackoff:  maxBackoff,
		},
	}
}

func overloadErr(code types.ResultCode) aerospike.Error {
	return &aerospike.AerospikeError{ResultCode: code}
}

func newTestBatchRecords(t *testing.T, n int) []aerospike.BatchRecordIfc {
	t.Helper()

	records := make([]aerospike.BatchRecordIfc, n)

	for i := 0; i < n; i++ {
		key, err := aerospike.NewKey("test", "test", i)
		require.NoError(t, err)

		records[i] = aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin("bin", i)))
	}

	return records
}

func TestIsOverloadError(t *testing.T) {
	t.Run("nil error is not overload", func(t *testing.T) {
		require.False(t, isOverloadError(nil))
	})

	t.Run("DEVICE_OVERLOAD is overload", func(t *testing.T) {
		require.True(t, isOverloadError(overloadErr(types.DEVICE_OVERLOAD)))
	})

	t.Run("MAX_ERROR_RATE is overload", func(t *testing.T) {
		require.True(t, isOverloadError(overloadErr(types.MAX_ERROR_RATE)))
	})

	t.Run("other codes are not overload", func(t *testing.T) {
		require.False(t, isOverloadError(overloadErr(types.KEY_NOT_FOUND_ERROR)))
		require.False(t, isOverloadError(overloadErr(types.TIMEOUT)))
		require.False(t, isOverloadError(overloadErr(types.KEY_BUSY)))
	})
}

func TestOverloadRetryDefaults(t *testing.T) {
	t.Run("defaults applied when no option given", func(t *testing.T) {
		cfg := newClientConfig(nil)
		require.Equal(t, defaultOverloadRetryMaxElapsed, cfg.overloadRetry.maxElapsed)
		require.Equal(t, defaultOverloadRetryBaseBackoff, cfg.overloadRetry.baseBackoff)
		require.Equal(t, defaultOverloadRetryMaxBackoff, cfg.overloadRetry.maxBackoff)
	})

	t.Run("WithOverloadRetry overrides defaults", func(t *testing.T) {
		cfg := newClientConfig([]ClientOption{WithOverloadRetry(time.Second, time.Millisecond, 10*time.Millisecond)})
		require.Equal(t, time.Second, cfg.overloadRetry.maxElapsed)
		require.Equal(t, time.Millisecond, cfg.overloadRetry.baseBackoff)
		require.Equal(t, 10*time.Millisecond, cfg.overloadRetry.maxBackoff)
	})
}

func TestJitteredBackoff(t *testing.T) {
	t.Run("non-positive durations are returned unchanged", func(t *testing.T) {
		require.Equal(t, time.Duration(0), jitteredBackoff(0))
		require.Equal(t, -time.Second, jitteredBackoff(-time.Second))
	})

	t.Run("stays within the jitter band and actually varies", func(t *testing.T) {
		const base = time.Second

		minWait := time.Duration(float64(base) * (1 - overloadRetryJitter))
		maxWait := time.Duration(float64(base) * (1 + overloadRetryJitter))

		seen := make(map[time.Duration]struct{})

		for i := 0; i < 1000; i++ {
			got := jitteredBackoff(base)
			require.GreaterOrEqual(t, got, minWait)
			require.LessOrEqual(t, got, maxWait)
			seen[got] = struct{}{}
		}

		require.Greater(t, len(seen), 1, "jitter must decorrelate retries, not return a constant")
	})
}

func TestRetryOnOverload(t *testing.T) {
	t.Run("success first try calls once", func(t *testing.T) {
		c := newOverloadTestClient(50*time.Millisecond, time.Millisecond, 4*time.Millisecond)

		calls := 0
		err := c.retryOnOverload(func() aerospike.Error {
			calls++
			return nil
		})

		require.Nil(t, err)
		require.Equal(t, 1, calls)
	})

	t.Run("overload twice then success", func(t *testing.T) {
		c := newOverloadTestClient(time.Second, time.Millisecond, 4*time.Millisecond)

		calls := 0
		err := c.retryOnOverload(func() aerospike.Error {
			calls++
			if calls <= 2 {
				return overloadErr(types.DEVICE_OVERLOAD)
			}
			return nil
		})

		require.Nil(t, err)
		require.Equal(t, 3, calls)
	})

	t.Run("non-overload error returned immediately", func(t *testing.T) {
		c := newOverloadTestClient(time.Second, time.Millisecond, 4*time.Millisecond)

		calls := 0
		err := c.retryOnOverload(func() aerospike.Error {
			calls++
			return overloadErr(types.KEY_NOT_FOUND_ERROR)
		})

		require.NotNil(t, err)
		require.True(t, err.Matches(types.KEY_NOT_FOUND_ERROR))
		require.Equal(t, 1, calls)
	})

	t.Run("overload turning into non-overload error stops retrying", func(t *testing.T) {
		c := newOverloadTestClient(time.Second, time.Millisecond, 4*time.Millisecond)

		calls := 0
		err := c.retryOnOverload(func() aerospike.Error {
			calls++
			if calls == 1 {
				return overloadErr(types.MAX_ERROR_RATE)
			}
			return overloadErr(types.PARAMETER_ERROR)
		})

		require.NotNil(t, err)
		require.True(t, err.Matches(types.PARAMETER_ERROR))
		require.Equal(t, 2, calls)
	})

	t.Run("permanent overload returns overload error after budget", func(t *testing.T) {
		c := newOverloadTestClient(20*time.Millisecond, time.Millisecond, 4*time.Millisecond)

		calls := 0
		start := time.Now()
		err := c.retryOnOverload(func() aerospike.Error {
			calls++
			return overloadErr(types.DEVICE_OVERLOAD)
		})
		elapsed := time.Since(start)

		require.NotNil(t, err)
		require.True(t, err.Matches(types.DEVICE_OVERLOAD))
		require.Greater(t, calls, 1, "should have retried at least once")
		require.GreaterOrEqual(t, elapsed, 20*time.Millisecond, "should have kept retrying until the budget elapsed")
	})

	t.Run("sleep is capped to the remaining budget", func(t *testing.T) {
		// baseBackoff far exceeds maxElapsed: without capping, the first sleep
		// alone overshoots the budget by the full backoff. The sleep must be
		// clamped to the time remaining so maxElapsed is a real ceiling on the
		// wall time spent retrying.
		c := newOverloadTestClient(20*time.Millisecond, 500*time.Millisecond, 500*time.Millisecond)

		calls := 0
		start := time.Now()
		err := c.retryOnOverload(func() aerospike.Error {
			calls++
			return overloadErr(types.DEVICE_OVERLOAD)
		})
		elapsed := time.Since(start)

		require.NotNil(t, err)
		require.True(t, err.Matches(types.DEVICE_OVERLOAD))
		require.Greater(t, calls, 1, "should have retried at least once")
		require.Less(t, elapsed, 250*time.Millisecond, "sleep must be capped to the remaining budget, not the full backoff")
	})

	t.Run("permit is released during the backoff sleep", func(t *testing.T) {
		// With a real capacity-1 semaphore, the retry loop must not hold the
		// permit across its backoff sleep — otherwise sustained overload starves
		// every other caller into ErrTimeout. do() always reports overload so
		// the loop runs for the whole budget; a second caller must still be able
		// to take the permit while the retrier sleeps.
		c := newOverloadTestClient(600*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond)
		c.connSemaphore = make(chan struct{}, 1)

		firstCall := make(chan struct{}, 1)
		done := make(chan struct{})

		go func() {
			defer close(done)

			// Mirror the wrapper methods: hold a permit for the whole call.
			_ = c.acquirePermit(nil)
			defer c.releasePermit()

			_ = c.retryOnOverload(func() aerospike.Error {
				select {
				case firstCall <- struct{}{}:
				default:
				}
				return overloadErr(types.DEVICE_OVERLOAD)
			})
		}()

		<-firstCall // first attempt done; the loop is now backing off

		select {
		case c.connSemaphore <- struct{}{}:
			<-c.connSemaphore // hand it back so the retrier can re-acquire
		case <-time.After(300 * time.Millisecond):
			t.Fatal("permit was not released during the backoff sleep (held across retries)")
		}

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("retry did not complete")
		}
	})

	t.Run("maxElapsed zero disables retry", func(t *testing.T) {
		c := newOverloadTestClient(0, time.Millisecond, 4*time.Millisecond)

		calls := 0
		err := c.retryOnOverload(func() aerospike.Error {
			calls++
			return overloadErr(types.DEVICE_OVERLOAD)
		})

		require.NotNil(t, err)
		require.True(t, err.Matches(types.DEVICE_OVERLOAD))
		require.Equal(t, 1, calls)
	})
}

func TestRetryBatchOnOverload(t *testing.T) {
	setResult := func(rec aerospike.BatchRecordIfc, code types.ResultCode) {
		br := rec.BatchRec()
		br.ResultCode = code

		if code == types.OK {
			br.Err = nil
		} else {
			br.Err = overloadErr(code)
		}
	}

	t.Run("all OK first try calls once", func(t *testing.T) {
		c := newOverloadTestClient(50*time.Millisecond, time.Millisecond, 4*time.Millisecond)
		records := newTestBatchRecords(t, 3)

		calls := 0
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			calls++
			for _, rec := range recs {
				setResult(rec, types.OK)
			}
			return nil
		})

		require.Nil(t, err)
		require.Equal(t, 1, calls)
	})

	t.Run("overloaded subset retried until success", func(t *testing.T) {
		c := newOverloadTestClient(time.Second, time.Millisecond, 4*time.Millisecond)
		records := newTestBatchRecords(t, 5)

		var callSizes []int
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			callSizes = append(callSizes, len(recs))

			for i, rec := range recs {
				if len(callSizes) == 1 && (i == 0 || i == 2) {
					setResult(rec, types.DEVICE_OVERLOAD)
				} else {
					setResult(rec, types.OK)
				}
			}
			return nil
		})

		require.Nil(t, err)
		require.Equal(t, []int{5, 2}, callSizes, "second call must resubmit exactly the overloaded records")

		for _, rec := range records {
			require.Equal(t, types.OK, rec.BatchRec().ResultCode)
		}
	})

	t.Run("non-overload per-record errors are preserved and not resubmitted", func(t *testing.T) {
		c := newOverloadTestClient(time.Second, time.Millisecond, 4*time.Millisecond)
		records := newTestBatchRecords(t, 3)

		var calls [][]aerospike.BatchRecordIfc
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			calls = append(calls, recs)

			for _, rec := range recs {
				if len(calls) == 1 {
					switch rec {
					case records[0]:
						setResult(rec, types.MAX_ERROR_RATE)
					case records[1]:
						setResult(rec, types.KEY_NOT_FOUND_ERROR)
					default:
						setResult(rec, types.OK)
					}
				} else {
					setResult(rec, types.OK)
				}
			}
			return nil
		})

		require.Nil(t, err)
		require.Len(t, calls, 2)
		require.Len(t, calls[1], 1)
		require.Same(t, records[0], calls[1][0])

		require.Equal(t, types.OK, records[0].BatchRec().ResultCode)
		require.Equal(t, types.KEY_NOT_FOUND_ERROR, records[1].BatchRec().ResultCode)
		require.NotNil(t, records[1].BatchRec().Err)
		require.Equal(t, types.OK, records[2].BatchRec().ResultCode)
	})

	t.Run("non-overload top-level error returned unchanged without retry", func(t *testing.T) {
		c := newOverloadTestClient(time.Second, time.Millisecond, 4*time.Millisecond)
		records := newTestBatchRecords(t, 2)

		calls := 0
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			calls++
			return overloadErr(types.NETWORK_ERROR)
		})

		require.NotNil(t, err)
		require.True(t, err.Matches(types.NETWORK_ERROR))
		require.Equal(t, 1, calls)
	})

	t.Run("top-level overload error retries unfinished records", func(t *testing.T) {
		c := newOverloadTestClient(time.Second, time.Millisecond, 4*time.Millisecond)
		records := newTestBatchRecords(t, 4)

		var callSizes []int
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			callSizes = append(callSizes, len(recs))

			if len(callSizes) == 1 {
				// Client-side rejection (e.g. MAX_ERROR_RATE breaker): records keep
				// their initial NO_RESPONSE result code.
				return overloadErr(types.MAX_ERROR_RATE)
			}

			for _, rec := range recs {
				setResult(rec, types.OK)
			}
			return nil
		})

		require.Nil(t, err)
		require.Equal(t, []int{4, 4}, callSizes, "all unfinished records must be resubmitted")
	})

	t.Run("exhaustion returns overload error", func(t *testing.T) {
		c := newOverloadTestClient(20*time.Millisecond, time.Millisecond, 4*time.Millisecond)
		records := newTestBatchRecords(t, 2)

		calls := 0
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			calls++
			for _, rec := range recs {
				setResult(rec, types.DEVICE_OVERLOAD)
			}
			return nil
		})

		require.NotNil(t, err)
		require.True(t, err.Matches(types.DEVICE_OVERLOAD))
		require.Greater(t, calls, 1, "should have retried at least once")
	})

	t.Run("sleep is capped to the remaining budget", func(t *testing.T) {
		// baseBackoff far exceeds maxElapsed: the per-attempt sleep must be
		// clamped to the remaining budget so maxElapsed bounds the batch retry
		// wall time too.
		c := newOverloadTestClient(20*time.Millisecond, 500*time.Millisecond, 500*time.Millisecond)
		records := newTestBatchRecords(t, 2)

		calls := 0
		start := time.Now()
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			calls++
			for _, rec := range recs {
				setResult(rec, types.DEVICE_OVERLOAD)
			}
			return nil
		})
		elapsed := time.Since(start)

		require.NotNil(t, err)
		require.True(t, err.Matches(types.DEVICE_OVERLOAD))
		require.Greater(t, calls, 1, "should have retried at least once")
		require.Less(t, elapsed, 250*time.Millisecond, "sleep must be capped to the remaining budget, not the full backoff")
	})

	t.Run("mixed batch preserves BATCH_FAILED when a non-overload record stays failed", func(t *testing.T) {
		// First call reports a top-level BATCH_FAILED carrying one overloaded
		// record and one genuinely-failed non-overload record. The overloaded
		// subset retries to success, but the non-overload failure remains — the
		// original BATCH_FAILED must NOT be swallowed, or top-level-only callers
		// would read it as success.
		c := newOverloadTestClient(time.Second, time.Millisecond, 4*time.Millisecond)
		records := newTestBatchRecords(t, 3)

		calls := 0
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			calls++

			if calls == 1 {
				setResult(records[0], types.DEVICE_OVERLOAD)
				setResult(records[1], types.FILTERED_OUT)
				setResult(records[2], types.OK)
				return overloadErr(types.BATCH_FAILED)
			}

			// Retry of the overloaded subset succeeds.
			for _, rec := range recs {
				setResult(rec, types.OK)
			}
			return nil
		})

		require.NotNil(t, err, "must not swallow BATCH_FAILED while a non-overload record is still failed")
		require.True(t, err.Matches(types.BATCH_FAILED))
		require.Equal(t, 2, calls)
		require.Equal(t, types.OK, records[0].BatchRec().ResultCode, "overloaded record retried to success")
		require.Equal(t, types.FILTERED_OUT, records[1].BatchRec().ResultCode, "non-overload failure preserved on the record")
	})

	t.Run("maxElapsed zero disables batch retry", func(t *testing.T) {
		c := newOverloadTestClient(0, time.Millisecond, 4*time.Millisecond)
		records := newTestBatchRecords(t, 2)

		calls := 0
		err := c.retryBatchOnOverload(records, func(recs []aerospike.BatchRecordIfc) aerospike.Error {
			calls++
			for _, rec := range recs {
				setResult(rec, types.DEVICE_OVERLOAD)
			}
			return nil
		})

		require.Nil(t, err, "disabled retry must return the original outcome unchanged")
		require.Equal(t, 1, calls)
	})
}
