package aerospike

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeTimeoutNetErr satisfies net.Error with Timeout()=true for classifier tests.
type fakeTimeoutNetErr struct{}

func (fakeTimeoutNetErr) Error() string   { return "fake net timeout" }
func (fakeTimeoutNetErr) Timeout() bool   { return true }
func (fakeTimeoutNetErr) Temporary() bool { return false }

// fakeNonTimeoutNetErr satisfies net.Error with Timeout()=false.
type fakeNonTimeoutNetErr struct{}

func (fakeNonTimeoutNetErr) Error() string   { return "fake net non-timeout" }
func (fakeNonTimeoutNetErr) Timeout() bool   { return false }
func (fakeNonTimeoutNetErr) Temporary() bool { return false }

func aErrWithCode(code types.ResultCode) error {
	return &aerospike.AerospikeError{ResultCode: code}
}

// plainErr is a non-Aerospike, non-timeout error used to exercise the
// classifier's default branch without tripping the forbidigo lint on fmt.Errorf.
type plainErr struct{ msg string }

func (p plainErr) Error() string { return p.msg }

// wrapErr wraps an inner error and exposes it via Unwrap so the classifier's
// errors.As call can reach the underlying AerospikeError or context.DeadlineExceeded.
// Used in place of fmt.Errorf %w (forbidden by project lint policy).
type wrapErr struct {
	msg   string
	inner error
}

func (w wrapErr) Error() string { return w.msg + ": " + w.inner.Error() }
func (w wrapErr) Unwrap() error { return w.inner }

func TestNewCircuitBreaker(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		cb := newCircuitBreaker(10, 3, 30*time.Second)
		require.NotNil(t, cb)
		assert.Equal(t, cbStateClosed, cb.state)
		assert.Equal(t, 10, cb.failureThreshold)
		assert.Equal(t, 3, cb.halfOpenMax)
		assert.Equal(t, 30*time.Second, cb.cooldown)
	})

	t.Run("DisabledWhenFailureThresholdZero", func(t *testing.T) {
		cb := newCircuitBreaker(0, 3, 30*time.Second)
		assert.Nil(t, cb)
	})

	t.Run("DisabledWhenFailureThresholdNegative", func(t *testing.T) {
		cb := newCircuitBreaker(-1, 3, 30*time.Second)
		assert.Nil(t, cb)
	})

	t.Run("DefaultHalfOpenMax", func(t *testing.T) {
		cb := newCircuitBreaker(10, 0, 30*time.Second)
		require.NotNil(t, cb)
		assert.Equal(t, 1, cb.halfOpenMax, "halfOpenMax should default to 1 when <= 0")
	})

	t.Run("DefaultCooldown", func(t *testing.T) {
		cb := newCircuitBreaker(10, 3, 0)
		require.NotNil(t, cb)
		assert.Equal(t, 30*time.Second, cb.cooldown, "cooldown should default to 30s when <= 0")
	})
}

func TestCircuitBreakerNilSafety(t *testing.T) {
	var cb *circuitBreaker

	t.Run("AllowWithNilCircuitBreaker", func(t *testing.T) {
		allowed := cb.Allow()
		assert.True(t, allowed, "nil circuit breaker should always allow")
	})

	t.Run("RecordSuccessWithNilCircuitBreaker", func(t *testing.T) {
		// Should not panic
		cb.RecordSuccess()
	})

	t.Run("RecordFailureWithNilCircuitBreaker", func(t *testing.T) {
		// Should not panic
		cb.RecordFailure()
	})
}

func TestCircuitBreakerClosedState(t *testing.T) {
	t.Run("AllowInClosedState", func(t *testing.T) {
		cb := newCircuitBreaker(3, 2, 100*time.Millisecond)
		require.NotNil(t, cb)

		assert.True(t, cb.Allow(), "should allow in closed state")
		assert.True(t, cb.Allow(), "should allow in closed state")
		assert.True(t, cb.Allow(), "should allow in closed state")
	})

	t.Run("RecordSuccessResetsFailureCount", func(t *testing.T) {
		cb := newCircuitBreaker(3, 2, 100*time.Millisecond)
		require.NotNil(t, cb)

		// Record 2 failures
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, 2, cb.consecutiveFailures)

		// Success should reset counter
		cb.RecordSuccess()
		assert.Equal(t, 0, cb.consecutiveFailures)
		assert.Equal(t, cbStateClosed, cb.state)
	})

	t.Run("TripsToOpenAfterThresholdFailures", func(t *testing.T) {
		cb := newCircuitBreaker(3, 2, 100*time.Millisecond)
		require.NotNil(t, cb)

		// Record failures up to threshold
		cb.RecordFailure() // 1
		assert.Equal(t, cbStateClosed, cb.state)
		cb.RecordFailure() // 2
		assert.Equal(t, cbStateClosed, cb.state)
		cb.RecordFailure() // 3 - should trip
		assert.Equal(t, cbStateOpen, cb.state)
	})
}

func TestCircuitBreakerOpenState(t *testing.T) {
	t.Run("RejectsRequestsInOpenState", func(t *testing.T) {
		cb := newCircuitBreaker(2, 2, 1*time.Second)
		require.NotNil(t, cb)

		// Trip the circuit
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, cbStateOpen, cb.state)

		// Should reject requests
		assert.False(t, cb.Allow(), "should reject in open state")
		assert.False(t, cb.Allow(), "should reject in open state")
	})

	t.Run("TransitionsToHalfOpenAfterCooldown", func(t *testing.T) {
		cb := newCircuitBreaker(2, 2, 50*time.Millisecond)
		require.NotNil(t, cb)

		// Trip the circuit
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, cbStateOpen, cb.state)

		// Should reject before cooldown
		assert.False(t, cb.Allow())

		// Wait for cooldown
		time.Sleep(60 * time.Millisecond)

		// Next Allow() should transition to half-open
		allowed := cb.Allow()
		assert.True(t, allowed, "should allow first request after cooldown")
		assert.Equal(t, cbStateHalfOpen, cb.state)
	})

	t.Run("NextAttemptSetCorrectly", func(t *testing.T) {
		cooldown := 100 * time.Millisecond
		cb := newCircuitBreaker(2, 2, cooldown)
		require.NotNil(t, cb)

		before := time.Now()
		cb.RecordFailure()
		cb.RecordFailure()
		after := time.Now()

		assert.Equal(t, cbStateOpen, cb.state)
		assert.True(t, cb.nextAttempt.After(before.Add(cooldown)))
		assert.True(t, cb.nextAttempt.Before(after.Add(cooldown+10*time.Millisecond)))
	})
}

func TestCircuitBreakerHalfOpenState(t *testing.T) {
	t.Run("AllowsLimitedRequestsInHalfOpen", func(t *testing.T) {
		halfOpenMax := 3
		cb := newCircuitBreaker(2, halfOpenMax, 50*time.Millisecond)
		require.NotNil(t, cb)

		// Trip to open
		cb.RecordFailure()
		cb.RecordFailure()

		// Wait for cooldown to transition to half-open
		time.Sleep(60 * time.Millisecond)
		assert.True(t, cb.Allow()) // First attempt transitions to half-open

		// Should allow halfOpenMax attempts
		for i := 1; i < halfOpenMax; i++ {
			allowed := cb.Allow()
			assert.True(t, allowed, "should allow attempt %d/%d in half-open", i+1, halfOpenMax)
		}

		// Should reject after reaching limit
		assert.False(t, cb.Allow(), "should reject after reaching halfOpenMax")
	})

	t.Run("SuccessfulAttemptsCloseCircuit", func(t *testing.T) {
		halfOpenMax := 3
		cb := newCircuitBreaker(2, halfOpenMax, 50*time.Millisecond)
		require.NotNil(t, cb)

		// Trip to open, then transition to half-open
		cb.RecordFailure()
		cb.RecordFailure()
		time.Sleep(60 * time.Millisecond)
		cb.Allow() // Transition to half-open

		// Record successes equal to halfOpenMax
		for i := 0; i < halfOpenMax; i++ {
			cb.RecordSuccess()
			if i < halfOpenMax-1 {
				assert.Equal(t, cbStateHalfOpen, cb.state, "should stay half-open until all successes recorded")
			}
		}

		// After all successes, should reset to closed
		assert.Equal(t, cbStateClosed, cb.state)
		assert.Equal(t, 0, cb.consecutiveFailures)
		assert.Equal(t, 0, cb.consecutiveSuccess)
		assert.Equal(t, 0, cb.halfOpenAttempts)
	})

	t.Run("FailureInHalfOpenTripsBackToOpen", func(t *testing.T) {
		cb := newCircuitBreaker(2, 3, 50*time.Millisecond)
		require.NotNil(t, cb)

		// Trip to open, then transition to half-open
		cb.RecordFailure()
		cb.RecordFailure()
		time.Sleep(60 * time.Millisecond)
		cb.Allow() // Transition to half-open

		// Record one success
		cb.RecordSuccess()
		assert.Equal(t, cbStateHalfOpen, cb.state)

		// Record failure - should trip back to open
		cb.RecordFailure()
		assert.Equal(t, cbStateOpen, cb.state)
	})

	t.Run("ConsecutiveSuccessCountReset", func(t *testing.T) {
		cb := newCircuitBreaker(2, 3, 50*time.Millisecond)
		require.NotNil(t, cb)

		// Trip to open, then transition to half-open
		cb.RecordFailure()
		cb.RecordFailure()
		time.Sleep(60 * time.Millisecond)
		cb.Allow()

		// Record partial successes
		cb.RecordSuccess()
		assert.Equal(t, 1, cb.consecutiveSuccess)
		cb.RecordSuccess()
		assert.Equal(t, 2, cb.consecutiveSuccess)

		// Failure should reset consecutive success count
		cb.RecordFailure()
		assert.Equal(t, 0, cb.consecutiveSuccess)
		assert.Equal(t, cbStateOpen, cb.state)
	})
}

func TestCircuitBreakerConcurrency(t *testing.T) {
	t.Run("ThreadSafetyUnderLoad", func(t *testing.T) {
		cb := newCircuitBreaker(50, 5, 100*time.Millisecond)
		require.NotNil(t, cb)

		// Run concurrent operations
		goroutines := 100
		iterations := 100

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					cb.Allow()
					if j%2 == 0 {
						cb.RecordSuccess()
					} else {
						cb.RecordFailure()
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify state is consistent (no panics or race conditions)
		// The exact state depends on timing, but should be valid
		assert.Contains(t, []cbState{cbStateClosed, cbStateOpen, cbStateHalfOpen}, cb.state)
	})

	t.Run("NoRaceConditionInStateTransitions", func(t *testing.T) {
		cb := newCircuitBreaker(3, 2, 50*time.Millisecond)
		require.NotNil(t, cb)

		var wg sync.WaitGroup

		// Concurrent failures to trigger state change
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				cb.RecordFailure()
			}()
		}
		wg.Wait()

		// Circuit should have tripped
		assert.Equal(t, cbStateOpen, cb.state)

		// Wait for cooldown
		time.Sleep(60 * time.Millisecond)

		// Concurrent Allow() calls
		wg.Add(10)
		allowCount := 0
		var mu sync.Mutex
		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				if cb.Allow() {
					mu.Lock()
					allowCount++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		// Should have allowed at most halfOpenMax attempts
		assert.LessOrEqual(t, allowCount, 2, "should not exceed halfOpenMax")
	})
}

func TestCircuitBreakerFullCycle(t *testing.T) {
	t.Run("CompleteCycleClosedToOpenToHalfOpenToClosed", func(t *testing.T) {
		cb := newCircuitBreaker(3, 2, 100*time.Millisecond)
		require.NotNil(t, cb)

		// Start in closed state
		assert.Equal(t, cbStateClosed, cb.state)
		assert.True(t, cb.Allow())

		// Trip to open
		cb.RecordFailure()
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, cbStateOpen, cb.state)
		assert.False(t, cb.Allow(), "should reject in open state")

		// Wait for cooldown
		time.Sleep(110 * time.Millisecond)

		// Transition to half-open
		assert.True(t, cb.Allow(), "should allow after cooldown")
		assert.Equal(t, cbStateHalfOpen, cb.state)

		// Succeed to close circuit
		cb.RecordSuccess()
		cb.RecordSuccess()
		assert.Equal(t, cbStateClosed, cb.state)
		assert.True(t, cb.Allow(), "should allow in closed state")
	})

	t.Run("MultipleOpenClosesCycles", func(t *testing.T) {
		cb := newCircuitBreaker(2, 1, 50*time.Millisecond)
		require.NotNil(t, cb)

		for cycle := 0; cycle < 3; cycle++ {
			// Closed -> Open
			cb.RecordFailure()
			cb.RecordFailure()
			assert.Equal(t, cbStateOpen, cb.state, "cycle %d: should be open", cycle)

			// Open -> Half-Open
			time.Sleep(60 * time.Millisecond)
			cb.Allow()
			assert.Equal(t, cbStateHalfOpen, cb.state, "cycle %d: should be half-open", cycle)

			// Half-Open -> Closed
			cb.RecordSuccess()
			assert.Equal(t, cbStateClosed, cb.state, "cycle %d: should be closed", cycle)
		}
	})
}

func TestCircuitBreakerEdgeCases(t *testing.T) {
	t.Run("MinimalConfiguration", func(t *testing.T) {
		cb := newCircuitBreaker(1, 1, 1*time.Millisecond)
		require.NotNil(t, cb)

		// Single failure should trip
		cb.RecordFailure()
		assert.Equal(t, cbStateOpen, cb.state)

		// Very short cooldown
		time.Sleep(2 * time.Millisecond)
		assert.True(t, cb.Allow())
		assert.Equal(t, cbStateHalfOpen, cb.state)

		// Single success should close
		cb.RecordSuccess()
		assert.Equal(t, cbStateClosed, cb.state)
	})

	t.Run("LargeConfiguration", func(t *testing.T) {
		cb := newCircuitBreaker(100, 10, 1*time.Second)
		require.NotNil(t, cb)

		// Should take 100 failures to trip
		for i := 0; i < 99; i++ {
			cb.RecordFailure()
			assert.Equal(t, cbStateClosed, cb.state, "should stay closed until 100 failures")
		}
		cb.RecordFailure()
		assert.Equal(t, cbStateOpen, cb.state)
	})

	t.Run("SuccessBetweenFailuresResetsCount", func(t *testing.T) {
		cb := newCircuitBreaker(3, 2, 50*time.Millisecond)
		require.NotNil(t, cb)

		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, 2, cb.consecutiveFailures)

		// Interleaved success
		cb.RecordSuccess()
		assert.Equal(t, 0, cb.consecutiveFailures)

		// Need 3 more failures to trip now
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, cbStateClosed, cb.state)
		cb.RecordFailure()
		assert.Equal(t, cbStateOpen, cb.state)
	})
}

func TestIsInfrastructureFailure(t *testing.T) {
	t.Run("NilIsNotInfra", func(t *testing.T) {
		require.False(t, isInfrastructureFailure(nil))
	})

	t.Run("DataStateCodesNotInfra", func(t *testing.T) {
		dataStateCodes := []types.ResultCode{
			types.KEY_NOT_FOUND_ERROR,
			types.FILTERED_OUT,
			types.GENERATION_ERROR,
			types.KEY_EXISTS_ERROR,
			types.PARAMETER_ERROR,
			types.BIN_EXISTS_ERROR,
		}
		for _, code := range dataStateCodes {
			require.False(t, isInfrastructureFailure(aErrWithCode(code)),
				"ResultCode %d (%s) must not count as infrastructure failure", code, types.ResultCodeToString(code))
		}
	})

	t.Run("InfraCodesAreInfra", func(t *testing.T) {
		infraCodes := []types.ResultCode{
			types.TIMEOUT,
			types.NETWORK_ERROR,
			types.NO_RESPONSE,
			types.MAX_RETRIES_EXCEEDED,
			types.MAX_ERROR_RATE,
			types.NO_AVAILABLE_CONNECTIONS_TO_NODE,
			types.SERVER_NOT_AVAILABLE,
			types.INVALID_NODE_ERROR,
			types.PARTITION_UNAVAILABLE,
			types.SERVER_MEM_ERROR,
			types.SERVER_ERROR,
			types.DEVICE_OVERLOAD,
			types.BATCH_FAILED,
			types.GRPC_ERROR,
		}
		for _, code := range infraCodes {
			require.True(t, isInfrastructureFailure(aErrWithCode(code)),
				"ResultCode %d (%s) must count as infrastructure failure", code, types.ResultCodeToString(code))
		}
	})

	t.Run("ConstSentinelErrorsAreClassified", func(t *testing.T) {
		// The aerospike client exposes some errors as package-level constants
		// of concrete type *constAerospikeError, not *AerospikeError. They must
		// still be classified by ResultCode — otherwise infra-grade sentinels
		// returned per-record fall through and the breaker never trips.
		require.True(t, isInfrastructureFailure(aerospike.ErrTimeout),
			"aerospike.ErrTimeout (const sentinel) must be infra")
		require.True(t, isInfrastructureFailure(aerospike.ErrNetwork),
			"aerospike.ErrNetwork (const sentinel) must be infra")
		require.True(t, isInfrastructureFailure(aerospike.ErrMaxRetriesExceeded),
			"aerospike.ErrMaxRetriesExceeded (const sentinel) must be infra")
		require.True(t, isInfrastructureFailure(aerospike.ErrConnectionPoolEmpty),
			"aerospike.ErrConnectionPoolEmpty (const sentinel) must be infra")
		// Data-state const sentinels must NOT be infra.
		require.False(t, isInfrastructureFailure(aerospike.ErrKeyNotFound),
			"aerospike.ErrKeyNotFound (const sentinel) must not be infra")
		require.False(t, isInfrastructureFailure(aerospike.ErrFilteredOut),
			"aerospike.ErrFilteredOut (const sentinel) must not be infra")
	})

	t.Run("StdlibTimeoutErrors", func(t *testing.T) {
		require.True(t, isInfrastructureFailure(context.DeadlineExceeded))
		require.True(t, isInfrastructureFailure(fakeTimeoutNetErr{}))
		require.False(t, isInfrastructureFailure(fakeNonTimeoutNetErr{}))
	})

	t.Run("PlainErrorIsNotInfra", func(t *testing.T) {
		require.False(t, isInfrastructureFailure(plainErr{msg: "some random failure"}))
	})

	t.Run("WrappedAerospikeError", func(t *testing.T) {
		// Wrapping via a type with Unwrap() preserves the AerospikeError so
		// errors.As inside the classifier can still find it.
		require.True(t, isInfrastructureFailure(wrapErr{msg: "outer", inner: aErrWithCode(types.TIMEOUT)}))
		require.False(t, isInfrastructureFailure(wrapErr{msg: "outer", inner: aErrWithCode(types.KEY_NOT_FOUND_ERROR)}))
	})

	t.Run("WrappedContextDeadline", func(t *testing.T) {
		require.True(t, isInfrastructureFailure(wrapErr{msg: "outer", inner: context.DeadlineExceeded}))
	})
}

// TestCircuitBreaker_KeyNotFoundDoesNotTrip is the regression test for issue #953.
// Per-record KEY_NOT_FOUND_ERROR results from aerospike batch spends represent
// data state (missing parents during catch-up sync), not infrastructure failure,
// and must not trip the breaker even at high volume.
func TestCircuitBreaker_KeyNotFoundDoesNotTrip(t *testing.T) {
	cb := newCircuitBreaker(3, 1, 30*time.Second)
	require.NotNil(t, cb)

	keyNotFound := aErrWithCode(types.KEY_NOT_FOUND_ERROR)

	// Simulate the gated call site: only record failure if the error is infra.
	for i := 0; i < 1000; i++ {
		if isInfrastructureFailure(keyNotFound) {
			cb.RecordFailure()
		}
	}

	require.Equal(t, cbStateClosed, cb.state, "breaker must stay closed under KEY_NOT_FOUND flood")
	require.True(t, cb.Allow(), "breaker must still allow requests")
}

// TestCircuitBreaker_InfraErrorStillTrips ensures the safety net still works:
// real infrastructure failures continue to open the breaker after threshold.
func TestCircuitBreaker_InfraErrorStillTrips(t *testing.T) {
	cb := newCircuitBreaker(3, 1, 30*time.Second)
	require.NotNil(t, cb)

	timeout := aErrWithCode(types.TIMEOUT)

	for i := 0; i < 3; i++ {
		if isInfrastructureFailure(timeout) {
			cb.RecordFailure()
		}
	}

	require.Equal(t, cbStateOpen, cb.state, "breaker must trip on 3 consecutive infra failures")
	require.False(t, cb.Allow(), "breaker must reject while open")
}

// ensure net.Error interface assertions compile (linter hint).
var _ net.Error = fakeTimeoutNetErr{}
var _ net.Error = fakeNonTimeoutNetErr{}
