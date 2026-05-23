package usql

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker_Disabled(t *testing.T) {
	// Circuit breaker with FailureThreshold = 0 should be nil (disabled)
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 0,
		Enabled:          true,
	})
	require.Nil(t, cb, "circuit breaker should be nil when threshold is 0")

	// Circuit breaker with Enabled = false should be nil
	cb = NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		Enabled:          false,
	})
	require.Nil(t, cb, "circuit breaker should be nil when disabled")

	// Nil circuit breaker should always allow
	require.True(t, cb.Allow(), "nil circuit breaker should always allow")

	// Nil circuit breaker operations should be safe
	cb.RecordSuccess()
	cb.RecordFailure()
	require.Equal(t, CircuitClosed, cb.State())
}

func TestCircuitBreaker_ClosedState(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      3,
		Cooldown:         30 * time.Second,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	require.Equal(t, CircuitClosed, cb.State())
	require.True(t, cb.Allow(), "closed circuit should allow requests")

	// Record some successes
	for i := 0; i < 10; i++ {
		cb.RecordSuccess()
	}
	require.Equal(t, CircuitClosed, cb.State())

	// Record failures below threshold
	for i := 0; i < 4; i++ {
		cb.RecordFailure()
	}
	require.Equal(t, CircuitClosed, cb.State(), "circuit should remain closed below threshold")
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	stateChanges := make([]string, 0)
	var mu sync.Mutex

	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      3,
		Cooldown:         100 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
		OnStateChange: func(from, to CircuitState, reason string) {
			mu.Lock()
			stateChanges = append(stateChanges, to.String())
			mu.Unlock()
		},
	})

	// Record 5 consecutive failures
	for i := 0; i < 5; i++ {
		cb.RecordFailure()
	}

	require.Equal(t, CircuitOpen, cb.State(), "circuit should be open after threshold failures")

	// Wait for callback to be processed
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	require.Contains(t, stateChanges, "open", "state change callback should be called")
	mu.Unlock()

	// Requests should be rejected
	require.False(t, cb.Allow(), "open circuit should reject requests")
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		HalfOpenMax:      2,
		Cooldown:         50 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	require.Equal(t, CircuitOpen, cb.State())

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	// Next Allow() should transition to half-open
	require.True(t, cb.Allow(), "should allow probe request after cooldown")
	require.Equal(t, CircuitHalfOpen, cb.State())
}

func TestCircuitBreaker_HalfOpenRecovery(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		HalfOpenMax:      2,
		Cooldown:         50 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	// Wait for cooldown and transition to half-open
	time.Sleep(60 * time.Millisecond)
	require.True(t, cb.Allow())
	require.Equal(t, CircuitHalfOpen, cb.State())

	// Record successful probes
	cb.RecordSuccess()
	require.Equal(t, CircuitHalfOpen, cb.State(), "should still be half-open after 1 success")

	cb.RecordSuccess()
	require.Equal(t, CircuitClosed, cb.State(), "should close after required successes")

	// Should allow normal requests now
	require.True(t, cb.Allow())
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		HalfOpenMax:      3,
		Cooldown:         50 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	// Wait for cooldown and transition to half-open
	time.Sleep(60 * time.Millisecond)
	require.True(t, cb.Allow())
	require.Equal(t, CircuitHalfOpen, cb.State())

	// A failure in half-open should reopen the circuit
	cb.RecordFailure()
	require.Equal(t, CircuitOpen, cb.State(), "should reopen on failure during half-open")
}

func TestCircuitBreaker_HalfOpenMaxProbes(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		HalfOpenMax:      2,
		Cooldown:         50 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	// Wait for cooldown and transition to half-open
	time.Sleep(60 * time.Millisecond)

	// Allow up to HalfOpenMax probes
	require.True(t, cb.Allow(), "first probe should be allowed")
	require.True(t, cb.Allow(), "second probe should be allowed")
	require.False(t, cb.Allow(), "third probe should be rejected (max reached)")
}

func TestCircuitBreaker_FailureWindowReset(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		HalfOpenMax:      2,
		Cooldown:         100 * time.Millisecond,
		FailureWindow:    50 * time.Millisecond, // Short window
		Enabled:          true,
	})

	// Record 2 failures
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for failure window to expire
	time.Sleep(60 * time.Millisecond)

	// Record 2 more failures (should start new window)
	cb.RecordFailure()
	cb.RecordFailure()

	// Circuit should still be closed because we didn't hit threshold in one window
	require.Equal(t, CircuitClosed, cb.State(), "circuit should remain closed when failures span windows")
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      3,
		Cooldown:         100 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Record 4 failures (just under threshold)
	for i := 0; i < 4; i++ {
		cb.RecordFailure()
	}

	// Record a success
	cb.RecordSuccess()

	// Now record 4 more failures
	for i := 0; i < 4; i++ {
		cb.RecordFailure()
	}

	// Circuit should still be closed because success reset the counter
	require.Equal(t, CircuitClosed, cb.State(), "success should reset failure counter")
}

func TestCircuitBreaker_Execute(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		HalfOpenMax:      2,
		Cooldown:         50 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Successful execution
	var callCount int
	err := cb.Execute(func() error {
		callCount++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, callCount)

	// Failing execution (retriable error)
	connectionError := errors.NewError("connection refused")
	for i := 0; i < 3; i++ {
		err = cb.Execute(func() error {
			return connectionError
		})
		require.Error(t, err)
	}

	// Circuit should be open now
	require.Equal(t, CircuitOpen, cb.State())

	// Execute should return ErrCircuitOpen without calling function
	callCount = 0
	err = cb.Execute(func() error {
		callCount++
		return nil
	})
	require.Equal(t, ErrCircuitOpen, err)
	require.Equal(t, 0, callCount, "function should not be called when circuit is open")
}

func TestCircuitBreaker_Stats(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      3,
		Cooldown:         100 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	stats := cb.Stats()
	require.Equal(t, CircuitClosed, stats.State)
	require.Equal(t, 0, stats.ConsecutiveFailures)

	// Record some failures
	cb.RecordFailure()
	cb.RecordFailure()

	stats = cb.Stats()
	require.Equal(t, CircuitClosed, stats.State)
	require.Equal(t, 2, stats.ConsecutiveFailures)
}

func TestCircuitBreaker_StateString(t *testing.T) {
	require.Equal(t, "closed", CircuitClosed.String())
	require.Equal(t, "open", CircuitOpen.String())
	require.Equal(t, "half-open", CircuitHalfOpen.String())
	require.Equal(t, "unknown", CircuitState(99).String())
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 100,
		HalfOpenMax:      10,
		Cooldown:         100 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	var wg sync.WaitGroup
	concurrency := 100
	iterations := 100

	// Concurrent Allow/RecordSuccess/RecordFailure
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
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

	// Should not panic or deadlock
	_ = cb.State()
	_ = cb.Stats()
}

func TestCircuitBreaker_RequiresExplicitConfig(t *testing.T) {
	// Circuit breaker requires all values to be explicitly set
	// An empty config should return nil (disabled)
	cb := NewCircuitBreaker(CircuitBreakerConfig{})
	require.Nil(t, cb, "empty config should return nil circuit breaker")

	// Only enabled but no other values should return nil
	cb = NewCircuitBreaker(CircuitBreakerConfig{Enabled: true})
	require.Nil(t, cb, "enabled without threshold values should return nil")
}

func TestCircuitBreaker_ZeroValuesDisable(t *testing.T) {
	// Circuit breaker with zero HalfOpenMax should be nil (disabled)
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      0, // Zero value = disabled
		Cooldown:         30 * time.Second,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})
	require.Nil(t, cb, "circuit breaker should be nil when HalfOpenMax is 0")

	// Circuit breaker with zero Cooldown should be nil (disabled)
	cb = NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      3,
		Cooldown:         0, // Zero value = disabled
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})
	require.Nil(t, cb, "circuit breaker should be nil when Cooldown is 0")

	// Circuit breaker with zero FailureWindow should be nil (disabled)
	cb = NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      3,
		Cooldown:         30 * time.Second,
		FailureWindow:    0, // Zero value = disabled
		Enabled:          true,
	})
	require.Nil(t, cb, "circuit breaker should be nil when FailureWindow is 0")

	// Circuit breaker with all values set should work
	cb = NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      3,
		Cooldown:         30 * time.Second,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})
	require.NotNil(t, cb, "circuit breaker should be created when all values are set")
}

func TestCircuitBreaker_NonRetriableErrorsIgnored(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		HalfOpenMax:      2,
		Cooldown:         50 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Non-retriable errors should not open the circuit
	businessError := errors.NewError("record not found")
	for i := 0; i < 10; i++ {
		// Execute with non-retriable error
		_ = cb.Execute(func() error {
			return businessError
		})
	}

	// Circuit should still be closed
	require.Equal(t, CircuitClosed, cb.State(), "non-retriable errors should not affect circuit breaker")
}

func TestCircuitBreaker_RetriableErrorsOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		HalfOpenMax:      2,
		Cooldown:         50 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Retriable errors should open the circuit
	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error {
			return errors.NewError("connection refused") // Matches retriable pattern
		})
	}

	// Circuit should be open
	require.Equal(t, CircuitOpen, cb.State(), "retriable errors should open circuit breaker")
}

func TestCircuitBreaker_FastFailMetrics(t *testing.T) {
	// This test verifies the circuit breaker increments the fast-fail counter
	// when requests are rejected. The actual metric values would need to be
	// verified through prometheus scraping in integration tests.

	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 2,
		HalfOpenMax:      2,
		Cooldown:         100 * time.Millisecond,
		FailureWindow:    10 * time.Second,
		Enabled:          true,
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()
	require.Equal(t, CircuitOpen, cb.State())

	// Count fast-fails
	fastFails := 0
	for i := 0; i < 5; i++ {
		if !cb.Allow() {
			fastFails++
		}
	}
	require.Equal(t, 5, fastFails, "all requests should be fast-failed while circuit is open")
}

func TestCircuitBreaker_AcceptanceCriteria(t *testing.T) {
	// Test the acceptance criteria from the requirements:
	// 1. Circuit opens after 5 consecutive failures within 10 seconds
	// 2. Circuit stays open for 30 seconds before testing recovery
	// 3. Half-open allows 3 successful requests before closing circuit

	var stateChangeCalls int32

	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 5,
		HalfOpenMax:      3,
		Cooldown:         100 * time.Millisecond, // Shortened for test
		FailureWindow:    10 * time.Second,
		Enabled:          true,
		OnStateChange: func(from, to CircuitState, reason string) {
			atomic.AddInt32(&stateChangeCalls, 1)
			t.Logf("Circuit breaker: %s -> %s (%s)", from.String(), to.String(), reason)
		},
	})

	// Criterion 1: Circuit opens after 5 consecutive failures
	for i := 0; i < 5; i++ {
		require.True(t, cb.Allow())
		cb.RecordFailure()
	}
	require.Equal(t, CircuitOpen, cb.State(), "circuit should open after 5 failures")

	// Fast-fail while open
	require.False(t, cb.Allow())

	// Wait for cooldown
	time.Sleep(120 * time.Millisecond)

	// Criterion 2: After cooldown, transitions to half-open
	require.True(t, cb.Allow())
	require.Equal(t, CircuitHalfOpen, cb.State())

	// Criterion 3: 3 successful requests close the circuit
	cb.RecordSuccess()
	require.Equal(t, CircuitHalfOpen, cb.State())
	cb.RecordSuccess()
	require.Equal(t, CircuitHalfOpen, cb.State())
	cb.RecordSuccess()
	require.Equal(t, CircuitClosed, cb.State(), "circuit should close after 3 successful probes")

	// Verify state changes were logged
	time.Sleep(20 * time.Millisecond) // Wait for async callbacks
	require.GreaterOrEqual(t, atomic.LoadInt32(&stateChangeCalls), int32(3),
		"should have logged state changes: closed->open, open->half-open, half-open->closed")
}
