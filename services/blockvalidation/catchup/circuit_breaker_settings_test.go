package catchup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCircuitBreakerFailureThresholdVariations(t *testing.T) {
	tests := []struct {
		name             string
		failureThreshold int
		failuresToInject int
		expectOpen       bool
	}{
		{"threshold 1 opens immediately", 1, 1, true},
		{"threshold 5 stays closed at 4 failures", 5, 4, false},
		{"threshold 5 opens at 5 failures", 5, 5, true},
		{"threshold 100 stays closed at 99 failures", 100, 99, false},
		{"threshold 100 opens at 100 failures", 100, 100, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := NewCircuitBreaker(CircuitBreakerConfig{
				FailureThreshold:    tt.failureThreshold,
				SuccessThreshold:    1,
				Timeout:             1 * time.Second,
				MaxHalfOpenRequests: 1,
			})

			for i := 0; i < tt.failuresToInject; i++ {
				cb.CanCall()
				cb.RecordFailure()
			}

			if tt.expectOpen {
				require.Equal(t, StateOpen, cb.GetState())
				require.False(t, cb.CanCall())
			} else {
				require.Equal(t, StateClosed, cb.GetState())
				require.True(t, cb.CanCall())
			}
		})
	}
}

func TestCircuitBreakerSuccessThresholdVariations(t *testing.T) {
	tests := []struct {
		name              string
		successThreshold  int
		successesToInject int
		expectClosed      bool
	}{
		{"threshold 1 closes after 1 success", 1, 1, true},
		{"threshold 2 stays half-open after 1 success", 2, 1, false},
		{"threshold 2 closes after 2 successes", 2, 2, true},
		{"threshold 5 stays half-open after 4 successes", 5, 4, false},
		{"threshold 5 closes after 5 successes", 5, 5, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := NewCircuitBreaker(CircuitBreakerConfig{
				FailureThreshold:    1,
				SuccessThreshold:    tt.successThreshold,
				Timeout:             10 * time.Millisecond,
				MaxHalfOpenRequests: tt.successesToInject + 1,
			})

			cb.RecordFailure()
			require.Equal(t, StateOpen, cb.GetState())

			time.Sleep(20 * time.Millisecond)
			require.True(t, cb.CanCall())
			require.Equal(t, StateHalfOpen, cb.GetState())

			for i := 0; i < tt.successesToInject; i++ {
				cb.RecordSuccess()
			}

			if tt.expectClosed {
				require.Equal(t, StateClosed, cb.GetState())
			} else {
				require.Equal(t, StateHalfOpen, cb.GetState())
			}
		})
	}
}

func TestCircuitBreakerTimeoutVariations(t *testing.T) {
	tests := []struct {
		name          string
		timeout       time.Duration
		waitDuration  time.Duration
		expectCanCall bool
	}{
		{"short timeout expires quickly", 10 * time.Millisecond, 20 * time.Millisecond, true},
		{"long timeout blocks during wait", 500 * time.Millisecond, 10 * time.Millisecond, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := NewCircuitBreaker(CircuitBreakerConfig{
				FailureThreshold:    1,
				SuccessThreshold:    1,
				Timeout:             tt.timeout,
				MaxHalfOpenRequests: 1,
			})

			cb.RecordFailure()
			require.Equal(t, StateOpen, cb.GetState())

			time.Sleep(tt.waitDuration)

			require.Equal(t, tt.expectCanCall, cb.CanCall())
		})
	}
}

func TestCircuitBreakerSuccessResetsBetweenFailures(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    1,
		Timeout:             1 * time.Second,
		MaxHalfOpenRequests: 1,
	})

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess()
	cb.RecordFailure()
	cb.RecordFailure()

	require.Equal(t, StateClosed, cb.GetState(), "success should reset the failure counter")
}

func TestCircuitBreakerZeroFailureThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold:    0,
		SuccessThreshold:    1,
		Timeout:             10 * time.Millisecond,
		MaxHalfOpenRequests: 1,
	})

	cb.RecordFailure()
	require.Equal(t, StateOpen, cb.GetState(), "zero threshold should open on any failure")
}
