package catchup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker_SuccessfulProbeReleasesSlot(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Timeout = time.Hour
	cb := NewCircuitBreaker(config)
	cb.RecordFailure(5)
	cb.lastStateChange = time.Now().Add(-2 * time.Hour)

	require.True(t, cb.CanCall())
	require.False(t, cb.CanCall(), "one in-flight probe allowed")
	cb.RecordSuccess()
	require.Equal(t, StateHalfOpen, cb.GetState(), "recovery still needs a second success")
	require.True(t, cb.CanCall(), "completed probe must release the in-flight slot")
	cb.RecordSuccess()
	require.Equal(t, StateClosed, cb.GetState())
}

func TestCircuitBreaker_CanceledProbeReleasesSlotWithoutOutcome(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Timeout = time.Hour
	cb := NewCircuitBreaker(config)
	cb.RecordFailure(5)
	cb.lastStateChange = time.Now().Add(-2 * time.Hour)
	allowed, cancelProbe := cb.CanCallWithCancel()
	require.True(t, allowed)
	require.False(t, cb.CanCall())
	_, failuresBefore, successesBefore, lastFailureBefore := cb.GetStats()

	cancelProbe()
	state, failures, successes, lastFailure := cb.GetStats()
	require.Equal(t, StateHalfOpen, state)
	require.Equal(t, failuresBefore, failures)
	require.Equal(t, successesBefore, successes)
	require.Equal(t, lastFailureBefore, lastFailure)
	require.True(t, cb.CanCall())
	cancelProbe() // A repeated cancel must not release the new caller's slot.
	require.False(t, cb.CanCall())
	cb.RecordFailure()
	require.Equal(t, StateOpen, cb.GetState())
	require.False(t, cb.CanCall())
}

func TestCircuitBreaker_StaleProbeCancellationCannotReleaseNewGeneration(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	config.Timeout = time.Hour
	config.MaxHalfOpenRequests = 2
	cb := NewCircuitBreaker(config)
	cb.RecordFailure(5)
	cb.lastStateChange = time.Now().Add(-2 * time.Hour)
	allowed, cancelOldProbe := cb.CanCallWithCancel()
	require.True(t, allowed)
	require.True(t, cb.CanCall())
	cb.RecordFailure() // The other probe fails while the old request is still running.
	require.Equal(t, StateOpen, cb.GetState())
	cb.lastStateChange = time.Now().Add(-2 * time.Hour)
	require.True(t, cb.CanCall())
	require.True(t, cb.CanCall())
	cancelOldProbe()
	require.False(t, cb.CanCall(), "old cancellation cannot release a new generation's probe")
}
