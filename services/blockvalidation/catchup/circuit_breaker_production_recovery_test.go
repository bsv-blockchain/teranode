//go:build bvgaps

package catchup

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

const (
	// productionMaxHalfOpenRequests is the literal Server.go (NewServer) passes as MaxHalfOpenRequests.
	productionMaxHalfOpenRequests = 1
	// productionSuccessThresholdDefault is the default blockvalidation CircuitBreakerSuccessThreshold.
	productionSuccessThresholdDefault = 2
	// productionFailureThresholdDefault is the default blockvalidation CircuitBreakerFailureThreshold.
	productionFailureThresholdDefault = 5
	// shortenedBreakerOpenTimeout replaces the production open timeout (seconds) so the test does not sleep long.
	shortenedBreakerOpenTimeout = 10 * time.Millisecond
	// successfulHeaderPhasesAfterTimeout is far more successful calls than SuccessThreshold requires.
	successfulHeaderPhasesAfterTimeout = 10
	// recoveringPeerID names the peer whose breaker is tripped and then expected to recover.
	recoveringPeerID = "recovering-peer"
)

// productionPeerBreakerConfig replicates the per-peer breaker config that Server.go (NewServer,
// ~292-297) builds: FailureThreshold and SuccessThreshold from the blockvalidation settings
// defaults (read via settings.NewSettings, so a settings change is picked up), MaxHalfOpenRequests
// hard-coded to 1. Only the Timeout is shortened (10ms instead of the production seconds value) so
// the test does not sleep; the circuit breaker uses the wall clock.
func productionPeerBreakerConfig() CircuitBreakerConfig {
	defaults := settings.NewSettings()

	return CircuitBreakerConfig{
		FailureThreshold:    defaults.BlockValidation.CircuitBreakerFailureThreshold,
		SuccessThreshold:    defaults.BlockValidation.CircuitBreakerSuccessThreshold,
		Timeout:             shortenedBreakerOpenTimeout,
		MaxHalfOpenRequests: productionMaxHalfOpenRequests, // literal in Server.go
	}
}

// waitForBreakerOpenTimeout sleeps past the breaker's open timeout so the next call may probe.
func waitForBreakerOpenTimeout(config CircuitBreakerConfig) {
	time.Sleep(2 * config.Timeout)
}

// TestKnownDefect_PeerCircuitBreaker_ClosesAfterSuccessfulProbesWithProductionConfig checks that a
// peer's catchup circuit breaker recovers under the configuration the server actually builds.
//
// Spec: BV-CATCHUP-025 and AC-BV-CATCHUP-025.1 (tracked as GAP-BV-022).
//
// Scenario: Given a breaker with the production config (FailureThreshold 5, SuccessThreshold 2,
// MaxHalfOpenRequests 1, open timeout shortened to 10ms) that has been opened by 5 failures, When
// the open timeout passes and the peer then completes 2 successful probes, Then the breaker must be
// closed again and admit calls. The second subtest drives the same story through the per-peer
// breaker set (NewPeerCircuitBreakers / GetBreaker / Call) as the server does, with 10 successful
// calls after the timeout.
//
// Today: in services/blockvalidation/catchup/circuit_breaker.go, the Open->HalfOpen transition in
// CanCall consumes the single half-open slot (halfOpenRequests=1) and nothing ever releases it, so
// after the first successful probe CanCall refuses every further call; the second success needed
// to close can never be recorded (RecordSuccess). The breaker stays half-open and refusing forever.
// The config comes from services/blockvalidation/Server.go NewServer (~292-297): SuccessThreshold
// exceeds the number of probes the half-open state admits per cycle.
func TestKnownDefect_PeerCircuitBreaker_ClosesAfterSuccessfulProbesWithProductionConfig(t *testing.T) {
	config := productionPeerBreakerConfig()
	require.Equal(t, productionMaxHalfOpenRequests, config.MaxHalfOpenRequests, "production MaxHalfOpenRequests (Server.go literal)")
	require.Equal(t, productionSuccessThresholdDefault, config.SuccessThreshold, "production SuccessThreshold default")
	require.Equal(t, productionFailureThresholdDefault, config.FailureThreshold, "production FailureThreshold default")

	t.Run("an opened breaker closes after the required number of successful probes", func(t *testing.T) {
		// Given: a breaker opened by FailureThreshold failures.
		breaker := NewCircuitBreaker(config)

		for failure := 0; failure < config.FailureThreshold; failure++ {
			require.True(t, breaker.CanCall())
			breaker.RecordFailure()
		}
		require.Equal(t, StateOpen, breaker.GetState(), "setup: breaker must be open after FailureThreshold failures")
		require.False(t, breaker.CanCall(), "setup: open breaker refuses before the timeout")

		// When: the open timeout passes and the peer answers SuccessThreshold probes successfully.
		waitForBreakerOpenTimeout(config)

		// A correct breaker admits every probe and closes after SuccessThreshold successes.
		for probe := 1; probe <= config.SuccessThreshold; probe++ {
			require.Truef(t, breaker.CanCall(), "probe %d of %d was refused: half-open slot is never released (state=%s)", probe, config.SuccessThreshold, breaker.GetState())
			breaker.RecordSuccess()
		}

		// Then: the breaker is closed and admits calls.
		require.Equal(t, StateClosed, breaker.GetState(), "breaker must be closed after SuccessThreshold successful probes")
		require.True(t, breaker.CanCall(), "closed breaker must admit calls")
	})

	t.Run("a per-peer breaker built as the server builds it closes after successful calls", func(t *testing.T) {
		// Given: Server.go builds NewPeerCircuitBreakers(*cbConfig) and obtains a breaker per peer with
		// GetBreaker; the catchup code drives it through CanCall / RecordSuccess|RecordFailure.
		breaker := NewPeerCircuitBreakers(config).GetBreaker(recoveringPeerID)

		for failure := 0; failure < config.FailureThreshold; failure++ {
			_ = breaker.Call(func() error { return ErrCircuitOpen })
		}
		require.Equal(t, StateOpen, breaker.GetState())

		// When: the open timeout passes and the peer completes many successful header phases.
		waitForBreakerOpenTimeout(config)

		for phase := 0; phase < successfulHeaderPhasesAfterTimeout; phase++ {
			_ = breaker.Call(func() error { return nil })
		}

		// Then: a breaker that can close must be closed well before then.
		require.Equal(t, StateClosed, breaker.GetState(), "peer breaker never closes with MaxHalfOpenRequests=%d, SuccessThreshold=%d", config.MaxHalfOpenRequests, config.SuccessThreshold)
	})
}
