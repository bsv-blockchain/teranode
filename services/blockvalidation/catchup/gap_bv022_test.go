//go:build bvgaps

package catchup

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// gap022ProductionConfig replicates the per-peer breaker config that Server.go (NewServer,
// ~292-297) builds: FailureThreshold, SuccessThreshold from the blockvalidation settings defaults
// (read via settings.NewSettings, so a settings change is picked up), MaxHalfOpenRequests hard-coded
// to 1. Only the Timeout is shortened (10ms instead of the production seconds value) so the test
// does not sleep; the circuit breaker uses the wall clock.
func gap022ProductionConfig() CircuitBreakerConfig {
	s := settings.NewSettings()

	return CircuitBreakerConfig{
		FailureThreshold:    s.BlockValidation.CircuitBreakerFailureThreshold,
		SuccessThreshold:    s.BlockValidation.CircuitBreakerSuccessThreshold,
		Timeout:             10 * time.Millisecond,
		MaxHalfOpenRequests: 1, // literal in Server.go
	}
}

// TestGapBV022_BreakerMustEventuallyCloseWithProductionConfig asserts BV-CATCHUP-025 /
// AC-BV-CATCHUP-025.1: a peer's breaker that opened and whose peer then completes the required
// number of successful probes MUST be closed again.
//
// With the production config (MaxHalfOpenRequests=1, SuccessThreshold=2) the Open->HalfOpen
// transition in CanCall consumes the single half-open slot (halfOpenRequests=1) and nothing ever
// releases it, so after the first successful probe CanCall refuses every further call; the second
// success needed to close can never be recorded. The breaker stays half-open and refusing forever,
// and this test fails today.
//
// The second assertion checks that the config Server.go builds is at all capable of closing:
// SuccessThreshold must not exceed the number of probes the half-open state admits per cycle.
//
// Code: services/blockvalidation/catchup/circuit_breaker.go CanCall / RecordSuccess;
// services/blockvalidation/Server.go NewServer (~292-297).
func TestGapBV022_BreakerMustEventuallyCloseWithProductionConfig(t *testing.T) {
	cfg := gap022ProductionConfig()
	require.Equal(t, 1, cfg.MaxHalfOpenRequests, "production MaxHalfOpenRequests (Server.go literal)")
	require.Equal(t, 2, cfg.SuccessThreshold, "production SuccessThreshold default")
	require.Equal(t, 5, cfg.FailureThreshold, "production FailureThreshold default")

	t.Run("breaker_closes_after_enough_successful_probes", func(t *testing.T) {
		cb := NewCircuitBreaker(cfg)

		for i := 0; i < cfg.FailureThreshold; i++ {
			require.True(t, cb.CanCall())
			cb.RecordFailure()
		}
		require.Equal(t, StateOpen, cb.GetState(), "setup: breaker must be open after FailureThreshold failures")
		require.False(t, cb.CanCall(), "setup: open breaker refuses before the timeout")

		time.Sleep(2 * cfg.Timeout)

		// Perform up to SuccessThreshold probes, each followed by RecordSuccess. A correct breaker
		// admits every probe and closes after SuccessThreshold successes.
		for probe := 1; probe <= cfg.SuccessThreshold; probe++ {
			require.Truef(t, cb.CanCall(), "probe %d of %d was refused: half-open slot is never released (state=%s)", probe, cfg.SuccessThreshold, cb.GetState())
			cb.RecordSuccess()
		}

		require.Equal(t, StateClosed, cb.GetState(), "breaker must be closed after SuccessThreshold successful probes")
		require.True(t, cb.CanCall(), "closed breaker must admit calls")
	})

	t.Run("per_peer_breaker_as_built_by_server_closes_via_Call", func(t *testing.T) {
		// Server.go builds NewPeerCircuitBreakers(*cbConfig) and obtains a breaker per peer with
		// GetBreaker; the catchup code drives it through CanCall / RecordSuccess|RecordFailure.
		cb := NewPeerCircuitBreakers(cfg).GetBreaker("peer-gap022")

		for i := 0; i < cfg.FailureThreshold; i++ {
			_ = cb.Call(func() error { return ErrCircuitOpen })
		}
		require.Equal(t, StateOpen, cb.GetState())

		time.Sleep(2 * cfg.Timeout)

		// Ten successful header phases is far more than SuccessThreshold; a breaker that can close
		// must be closed well before then.
		for i := 0; i < 10; i++ {
			_ = cb.Call(func() error { return nil })
		}

		require.Equal(t, StateClosed, cb.GetState(), "peer breaker never closes with MaxHalfOpenRequests=%d, SuccessThreshold=%d", cfg.MaxHalfOpenRequests, cfg.SuccessThreshold)
	})
}
