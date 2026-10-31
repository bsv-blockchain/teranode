package blockchain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReputation_OneStaleFailureSettlesAtUsableCutoff pins the score a peer
// with one historical failure and no success since settles at, and that it is
// usable. The score bottoms out at 5 while the failure is recent, recovery
// lifts it once the cooldown passes, and the next recomputation (any metrics
// update) brings it back to 0%*0.6 + 50*0.4 = 20.0. That value has to be at or
// above MinUsablePeerReputation, or the peer is excluded from catchup forever
// with nothing able to move it: ReconsiderBadPeers only recovers peers below
// the cut-off.
func TestReputation_OneStaleFailureSettlesAtUsableCutoff(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "stale-failure"})

	r.RecordCatchupFailure("stale-failure")

	got, _ := r.Get("stale-failure")
	require.Less(t, got.ReputationScore, MinUsablePeerReputation, "a recent failure with no success must exclude the peer")

	// Backdate the failure past both the 1h recency window and the cooldown.
	r.mu.Lock()
	r.peers["stale-failure"].LastInteractionFailure = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

	require.Equal(t, 1, r.ReconsiderBadPeers(5*time.Minute))

	got, _ = r.Get("stale-failure")
	require.GreaterOrEqual(t, got.ReputationScore, MinUsablePeerReputation, "a recovered peer must be usable")

	// A metrics update with no interaction outcome recomputes the score from
	// the unchanged history: 1 failure, 0 successes, failure outside the window.
	r.UpdateMetrics("stale-failure", 0, 0, 1024, false, false, false, 0)

	got, _ = r.Get("stale-failure")
	require.InDelta(t, 20.0, got.ReputationScore, 1e-9)
	require.GreaterOrEqual(t, got.ReputationScore, MinUsablePeerReputation, "one stale failure must not pin a peer below the usable cut-off")

	// It is no longer a recovery candidate, so nothing else will move it.
	require.Equal(t, 0, r.ReconsiderBadPeers(0))
}

// TestReconsideredPeerReputation_IsUsable guards the band ordering: recovery
// must hand back a peer the gates accept.
func TestReconsideredPeerReputation_IsUsable(t *testing.T) {
	require.GreaterOrEqual(t, reconsideredPeerReputation, MinUsablePeerReputation)
}
