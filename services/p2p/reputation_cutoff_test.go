package p2p

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/stretchr/testify/require"
)

// TestReputationCutoff_CatchupGateAgreesWithSyncEligibilityOnScore drives one
// peer through the reputation bands a real peer passes through and checks, at
// each, that the block-validation catchup gate (IsPeerUnhealthy) and sync-peer
// eligibility (isViableSyncCandidate) reach the same verdict on the score. When
// the catchup gate cut off at 40 and sync eligibility at 20, a peer between the
// two was picked as the sync peer and then refused as its catchup source, and a
// node whose only peer ahead it was stopped advancing. The catchup gate's
// separate success-rate rule (more than 10 interactions, under half successful)
// has no sync-side counterpart and is not exercised here; block validation
// falls back to such a peer when no alternative can serve the block.
func TestReputationCutoff_CatchupGateAgreesWithSyncEligibilityOnScore(t *testing.T) {
	ctx := context.Background()
	s, reg, pid := freshTestServer(t)
	id := pid.String()
	reg.Register(&blockchain.PeerInfo{ID: id, DataHubURL: "http://peer.example"})

	check := func(stage string, wantUsable bool) {
		t.Helper()

		info, ok := reg.Get(id)
		require.True(t, ok)

		resp, err := s.IsPeerUnhealthy(ctx, &p2p_api.IsPeerUnhealthyRequest{PeerId: id})
		require.NoError(t, err)

		require.Equal(t, wantUsable, isViableSyncCandidate(info), "%s: sync eligibility at reputation %.2f", stage, info.ReputationScore)
		require.Equal(t, !wantUsable, resp.IsUnhealthy, "%s: catchup gate at reputation %.2f (reason %q)", stage, info.ReputationScore, resp.Reason)
	}

	check("new peer", true)

	// One failure, no success: 0%*0.6 + 20 - 15 (recent failure) = 5.
	reg.UpdateMetrics(id, 0, 0, 0, false, true, false, 0)
	check("after a failure", false)

	// Recovery after the cooldown (zero here) resets to the recovery score.
	require.Equal(t, 1, reg.ReconsiderBadPeers(0))
	check("after recovery", true)

	// One success (no response time) and the failure above plus one more:
	// 33.3%*0.6 + 20 - 15 (recent failure) + 10 (recent success) = 35.
	reg.UpdateMetrics(id, 0, 0, 0, true, false, false, 0)
	reg.UpdateMetrics(id, 0, 0, 0, false, true, false, 0)
	info, _ := reg.Get(id)
	require.InDelta(t, 35.0, info.ReputationScore, 1e-9)
	check("mid band", true)
}
