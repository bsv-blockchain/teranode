package p2p

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Reputation Increase Tests
// =============================================================================

func TestPeerRegistry_ReputationIncrease_ValidSubtreeReceived(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-subtree")

	// Add peer with neutral reputation
	pr.AddPeer(peerID, "")
	initialInfo, _ := pr.GetPeer(peerID)
	initialReputation := initialInfo.ReputationScore
	assert.Equal(t, 50.0, initialReputation, "Should start with neutral reputation")

	// Record successful subtree received
	pr.RecordSubtreeReceived(peerID, 100*time.Millisecond)

	// Verify reputation increased
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.SubtreesReceived)
	assert.Equal(t, int64(1), info.InteractionSuccesses)
	assert.Greater(t, info.ReputationScore, initialReputation, "Reputation should increase after successful subtree")
	assert.Equal(t, 100*time.Millisecond, info.AvgResponseTime)
	assert.False(t, info.LastInteractionSuccess.IsZero())
}

func TestPeerRegistry_ReputationIncrease_ValidBlockReceived(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-block")

	// Add peer with neutral reputation
	pr.AddPeer(peerID, "")
	initialInfo, _ := pr.GetPeer(peerID)
	initialReputation := initialInfo.ReputationScore

	// Record successful block received
	pr.RecordBlockReceived(peerID, 200*time.Millisecond)

	// Verify reputation increased
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.BlocksReceived)
	assert.Equal(t, int64(1), info.InteractionSuccesses)
	assert.Greater(t, info.ReputationScore, initialReputation, "Reputation should increase after successful block")
	assert.Equal(t, 200*time.Millisecond, info.AvgResponseTime)
	assert.False(t, info.LastInteractionSuccess.IsZero())
}

func TestPeerRegistry_ReputationIncrease_SuccessfulCatchup(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-catchup")

	// Add peer with neutral reputation
	pr.AddPeer(peerID, "")
	pr.UpdateDataHubURL(peerID, "http://test.com")
	initialInfo, _ := pr.GetPeer(peerID)
	initialReputation := initialInfo.ReputationScore

	// Simulate successful catchup of 10 blocks
	SimulateSuccessfulCatchup(pr, peerID, 10)

	// Verify reputation increased substantially
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(10), info.InteractionAttempts)
	assert.Equal(t, int64(10), info.InteractionSuccesses)
	assert.Equal(t, int64(0), info.InteractionFailures)
	assert.Greater(t, info.ReputationScore, 70.0, "Reputation should be high after successful catchup")
	assert.Greater(t, info.ReputationScore, initialReputation, "Reputation should increase significantly")

	// Verify peer is prioritized in catchup selection
	peers := pr.GetPeersForCatchup()
	require.NotEmpty(t, peers)
	assert.Equal(t, peerID, peers[0].ID, "Peer with high reputation should be first for catchup")
}

func TestPeerRegistry_ReputationIncrease_MultipleSuccessfulInteractions(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-multiple")

	// Add peer with low reputation
	pr.AddPeer(peerID, "")
	pr.UpdateReputation(peerID, 30.0)
	pr.UpdateDataHubURL(peerID, "http://test.com")

	// Record multiple successful interactions of different types
	pr.RecordBlockReceived(peerID, 100*time.Millisecond)
	pr.RecordBlockReceived(peerID, 120*time.Millisecond)
	pr.RecordSubtreeReceived(peerID, 80*time.Millisecond)
	pr.RecordSubtreeReceived(peerID, 90*time.Millisecond)
	pr.RecordTransactionReceived(peerID)
	pr.RecordTransactionReceived(peerID)
	pr.RecordInteractionSuccess(peerID, 150*time.Millisecond)
	pr.RecordInteractionSuccess(peerID, 130*time.Millisecond)
	pr.RecordInteractionSuccess(peerID, 110*time.Millisecond)
	pr.RecordInteractionSuccess(peerID, 140*time.Millisecond)

	// Verify reputation gradually increased
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(10), info.InteractionSuccesses)
	assert.Equal(t, int64(2), info.BlocksReceived)
	assert.Equal(t, int64(2), info.SubtreesReceived)
	assert.Equal(t, int64(2), info.TransactionsReceived)

	// With 100% success rate, reputation should be very high
	assert.Greater(t, info.ReputationScore, 80.0, "Reputation should increase towards 100 with consistent success")

	// Success rate should be 100%
	// Note: Some methods like RecordBlockReceived don't increment InteractionAttempts
	// So we calculate based on InteractionSuccesses and InteractionFailures
	totalAttempts := info.InteractionSuccesses + info.InteractionFailures
	if totalAttempts > 0 {
		successRate := float64(info.InteractionSuccesses) / float64(totalAttempts) * 100.0
		assert.Equal(t, 100.0, successRate, "Success rate should be 100%")
	}
}

// =============================================================================
// Reputation Decrease Tests
// =============================================================================

func TestPeerRegistry_ReputationDecrease_InvalidBlockReceived(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-invalid-block")

	// Add peer with good reputation
	pr.AddPeer(peerID, "")
	pr.UpdateReputation(peerID, 80.0)
	initialInfo, _ := pr.GetPeer(peerID)
	initialReputation := initialInfo.ReputationScore

	// Record failed interaction (invalid block)
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionFailure(peerID)

	// Verify reputation decreased
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.InteractionAttempts)
	assert.Equal(t, int64(1), info.InteractionFailures)
	assert.Less(t, info.ReputationScore, initialReputation, "Reputation should decrease after failure")
	assert.False(t, info.LastInteractionFailure.IsZero())
}

func TestPeerRegistry_ReputationDecrease_InvalidForkDetected(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-invalid-fork")

	// Add peer with excellent reputation
	pr.AddPeer(peerID, "")
	pr.UpdateReputation(peerID, 90.0)
	pr.UpdateDataHubURL(peerID, "http://test.com")

	// Record malicious behavior (invalid fork / secret mining)
	SimulateInvalidFork(pr, peerID)

	// Verify reputation dropped to floor
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.MaliciousCount)
	assert.Equal(t, int64(1), info.InteractionFailures)
	assert.Equal(t, 5.0, info.ReputationScore, "Reputation should drop to 5.0 for malicious behavior")

	// Verify peer is excluded or ranked last in catchup selection
	peers := pr.GetPeersForCatchup()
	if len(peers) > 0 {
		// Peer should either not be included or be last
		for i, p := range peers {
			if p.ID == peerID {
				assert.Equal(t, len(peers)-1, i, "Malicious peer should be ranked last")
			}
		}
	}
}

func TestPeerRegistry_ReputationDecrease_MultipleFailures(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-multiple-failures")

	// Add peer with good reputation
	pr.AddPeer(peerID, "")
	pr.UpdateReputation(peerID, 75.0)

	// Record 1 success to establish a baseline
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionSuccess(peerID, 100*time.Millisecond)

	// Record 4 failures within short time window (need > 2 failures since last success)
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionFailure(peerID)
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionFailure(peerID)
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionFailure(peerID)
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionFailure(peerID)

	// Verify harsh penalty for repeated recent failures
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.InteractionSuccesses)
	assert.Equal(t, int64(4), info.InteractionFailures)
	// Reputation should drop significantly due to multiple recent failures
	// The condition triggers when failuresSinceSuccess > 2 (i.e., 4-1=3 > 2)
	assert.Less(t, info.ReputationScore, 20.0, "Reputation should drop to 15.0 for multiple recent failures")
}

func TestPeerRegistry_ReputationDecrease_CatchupFailure(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-catchup-failure")

	// Add peer with neutral reputation
	pr.AddPeer(peerID, "")
	initialInfo, _ := pr.GetPeer(peerID)
	initialReputation := initialInfo.ReputationScore

	// Record catchup failure
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionFailure(peerID)
	pr.UpdateCatchupError(peerID, "validation failed: invalid merkle root")

	// Verify reputation decreased and error is recorded
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.InteractionFailures)
	assert.Less(t, info.ReputationScore, initialReputation, "Reputation should decrease after catchup failure")
	assert.Equal(t, "validation failed: invalid merkle root", info.LastCatchupError)
	assert.False(t, info.LastCatchupErrorTime.IsZero())
}

// =============================================================================
// Reputation Calculation Tests
// =============================================================================

func TestPeerRegistry_ReputationCalculation_SuccessRate(t *testing.T) {
	tests := []struct {
		name           string
		successes      int64
		failures       int64
		expectedMinRep float64
		expectedMaxRep float64
	}{
		{
			name:           "100% success rate",
			successes:      10,
			failures:       0,
			expectedMinRep: 85.0,
			expectedMaxRep: 100.0,
		},
		{
			name:           "75% success rate",
			successes:      6,
			failures:       2,
			expectedMinRep: 55.0,
			expectedMaxRep: 75.0,
		},
		{
			name:           "50% success rate",
			successes:      5,
			failures:       5,
			expectedMinRep: 45.0,
			expectedMaxRep: 55.0,
		},
		{
			name:           "25% success rate",
			successes:      2,
			failures:       6,
			expectedMinRep: 10.0, // Lower due to recency penalty for recent failures
			expectedMaxRep: 45.0,
		},
		{
			name:           "0% success rate (all failures)",
			successes:      0,
			failures:       10,
			expectedMinRep: 0.0,
			expectedMaxRep: 35.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := NewPeerRegistry()
			peerID := peer.ID("test-peer-" + tt.name)

			pr.AddPeer(peerID, "")

			// Record interactions
			for i := int64(0); i < tt.successes; i++ {
				pr.RecordInteractionAttempt(peerID)
				pr.RecordInteractionSuccess(peerID, 100*time.Millisecond)
			}
			for i := int64(0); i < tt.failures; i++ {
				pr.RecordInteractionAttempt(peerID)
				pr.RecordInteractionFailure(peerID)
			}

			// Verify reputation is in expected range
			info, exists := pr.GetPeer(peerID)
			require.True(t, exists)
			assert.GreaterOrEqual(t, info.ReputationScore, tt.expectedMinRep, "Reputation should be above minimum")
			assert.LessOrEqual(t, info.ReputationScore, tt.expectedMaxRep, "Reputation should be below maximum")
		})
	}
}

func TestPeerRegistry_ReputationCalculation_RecencyBonus(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-recency-bonus")

	pr.AddPeer(peerID, "")

	// Establish 50% success rate (5 successes, 5 failures)
	for i := 0; i < 5; i++ {
		pr.RecordInteractionAttempt(peerID)
		pr.RecordInteractionSuccess(peerID, 100*time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		pr.RecordInteractionAttempt(peerID)
		pr.RecordInteractionFailure(peerID)
	}

	// Record baseline reputation before recent success
	pr.peers[peerID].LastInteractionSuccess = time.Now().Add(-2 * time.Hour)
	pr.calculateAndUpdateReputation(pr.peers[peerID])
	baselineReputation := pr.peers[peerID].ReputationScore

	// Now record recent success (within last hour)
	pr.peers[peerID].LastInteractionSuccess = time.Now()
	pr.calculateAndUpdateReputation(pr.peers[peerID])
	newReputation := pr.peers[peerID].ReputationScore

	// Verify recency bonus was applied
	assert.Greater(t, newReputation, baselineReputation, "Recent success should increase reputation via recency bonus")
	// The bonus should be around 10 points
	assert.InDelta(t, 10.0, newReputation-baselineReputation, 5.0, "Recency bonus should be approximately 10 points")
}

func TestPeerRegistry_ReputationCalculation_RecencyPenalty(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-recency-penalty")

	pr.AddPeer(peerID, "")

	// Establish 75% success rate (6 successes, 2 failures)
	for i := 0; i < 6; i++ {
		pr.RecordInteractionAttempt(peerID)
		pr.RecordInteractionSuccess(peerID, 100*time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		pr.RecordInteractionAttempt(peerID)
		pr.RecordInteractionFailure(peerID)
	}

	// Record baseline reputation before recent failure
	pr.peers[peerID].LastInteractionFailure = time.Now().Add(-2 * time.Hour)
	pr.calculateAndUpdateReputation(pr.peers[peerID])
	baselineReputation := pr.peers[peerID].ReputationScore

	// Now record recent failure (within last hour)
	pr.peers[peerID].LastInteractionFailure = time.Now()
	pr.calculateAndUpdateReputation(pr.peers[peerID])
	newReputation := pr.peers[peerID].ReputationScore

	// Verify recency penalty was applied
	assert.Less(t, newReputation, baselineReputation, "Recent failure should decrease reputation via recency penalty")
	// The penalty should be around 15 points
	assert.InDelta(t, 15.0, baselineReputation-newReputation, 5.0, "Recency penalty should be approximately 15 points")
}

func TestPeerRegistry_ReputationCalculation_MaliciousCap(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-malicious-cap")

	pr.AddPeer(peerID, "")
	pr.UpdateReputation(peerID, 90.0)

	// Record malicious behavior
	pr.RecordMaliciousInteraction(peerID)

	info, _ := pr.GetPeer(peerID)
	assert.Equal(t, 5.0, info.ReputationScore, "Reputation should be capped at 5.0 for malicious peers")

	// Try to increase reputation with multiple successes
	for i := 0; i < 10; i++ {
		pr.RecordInteractionAttempt(peerID)
		pr.RecordInteractionSuccess(peerID, 100*time.Millisecond)
	}

	// Reputation should stay at 5.0 while malicious count is > 0
	info, _ = pr.GetPeer(peerID)
	assert.Equal(t, 5.0, info.ReputationScore, "Reputation should remain at 5.0 while malicious count is set")
	assert.Greater(t, info.MaliciousCount, int64(0))
}

// =============================================================================
// Reputation Recovery Tests
// =============================================================================

func TestPeerRegistry_ReconsiderBadPeers_ReputationRecovery(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-recovery")

	pr.AddPeer(peerID, "")

	// Create peer with very low reputation due to failures (90% failure rate)
	// Record 1 success first, then 9 failures (so last interaction is a failure)
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionSuccess(peerID, 100*time.Millisecond)
	for i := 0; i < 9; i++ {
		pr.RecordInteractionAttempt(peerID)
		pr.RecordInteractionFailure(peerID)
	}

	info, _ := pr.GetPeer(peerID)
	t.Logf("Initial reputation: %.2f (should be < 20 with 90%% failure and recent failure penalty)", info.ReputationScore)
	assert.Less(t, info.ReputationScore, 20.0, "Reputation should be very low")

	// Simulate cooldown period passing
	pr.peers[peerID].LastInteractionFailure = time.Now().Add(-25 * time.Hour)

	// Call ReconsiderBadPeers
	recovered := pr.ReconsiderBadPeers(24 * time.Hour)

	// Verify reputation was recovered
	assert.Equal(t, 1, recovered, "Should recover one peer")
	info, _ = pr.GetPeer(peerID)
	assert.Equal(t, 30.0, info.ReputationScore, "Reputation should be reset to 30.0")
	assert.Equal(t, int64(0), info.MaliciousCount, "Malicious count should be cleared")
	assert.False(t, info.LastReputationReset.IsZero())
	assert.Equal(t, 1, info.ReputationResetCount)
}

func TestPeerRegistry_ReconsiderBadPeers_ExponentialCooldown(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-exponential")

	pr.AddPeer(peerID, "")

	// Create peer with low reputation (90% failure rate, last interaction is failure)
	pr.RecordInteractionAttempt(peerID)
	pr.RecordInteractionSuccess(peerID, 100*time.Millisecond)
	for i := 0; i < 9; i++ {
		pr.RecordInteractionAttempt(peerID)
		pr.RecordInteractionFailure(peerID)
	}

	info, _ := pr.GetPeer(peerID)
	t.Logf("Initial reputation: %.2f (should be < 20)", info.ReputationScore)

	// First reset
	pr.peers[peerID].LastInteractionFailure = time.Now().Add(-25 * time.Hour)
	recovered := pr.ReconsiderBadPeers(24 * time.Hour)
	assert.Equal(t, 1, recovered)

	// Lower reputation again
	for i := 0; i < 5; i++ {
		pr.RecordInteractionAttempt(peerID)
		pr.RecordInteractionFailure(peerID)
	}

	// Second reset - needs 3x cooldown
	pr.peers[peerID].LastInteractionFailure = time.Now().Add(-25 * time.Hour)
	recovered = pr.ReconsiderBadPeers(24 * time.Hour)
	assert.Equal(t, 0, recovered, "Should not recover - cooldown not met (needs 3x = 72 hours)")

	// Simulate sufficient cooldown (3x = 72 hours)
	pr.peers[peerID].LastInteractionFailure = time.Now().Add(-73 * time.Hour)
	pr.peers[peerID].LastReputationReset = time.Now().Add(-73 * time.Hour)
	recovered = pr.ReconsiderBadPeers(24 * time.Hour)
	assert.Equal(t, 1, recovered, "Should recover after 3x cooldown period")

	info, _ = pr.GetPeer(peerID)
	assert.Equal(t, 2, info.ReputationResetCount, "Reset count should be 2")
}

func TestPeerRegistry_ReputationRecovery_AfterInvalidBlock(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-recovery-after-invalid")

	// Add peer with good reputation
	pr.AddPeer(peerID, "")
	pr.UpdateReputation(peerID, 80.0)
	pr.UpdateDataHubURL(peerID, "http://test.com")

	// Verify initial state
	initialInfo, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, 80.0, initialInfo.ReputationScore, "Should start with good reputation")

	// ========================================================================
	// STEP 1: Peer sends invalid block - reputation drops to 5.0
	// ========================================================================
	pr.RecordMaliciousInteraction(peerID)

	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, 5.0, info.ReputationScore, "Reputation should drop to 5.0 for malicious behavior (invalid block)")
	assert.Equal(t, int64(1), info.MaliciousCount, "Malicious count should be 1")
	assert.Equal(t, int64(1), info.InteractionFailures, "Should record 1 failure")

	// ========================================================================
	// STEP 2: Verify reputation cannot increase while malicious count > 0
	// ========================================================================
	// Try sending valid blocks - reputation should stay at 5.0
	for i := 0; i < 5; i++ {
		pr.RecordBlockReceived(peerID, 100*time.Millisecond)
	}

	info, _ = pr.GetPeer(peerID)
	assert.Equal(t, 5.0, info.ReputationScore, "Reputation should remain at 5.0 while malicious count is set")
	assert.Equal(t, int64(5), info.BlocksReceived, "Should still track blocks received")
	assert.Equal(t, int64(5), info.InteractionSuccesses, "Should still track successes")

	// ========================================================================
	// STEP 3: After cooldown period, peer reputation is reconsidered
	// ========================================================================
	// Simulate cooldown period passing (25 hours > 24 hour default)
	pr.peers[peerID].LastInteractionFailure = time.Now().Add(-25 * time.Hour)

	// Call ReconsiderBadPeers to give the peer a second chance
	recovered := pr.ReconsiderBadPeers(24 * time.Hour)

	assert.Equal(t, 1, recovered, "Should recover one peer")
	info, _ = pr.GetPeer(peerID)
	assert.Equal(t, 30.0, info.ReputationScore, "Reputation should be reset to 30.0 (second chance)")
	assert.Equal(t, int64(0), info.MaliciousCount, "Malicious count should be cleared")
	assert.False(t, info.LastReputationReset.IsZero(), "Should record reputation reset time")
	assert.Equal(t, 1, info.ReputationResetCount, "Should track number of resets")

	// ========================================================================
	// STEP 4: Peer sends valid blocks/subtrees - reputation increases
	// ========================================================================
	// Record multiple successful interactions
	pr.RecordBlockReceived(peerID, 150*time.Millisecond)
	info, _ = pr.GetPeer(peerID)
	reputationAfterFirstBlock := info.ReputationScore
	assert.Greater(t, reputationAfterFirstBlock, 30.0, "Reputation should increase after valid block")

	pr.RecordSubtreeReceived(peerID, 100*time.Millisecond)
	info, _ = pr.GetPeer(peerID)
	reputationAfterFirstSubtree := info.ReputationScore
	assert.Greater(t, reputationAfterFirstSubtree, reputationAfterFirstBlock, "Reputation should continue to increase")

	// Record several more successful interactions
	for i := 0; i < 8; i++ {
		pr.RecordBlockReceived(peerID, 120*time.Millisecond)
	}

	// Verify final reputation
	info, _ = pr.GetPeer(peerID)
	assert.Greater(t, info.ReputationScore, 60.0, "Reputation should recover significantly with consistent success")
	assert.Equal(t, int64(14), info.BlocksReceived, "Should have 14 blocks received total (5 during malicious + 9 after)")
	assert.Equal(t, int64(1), info.SubtreesReceived, "Should have 1 subtree received")
	assert.Equal(t, int64(15), info.InteractionSuccesses, "Should have 15 total successes")
	assert.Equal(t, int64(1), info.InteractionFailures, "Should still have 1 failure from initial malicious interaction")

	// Calculate success rate
	totalAttempts := info.InteractionSuccesses + info.InteractionFailures
	successRate := float64(info.InteractionSuccesses) / float64(totalAttempts) * 100.0
	assert.Greater(t, successRate, 90.0, "Success rate should be > 90% after recovery")

	// ========================================================================
	// STEP 5: Verify peer is now prioritized in catchup selection
	// ========================================================================
	peers := pr.GetPeersForCatchup()
	require.NotEmpty(t, peers)
	// Peer should be ranked highly (likely first if only peer in registry)
	foundPeer := false
	for i, p := range peers {
		if p.ID == peerID {
			foundPeer = true
			// Peer should be in the top half at least
			assert.Less(t, i, len(peers)/2+1, "Recovered peer with good reputation should be prioritized")
			break
		}
	}
	assert.True(t, foundPeer, "Recovered peer should be included in catchup selection")
}
