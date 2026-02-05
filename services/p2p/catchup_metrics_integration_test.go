// Integration tests for distributed catchup metrics system
// Tests the full flow: BlockValidation → P2P Client → P2P Service → Peer Registry
package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDistributedCatchupMetrics_RecordAttempt tests recording catchup attempts
// through the distributed system
func TestDistributedCatchupMetrics_RecordAttempt(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry with all data atomically
	testHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	p2pRegistry.Put(testPeerID, "", 1000, testHash, "http://localhost:8090")

	// Verify initial state
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	require.NotNil(t, info)
	assert.Equal(t, int64(0), info.InteractionAttempts)

	// Record catchup attempt via gRPC handler
	req := &p2p_api.RecordCatchupAttemptRequest{
		PeerId: testPeerID.String(),
	}
	resp, err := p2pServer.RecordCatchupAttempt(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Ok)

	// Verify attempt was recorded
	info, exists = p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.InteractionAttempts)
	assert.False(t, info.LastInteractionAttempt.IsZero())
}

// TestDistributedCatchupMetrics_RecordSuccess tests recording catchup success
// with duration tracking
func TestDistributedCatchupMetrics_RecordSuccess(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry
	p2pRegistry.Put(testPeerID, "", 1000, nil, "http://localhost:8090")

	// Record first success with 100ms duration
	req1 := &p2p_api.RecordCatchupSuccessRequest{
		PeerId:     testPeerID.String(),
		DurationMs: 100,
	}
	resp1, err := p2pServer.RecordCatchupSuccess(ctx, req1)
	require.NoError(t, err)
	assert.True(t, resp1.Ok)

	// Verify success was recorded
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.InteractionSuccesses)
	assert.Equal(t, 100*time.Millisecond, info.AvgResponseTime)
	assert.False(t, info.LastInteractionSuccess.IsZero())

	// Record second success with 200ms duration
	req2 := &p2p_api.RecordCatchupSuccessRequest{
		PeerId:     testPeerID.String(),
		DurationMs: 200,
	}
	resp2, err := p2pServer.RecordCatchupSuccess(ctx, req2)
	require.NoError(t, err)
	assert.True(t, resp2.Ok)

	// Verify weighted average: 80% of 100ms + 20% of 200ms = 120ms
	info, exists = p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(2), info.InteractionSuccesses)
	expectedAvg := time.Duration(int64(float64(100*time.Millisecond)*0.8 + float64(200*time.Millisecond)*0.2))
	assert.Equal(t, expectedAvg, info.AvgResponseTime)
}

// TestDistributedCatchupMetrics_RecordFailure tests recording catchup failures
func TestDistributedCatchupMetrics_RecordFailure(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry
	p2pRegistry.Put(testPeerID, "", 1000, nil, "http://localhost:8090")

	// Record failure via gRPC handler
	req := &p2p_api.RecordCatchupFailureRequest{
		PeerId: testPeerID.String(),
	}
	resp, err := p2pServer.RecordCatchupFailure(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Ok)

	// Verify failure was recorded
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.InteractionFailures)
	assert.False(t, info.LastInteractionFailure.IsZero())
}

// TestDistributedCatchupMetrics_RecordMalicious tests recording malicious behavior
func TestDistributedCatchupMetrics_RecordMalicious(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry
	p2pRegistry.Put(testPeerID, "", 1000, nil, "http://localhost:8090")

	// Record malicious behavior
	req := &p2p_api.RecordCatchupMaliciousRequest{
		PeerId: testPeerID.String(),
	}
	resp, err := p2pServer.RecordCatchupMalicious(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Ok)

	// Verify malicious count was incremented
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.MaliciousCount)
}

// TestDistributedCatchupMetrics_UpdateReputation tests updating reputation scores
func TestDistributedCatchupMetrics_UpdateReputation(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry
	p2pRegistry.Put(testPeerID, "", 1000, nil, "http://localhost:8090")

	// Update reputation score
	req := &p2p_api.UpdateCatchupReputationRequest{
		PeerId: testPeerID.String(),
		Score:  75.5,
	}
	resp, err := p2pServer.UpdateCatchupReputation(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Ok)

	// Verify reputation was updated
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, 75.5, info.ReputationScore)
}

// TestDistributedCatchupMetrics_GetPeersForCatchup tests intelligent peer selection
func TestDistributedCatchupMetrics_GetPeersForCatchup(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Create test peers with different reputation scores
	peer1ID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
	peer2ID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")
	peer3ID, _ := peer.Decode("12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd")

	// Add peers with different characteristics
	p2pRegistry.Put(peer1ID, "", 1000, nil, "http://peer1:8090")
	p2pRegistry.UpdateReputation(peer1ID, 95.0) // Best

	p2pRegistry.Put(peer2ID, "", 1001, nil, "http://peer2:8090")
	p2pRegistry.UpdateReputation(peer2ID, 85.0) // Second best

	p2pRegistry.Put(peer3ID, "", 999, nil, "http://peer3:8090")
	p2pRegistry.UpdateReputation(peer3ID, 75.0) // Third

	// Query for peers suitable for catchup
	req := &p2p_api.GetPeersForCatchupRequest{}
	resp, err := p2pServer.GetPeersForCatchup(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 3, len(resp.Peers), "Expected 3 peers to be returned")

	// Verify peers are sorted by reputation (highest first)
	if len(resp.Peers) >= 1 {
		assert.Equal(t, peer1ID.String(), resp.Peers[0].Id)
		assert.Equal(t, 95.0, resp.Peers[0].CatchupReputationScore)
	}
	if len(resp.Peers) >= 2 {
		assert.Equal(t, peer2ID.String(), resp.Peers[1].Id)
		assert.Equal(t, 85.0, resp.Peers[1].CatchupReputationScore)
	}
	if len(resp.Peers) >= 3 {
		assert.Equal(t, peer3ID.String(), resp.Peers[2].Id)
		assert.Equal(t, 75.0, resp.Peers[2].CatchupReputationScore)
	}
}

// TestDistributedCatchupMetrics_ReputationCalculation tests the reputation
// calculation algorithm through multiple operations
func TestDistributedCatchupMetrics_ReputationCalculation(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry
	p2pRegistry.Put(testPeerID, "", 1000, nil, "http://localhost:8090")

	// Initial reputation should be 50.0 (neutral)
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, 50.0, info.ReputationScore)

	// Record first successful catchup - reputation should be automatically calculated
	p2pRegistry.RecordCatchupSuccess(testPeerID, 100*time.Millisecond)
	info, _ = p2pRegistry.Get(testPeerID)
	assert.Equal(t, int64(1), info.InteractionSuccesses)
	// With 100% success rate (1/1), reputation should be high
	// Formula: (100 * 0.6 + 50 * 0.4 + 10 (recency bonus)) * 1.2 (speed factor for <200ms) = (60 + 20 + 10) * 1.2 = 108, clamped to 100
	assert.Equal(t, 100.0, info.ReputationScore, "First success with fast response should give 100 reputation")

	// Record more successful catchups
	for i := 0; i < 4; i++ {
		p2pRegistry.RecordCatchupSuccess(testPeerID, 100*time.Millisecond)
	}

	// Verify successes were recorded and reputation is still high
	info, _ = p2pRegistry.Get(testPeerID)
	assert.Equal(t, int64(5), info.InteractionSuccesses)
	// Still 100% success rate, reputation should remain high
	assert.Greater(t, info.ReputationScore, 85.0, "Perfect success rate should maintain high reputation")

	// Record a failure - reputation should decrease
	p2pRegistry.RecordCatchupFailure(testPeerID)
	info, _ = p2pRegistry.Get(testPeerID)
	assert.Equal(t, int64(1), info.InteractionFailures)
	// Success rate is now 5/6 = 83.3%
	// Formula: 83.3 * 0.6 + 50 * 0.4 = 50 + 20 = 70 (roughly)
	assert.Less(t, info.ReputationScore, 90.0, "Failure should decrease reputation")
	assert.Greater(t, info.ReputationScore, 60.0, "One failure shouldn't tank reputation")

	// Record malicious behavior - reputation should drop significantly
	previousScore := info.ReputationScore
	p2pRegistry.RecordCatchupMalicious(testPeerID)
	info, _ = p2pRegistry.Get(testPeerID)
	assert.Equal(t, int64(1), info.MaliciousCount)
	// Malicious penalty is -20 per occurrence
	assert.Less(t, info.ReputationScore, previousScore-15.0, "Malicious behavior should significantly reduce reputation")

	// Test manual reputation score update via gRPC
	req := &p2p_api.UpdateCatchupReputationRequest{
		PeerId: testPeerID.String(),
		Score:  95.5,
	}
	resp, err := p2pServer.UpdateCatchupReputation(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Ok)

	info, _ = p2pRegistry.Get(testPeerID)
	assert.Equal(t, 95.5, info.ReputationScore)

	// Reputation should never exceed 100 (clamping test)
	req = &p2p_api.UpdateCatchupReputationRequest{
		PeerId: testPeerID.String(),
		Score:  150.0,
	}
	resp, err = p2pServer.UpdateCatchupReputation(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Ok)

	info, _ = p2pRegistry.Get(testPeerID)
	assert.Equal(t, 100.0, info.ReputationScore)

	// Reputation should never go below 0 (clamping test)
	req = &p2p_api.UpdateCatchupReputationRequest{
		PeerId: testPeerID.String(),
		Score:  -50.0,
	}
	resp, err = p2pServer.UpdateCatchupReputation(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Ok)

	info, _ = p2pRegistry.Get(testPeerID)
	assert.Equal(t, 0.0, info.ReputationScore)
}

// TestDistributedCatchupMetrics_ConcurrentUpdates tests that concurrent
// metric updates are handled correctly
func TestDistributedCatchupMetrics_ConcurrentUpdates(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry
	p2pRegistry.Put(testPeerID, "", 1000, nil, "http://localhost:8090")

	// Simulate concurrent updates from multiple BlockValidation instances
	const numGoroutines = 10
	const updatesPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < updatesPerGoroutine; j++ {
				// Randomly record success or failure
				if j%2 == 0 {
					req := &p2p_api.RecordCatchupSuccessRequest{
						PeerId:     testPeerID.String(),
						DurationMs: 100,
					}
					_, _ = p2pServer.RecordCatchupSuccess(ctx, req)
				} else {
					req := &p2p_api.RecordCatchupFailureRequest{
						PeerId: testPeerID.String(),
					}
					_, _ = p2pServer.RecordCatchupFailure(ctx, req)
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify final counts
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	expectedSuccesses := int64(numGoroutines * updatesPerGoroutine / 2)
	expectedFailures := int64(numGoroutines * updatesPerGoroutine / 2)

	assert.Equal(t, expectedSuccesses, info.InteractionSuccesses)
	assert.Equal(t, expectedFailures, info.InteractionFailures)
	assert.False(t, info.LastInteractionSuccess.IsZero())
	assert.False(t, info.LastInteractionFailure.IsZero())
}

// TestDistributedCatchupMetrics_InvalidPeerID tests error handling for invalid peer IDs
func TestDistributedCatchupMetrics_InvalidPeerID(t *testing.T) {
	ctx := context.Background()

	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
	}

	// Try to record attempt with invalid peer ID
	req := &p2p_api.RecordCatchupAttemptRequest{
		PeerId: "invalid_peer_id",
	}
	resp, err := p2pServer.RecordCatchupAttempt(ctx, req)

	// Should return error
	assert.Error(t, err)
	assert.False(t, resp.Ok)
}

// TestDistributedCatchupMetrics_NilRegistry tests error handling when registry is not initialized
func TestDistributedCatchupMetrics_NilRegistry(t *testing.T) {
	ctx := context.Background()

	// Create P2P server without registry
	p2pServer := &Server{}

	// Try to record attempt with valid peer ID format
	req := &p2p_api.RecordCatchupAttemptRequest{
		PeerId: "12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW",
	}
	resp, err := p2pServer.RecordCatchupAttempt(ctx, req)

	// Should return error
	assert.Error(t, err)
	assert.False(t, resp.Ok)
}

// TestReportValidSubtree_IncreasesReputation tests that reporting a valid subtree
// increases a peer's reputation score through the automatic reputation calculation
func TestReportValidSubtree_IncreasesReputation(t *testing.T) {
	// Create P2P service with peer registry
	p2pRegistry := NewPeerRegistry()

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry with initial state
	p2pRegistry.Put(testPeerID, "", 1000, nil, "http://localhost:8090")

	// Verify initial reputation (should be neutral at 50.0)
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, 50.0, info.ReputationScore, "New peer should start with neutral reputation of 50.0")
	assert.Equal(t, int64(0), info.SubtreesReceived)
	assert.Equal(t, int64(0), info.InteractionSuccesses)

	// Simulate receiving a valid subtree by directly calling RecordSubtreeReceived
	// (which is what reportValidSubtreeInternal does)
	duration := 150 * time.Millisecond
	p2pRegistry.RecordSubtreeReceived(testPeerID, duration)

	// Verify subtree was recorded
	info, exists = p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.SubtreesReceived)
	assert.Equal(t, int64(1), info.InteractionSuccesses)
	assert.Equal(t, duration, info.AvgResponseTime)
	assert.False(t, info.LastInteractionSuccess.IsZero())

	// Verify reputation increased due to successful interaction
	// With 100% success rate (1 success / 1 total):
	// Formula: (100 * 0.6 + 50 * 0.4 + 10 (recency bonus)) * 1.2 (speed factor for <200ms) = (60 + 20 + 10) * 1.2 = 108, clamped to 100
	assert.Equal(t, 100.0, info.ReputationScore, "First successful subtree with fast response should increase reputation to 100")

	// Record multiple successful subtrees
	for i := 0; i < 4; i++ {
		p2pRegistry.RecordSubtreeReceived(testPeerID, 100*time.Millisecond)
	}

	// Verify reputation remains high with perfect success rate
	info, exists = p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(5), info.SubtreesReceived)
	assert.Equal(t, int64(5), info.InteractionSuccesses)
	assert.Greater(t, info.ReputationScore, 85.0, "Multiple successful subtrees should maintain high reputation")

	// Record a failure to see reputation decrease
	p2pRegistry.RecordInteractionFailure(testPeerID)
	info, exists = p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.InteractionFailures)
	// Success rate is now 5/6 = 83.3%
	assert.Less(t, info.ReputationScore, 90.0, "Failure should decrease reputation")
	assert.Greater(t, info.ReputationScore, 60.0, "One failure shouldn't dramatically reduce reputation with good history")

	// Verify average response time calculation
	// Should be weighted average: 80% previous + 20% new
	assert.Greater(t, info.AvgResponseTime, time.Duration(0), "Average response time should be tracked")
}

// TestReportValidSubtree_GRPCEndpoint tests the gRPC endpoint validation
func TestReportValidSubtree_GRPCEndpoint(t *testing.T) {
	ctx := context.Background()

	// Create P2P service
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
		logger:       ulogger.TestLogger{},
	}

	// Create a test peer
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err)

	// Add peer to registry
	p2pRegistry.Put(testPeerID, "", 0, nil, "")

	// Test valid request returns success
	req := &p2p_api.ReportValidSubtreeRequest{
		PeerId:      testPeerID.String(),
		SubtreeHash: "test_subtree_hash_123",
	}
	resp, err := p2pServer.ReportValidSubtree(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "subtree validation recorded", resp.Message)

	// Verify peer metrics were updated
	info, exists := p2pRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, int64(1), info.SubtreesReceived)
	assert.Equal(t, int64(1), info.InteractionSuccesses)
}

// TestReportValidSubtree_MissingHash tests error handling when subtree hash is missing
func TestReportValidSubtree_MissingHash(t *testing.T) {
	ctx := context.Background()

	// Create P2P service
	p2pRegistry := NewPeerRegistry()
	p2pServer := &Server{
		peerRegistry: p2pRegistry,
		logger:       ulogger.TestLogger{},
	}

	// Test missing peer ID
	req1 := &p2p_api.ReportValidSubtreeRequest{
		PeerId:      "",
		SubtreeHash: "test_hash",
	}
	resp1, err1 := p2pServer.ReportValidSubtree(ctx, req1)
	assert.Error(t, err1)
	assert.False(t, resp1.Success)
	assert.Contains(t, resp1.Message, "peer ID is required")

	// Test missing subtree hash
	req2 := &p2p_api.ReportValidSubtreeRequest{
		PeerId:      "12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW",
		SubtreeHash: "",
	}
	resp2, err2 := p2pServer.ReportValidSubtree(ctx, req2)
	assert.Error(t, err2)
	assert.False(t, resp2.Success)
	assert.Contains(t, resp2.Message, "subtree hash is required")
}
