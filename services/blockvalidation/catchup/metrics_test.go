package catchup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPeerCatchupMetrics_RecordSuccess(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 50.0,
	}

	metrics.RecordSuccess()

	assert.Equal(t, int64(1), metrics.SuccessfulRequests)
	assert.Equal(t, int64(1), metrics.TotalRequests)
	assert.Equal(t, 0, metrics.ConsecutiveFailures)
	assert.Equal(t, float64(60.0), metrics.ReputationScore) // 50 + 10
	assert.True(t, time.Since(metrics.LastSuccessTime) < time.Second)
}

func TestPeerCatchupMetrics_RecordSuccess_ReputationCap(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 95.0,
	}

	metrics.RecordSuccess()

	assert.Equal(t, float64(100.0), metrics.ReputationScore) // Capped at 100
}

func TestPeerCatchupMetrics_RecordFailure(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 50.0,
	}

	metrics.RecordFailure()

	assert.Equal(t, int64(1), metrics.FailedRequests)
	assert.Equal(t, int64(1), metrics.TotalRequests)
	assert.Equal(t, 1, metrics.ConsecutiveFailures)
	assert.Equal(t, float64(48.0), metrics.ReputationScore) // 50 - 2
	assert.True(t, time.Since(metrics.LastFailureTime) < time.Second)
}

func TestPeerCatchupMetrics_RecordFailure_ReputationFloor(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 0.0,
	}

	metrics.RecordFailure()

	assert.Equal(t, float64(0.0), metrics.ReputationScore) // Floor at 0
}

func TestPeerCatchupMetrics_RecordMaliciousAttempt(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 75.0,
	}

	metrics.RecordMaliciousAttempt()

	assert.Equal(t, int64(1), metrics.MaliciousAttempts)
	assert.Equal(t, float64(0.0), metrics.ReputationScore) // Reset to 0
}

func TestPeerCatchupMetrics_IsTrusted(t *testing.T) {
	tests := []struct {
		name              string
		reputationScore   float64
		maliciousAttempts int64
		expected          bool
	}{
		{
			name:              "trusted peer with high reputation",
			reputationScore:   75.0,
			maliciousAttempts: 0,
			expected:          true,
		},
		{
			name:              "untrusted peer with low reputation",
			reputationScore:   30.0,
			maliciousAttempts: 0,
			expected:          false,
		},
		{
			name:              "untrusted peer with malicious attempts",
			reputationScore:   75.0,
			maliciousAttempts: 1,
			expected:          false,
		},
		{
			name:              "edge case - reputation exactly 50",
			reputationScore:   50.0,
			maliciousAttempts: 0,
			expected:          false,
		},
		{
			name:              "edge case - reputation just above 50",
			reputationScore:   50.1,
			maliciousAttempts: 0,
			expected:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := &PeerCatchupMetrics{
				PeerID:            "test-peer",
				ReputationScore:   tt.reputationScore,
				MaliciousAttempts: tt.maliciousAttempts,
			}

			result := metrics.IsTrusted()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPeerCatchupMetrics_IsMalicious(t *testing.T) {
	tests := []struct {
		name              string
		reputationScore   float64
		maliciousAttempts int64
		expected          bool
	}{
		{
			name:              "malicious peer with low reputation and attempts",
			reputationScore:   5.0,
			maliciousAttempts: 1,
			expected:          true,
		},
		{
			name:              "not malicious - high reputation",
			reputationScore:   75.0,
			maliciousAttempts: 1,
			expected:          false,
		},
		{
			name:              "not malicious - no attempts",
			reputationScore:   5.0,
			maliciousAttempts: 0,
			expected:          false,
		},
		{
			name:              "edge case - reputation exactly 10",
			reputationScore:   10.0,
			maliciousAttempts: 1,
			expected:          false,
		},
		{
			name:              "edge case - reputation just under 10",
			reputationScore:   9.9,
			maliciousAttempts: 1,
			expected:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := &PeerCatchupMetrics{
				PeerID:            "test-peer",
				ReputationScore:   tt.reputationScore,
				MaliciousAttempts: tt.maliciousAttempts,
			}

			result := metrics.IsMalicious()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPeerCatchupMetrics_IsBad(t *testing.T) {
	tests := []struct {
		name            string
		reputationScore float64
		expected        bool
	}{
		{
			name:            "bad peer with very low reputation",
			reputationScore: 5.0,
			expected:        true,
		},
		{
			name:            "good peer with high reputation",
			reputationScore: 75.0,
			expected:        false,
		},
		{
			name:            "edge case - reputation exactly 10",
			reputationScore: 10.0,
			expected:        false,
		},
		{
			name:            "edge case - reputation just under 10",
			reputationScore: 9.9,
			expected:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := &PeerCatchupMetrics{
				PeerID:          "test-peer",
				ReputationScore: tt.reputationScore,
			}

			result := metrics.IsBad()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPeerCatchupMetrics_GetReputation(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 42.5,
	}

	result := metrics.GetReputation()
	assert.Equal(t, 42.5, result)
}

func TestPeerCatchupMetrics_GetMaliciousAttempts(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:            "test-peer",
		MaliciousAttempts: 3,
	}

	result := metrics.GetMaliciousAttempts()
	assert.Equal(t, int64(3), result)
}

func TestPeerCatchupMetrics_UpdateReputation_Success(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:             "test-peer",
		ReputationScore:    75.0,
		SuccessfulRequests: 0,
	}
	responseTime := 100 * time.Millisecond

	metrics.UpdateReputation(true, responseTime)

	assert.Equal(t, 76.0, metrics.ReputationScore)
	assert.Equal(t, 0, metrics.ConsecutiveFailures)
	assert.Equal(t, responseTime, metrics.LastResponseTime)
	assert.Equal(t, responseTime, metrics.AverageResponseTime) // First success
}

func TestPeerCatchupMetrics_UpdateReputation_Success_WithExistingAverage(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:              "test-peer",
		ReputationScore:     75.0,
		SuccessfulRequests:  1,
		AverageResponseTime: 50 * time.Millisecond,
	}
	responseTime := 150 * time.Millisecond

	metrics.UpdateReputation(true, responseTime)

	assert.Equal(t, 76.0, metrics.ReputationScore)
	// Average should be (50ms * 1 + 150ms) / 2 = 100ms
	expectedAverage := (50*time.Millisecond*1 + 150*time.Millisecond) / 2
	assert.Equal(t, expectedAverage, metrics.AverageResponseTime)
}

func TestPeerCatchupMetrics_UpdateReputation_Success_ReputationCap(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 100.0,
	}
	responseTime := 100 * time.Millisecond

	metrics.UpdateReputation(true, responseTime)

	assert.Equal(t, 100.0, metrics.ReputationScore) // Should not exceed 100
}

func TestPeerCatchupMetrics_UpdateReputation_Failure(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 50.0,
	}

	metrics.UpdateReputation(false, 0)

	assert.Equal(t, 48.0, metrics.ReputationScore) // 50 - 2
	assert.Equal(t, 1, metrics.ConsecutiveFailures)
}

func TestPeerCatchupMetrics_UpdateReputation_Failure_ReputationFloor(t *testing.T) {
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 0.0,
	}

	metrics.UpdateReputation(false, 0)

	assert.Equal(t, 0.0, metrics.ReputationScore) // Should not go below 0
	assert.Equal(t, 1, metrics.ConsecutiveFailures)
}

func TestPeerCatchupMetrics_ThreadSafety(t *testing.T) {
	// Test basic thread safety by calling methods sequentially
	metrics := &PeerCatchupMetrics{
		PeerID:          "test-peer",
		ReputationScore: 50.0,
	}

	// Verify mutex protects data by checking consistent state
	metrics.RecordSuccess()
	metrics.RecordFailure()
	metrics.RecordMaliciousAttempt()

	// Check state is consistent
	assert.Equal(t, int64(1), metrics.SuccessfulRequests)
	assert.Equal(t, int64(1), metrics.FailedRequests)
	assert.Equal(t, int64(2), metrics.TotalRequests)
	assert.Equal(t, int64(1), metrics.MaliciousAttempts)
	assert.Equal(t, 0.0, metrics.ReputationScore) // Reset by malicious attempt

	// Test read methods
	_ = metrics.IsTrusted()
	_ = metrics.IsMalicious()
	_ = metrics.IsBad()
	_ = metrics.GetReputation()
	_ = metrics.GetMaliciousAttempts()
}

func TestNewCatchupMetrics(t *testing.T) {
	metrics := NewCatchupMetrics()

	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.PeerMetrics)
	assert.Equal(t, 0, len(metrics.PeerMetrics))
}

func TestCatchupMetrics_GetOrCreatePeerMetrics(t *testing.T) {
	metrics := NewCatchupMetrics()
	peerID := "test-peer-123"

	// First call should create new metrics
	peerMetrics1 := metrics.GetOrCreatePeerMetrics(peerID)
	assert.NotNil(t, peerMetrics1)
	assert.Equal(t, peerID, peerMetrics1.PeerID)
	assert.Equal(t, 50.0, peerMetrics1.ReputationScore) // Default neutral reputation
	assert.Equal(t, 1, len(metrics.PeerMetrics))

	// Second call should return existing metrics
	peerMetrics2 := metrics.GetOrCreatePeerMetrics(peerID)
	assert.Same(t, peerMetrics1, peerMetrics2) // Should be the same instance
	assert.Equal(t, 1, len(metrics.PeerMetrics))

	// Different peer should create new metrics
	peerMetrics3 := metrics.GetOrCreatePeerMetrics("different-peer")
	assert.NotSame(t, peerMetrics1, peerMetrics3)
	assert.Equal(t, "different-peer", peerMetrics3.PeerID)
	assert.Equal(t, 2, len(metrics.PeerMetrics))
}

func TestCatchupMetrics_GetPeerMetrics(t *testing.T) {
	metrics := NewCatchupMetrics()
	peerID := "test-peer-456"

	// Getting non-existent peer should return nil and false
	peerMetrics, exists := metrics.GetPeerMetrics(peerID)
	assert.Nil(t, peerMetrics)
	assert.False(t, exists)

	// Create a peer
	createdMetrics := metrics.GetOrCreatePeerMetrics(peerID)
	createdMetrics.ReputationScore = 75.0

	// Getting existing peer should return metrics and true
	peerMetrics, exists = metrics.GetPeerMetrics(peerID)
	assert.NotNil(t, peerMetrics)
	assert.True(t, exists)
	assert.Same(t, createdMetrics, peerMetrics)
	assert.Equal(t, 75.0, peerMetrics.ReputationScore)
}

func TestCatchupMetrics_ThreadSafety(t *testing.T) {
	// Test basic thread safety by calling methods sequentially
	metrics := NewCatchupMetrics()

	// Verify mutex protects collection operations
	peer1 := "peer1"
	peer2 := "peer2"

	metrics1 := metrics.GetOrCreatePeerMetrics(peer1)
	metrics2 := metrics.GetOrCreatePeerMetrics(peer2)
	metrics1.RecordSuccess()
	metrics2.RecordFailure()

	// Verify operations worked correctly
	retrievedMetrics1, exists1 := metrics.GetPeerMetrics(peer1)
	retrievedMetrics2, exists2 := metrics.GetPeerMetrics(peer2)

	assert.True(t, exists1)
	assert.True(t, exists2)
	assert.Equal(t, int64(1), retrievedMetrics1.SuccessfulRequests)
	assert.Equal(t, int64(1), retrievedMetrics2.FailedRequests)
	assert.Equal(t, 2, len(metrics.PeerMetrics))
}

func TestPeerCatchupMetrics_IntegrationScenario(t *testing.T) {
	// Test a realistic scenario with multiple operations
	metrics := &PeerCatchupMetrics{
		PeerID:          "integration-peer",
		ReputationScore: 50.0,
	}

	// Scenario: peer starts neutral, has some successes, then a failure, then malicious behavior

	// Initial state
	assert.Equal(t, 50.0, metrics.ReputationScore)
	assert.False(t, metrics.IsTrusted()) // 50 is not > 50
	assert.False(t, metrics.IsBad())
	assert.False(t, metrics.IsMalicious())

	// Multiple successes
	for range 3 {
		metrics.RecordSuccess()
	}
	assert.Equal(t, 80.0, metrics.ReputationScore) // 50 + 3*10
	assert.True(t, metrics.IsTrusted())
	assert.False(t, metrics.IsBad())
	assert.False(t, metrics.IsMalicious())

	// Some failures
	for range 2 {
		metrics.RecordFailure()
	}
	assert.Equal(t, 76.0, metrics.ReputationScore) // 80 - 2*2
	assert.True(t, metrics.IsTrusted())
	assert.Equal(t, 2, metrics.ConsecutiveFailures)

	// Update reputation with mixed results
	metrics.UpdateReputation(true, 100*time.Millisecond)
	assert.Equal(t, 77.0, metrics.ReputationScore)  // 76 + 1
	assert.Equal(t, 0, metrics.ConsecutiveFailures) // Reset on success

	// Malicious behavior
	metrics.RecordMaliciousAttempt()
	assert.Equal(t, 0.0, metrics.ReputationScore)
	assert.False(t, metrics.IsTrusted())
	assert.True(t, metrics.IsBad())
	assert.True(t, metrics.IsMalicious()) // Low reputation AND malicious attempts
	assert.Equal(t, int64(1), metrics.GetMaliciousAttempts())
}

func TestCatchupMetrics_IntegrationScenario(t *testing.T) {
	// Test realistic scenario with multiple peers
	catchupMetrics := NewCatchupMetrics()

	// Add several peers with different behaviors
	goodPeer := catchupMetrics.GetOrCreatePeerMetrics("good-peer")
	badPeer := catchupMetrics.GetOrCreatePeerMetrics("bad-peer")
	maliciousPeer := catchupMetrics.GetOrCreatePeerMetrics("malicious-peer")

	// Good peer: multiple successes
	for range 5 {
		goodPeer.RecordSuccess()
	}
	assert.True(t, goodPeer.IsTrusted())
	assert.False(t, goodPeer.IsBad())

	// Bad peer: multiple failures
	for range 25 {
		badPeer.RecordFailure()
	}
	assert.False(t, badPeer.IsTrusted())
	assert.True(t, badPeer.IsBad())

	// Malicious peer: malicious behavior
	maliciousPeer.RecordMaliciousAttempt()
	assert.False(t, maliciousPeer.IsTrusted())
	assert.True(t, maliciousPeer.IsBad())
	assert.True(t, maliciousPeer.IsMalicious())

	// Verify all peers are tracked
	assert.Equal(t, 3, len(catchupMetrics.PeerMetrics))

	// Test retrieval
	retrievedGood, exists := catchupMetrics.GetPeerMetrics("good-peer")
	assert.True(t, exists)
	assert.Same(t, goodPeer, retrievedGood)

	retrievedNonExistent, exists := catchupMetrics.GetPeerMetrics("non-existent")
	assert.False(t, exists)
	assert.Nil(t, retrievedNonExistent)
}
