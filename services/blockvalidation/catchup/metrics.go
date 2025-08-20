package catchup

import (
	"sync"
	"time"
)

// PeerCatchupMetrics tracks performance and reputation metrics for a specific peer during catchup
type PeerCatchupMetrics struct {
	mu sync.RWMutex

	// Identification
	PeerURL string

	// Request statistics
	SuccessfulRequests int64
	FailedRequests     int64
	TotalRequests      int64

	// Performance metrics
	AverageResponseTime time.Duration
	LastResponseTime    time.Duration

	// Reputation tracking
	ReputationScore     float64
	MaliciousAttempts   int64
	ConsecutiveFailures int

	// Timestamps
	LastSuccessTime time.Time
	LastFailureTime time.Time
	LastRequestTime time.Time

	// Data tracking
	TotalHeadersFetched int64
}

// RecordSuccess records a successful request
func (pm *PeerCatchupMetrics) RecordSuccess() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.SuccessfulRequests++
	pm.TotalRequests++
	pm.ConsecutiveFailures = 0
	pm.LastSuccessTime = time.Now()

	// Improve reputation on success
	if pm.ReputationScore < 100 {
		pm.ReputationScore += 1.0
	}
}

// RecordFailure records a failed request
func (pm *PeerCatchupMetrics) RecordFailure() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.FailedRequests++
	pm.TotalRequests++
	pm.ConsecutiveFailures++
	pm.LastFailureTime = time.Now()

	// Decrease reputation on failure
	if pm.ReputationScore > 0 {
		pm.ReputationScore -= 2.0
	}
}

// RecordMaliciousAttempt records detected malicious behavior
func (pm *PeerCatchupMetrics) RecordMaliciousAttempt() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.MaliciousAttempts++
	// Significant reputation penalty for malicious behavior
	pm.ReputationScore = 0
}

// IsTrusted returns whether the peer is considered trusted
func (pm *PeerCatchupMetrics) IsTrusted() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.ReputationScore > 50 && pm.MaliciousAttempts == 0
}

// IsMalicious returns whether the peer is malicious
func (pm *PeerCatchupMetrics) IsMalicious() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.ReputationScore < 30 && pm.MaliciousAttempts > 0
}

// UpdateReputation updates reputation based on success/failure and response time
func (pm *PeerCatchupMetrics) UpdateReputation(success bool, responseTime time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if success {
		// Improve reputation on success
		if pm.ReputationScore < 100 {
			pm.ReputationScore += 1.0
		}
		pm.ConsecutiveFailures = 0
		pm.LastResponseTime = responseTime

		// Update average response time
		if pm.AverageResponseTime == 0 {
			pm.AverageResponseTime = responseTime
		} else {
			// Weighted average
			pm.AverageResponseTime = (pm.AverageResponseTime*time.Duration(pm.SuccessfulRequests) + responseTime) / time.Duration(pm.SuccessfulRequests+1)
		}
	} else {
		// Decrease reputation on failure
		if pm.ReputationScore > 0 {
			pm.ReputationScore -= 2.0
		}
		pm.ConsecutiveFailures++
	}
}

// CatchupMetrics manages metrics for all peers involved in catchup
type CatchupMetrics struct {
	mu          sync.RWMutex
	PeerMetrics map[string]*PeerCatchupMetrics
}

// NewCatchupMetrics creates a new CatchupMetrics instance
func NewCatchupMetrics() *CatchupMetrics {
	return &CatchupMetrics{
		PeerMetrics: make(map[string]*PeerCatchupMetrics),
	}
}

// GetOrCreatePeerMetrics gets or creates metrics for a peer
func (cm *CatchupMetrics) GetOrCreatePeerMetrics(peerURL string) *PeerCatchupMetrics {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if metric, exists := cm.PeerMetrics[peerURL]; exists {
		return metric
	}

	metric := &PeerCatchupMetrics{
		PeerURL:         peerURL,
		ReputationScore: 50.0, // Start with neutral reputation
	}
	cm.PeerMetrics[peerURL] = metric
	return metric
}

// GetPeerMetrics safely retrieves metrics for a peer if they exist
func (cm *CatchupMetrics) GetPeerMetrics(peerURL string) (*PeerCatchupMetrics, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	metric, exists := cm.PeerMetrics[peerURL]
	return metric, exists
}
