package catchup

import (
	"math"
	"sync"
	"time"
)

// PeerMetrics tracks performance metrics for individual peers during catchup operations
type PeerMetrics struct {
	mu sync.RWMutex

	// Request statistics
	SuccessfulRequests int64
	FailedRequests     int64
	TotalRequests      int64

	// Performance metrics
	AverageResponseTime time.Duration
	FastestResponse     time.Duration
	SlowestResponse     time.Duration
	LastResponseTime    time.Duration

	// Data transfer metrics
	BytesReceived   int64
	HeadersReceived int64

	// Reliability metrics
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	LastFailureTime      time.Time
	LastSuccessTime      time.Time

	// Trust metrics
	MaliciousAttempts   int64
	InvalidDataReceived int64
	TrustScore          float64

	// Circuit breaker state
	CircuitBreakerState CircuitBreakerState
	CircuitBreakerTrips int
}

// NewPeerMetrics creates a new peer metrics instance
func NewPeerMetrics() *PeerMetrics {
	return &PeerMetrics{
		TrustScore:          1.0,       // Start with full trust
		FastestResponse:     time.Hour, // Initialize with high value
		CircuitBreakerState: StateClosed,
	}
}

// RecordSuccess records a successful request
func (pm *PeerMetrics) RecordSuccess(responseTime time.Duration, bytesReceived int64, headersReceived int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.SuccessfulRequests++
	pm.TotalRequests++
	pm.ConsecutiveSuccesses++
	pm.ConsecutiveFailures = 0
	pm.LastSuccessTime = time.Now()
	pm.LastResponseTime = responseTime
	pm.BytesReceived += bytesReceived
	pm.HeadersReceived += int64(headersReceived)

	// Update response time metrics
	if responseTime < pm.FastestResponse {
		pm.FastestResponse = responseTime
	}
	if responseTime > pm.SlowestResponse {
		pm.SlowestResponse = responseTime
	}

	// Calculate rolling average
	if pm.AverageResponseTime == 0 {
		pm.AverageResponseTime = responseTime
	} else {
		weight := 0.9 // Weight for existing average
		pm.AverageResponseTime = time.Duration(
			float64(pm.AverageResponseTime)*weight + float64(responseTime)*(1-weight),
		)
	}

	// Improve trust score on success
	pm.TrustScore = math.Min(1.0, pm.TrustScore*1.01)
}

// RecordFailure records a failed request
func (pm *PeerMetrics) RecordFailure() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.FailedRequests++
	pm.TotalRequests++
	pm.ConsecutiveFailures++
	pm.ConsecutiveSuccesses = 0
	pm.LastFailureTime = time.Now()

	// Decrease trust score on failure
	pm.TrustScore = math.Max(0, pm.TrustScore*0.95)
}

// RecordMaliciousActivity records detected malicious behavior
func (pm *PeerMetrics) RecordMaliciousActivity() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.MaliciousAttempts++
	pm.TrustScore = math.Max(0, pm.TrustScore*0.5) // Significant trust penalty
}

// RecordInvalidData records receipt of invalid data
func (pm *PeerMetrics) RecordInvalidData() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.InvalidDataReceived++
	pm.TrustScore = math.Max(0, pm.TrustScore*0.8)
}

// GetSuccessRate returns the success rate as a percentage
func (pm *PeerMetrics) GetSuccessRate() float64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.TotalRequests == 0 {
		return 0
	}
	return float64(pm.SuccessfulRequests) / float64(pm.TotalRequests) * 100
}

// IsTrusted returns whether the peer is considered trusted
func (pm *PeerMetrics) IsTrusted() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Consider a peer trusted if:
	// - Trust score is above threshold
	// - Has successful requests
	// - Not too many consecutive failures
	// - No recent malicious attempts
	return pm.TrustScore > 0.5 &&
		pm.SuccessfulRequests > 0 &&
		pm.ConsecutiveFailures < 5 &&
		pm.MaliciousAttempts == 0
}

// IsHealthy returns whether the peer is considered healthy for use
func (pm *PeerMetrics) IsHealthy() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Check if peer is responding well
	recentFailure := time.Since(pm.LastFailureTime) < 1*time.Minute
	recentSuccess := time.Since(pm.LastSuccessTime) < 5*time.Minute
	goodSuccessRate := pm.GetSuccessRate() > 50

	return !recentFailure && (recentSuccess || goodSuccessRate) && pm.CircuitBreakerState != StateOpen
}

// GetStats returns a copy of the current metrics
func (pm *PeerMetrics) GetStats() PeerMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Create a copy without the mutex
	return PeerMetrics{
		SuccessfulRequests:   pm.SuccessfulRequests,
		FailedRequests:       pm.FailedRequests,
		TotalRequests:        pm.TotalRequests,
		AverageResponseTime:  pm.AverageResponseTime,
		FastestResponse:      pm.FastestResponse,
		SlowestResponse:      pm.SlowestResponse,
		LastResponseTime:     pm.LastResponseTime,
		BytesReceived:        pm.BytesReceived,
		HeadersReceived:      pm.HeadersReceived,
		ConsecutiveFailures:  pm.ConsecutiveFailures,
		ConsecutiveSuccesses: pm.ConsecutiveSuccesses,
		LastFailureTime:      pm.LastFailureTime,
		LastSuccessTime:      pm.LastSuccessTime,
		MaliciousAttempts:    pm.MaliciousAttempts,
		InvalidDataReceived:  pm.InvalidDataReceived,
		TrustScore:           pm.TrustScore,
		CircuitBreakerState:  pm.CircuitBreakerState,
		CircuitBreakerTrips:  pm.CircuitBreakerTrips,
	}
}

// Reset resets all metrics
func (pm *PeerMetrics) Reset() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	*pm = PeerMetrics{
		TrustScore:          1.0,
		FastestResponse:     time.Hour,
		CircuitBreakerState: StateClosed,
	}
}

// PeerMetricsCollection manages metrics for multiple peers
type PeerMetricsCollection struct {
	mu          sync.RWMutex
	PeerMetrics map[string]*PeerMetrics
}

// NewPeerMetricsCollection creates a new collection
func NewPeerMetricsCollection() *PeerMetricsCollection {
	return &PeerMetricsCollection{
		PeerMetrics: make(map[string]*PeerMetrics),
	}
}

// GetOrCreate gets metrics for a peer, creating if necessary
func (pmc *PeerMetricsCollection) GetOrCreate(peerURL string) *PeerMetrics {
	pmc.mu.Lock()
	defer pmc.mu.Unlock()

	if metrics, exists := pmc.PeerMetrics[peerURL]; exists {
		return metrics
	}

	metrics := NewPeerMetrics()
	pmc.PeerMetrics[peerURL] = metrics
	return metrics
}

// GetHealthyPeers returns a list of healthy peer URLs
func (pmc *PeerMetricsCollection) GetHealthyPeers() []string {
	pmc.mu.RLock()
	defer pmc.mu.RUnlock()

	var healthy []string
	for url, metrics := range pmc.PeerMetrics {
		if metrics.IsHealthy() {
			healthy = append(healthy, url)
		}
	}
	return healthy
}

// GetBestPeer returns the best performing peer URL
func (pmc *PeerMetricsCollection) GetBestPeer() string {
	pmc.mu.RLock()
	defer pmc.mu.RUnlock()

	var bestPeer string
	var bestScore float64

	for url, metrics := range pmc.PeerMetrics {
		if !metrics.IsHealthy() {
			continue
		}

		// Score based on trust, success rate, and response time
		score := metrics.TrustScore * metrics.GetSuccessRate() / float64(metrics.AverageResponseTime+1)
		if score > bestScore {
			bestScore = score
			bestPeer = url
		}
	}

	return bestPeer
}
