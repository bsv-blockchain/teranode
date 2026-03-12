// Package blockvalidation provides block validation functionality.
// This file implements performance monitoring and dynamic peer switching during catchup.
package blockvalidation

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// P2PClientForParallelFetch is a subset of P2PClientI needed for parallel fetch operations
type P2PClientForParallelFetch interface {
	GetPeersForCatchup(ctx context.Context) ([]*p2p.PeerInfo, error)
	RecordBytesDownloaded(ctx context.Context, peerID string, bytesDownloaded uint64) error
}

// CatchupPerformanceMonitor tracks performance metrics during catchup and decides
// when to switch to a faster peer.
type CatchupPerformanceMonitor struct {
	logger ulogger.Logger

	// Current peer info
	currentPeerID  string
	currentBaseURL string

	// Performance metrics
	startTime        time.Time
	bytesReceived    atomic.Int64
	blocksProcessed  atomic.Int64
	subtreesFetched  atomic.Int64
	lastCheckTime    time.Time
	lastBytesAtCheck int64

	// Thresholds (configurable)
	minThroughputBytesPerSec float64       // Minimum acceptable throughput
	checkInterval            time.Duration // How often to check performance
	warmupPeriod             time.Duration // Initial period before enforcing thresholds

	// Peer switching
	switchCount       int       // Number of times we've switched peers
	maxSwitches       int       // Maximum switches allowed per catchup
	lastSwitchTime    time.Time // When we last switched peers
	switchCooldown    time.Duration
	peerSwitchPending atomic.Bool // Flag to signal peer switch is needed

	mu sync.RWMutex
}

// PerformanceMonitorConfig holds configuration for the performance monitor
type PerformanceMonitorConfig struct {
	MinThroughputKBPerSec float64       // Minimum throughput in KB/s (default: 100 KB/s)
	CheckInterval         time.Duration // How often to check (default: 10s)
	WarmupPeriod          time.Duration // Warmup before enforcing (default: 30s)
	MaxSwitches           int           // Max peer switches (default: 3)
	SwitchCooldown        time.Duration // Cooldown between switches (default: 60s)
}

// DefaultPerformanceMonitorConfig returns sensible defaults
func DefaultPerformanceMonitorConfig() PerformanceMonitorConfig {
	return PerformanceMonitorConfig{
		MinThroughputKBPerSec: 100,              // 100 KB/s minimum
		CheckInterval:         10 * time.Second, // Check every 10 seconds
		WarmupPeriod:          30 * time.Second, // 30 second warmup
		MaxSwitches:           3,                // Allow up to 3 peer switches
		SwitchCooldown:        60 * time.Second, // 60 second cooldown between switches
	}
}

// NewCatchupPerformanceMonitor creates a new performance monitor
func NewCatchupPerformanceMonitor(logger ulogger.Logger, peerID, baseURL string, config PerformanceMonitorConfig) *CatchupPerformanceMonitor {
	return &CatchupPerformanceMonitor{
		logger:                   logger,
		currentPeerID:            peerID,
		currentBaseURL:           baseURL,
		startTime:                time.Now(),
		lastCheckTime:            time.Now(),
		minThroughputBytesPerSec: config.MinThroughputKBPerSec * 1024,
		checkInterval:            config.CheckInterval,
		warmupPeriod:             config.WarmupPeriod,
		maxSwitches:              config.MaxSwitches,
		switchCooldown:           config.SwitchCooldown,
	}
}

// RecordBytes records bytes received during catchup
func (m *CatchupPerformanceMonitor) RecordBytes(bytes int64) {
	m.bytesReceived.Add(bytes)
}

// RecordBlock records a block processed
func (m *CatchupPerformanceMonitor) RecordBlock() {
	m.blocksProcessed.Add(1)
}

// RecordSubtree records a subtree fetched
func (m *CatchupPerformanceMonitor) RecordSubtree() {
	m.subtreesFetched.Add(1)
}

// GetCurrentThroughput returns the current throughput in bytes/second
func (m *CatchupPerformanceMonitor) GetCurrentThroughput() float64 {
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(m.bytesReceived.Load()) / elapsed
}

// GetRecentThroughput returns throughput since last check in bytes/second
func (m *CatchupPerformanceMonitor) GetRecentThroughput() float64 {
	m.mu.RLock()
	lastCheck := m.lastCheckTime
	lastBytes := m.lastBytesAtCheck
	m.mu.RUnlock()

	elapsed := time.Since(lastCheck).Seconds()
	if elapsed == 0 {
		return 0
	}

	currentBytes := m.bytesReceived.Load()
	return float64(currentBytes-lastBytes) / elapsed
}

// ShouldSwitchPeer checks if we should switch to a different peer due to poor performance
// Returns true if performance is below threshold and we haven't exceeded switch limits
func (m *CatchupPerformanceMonitor) ShouldSwitchPeer() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// Don't check too frequently
	if now.Sub(m.lastCheckTime) < m.checkInterval {
		return false
	}

	// Update check time
	currentBytes := m.bytesReceived.Load()
	elapsed := now.Sub(m.lastCheckTime).Seconds()

	// Calculate recent throughput
	recentThroughput := float64(currentBytes-m.lastBytesAtCheck) / elapsed

	// Update for next check
	m.lastCheckTime = now
	m.lastBytesAtCheck = currentBytes

	// Still in warmup period - don't enforce thresholds
	if now.Sub(m.startTime) < m.warmupPeriod {
		m.logger.Debugf("[CatchupPerformanceMonitor] In warmup period, throughput: %.2f KB/s", recentThroughput/1024)
		return false
	}

	// Check if we've exceeded max switches
	if m.switchCount >= m.maxSwitches {
		m.logger.Debugf("[CatchupPerformanceMonitor] Max switches (%d) reached, not switching", m.maxSwitches)
		return false
	}

	// Check cooldown
	if !m.lastSwitchTime.IsZero() && now.Sub(m.lastSwitchTime) < m.switchCooldown {
		m.logger.Debugf("[CatchupPerformanceMonitor] In switch cooldown, throughput: %.2f KB/s", recentThroughput/1024)
		return false
	}

	// Check if throughput is below threshold
	if recentThroughput < m.minThroughputBytesPerSec {
		m.logger.Warnf("[CatchupPerformanceMonitor] Throughput %.2f KB/s below threshold %.2f KB/s, recommending peer switch",
			recentThroughput/1024, m.minThroughputBytesPerSec/1024)
		return true
	}

	m.logger.Debugf("[CatchupPerformanceMonitor] Throughput OK: %.2f KB/s (threshold: %.2f KB/s)",
		recentThroughput/1024, m.minThroughputBytesPerSec/1024)
	return false
}

// RecordPeerSwitch records that we switched to a new peer
func (m *CatchupPerformanceMonitor) RecordPeerSwitch(newPeerID, newBaseURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentPeerID = newPeerID
	m.currentBaseURL = newBaseURL
	m.switchCount++
	m.lastSwitchTime = time.Now()
	// Reset metrics for new peer
	m.lastBytesAtCheck = m.bytesReceived.Load()
	m.lastCheckTime = time.Now()

	m.logger.Infof("[CatchupPerformanceMonitor] Switched to peer %s (switch #%d)", newPeerID, m.switchCount)
}

// GetStats returns current performance statistics
func (m *CatchupPerformanceMonitor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"currentPeer":      m.currentPeerID,
		"bytesReceived":    m.bytesReceived.Load(),
		"blocksProcessed":  m.blocksProcessed.Load(),
		"subtreesFetched":  m.subtreesFetched.Load(),
		"throughputKBps":   m.GetCurrentThroughput() / 1024,
		"elapsed":          time.Since(m.startTime).String(),
		"switchCount":      m.switchCount,
		"peerSwitchNeeded": m.peerSwitchPending.Load(),
	}
}

// SetPeerSwitchPending marks that a peer switch is needed
func (m *CatchupPerformanceMonitor) SetPeerSwitchPending(pending bool) {
	m.peerSwitchPending.Store(pending)
}

// IsPeerSwitchPending returns true if a peer switch has been requested
func (m *CatchupPerformanceMonitor) IsPeerSwitchPending() bool {
	return m.peerSwitchPending.Load()
}

// CanSwitchPeer returns true if we can still switch peers (haven't exceeded limits)
func (m *CatchupPerformanceMonitor) CanSwitchPeer() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.switchCount < m.maxSwitches
}

// PeerForSubtreeFetch holds peer information for distributing subtree fetches
type PeerForSubtreeFetch struct {
	PeerID        string
	BaseURL       string
	PeerInfo      *p2p.PeerInfo
	AssignedCount int // Number of subtrees assigned to this peer
}

// DistributeSubtreesAcrossPeers returns a round-robin distribution of peers for fetching subtrees.
// This spreads the load across multiple peers to avoid being bottlenecked by a single slow peer.
func DistributeSubtreesAcrossPeers(
	ctx context.Context,
	logger ulogger.Logger,
	p2pClient P2PClientForParallelFetch,
	primaryPeerID string,
	primaryBaseURL string,
	numSubtrees int,
) ([]*PeerForSubtreeFetch, error) {
	// Start with just the primary peer
	peers := []*PeerForSubtreeFetch{
		{PeerID: primaryPeerID, BaseURL: primaryBaseURL, AssignedCount: 0},
	}

	// Try to get additional peers at max height
	if p2pClient != nil {
		additionalPeers, err := GetPeersAtMaxHeight(ctx, logger, p2pClient, primaryPeerID)
		if err == nil && len(additionalPeers) > 0 {
			for _, p := range additionalPeers {
				if p.DataHubURL != "" && p.ID.String() != primaryPeerID {
					peers = append(peers, &PeerForSubtreeFetch{
						PeerID:   p.ID.String(),
						BaseURL:  p.DataHubURL,
						PeerInfo: p,
					})
				}
			}
		}
	}

	// Create distribution: round-robin assign peers to subtree indices
	result := make([]*PeerForSubtreeFetch, numSubtrees)
	for i := 0; i < numSubtrees; i++ {
		peerIdx := i % len(peers)
		result[i] = peers[peerIdx]
		peers[peerIdx].AssignedCount++
	}

	// Log distribution
	if len(peers) > 1 {
		logger.Infof("[DistributeSubtrees] Distributing %d subtrees across %d peers", numSubtrees, len(peers))
		for _, p := range peers {
			if p.AssignedCount > 0 {
				logger.Debugf("[DistributeSubtrees] Peer %s: %d subtrees", p.PeerID, p.AssignedCount)
			}
		}
	}

	return result, nil
}

// SelectAlternativePeer selects a different peer for catchup, excluding the current one.
// It only selects from peers that are at the maximum network height to ensure we sync
// from the most up-to-date peers.
func SelectAlternativePeer(
	ctx context.Context,
	logger ulogger.Logger,
	p2pClient P2PClientForParallelFetch,
	currentPeerID string,
	targetHeight uint32,
) (*p2p.PeerInfo, error) {
	if p2pClient == nil {
		return nil, errors.NewInvalidArgumentError("p2pClient is nil")
	}

	// Get all peers for catchup
	peers, err := p2pClient.GetPeersForCatchup(ctx)
	if err != nil {
		return nil, errors.NewServiceError("failed to get peers for catchup: %v", err)
	}

	// First pass: find the maximum height across all peers
	var maxHeight uint32
	for _, peer := range peers {
		if peer.Height > maxHeight {
			maxHeight = peer.Height
		}
	}

	// Only consider peers at max height (or very close to it - within 1 block for propagation delay)
	maxHeightThreshold := maxHeight
	if maxHeight > 0 {
		maxHeightThreshold = maxHeight - 1 // Allow 1 block tolerance
	}

	logger.Debugf("[SelectAlternativePeer] Max network height: %d, threshold: %d", maxHeight, maxHeightThreshold)

	// Second pass: filter and find best alternative peer at max height
	var bestPeer *p2p.PeerInfo
	for _, peer := range peers {
		// Skip current peer
		if peer.ID.String() == currentPeerID {
			continue
		}

		// Skip peers without DataHubURL
		if peer.DataHubURL == "" {
			continue
		}

		// Only select peers at max network height
		if peer.Height < maxHeightThreshold {
			logger.Debugf("[SelectAlternativePeer] Skipping peer %s at height %d (below threshold %d)",
				peer.ID, peer.Height, maxHeightThreshold)
			continue
		}

		// Skip banned peers
		if peer.IsBanned {
			continue
		}

		// Skip peers with very low reputation
		if peer.ReputationScore < 20.0 {
			continue
		}

		// Select peer with best reputation and speed
		if bestPeer == nil {
			bestPeer = peer
			continue
		}

		// Prefer higher reputation
		if peer.ReputationScore > bestPeer.ReputationScore {
			bestPeer = peer
			continue
		}

		// If same reputation, prefer faster peer
		if peer.ReputationScore == bestPeer.ReputationScore &&
			peer.AvgResponseTime > 0 && bestPeer.AvgResponseTime > 0 &&
			peer.AvgResponseTime < bestPeer.AvgResponseTime {
			bestPeer = peer
		}
	}

	if bestPeer == nil {
		return nil, errors.NewNotFoundError("no alternative peer found at max height %d", maxHeight)
	}

	logger.Infof("[SelectAlternativePeer] Selected alternative peer %s (height=%d, maxHeight=%d, reputation=%.2f, avgResponseTime=%v)",
		bestPeer.ID, bestPeer.Height, maxHeight, bestPeer.ReputationScore, bestPeer.AvgResponseTime)

	return bestPeer, nil
}

// GetPeersAtMaxHeight returns peers that are at the maximum network height,
// sorted by reputation and speed. Useful for parallel fetching.
func GetPeersAtMaxHeight(
	ctx context.Context,
	logger ulogger.Logger,
	p2pClient P2PClientForParallelFetch,
	excludePeerID string,
) ([]*p2p.PeerInfo, error) {
	if p2pClient == nil {
		return nil, errors.NewInvalidArgumentError("p2pClient is nil")
	}

	// Get all peers for catchup
	peers, err := p2pClient.GetPeersForCatchup(ctx)
	if err != nil {
		return nil, errors.NewServiceError("failed to get peers for catchup: %v", err)
	}

	// Find max height
	var maxHeight uint32
	for _, peer := range peers {
		if peer.Height > maxHeight {
			maxHeight = peer.Height
		}
	}

	// Allow 1 block tolerance for propagation delay
	maxHeightThreshold := maxHeight
	if maxHeight > 0 {
		maxHeightThreshold = maxHeight - 1
	}

	// Filter to peers at max height
	eligiblePeers := make([]*p2p.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		// Skip excluded peer
		if excludePeerID != "" && peer.ID.String() == excludePeerID {
			continue
		}

		// Skip peers without DataHubURL
		if peer.DataHubURL == "" {
			continue
		}

		// Only include peers at max height
		if peer.Height < maxHeightThreshold {
			continue
		}

		// Skip banned peers
		if peer.IsBanned {
			continue
		}

		// Skip peers with very low reputation
		if peer.ReputationScore < 20.0 {
			continue
		}

		eligiblePeers = append(eligiblePeers, peer)
	}

	// Sort by reputation (descending) then by response time (ascending)
	sort.Slice(eligiblePeers, func(i, j int) bool {
		// First priority: Higher reputation score is better
		if eligiblePeers[i].ReputationScore != eligiblePeers[j].ReputationScore {
			return eligiblePeers[i].ReputationScore > eligiblePeers[j].ReputationScore
		}

		// Second priority: Lower response time is better (faster peer)
		// Peers with 0 response time (no data) are sorted after peers with measurements
		iHasTime := eligiblePeers[i].AvgResponseTime > 0
		jHasTime := eligiblePeers[j].AvgResponseTime > 0
		if iHasTime != jHasTime {
			return iHasTime // Prefer peer with measured response time
		}
		if iHasTime && jHasTime && eligiblePeers[i].AvgResponseTime != eligiblePeers[j].AvgResponseTime {
			return eligiblePeers[i].AvgResponseTime < eligiblePeers[j].AvgResponseTime
		}

		// If all else is equal, maintain stable order
		return false
	})

	logger.Debugf("[GetPeersAtMaxHeight] Found %d eligible peers at max height %d", len(eligiblePeers), maxHeight)

	return eligiblePeers, nil
}
