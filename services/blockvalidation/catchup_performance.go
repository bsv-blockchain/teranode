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

// isPrunedPeer reports whether a peer announced pruned storage and will therefore
// 404 on archival subtree / subtree_data fetches during IBD (the second wedge cause
// in issue #1174). Peers with empty Storage ("" = legacy/unknown version) are NOT
// treated as pruned, to avoid excluding older archival peers; if such a peer turns
// out to lack the data, the fetch fails over (with backoff) rather than wedging.
func isPrunedPeer(storage string) bool {
	return storage == "pruned"
}

// nonPrunedPeers returns the peers that do not announce pruned storage, preserving order. Used to
// keep pruned peers out of proactive subtree distribution (they remain reachable only on failover
// via filterMaxHeightPeers' deprioritized tail).
func nonPrunedPeers(peers []*p2p.PeerInfo) []*p2p.PeerInfo {
	out := make([]*p2p.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		if peer != nil && !isPrunedPeer(peer.Storage) {
			out = append(out, peer)
		}
	}
	return out
}

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

// DistributeSubtreesAcrossPeers returns a round-robin distribution of peers for fetching
// subtrees, spreading load so a single slow peer can't bottleneck the block. alts is the
// block-level non-pruned max-height peer set (computed once by the caller via catchupAltPeers);
// primaryPruned drops the primary from the seed when it is pruned (it would only 404 on
// archival subtree data). If that leaves no peers (all-pruned segment), the primary is kept
// anyway so round-robin has a target — better a diagnosable 404 than a divide-by-zero.
func DistributeSubtreesAcrossPeers(
	logger ulogger.Logger,
	primaryPeerID string,
	primaryBaseURL string,
	primaryPruned bool,
	alts []*p2p.PeerInfo,
	numSubtrees int,
) []*PeerForSubtreeFetch {
	peers := make([]*PeerForSubtreeFetch, 0, len(alts)+1)
	if !primaryPruned {
		peers = append(peers, &PeerForSubtreeFetch{PeerID: primaryPeerID, BaseURL: primaryBaseURL})
	}
	for _, p := range alts {
		if p.DataHubURL != "" && p.ID.String() != primaryPeerID {
			peers = append(peers, &PeerForSubtreeFetch{PeerID: p.ID.String(), BaseURL: p.DataHubURL, PeerInfo: p})
		}
	}

	// All-pruned segment (pruned primary, no non-pruned alternatives): keep the primary so
	// the round-robin below has a target and 404s surface, rather than dividing by zero.
	if len(peers) == 0 {
		peers = append(peers, &PeerForSubtreeFetch{PeerID: primaryPeerID, BaseURL: primaryBaseURL})
	}

	// Round-robin assign peers to subtree indices.
	result := make([]*PeerForSubtreeFetch, numSubtrees)
	for i := 0; i < numSubtrees; i++ {
		peerIdx := i % len(peers)
		result[i] = peers[peerIdx]
		peers[peerIdx].AssignedCount++
	}

	if len(peers) > 1 {
		logger.Infof("[DistributeSubtrees] Distributing %d subtrees across %d peers", numSubtrees, len(peers))
	}

	return result
}

// peersAtHeightThreshold returns the candidates at or within one block of maxHeight, preserving
// input order.
func peersAtHeightThreshold(candidates []*p2p.PeerInfo, maxHeight uint32) []*p2p.PeerInfo {
	threshold := maxHeight
	if maxHeight > 0 {
		threshold = maxHeight - 1
	}
	out := make([]*p2p.PeerInfo, 0, len(candidates))
	for _, peer := range candidates {
		if peer.Height >= threshold {
			out = append(out, peer)
		}
	}
	return out
}

// sortPeersByReputationThenSpeed orders peers by reputation (descending) then response time
// (ascending), stably so equal-key input order is preserved.
func sortPeersByReputationThenSpeed(peers []*p2p.PeerInfo) {
	sort.SliceStable(peers, func(i, j int) bool {
		if peers[i].ReputationScore != peers[j].ReputationScore {
			return peers[i].ReputationScore > peers[j].ReputationScore
		}
		iHasTime := peers[i].AvgResponseTime > 0
		jHasTime := peers[j].AvgResponseTime > 0
		if iHasTime != jHasTime {
			return iHasTime // Prefer peer with a measured response time
		}
		if iHasTime && jHasTime && peers[i].AvgResponseTime != peers[j].AvgResponseTime {
			return peers[i].AvgResponseTime < peers[j].AvgResponseTime
		}
		return false
	})
}

// filterMaxHeightPeers filters an already-fetched peer set down to non-banned, adequately-reputed
// peers at (near) max height that advertise a DataHubURL, excluding excludePeerID (empty string
// excludes none), sorted by reputation then response time. Pure helper over a provided list so the
// peer set can be fetched once per block.
//
// Pruned peers are NOT excluded: a warm follower catching up recent blocks may find that the only
// peers holding the recent subtree_data are pruned (pruned != useless for recent data). Instead
// they are deprioritized to the tail so they are only reached on failover after every non-pruned
// peer, and the tip/height threshold is computed from the non-pruned candidates alone so a pruned
// peer advertising a higher height cannot lift the bar above the archival peers that serve deep
// IBD. Subtree distribution filters pruned peers out at the call site, so they are never
// proactively assigned — only used as last-resort failover.
func filterMaxHeightPeers(peers []*p2p.PeerInfo, excludePeerID string) []*p2p.PeerInfo {
	candidates := make([]*p2p.PeerInfo, 0, len(peers))
	prunedCandidates := make([]*p2p.PeerInfo, 0)
	var maxHeight, prunedMaxHeight uint32
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		if excludePeerID != "" && peer.ID.String() == excludePeerID {
			continue
		}
		if peer.DataHubURL == "" || peer.IsBanned || peer.ReputationScore < 20.0 {
			continue
		}
		if isPrunedPeer(peer.Storage) {
			prunedCandidates = append(prunedCandidates, peer)
			if peer.Height > prunedMaxHeight {
				prunedMaxHeight = peer.Height
			}
			continue
		}
		candidates = append(candidates, peer)
		if peer.Height > maxHeight {
			maxHeight = peer.Height
		}
	}

	eligible := peersAtHeightThreshold(candidates, maxHeight)
	sortPeersByReputationThenSpeed(eligible)

	if len(eligible) > 0 {
		// Append pruned peers reaching the same non-pruned tip as a deprioritized failover tail.
		prunedTail := peersAtHeightThreshold(prunedCandidates, maxHeight)
		sortPeersByReputationThenSpeed(prunedTail)
		return append(eligible, prunedTail...)
	}

	// No eligible non-pruned peer: pruned peers are the only failover option, so compute their own
	// tip and return them rather than stranding a warm follower with no target.
	prunedOnly := peersAtHeightThreshold(prunedCandidates, prunedMaxHeight)
	sortPeersByReputationThenSpeed(prunedOnly)
	return prunedOnly
}

// catchupAltPeers fetches the peer set ONCE per block and returns (a) the non-pruned
// max-height alternatives with NO peer excluded (callers filter per assigned peer) and
// (b) whether primaryPeerID is itself pruned (so subtree distribution can drop a pruned
// primary from the round-robin seed). Replaces the previous per-subtree GetPeersAtMaxHeight
// gRPC (up to ~SubtreeFetchConcurrency calls per block on primary failure).
func catchupAltPeers(ctx context.Context, logger ulogger.Logger, p2pClient P2PClientForParallelFetch, primaryPeerID string) ([]*p2p.PeerInfo, bool, error) {
	if p2pClient == nil {
		return nil, false, errors.NewInvalidArgumentError("p2pClient is nil")
	}

	peers, err := p2pClient.GetPeersForCatchup(ctx)
	if err != nil {
		return nil, false, errors.NewServiceError("failed to get peers for catchup: %v", err)
	}

	// primaryPruned is only true when the primary is present in the gossiped set AND marked
	// pruned; an unknown primary is left as-is (don't drop a peer we can't classify).
	primaryPruned := false
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		if peer.ID.String() == primaryPeerID && isPrunedPeer(peer.Storage) {
			primaryPruned = true
			break
		}
	}

	alts := filterMaxHeightPeers(peers, "")
	logger.Debugf("[catchupAltPeers] %d alternative peers, primaryPruned=%v", len(alts), primaryPruned)

	return alts, primaryPruned, nil
}

type catchupPeerSnapshot struct {
	once          sync.Once
	load          func() ([]*p2p.PeerInfo, bool, error)
	onError       func(error)
	peers         []*p2p.PeerInfo
	primaryPruned bool
	err           error
}

func newCatchupPeerSnapshot(
	ctx context.Context,
	logger ulogger.Logger,
	p2pClient P2PClientForParallelFetch,
	primaryPeerID string,
	blockHash string,
) *catchupPeerSnapshot {
	return &catchupPeerSnapshot{
		load: func() ([]*p2p.PeerInfo, bool, error) {
			return catchupAltPeers(ctx, logger, p2pClient, primaryPeerID)
		},
		onError: func(err error) {
			logger.Warnf("[catchup:fetchSubtreeDataForBlock][%s] Failed to get alternative peers: %v", blockHash, err)
		},
	}
}

func (s *catchupPeerSnapshot) get() ([]*p2p.PeerInfo, bool, error) {
	if s == nil {
		return nil, false, nil
	}
	s.once.Do(func() {
		s.peers, s.primaryPruned, s.err = s.load()
		if s.err != nil && s.onError != nil {
			s.onError(s.err)
		}
	})
	return s.peers, s.primaryPruned, s.err
}
