package p2p

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/libp2p/go-libp2p/core/peer"
)

// SelectionCriteria defines criteria for peer selection
type SelectionCriteria struct {
	LocalHeight         int32
	ForcedPeerID        peer.ID       // If set, only this peer will be selected
	PreviousPeer        peer.ID       // The previously selected peer, if any
	SyncAttemptCooldown time.Duration // Cooldown period before retrying a peer
}

// PeerSelector handles peer selection logic
// This is a stateless, pure function component
type PeerSelector struct {
	logger   ulogger.Logger
	settings *settings.Settings
}

// NewPeerSelector creates a new peer selector
func NewPeerSelector(logger ulogger.Logger, settings *settings.Settings) *PeerSelector {
	return &PeerSelector{
		logger:   logger,
		settings: settings,
	}
}

// SelectSyncPeer selects the best peer for syncing using two-phase selection:
// Phase 1: Try to select from full nodes (nodes with complete block data)
// Phase 2: If no full nodes and fallback enabled, select youngest pruned node
// This is a pure function - no side effects, no network calls
func (ps *PeerSelector) SelectSyncPeer(peers []*PeerInfo, criteria SelectionCriteria) peer.ID {
	// Handle forced peer - always select it if it exists, regardless of eligibility
	if criteria.ForcedPeerID != "" {
		for _, p := range peers {
			if p.ID == criteria.ForcedPeerID {
				ps.logger.Infof("[PeerSelector] Using forced peer %s", p.ID)
				return p.ID
			}
		}
		ps.logger.Infof("[PeerSelector] Forced peer %s not connected", criteria.ForcedPeerID)
		return ""
	}

	// PHASE 1: Try to select from full nodes
	fullNodeCandidates := ps.getFullNodeCandidates(peers, criteria)
	if len(fullNodeCandidates) > 0 {
		selected := ps.selectFromCandidates(fullNodeCandidates, criteria, true)
		if selected != "" {
			ps.logger.Infof("[PeerSelector] Selected FULL node %s", selected)
			return selected
		}
	}

	// PHASE 2: Fall back to pruned nodes if enabled (enabled by default if settings is nil)
	allowFallback := true // Default: allow fallback
	if ps.settings != nil {
		allowFallback = ps.settings.P2P.AllowPrunedNodeFallback
	}

	if allowFallback {
		ps.logger.Infof("[PeerSelector] No full nodes available, attempting pruned node fallback")
		prunedCandidates := ps.getPrunedNodeCandidates(peers, criteria)
		if len(prunedCandidates) > 0 {
			selected := ps.selectFromCandidates(prunedCandidates, criteria, false)
			if selected != "" {
				ps.logger.Warnf("[PeerSelector] Selected PRUNED node %s (smallest height to minimize UTXO pruning risk)", selected)
				return selected
			}
		}
	} else {
		ps.logger.Infof("[PeerSelector] No full nodes available and pruned node fallback disabled")
	}

	ps.logger.Debugf("[PeerSelector] No suitable sync peer found")
	return ""
}

// getFullNodeCandidates returns eligible full nodes that are ahead of local height
func (ps *PeerSelector) getFullNodeCandidates(peers []*PeerInfo, criteria SelectionCriteria) []*PeerInfo {
	var candidates []*PeerInfo
	for _, p := range peers {
		if ps.isEligibleFullNode(p, criteria) && int32(p.Height) > criteria.LocalHeight {
			candidates = append(candidates, p)
			ps.logger.Debugf("[PeerSelector] Full node candidate: %s at height %d (mode: %s)", p.ID, p.Height, p.Storage)
		}
	}
	return candidates
}

// getPrunedNodeCandidates returns eligible pruned nodes that are ahead of local height
func (ps *PeerSelector) getPrunedNodeCandidates(peers []*PeerInfo, criteria SelectionCriteria) []*PeerInfo {
	var candidates []*PeerInfo
	for _, p := range peers {
		// Only include if eligible but NOT a full node
		if ps.isEligible(p, criteria) && p.Storage != "full" && int32(p.Height) > criteria.LocalHeight {
			candidates = append(candidates, p)
			ps.logger.Debugf("[PeerSelector] Pruned node candidate: %s at height %d (mode: %s)", p.ID, p.Height, p.Storage)
		}
	}
	return candidates
}

// selectFromCandidates selects the best peer from a list of candidates
// If isFullNode is true, sorts by height descending (prefer highest)
// If isFullNode is false (pruned), sorts by height ascending (prefer lowest/youngest)
func (ps *PeerSelector) selectFromCandidates(candidates []*PeerInfo, criteria SelectionCriteria, isFullNode bool) peer.ID {
	if len(candidates) == 0 {
		return ""
	}

	// Sort candidates by: 1) ReputationScore (descending), 2) BanScore (ascending), 3) Height (descending), 4) PeerID (for stability)
	//
	// Reputation score is prioritized because:
	// - It's a comprehensive measure of peer reliability (0-100 scale)
	// - It takes into account success rate, failure rate, malicious behavior, and response time
	// - A peer with a higher reputation score is more trustworthy and likely to provide valid data
	// - Example: If we're at height 700, and have two peers:
	//   * Peer A at height 1000 with reputation 30 (low reliability, many failures)
	//   * Peer B at height 800 with reputation 85 (high reliability, few failures)
	//   We prefer Peer B despite its lower height, as it's more reliable
	// - Ban score is still considered as a secondary factor for additional safety
	// - This strategy minimizes the risk of syncing invalid data and reduces wasted effort
	sort.Slice(candidates, func(i, j int) bool {
		// First priority: Higher reputation score is better (more trustworthy peer)
		if candidates[i].ReputationScore != candidates[j].ReputationScore {
			return candidates[i].ReputationScore > candidates[j].ReputationScore
		}
		// Second priority: Lower ban score is better (additional safety check)
		if candidates[i].BanScore != candidates[j].BanScore {
			return candidates[i].BanScore < candidates[j].BanScore
		}
		// Third priority: Higher block height is better (more data available)
		if candidates[i].Height != candidates[j].Height {
			if isFullNode {
				// Full nodes: prefer higher height (more data)
				return candidates[i].Height > candidates[j].Height
			}
			// Pruned nodes: prefer LOWER height (youngest, less UTXO pruning)
			return candidates[i].Height < candidates[j].Height
		}
		// Fourth priority: Sort by peer ID for deterministic ordering
		// This ensures consistent selection when peers have identical scores and heights
		return candidates[i].ID < candidates[j].ID
	})

	// Select the first peer by default
	// If the previous peer was the first in the list, select the second (if available)
	selectedIndex := 0
	if len(candidates) > 1 && criteria.PreviousPeer != "" && candidates[0].ID == criteria.PreviousPeer {
		// Previous peer was the top candidate, try the second one
		selectedIndex = 1
		ps.logger.Debugf("[PeerSelector] Previous peer %s was top candidate, selecting second", criteria.PreviousPeer)
	}

	selected := candidates[selectedIndex]
	nodeType := "FULL"
	if !isFullNode {
		nodeType = "PRUNED"
	}
	ps.logger.Infof("[PeerSelector] Selected %s node peer %s (height=%d, banScore=%d) from %d candidates (index=%d)",
		nodeType, selected.ID, selected.Height, selected.BanScore, len(candidates), selectedIndex)

	// Log top 3 candidates for debugging
	for i := 0; i < len(candidates) && i < 3; i++ {
		ps.logger.Debugf("[PeerSelector] Candidate %d: %s (height=%d, banScore=%d, mode=%s, url=%s)",
			i+1, candidates[i].ID, candidates[i].Height, candidates[i].BanScore, candidates[i].Storage, candidates[i].DataHubURL)
	}

	return selected.ID
}

// isEligible checks if a peer meets selection criteria
func (ps *PeerSelector) isEligible(p *PeerInfo, criteria SelectionCriteria) bool {
	// Always exclude banned peers
	if p.IsBanned {
		ps.logger.Debugf("[PeerSelector] Peer %s is banned (score: %d)", p.ID, p.BanScore)
		return false
	}

	// Check DataHub URL requirement - this protects against listen-only nodes
	if p.DataHubURL == "" {
		ps.logger.Debugf("[PeerSelector] Peer %s has no DataHub URL (listen-only node)", p.ID)
		return false
	}

	// Check valid height
	if p.Height <= 0 {
		ps.logger.Debugf("[PeerSelector] Peer %s has invalid height %d", p.ID, p.Height)
		return false
	}

	// Check reputation threshold - peers with very low reputation should not be selected
	if p.ReputationScore < 20.0 {
		ps.logger.Debugf("[PeerSelector] Peer %s has very low reputation %.2f (below threshold 20.0)", p.ID, p.ReputationScore)
		return false
	}

	// Check sync attempt cooldown BEFORE health check (avoids re-checking failed peers)
	if criteria.SyncAttemptCooldown > 0 && !p.LastSyncAttempt.IsZero() {
		timeSinceLastAttempt := time.Since(p.LastSyncAttempt)
		if timeSinceLastAttempt < criteria.SyncAttemptCooldown {
			ps.logger.Debugf("[PeerSelector] Peer %s attempted recently (%v ago, cooldown: %v)",
				p.ID, timeSinceLastAttempt.Round(time.Second), criteria.SyncAttemptCooldown)
			return false
		}
	}

	// Check HTTP availability if enabled
	// Note: Health check failures are NOT recorded as sync attempts - they're filtered out early.
	// The caller (SyncCoordinator) will record sync attempt after selecting the peer.
	if ps.settings != nil && ps.settings.P2P.HealthCheckEnabled {
		ps.logger.Debugf("[PeerSelector] Checking availability for peer %s", p.ID)

		isHealthy, err := checkPeerAvailability(context.Background(), p.DataHubURL)

		if !isHealthy {
			ps.logger.Debugf("[PeerSelector] Peer %s is unhealthy: %v", p.ID, err)
			return false
		}
	}

	return true
}

// isEligibleFullNode checks if a peer is eligible as a full node for catchup
// Only peers explicitly announcing as "full" are considered full nodes
func (ps *PeerSelector) isEligibleFullNode(p *PeerInfo, criteria SelectionCriteria) bool {
	if !ps.isEligible(p, criteria) {
		return false // Must pass basic eligibility first
	}

	// Only peers announcing as "full" are considered full nodes
	// Unknown/empty mode is treated as pruned
	if p.Storage != "full" {
		return false
	}

	return true
}

// checkPeerAvailability tests if a peer's DataHub URL is reachable via HTTP.
// DataHubURL already includes /api/v1 prefix, so we just append the endpoint path.
// Uses existing util/health infrastructure with built-in 2s timeout.
func checkPeerAvailability(ctx context.Context, dataHubURL string) (bool, error) {
	if dataHubURL == "" {
		return false, nil
	}

	// DataHubURL format: "https://host/api/v1"
	// Append /bestblockheader to get full endpoint path
	checker := health.CheckHTTPServer(dataHubURL, "/bestblockheader")

	statusCode, _, err := checker(ctx, false)

	// Only accept 200 OK - API endpoints should return exactly 200
	return statusCode == http.StatusOK, err
}
