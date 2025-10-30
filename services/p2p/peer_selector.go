package p2p

import (
	"sort"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
)

// SelectionCriteria defines criteria for peer selection
type SelectionCriteria struct {
	LocalHeight  int32
	ForcedPeerID peer.ID // If set, only this peer will be selected
	PreviousPeer peer.ID // The previously selected peer, if any
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
		if ps.isEligibleFullNode(p, criteria) && p.Height > criteria.LocalHeight {
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
		if ps.isEligible(p, criteria) && p.Storage != "full" && p.Height > criteria.LocalHeight {
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

	// Sort candidates
	// Priority: 1) BanScore (asc), 2) Height (desc for full, asc for pruned), 3) HealthDuration (asc), 4) PeerID
	sort.Slice(candidates, func(i, j int) bool {
		// First priority: Lower ban score is better (more trustworthy peer)
		if candidates[i].BanScore != candidates[j].BanScore {
			return candidates[i].BanScore < candidates[j].BanScore
		}
		// Second priority: Height preference depends on node type
		if candidates[i].Height != candidates[j].Height {
			if isFullNode {
				// Full nodes: prefer higher height (more data)
				return candidates[i].Height > candidates[j].Height
			}
			// Pruned nodes: prefer LOWER height (youngest, less UTXO pruning)
			return candidates[i].Height < candidates[j].Height
		}
		// Third priority: Sort by peer health duration (lower is better)
		if candidates[i].HealthDuration != candidates[j].HealthDuration {
			return candidates[i].HealthDuration < candidates[j].HealthDuration
		}
		// Fourth priority: Sort by peer ID for deterministic ordering
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
func (ps *PeerSelector) isEligible(p *PeerInfo, _ SelectionCriteria) bool {
	// Always exclude banned peers
	if p.IsBanned {
		ps.logger.Debugf("[PeerSelector] Peer %s is banned (score: %d)", p.ID, p.BanScore)
		return false
	}

	// Check health
	if !p.IsHealthy {
		ps.logger.Debugf("[PeerSelector] Peer %s not healthy", p.ID)
		return false
	}

	// Check DataHub requirement
	if p.DataHubURL == "" {
		ps.logger.Debugf("[PeerSelector] Peer %s has no DataHub URL", p.ID)
		return false
	}

	// Check URL responsiveness
	if p.DataHubURL != "" && !p.URLResponsive {
		ps.logger.Debugf("[PeerSelector] Peer %s URL is not responsive", p.ID)
		return false
	}

	// Check valid height
	if p.Height <= 0 {
		ps.logger.Debugf("[PeerSelector] Peer %s has invalid height %d", p.ID, p.Height)
		return false
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
