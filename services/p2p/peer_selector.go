package p2p

import (
	"sort"

	"github.com/bitcoin-sv/teranode/ulogger"
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
	logger ulogger.Logger
}

// NewPeerSelector creates a new peer selector
func NewPeerSelector(logger ulogger.Logger) *PeerSelector {
	return &PeerSelector{
		logger: logger,
	}
}

// SelectSyncPeer selects the best peer for syncing
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

	// Filter eligible peers
	var eligible []*PeerInfo
	for _, p := range peers {
		if ps.isEligible(p, criteria) {
			eligible = append(eligible, p)
		}
	}

	if len(eligible) == 0 {
		ps.logger.Debugf("[PeerSelector] No eligible peers found")
		return ""
	}

	// Filter peers that are ahead of us
	var candidates []*PeerInfo
	for _, p := range eligible {
		if p.Height > criteria.LocalHeight {
			candidates = append(candidates, p)
			ps.logger.Debugf("[PeerSelector] Peer %s at height %d > local %d (eligible)",
				p.ID, p.Height, criteria.LocalHeight)
		} else {
			ps.logger.Debugf("[PeerSelector] Peer %s at height %d <= local %d (skip)",
				p.ID, p.Height, criteria.LocalHeight)
		}
	}

	if len(candidates) == 0 {
		ps.logger.Debugf("[PeerSelector] No peers ahead of local height %d", criteria.LocalHeight)
		return ""
	}

	// Sort candidates by: 1) BanScore (ascending), 2) Height (descending), 3) PeerID (for stability)
	//
	// The ban score is prioritized over height because:
	// - Ban scores increase when peers send invalid subtrees or invalid blocks
	// - A peer with a lower ban score is more trustworthy, even if at a lower height
	// - Example: If we're at height 700, and have two peers:
	//   * Peer A at height 1000 with ban score 50 (has sent invalid data)
	//   * Peer B at height 800 with ban score 0 (clean record)
	//   We prefer Peer B despite its lower height, as it's more reliable
	// - Once we sync to height 800 from Peer B, the selector will be invoked again
	//   and can then choose Peer A if it's the only one ahead (and if its ban score
	//   hasn't increased further)
	// - This strategy minimizes the risk of syncing invalid data and reduces
	//   wasted effort from failed sync attempts
	sort.Slice(candidates, func(i, j int) bool {
		// First priority: Lower ban score is better (more trustworthy peer)
		if candidates[i].BanScore != candidates[j].BanScore {
			return candidates[i].BanScore < candidates[j].BanScore
		}
		// Second priority: Higher block height is better (more data available)
		if candidates[i].Height != candidates[j].Height {
			return candidates[i].Height > candidates[j].Height
		}
		// Third priority: Sort by peer ID for deterministic ordering
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
	ps.logger.Infof("[PeerSelector] Selected peer %s (height=%d, banScore=%d) from %d candidates (index=%d)",
		selected.ID, selected.Height, selected.BanScore, len(candidates), selectedIndex)

	// Log top 3 candidates for debugging
	for i := 0; i < len(candidates) && i < 3; i++ {
		ps.logger.Debugf("[PeerSelector] Candidate %d: %s (height=%d, banScore=%d)",
			i+1, candidates[i].ID, candidates[i].Height, candidates[i].BanScore)
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
