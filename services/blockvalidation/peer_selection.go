package blockvalidation

import (
	"context"
)

// PeerForCatchup represents a peer suitable for catchup operations with its metadata
type PeerForCatchup struct {
	ID                     string
	Storage                string
	DataHubURL             string
	Height                 int32
	BlockHash              string
	CatchupReputationScore float64
	CatchupAttempts        int64
	CatchupSuccesses       int64
	CatchupFailures        int64
}

// selectBestPeersForCatchup queries the P2P service for peers suitable for catchup,
// sorted by reputation score (highest first).
//
// Parameters:
//   - ctx: Context for the gRPC call
//   - targetHeight: The height we're trying to catch up to (for filtering peers)
//
// Returns:
//   - []PeerForCatchup: List of peers sorted by reputation (best first)
//   - error: If the query fails
func (u *Server) selectBestPeersForCatchup(ctx context.Context, targetHeight int32) ([]PeerForCatchup, error) {
	// If P2P client is not available, return empty list
	if u.p2pClient == nil {
		u.logger.Debugf("[peer_selection] P2P client not available, using fallback peer selection")
		return nil, nil
	}

	// Query P2P service for peers suitable for catchup
	peerInfos, err := u.p2pClient.GetPeersForCatchup(ctx)
	if err != nil {
		u.logger.Warnf("[peer_selection] Failed to get peers from P2P service: %v", err)
		return nil, err
	}

	if len(peerInfos) == 0 {
		u.logger.Debugf("[peer_selection] No peers available from P2P service")
		return nil, nil
	}

	// Convert PeerInfo to our internal type
	peers := make([]PeerForCatchup, 0, len(peerInfos))
	for _, p := range peerInfos {
		// Filter out peers that don't have the target height yet
		// (we only want peers that are at or above our target)
		if p.Height < targetHeight {
			u.logger.Debugf("[peer_selection] Skipping peer %s (height %d < target %d)", p.ID.String(), p.Height, targetHeight)
			continue
		}

		// Filter out peers without DataHub URLs (listen-only nodes)
		if p.DataHubURL == "" {
			u.logger.Debugf("[peer_selection] Skipping peer %s (no DataHub URL - listen-only node)", p.ID.String())
			continue
		}

		peers = append(peers, PeerForCatchup{
			ID:                     p.ID.String(),
			Storage:                p.Storage,
			DataHubURL:             p.DataHubURL,
			Height:                 p.Height,
			BlockHash:              p.BlockHash,
			CatchupReputationScore: p.ReputationScore,
			CatchupAttempts:        p.InteractionAttempts,
			CatchupSuccesses:       p.InteractionSuccesses,
			CatchupFailures:        p.InteractionFailures,
		})
	}

	u.logger.Infof("[peer_selection] Selected %d peers for catchup (from %d total)", len(peers), len(peerInfos))
	for i, p := range peers {
		successRate := float64(0)

		if p.CatchupAttempts > 0 {
			successRate = float64(p.CatchupSuccesses) / float64(p.CatchupAttempts) * 100
		}

		u.logger.Debugf("[peer_selection] Peer %d: %s (score: %.2f, success: %d/%d = %.1f%%, height: %d)", i+1, p.ID, p.CatchupReputationScore, p.CatchupSuccesses, p.CatchupAttempts, successRate, p.Height)
	}

	return peers, nil
}

// selectBestPeerForBlock selects the best peer to fetch a specific block from.
// This is a convenience wrapper around selectBestPeersForCatchup that returns
// the single best peer.
//
// Parameters:
//   - ctx: Context for the gRPC call
//   - targetHeight: The height of the block we're trying to fetch
//
// Returns:
//   - *PeerForCatchup: The best peer, or nil if none available
//   - error: If the query fails
func (u *Server) selectBestPeerForBlock(ctx context.Context, targetHeight int32) (*PeerForCatchup, error) {
	peers, err := u.selectBestPeersForCatchup(ctx, targetHeight)
	if err != nil {
		return nil, err
	}

	if len(peers) == 0 {
		return nil, nil
	}

	return &peers[0], nil
}
