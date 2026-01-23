package blockvalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// PeerForCatchup represents a peer suitable for catchup operations with its metadata
type PeerForCatchup struct {
	ID                     string
	Storage                string
	DataHubURL             string
	Height                 uint32
	BlockHash              *chainhash.Hash
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
func (u *Server) selectBestPeersForCatchup(ctx context.Context, targetHeight uint32) ([]PeerForCatchup, error) {
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

// tryAlternativePeersForCatchup attempts catchup with alternative peers from the P2P service.
// It skips the excludePeerID and any peers marked as malicious.
// Returns true if catchup succeeded with any peer.
func (u *Server) tryAlternativePeersForCatchup(ctx context.Context, block *model.Block, excludePeerID string) bool {
	blockHash := block.Hash()
	bestPeers, peerErr := u.selectBestPeersForCatchup(ctx, block.Height)
	if peerErr != nil {
		u.logger.Warnf("[catchup] Failed to get best peers from P2P service: %v", peerErr)
	}

	if len(bestPeers) == 0 {
		return false
	}

	u.logger.Infof("[catchup] Trying %d alternative peers for block %s", len(bestPeers), blockHash.String())

	for _, bestPeer := range bestPeers {
		if bestPeer.ID == excludePeerID {
			continue
		}

		if u.isPeerMalicious(ctx, bestPeer.ID) {
			u.logger.Debugf("[catchup] Skipping peer %s - marked as malicious", bestPeer.ID)
			continue
		}

		u.logger.Infof("[catchup] Trying peer %s (score: %.2f) for block %s", bestPeer.ID, bestPeer.CatchupReputationScore, blockHash.String())

		altErr := u.catchup(ctx, block, bestPeer.ID, bestPeer.DataHubURL)
		if altErr == nil {
			u.logger.Infof("[catchup] Successfully processed block %s from peer %s", blockHash.String(), bestPeer.ID)
			u.processBlockNotify.Delete(*blockHash)
			u.catchupAlternatives.Delete(*blockHash)
			return true
		}

		u.logger.Warnf("[catchup] Peer %s failed for block %s: %v", bestPeer.ID, blockHash.String(), altErr)
		u.reportCatchupFailure(ctx, bestPeer.ID)
	}

	return false
}
