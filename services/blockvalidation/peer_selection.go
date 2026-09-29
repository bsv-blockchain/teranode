package blockvalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
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
	// Locally validated chain progress. Zero/nil when no headers from this peer
	// have been validated locally via ReportValidatedChainProgress.
	ValidatedHeight    uint32
	ValidatedBlockHash *chainhash.Hash
	ValidatedChainWork []byte
}

// selectBestPeersForCatchup queries the P2P service for peers suitable for catchup,
// sorted by reputation score (highest first).
//
// A peer is eligible if:
//  1. Its locally-validated chainwork exceeds localChainWork (primary gate), OR
//  2. It has no validated history but its advertised height meets the target and is
//     bounded against the target to reject absurd claims (probe fallback).
//
// Parameters:
//   - ctx: Context for the gRPC call
//   - targetHeight: The height we're trying to catch up to
//   - localChainWork: The node's own best validated chainwork for comparison
//
// Returns:
//   - []PeerForCatchup: List of eligible peers sorted by reputation (best first)
//   - error: If the query fails
func (u *Server) selectBestPeersForCatchup(ctx context.Context, targetHeight uint32, localChainWork []byte) ([]PeerForCatchup, error) {
	if u.p2pClient == nil {
		u.logger.Debugf("[peer_selection] P2P client not available, using fallback peer selection")
		return nil, nil
	}

	peerInfos, err := u.p2pClient.GetPeersForCatchup(ctx)
	if err != nil {
		u.logger.Warnf("[peer_selection] Failed to get peers from P2P service: %v", err)
		return nil, err
	}

	if len(peerInfos) == 0 {
		u.logger.Debugf("[peer_selection] No peers available from P2P service")
		return nil, nil
	}

	maxUnvalidatedLead := uint64(maxUnvalidatedCatchupHeightLead(u))

	peers := make([]PeerForCatchup, 0, len(peerInfos))
	for _, p := range peerInfos {
		if p.DataHubURL == "" {
			u.logger.Debugf("[peer_selection] Skipping peer %s (no DataHub URL - listen-only node)", p.ID.String())
			continue
		}

		hasValidated := p.ValidatedBlockHash != nil && len(p.ValidatedChainWork) > 0
		aheadByValidatedWork := hasValidated && len(localChainWork) > 0 &&
			work.CompareChainWork(p.ValidatedChainWork, localChainWork) > 0

		// Probe fallback: a peer with no validated history may still be tried
		// if its advertised height meets the target and is plausible — rejecting
		// absurd claims like math.MaxUint32. A peer that HAS validated work but
		// does not exceed the local tip is not given the probe path: it has been
		// observed and is not ahead.
		probeEligible := !hasValidated &&
			p.Height >= targetHeight &&
			uint64(p.Height)-uint64(targetHeight) <= maxUnvalidatedLead

		if !aheadByValidatedWork && !probeEligible {
			u.logger.Debugf("[peer_selection] Skipping peer %s (validated work not ahead of local tip, height %d ineligible as probe for target %d)", p.ID.String(), p.Height, targetHeight)
			continue
		}

		peers = append(peers, PeerForCatchup{
			ID:                     p.ID.String(),
			Storage:                p.Storage,
			DataHubURL:             p.DataHubURL,
			Height:                 p.Height,
			BlockHash:              p.BlockHash,
			CatchupReputationScore: p.ReputationScore,
			CatchupAttempts:        p.CatchupAttempts,
			CatchupSuccesses:       p.CatchupSuccesses,
			CatchupFailures:        p.CatchupFailures,
			ValidatedHeight:        p.ValidatedHeight,
			ValidatedBlockHash:     p.ValidatedBlockHash,
			ValidatedChainWork:     append([]byte(nil), p.ValidatedChainWork...),
		})
	}

	u.logger.Infof("[peer_selection] Selected %d peers for catchup (from %d total)", len(peers), len(peerInfos))
	for i, p := range peers {
		successRate := float64(0)
		if resolved := p.CatchupSuccesses + p.CatchupFailures; resolved > 0 {
			successRate = float64(p.CatchupSuccesses) / float64(resolved) * 100
		}
		u.logger.Debugf("[peer_selection] Peer %d: %s (score: %.2f, success: %d ok / %d failed of %d attempts = %.1f%%, height: %d, validated: %v)", i+1, p.ID, p.CatchupReputationScore, p.CatchupSuccesses, p.CatchupFailures, p.CatchupAttempts, successRate, p.Height, len(p.ValidatedChainWork) > 0)
	}

	return peers, nil
}

// maxUnvalidatedCatchupHeightLead returns the max number of blocks an unvalidated
// peer's advertised height may exceed targetHeight and still be probe-eligible.
// Reuses the same setting as gossip sanitization so the policy is consistent.
func maxUnvalidatedCatchupHeightLead(u *Server) uint32 {
	if u.settings != nil {
		return u.settings.P2P.MaxUnvalidatedAdvertisedHeightLead
	}
	return 10_000
}

// tryAlternativePeersForCatchup attempts catchup with alternative peers from the P2P service.
// It skips the excludePeerID and any peers marked as malicious.
// Returns true if catchup succeeded with any peer.
func (u *Server) tryAlternativePeersForCatchup(ctx context.Context, block *model.Block, excludePeerID string) bool {
	blockHash := block.Hash()

	var localChainWork []byte
	if u.blockchainClient != nil {
		if _, meta, err := u.blockchainClient.GetBestBlockHeader(ctx); err == nil && meta != nil {
			localChainWork = meta.ChainWork
		}
	}

	bestPeers, peerErr := u.selectBestPeersForCatchup(ctx, block.Height, localChainWork)
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

		u.logger.Debugf("[catchup] Trying peer %s (score: %.2f) for block %s", bestPeer.ID, bestPeer.CatchupReputationScore, blockHash.String())

		altErr := u.catchup(ctx, block, bestPeer.ID, bestPeer.DataHubURL)
		if altErr == nil {
			u.logger.Debugf("[catchup] Successfully processed block %s from peer %s", blockHash.String(), bestPeer.ID)
			u.processBlockNotify.Delete(*blockHash)
			u.catchupAlternatives.Delete(*blockHash)
			return true
		}

		u.logger.Warnf("[catchup] Peer %s failed for block %s: %v", bestPeer.ID, blockHash.String(), altErr)
		u.reportCatchupFailureForError(ctx, bestPeer.ID, altErr)
	}

	return false
}
