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

// selectBestPeersForCatchup queries the P2P service for peers suitable for catchup.
//
// A peer is eligible under a two-tier gate:
//  1. Validated-ahead (primary): its locally-validated chainwork exceeds localChainWork.
//  2. Probe fallback: its advertised height is at or above targetHeight AND within
//     MaxUnvalidatedAdvertisedHeightLead of our local best height. The lead is bounded
//     against localBestHeight (our own tip), not targetHeight, so a hostile block with
//     a manipulated height field cannot shrink or expand the probe window.
//
// Peers with validated history that do NOT exceed the local tip remain probe-eligible —
// they are known-good sources and may still have the block we need. The validated-work
// gate determines tier ordering (and therefore retry priority), not admission.
//
// Validated-ahead peers are returned before probe candidates. Both tiers preserve the
// p2p server's reputation ordering (highest score first).
func (u *Server) selectBestPeersForCatchup(ctx context.Context, targetHeight uint32, localChainWork []byte, localBestHeight uint32) ([]PeerForCatchup, error) {
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

	var validatedAhead, probes []PeerForCatchup
	for _, p := range peerInfos {
		if p.DataHubURL == "" {
			u.logger.Debugf("[peer_selection] Skipping peer %s (no DataHub URL - listen-only node)", p.ID.String())
			continue
		}

		hasValidated := p.ValidatedBlockHash != nil && len(p.ValidatedChainWork) > 0
		// Primary gate: we have locally validated this peer's chain beyond our tip.
		// When localChainWork is nil (blockchain client unavailable or returned empty
		// work), this is always false — the probe path below still allows known-good
		// peers through based on advertised height.
		aheadByValidatedWork := hasValidated && len(localChainWork) > 0 &&
			work.CompareChainWork(p.ValidatedChainWork, localChainWork) > 0

		// Probe fallback: open to any peer (validated or not) claiming a plausible
		// height. Bounding against localBestHeight (not targetHeight) means a hostile
		// block height field cannot manipulate which peers are reachable. Peers with
		// validated work that does not exceed the local tip are still known-good
		// sources and remain probe-eligible here.
		probeEligible := p.Height >= targetHeight &&
			uint64(p.Height) <= uint64(localBestHeight)+maxUnvalidatedLead

		if !aheadByValidatedWork && !probeEligible {
			u.logger.Debugf("[peer_selection] Skipping peer %s (work not ahead, height %d ineligible for target %d with local tip %d)", p.ID.String(), p.Height, targetHeight, localBestHeight)
			continue
		}

		candidate := PeerForCatchup{
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
		}

		if aheadByValidatedWork {
			validatedAhead = append(validatedAhead, candidate)
		} else {
			probes = append(probes, candidate)
		}
	}

	// Validated-ahead peers first (strongly preferred), then probes. Both tiers
	// preserve the p2p server's reputation ordering (highest score first).
	peers := append(validatedAhead, probes...)

	u.logger.Infof("[peer_selection] Selected %d peers for catchup (%d validated-ahead, %d probes, from %d total)", len(peers), len(validatedAhead), len(probes), len(peerInfos))
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
// peer's advertised height may exceed localBestHeight and still be probe-eligible.
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
	// Guard before the GetBestBlockHeader call: existing tests set a blockchain mock
	// without a GetBestBlockHeader expectation and rely on p2pClient==nil causing an
	// early return. Checking here prevents an unexpected RPC on the mock.
	if u.p2pClient == nil {
		u.logger.Debugf("[peer_selection] P2P client not available, skipping alternative peer catchup")
		return false
	}

	blockHash := block.Hash()

	var localChainWork []byte
	var localBestHeight uint32
	if u.blockchainClient != nil {
		tipCtx, cancel := context.WithTimeout(ctx, catchupReputationReportTimeout)
		defer cancel()
		if _, meta, err := u.blockchainClient.GetBestBlockHeader(tipCtx); err == nil && meta != nil {
			localChainWork = meta.ChainWork
			localBestHeight = meta.Height
		} else if err != nil {
			u.logger.Warnf("[peer_selection] Failed to read local chain tip for catchup peer gating: %v", err)
		}
	}

	bestPeers, peerErr := u.selectBestPeersForCatchup(ctx, block.Height, localChainWork, localBestHeight)
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

