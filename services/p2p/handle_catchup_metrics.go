package p2p

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	errPeerRegistryNotInitialized = "peer registry not initialized"
	errInvalidPeerIDFormat        = "invalid peer ID: %v"
	errPeerIDRequired             = "peer ID is required"
	errInvalidPeerID              = "invalid peer ID"
	errBlockHashRequired          = "block hash is required"
)

const (
	// maxReportedChainWorkBytes bounds inbound advisory chainwork on the
	// ReportValidatedChainProgress RPC. It aliases the registry's single source of
	// truth so the two stay in lockstep.
	maxReportedChainWorkBytes = blockchain.MaxValidatedChainWorkBytes

	catchupFailureKindGeneric         = "generic"
	catchupFailureKindBlockIncomplete = "block_incomplete"

	defaultFullStoragePenaltyDuration = time.Hour
)

// RecordCatchupAttempt records that a catchup attempt was made to a peer.
// Backed by the centralized peer registry's catchup-attempt counter, which
// also updates the sync backoff tracking fields.
func (s *Server) RecordCatchupAttempt(ctx context.Context, req *p2p_api.RecordCatchupAttemptRequest) (*p2p_api.RecordCatchupAttemptResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError(errInvalidPeerIDFormat, err))
	}

	if err := s.peerRegistry.RecordCatchupAttempt(ctx, req.PeerId); err != nil {
		return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("record catchup attempt", err))
	}

	return &p2p_api.RecordCatchupAttemptResponse{Ok: true}, nil
}

// RecordCatchupSuccess records a successful catchup from a peer.
func (s *Server) RecordCatchupSuccess(ctx context.Context, req *p2p_api.RecordCatchupSuccessRequest) (*p2p_api.RecordCatchupSuccessResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError(errInvalidPeerIDFormat, err))
	}

	if err := s.peerRegistry.RecordCatchupSuccess(ctx, req.PeerId, req.DurationMs); err != nil {
		return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("record catchup success", err))
	}

	return &p2p_api.RecordCatchupSuccessResponse{Ok: true}, nil
}

// RecordCatchupFailure records a failed catchup attempt from a peer.
func (s *Server) RecordCatchupFailure(ctx context.Context, req *p2p_api.RecordCatchupFailureRequest) (*p2p_api.RecordCatchupFailureResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError(errInvalidPeerIDFormat, err))
	}

	if err := s.peerRegistry.RecordCatchupFailure(ctx, req.PeerId); err != nil {
		return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("record catchup failure", err))
	}

	if normalizeCatchupFailureKind(req.FailureKind) == catchupFailureKindBlockIncomplete {
		if err := s.recordBlockIncompleteCatchupFailure(ctx, req.PeerId, req.BlockHash); err != nil {
			return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(err)
		}
	}

	return &p2p_api.RecordCatchupFailureResponse{Ok: true}, nil
}

func normalizeCatchupFailureKind(kind string) string {
	switch kind {
	case "", catchupFailureKindGeneric:
		return catchupFailureKindGeneric
	case catchupFailureKindBlockIncomplete:
		return catchupFailureKindBlockIncomplete
	default:
		return catchupFailureKindGeneric
	}
}

func (s *Server) recordBlockIncompleteCatchupFailure(ctx context.Context, peerID, blockHash string) error {
	info, found, err := s.peerRegistry.GetPeer(ctx, peerID)
	if err != nil {
		return errors.NewServiceError("get peer for block-incomplete catchup failure", err)
	}

	if err := s.peerRegistry.RecordCatchupError(ctx, peerID, blockIncompleteCatchupError(blockHash)); err != nil {
		return errors.NewServiceError("record block-incomplete catchup error", err)
	}

	if !found {
		return nil
	}

	// A block-incomplete failure means the peer served header work it could not
	// back with a full block body. Validated header work is credited before the
	// block body is delivered, so without a penalty a header-only non-deliverer
	// keeps top-of-pool ranking. Open a penalty window so the credited validated
	// work stops conferring top-tier sync eligibility (see peerAheadByValidatedWork)
	// until fresh delivery re-establishes confidence — for any storage class, not
	// just peers that claimed full storage. A full-storage claim additionally
	// counts as a contradiction and is demoted to non-full for the duration.
	penalty := &blockchain.PeerInfo{
		ID:                      peerID,
		FullStoragePenaltyUntil: time.Now().Add(s.fullStoragePenaltyDuration()),
	}
	if info.Storage == "full" {
		// Delta, not absolute: the registry adds this under its lock, so the
		// increment can't be lost to a concurrent read-modify-write.
		penalty.FullStorageContradictions = 1
	}
	if err := s.peerRegistry.RegisterPeer(ctx, penalty); err != nil {
		return errors.NewServiceError("record block-incomplete penalty", err)
	}

	if info.Storage == "full" {
		if err := s.peerRegistry.UpdateStorage(ctx, peerID, ""); err != nil {
			return errors.NewServiceError("clear contradicted full-storage claim", err)
		}
	}

	return nil
}

func (s *Server) fullStoragePenaltyDuration() time.Duration {
	if s != nil && s.settings != nil && s.settings.P2P.FullStoragePenaltyDuration > 0 {
		return s.settings.P2P.FullStoragePenaltyDuration
	}
	return defaultFullStoragePenaltyDuration
}

func blockIncompleteCatchupError(blockHash string) string {
	if blockHash == "" {
		return "block incomplete during catchup"
	}
	return fmt.Sprintf("block incomplete during catchup: %s", blockHash)
}

// RecordCatchupMalicious records malicious behavior detected during catchup.
func (s *Server) RecordCatchupMalicious(ctx context.Context, req *p2p_api.RecordCatchupMaliciousRequest) (*p2p_api.RecordCatchupMaliciousResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupMaliciousResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.RecordCatchupMaliciousResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError(errInvalidPeerIDFormat, err))
	}

	if err := s.peerRegistry.UpdatePeerMetrics(ctx, req.PeerId, 0, 0, 0, false, false, true, 0); err != nil {
		return &p2p_api.RecordCatchupMaliciousResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("update peer metrics", err))
	}

	return &p2p_api.RecordCatchupMaliciousResponse{Ok: true}, nil
}

// UpdateCatchupReputation is preserved for API compatibility but no longer
// applies a manual reputation override; the centralized registry computes
// reputation deterministically from interaction outcomes. The RPC remains so
// existing consumers (legacy service) compile without changes. Remove once
// legacy consumers are migrated to blockchain.PeerRegistryClientI in a
// follow-up PR.
func (s *Server) UpdateCatchupReputation(_ context.Context, _ *p2p_api.UpdateCatchupReputationRequest) (*p2p_api.UpdateCatchupReputationResponse, error) {
	return &p2p_api.UpdateCatchupReputationResponse{Ok: true}, nil
}

// UpdateCatchupError records the most recent catchup error reported against a peer.
func (s *Server) UpdateCatchupError(ctx context.Context, req *p2p_api.UpdateCatchupErrorRequest) (*p2p_api.UpdateCatchupErrorResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.UpdateCatchupErrorResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.UpdateCatchupErrorResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError(errInvalidPeerIDFormat, err))
	}

	if err := s.peerRegistry.RecordCatchupError(ctx, req.PeerId, req.ErrorMsg); err != nil {
		return &p2p_api.UpdateCatchupErrorResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("record catchup error", err))
	}

	return &p2p_api.UpdateCatchupErrorResponse{Ok: true}, nil
}

// GetPeersForCatchup returns peers suitable for catchup operations: HTTP transport,
// non-banned, with a DataHub URL, sorted by storage preference and reputation.
func (s *Server) GetPeersForCatchup(ctx context.Context, _ *p2p_api.GetPeersForCatchupRequest) (*p2p_api.GetPeersForCatchupResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.GetPeersForCatchupResponse{Peers: []*p2p_api.PeerInfoForCatchup{}}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	peers, err := s.peerRegistry.ListPeers(ctx, nil, 0, 0, true, true)
	if err != nil {
		return &p2p_api.GetPeersForCatchupResponse{Peers: []*p2p_api.PeerInfoForCatchup{}}, errors.WrapGRPC(errors.NewServiceError("list peers", err))
	}

	protoPeers := make([]*p2p_api.PeerInfoForCatchup, 0, len(peers))
	for _, p := range peers {
		if p.DataHubURL == "" {
			continue
		}

		// Enforce the operator blacklist at the point of use: a URL stored in
		// the registry before its host was blacklisted must not be handed to
		// catchup (gossip-time checks cannot evict already-stored URLs).
		if s.isBlacklistedBaseURL(p.DataHubURL) {
			s.logger.Debugf("[GetPeersForCatchup] skipping peer %s with blacklisted DataHubURL %s", p.ID, p.DataHubURL)
			continue
		}

		blockHashStr := ""
		if p.BlockHash != nil {
			blockHashStr = p.BlockHash.String()
		}

		protoPeers = append(protoPeers, &p2p_api.PeerInfoForCatchup{
			Id:                     p.ID,
			Height:                 p.Height,
			BlockHash:              blockHashStr,
			DataHubUrl:             p.DataHubURL,
			CatchupReputationScore: p.ReputationScore,
			CatchupAttempts:        p.CatchupAttempts,
			CatchupSuccesses:       p.CatchupSuccesses,
			CatchupFailures:        p.CatchupFailures,
		})
	}

	return &p2p_api.GetPeersForCatchupResponse{Peers: protoPeers}, nil
}

// ReportValidSubtree records successful subtree reception against a peer.
func (s *Server) ReportValidSubtree(ctx context.Context, req *p2p_api.ReportValidSubtreeRequest) (*p2p_api.ReportValidSubtreeResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: errPeerRegistryNotInitialized,
		}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	if req.PeerId == "" {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: errPeerIDRequired,
		}, errors.WrapGRPC(errors.NewInvalidArgumentError(errPeerIDRequired))
	}

	if req.SubtreeHash == "" {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: "subtree hash is required",
		}, errors.WrapGRPC(errors.NewInvalidArgumentError("subtree hash is required"))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: errInvalidPeerID,
		}, errors.WrapGRPC(errors.NewProcessingError(errInvalidPeerIDFormat, err))
	}

	if err := s.peerRegistry.RecordSubtreeReceived(ctx, req.PeerId, 0); err != nil {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: "failed to record subtree",
		}, errors.WrapGRPC(errors.NewServiceError("record subtree received", err))
	}
	s.logger.Debugf("[ReportValidSubtree] Recorded successful subtree %s from peer %s", req.SubtreeHash, req.PeerId)

	return &p2p_api.ReportValidSubtreeResponse{
		Success: true,
		Message: "subtree validation recorded",
	}, nil
}

// ReportValidBlock records successful block reception against a peer.
func (s *Server) ReportValidBlock(ctx context.Context, req *p2p_api.ReportValidBlockRequest) (*p2p_api.ReportValidBlockResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: errPeerRegistryNotInitialized,
		}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	if req.PeerId == "" {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: errPeerIDRequired,
		}, errors.WrapGRPC(errors.NewInvalidArgumentError(errPeerIDRequired))
	}

	if req.BlockHash == "" {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: errBlockHashRequired,
		}, errors.WrapGRPC(errors.NewInvalidArgumentError(errBlockHashRequired))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: errInvalidPeerID,
		}, errors.WrapGRPC(errors.NewProcessingError(errInvalidPeerIDFormat, err))
	}

	if err := s.peerRegistry.RecordBlockReceived(ctx, req.PeerId, 0); err != nil {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: "failed to record block",
		}, errors.WrapGRPC(errors.NewServiceError("record block received", err))
	}
	s.logger.Debugf("[ReportValidBlock] Recorded successful block %s from peer %s", req.BlockHash, req.PeerId)

	return &p2p_api.ReportValidBlockResponse{
		Success: true,
		Message: "block validation recorded",
	}, nil
}

// ReportValidatedChainProgress forwards locally validated header-chain progress
// to the blockchain peer registry. Downstream registry failures are non-fatal
// so mixed-version services keep validating and syncing.
func (s *Server) ReportValidatedChainProgress(ctx context.Context, req *p2p_api.ReportValidatedChainProgressRequest) (*p2p_api.ReportValidatedChainProgressResponse, error) {
	if req.PeerId == "" {
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: false,
			Message: errPeerIDRequired,
		}, errors.WrapGRPC(errors.NewInvalidArgumentError(errPeerIDRequired))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: false,
			Message: errInvalidPeerID,
		}, errors.WrapGRPC(errors.NewInvalidArgumentError(errInvalidPeerIDFormat, err))
	}

	if req.Height == 0 {
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: false,
			Message: "height is required",
		}, errors.WrapGRPC(errors.NewInvalidArgumentError("height is required"))
	}

	if req.BlockHash == "" {
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: false,
			Message: errBlockHashRequired,
		}, errors.WrapGRPC(errors.NewInvalidArgumentError(errBlockHashRequired))
	}

	blockHash, err := chainhash.NewHashFromStr(req.BlockHash)
	if err != nil {
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: false,
			Message: "invalid block hash",
		}, errors.WrapGRPC(errors.NewInvalidArgumentError("invalid block hash: %v", err))
	}

	if len(req.ChainWork) == 0 {
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: false,
			Message: "chainwork is required",
		}, errors.WrapGRPC(errors.NewInvalidArgumentError("chainwork is required"))
	}

	if len(req.ChainWork) > maxReportedChainWorkBytes {
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: false,
			Message: "chainwork is too large",
		}, errors.WrapGRPC(errors.NewInvalidArgumentError("chainwork is too large: %d bytes", len(req.ChainWork)))
	}

	if s.peerRegistry == nil {
		s.logger.Warnf("[ReportValidatedChainProgress] Peer registry unavailable while recording peer %s at height %d", req.PeerId, req.Height)
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: true,
			Message: "validated chain progress accepted",
		}, nil
	}

	if err := s.peerRegistry.RecordValidatedPeerProgress(ctx, req.PeerId, req.Height, blockHash, req.ChainWork); err != nil {
		s.logger.Warnf("[ReportValidatedChainProgress] Failed to record validated progress for peer %s at height %d: %v", req.PeerId, req.Height, err)
		return &p2p_api.ReportValidatedChainProgressResponse{
			Success: true,
			Message: "validated chain progress accepted",
		}, nil
	}

	s.logger.Debugf("[ReportValidatedChainProgress] Recorded validated progress for peer %s at height %d", req.PeerId, req.Height)
	return &p2p_api.ReportValidatedChainProgressResponse{
		Success: true,
		Message: "validated chain progress recorded",
	}, nil
}

// ReportValidBlockHeaders records that a peer successfully served a batch of
// block headers during catchup. This credits the peer with a generic
// interaction success (reputation and response time) WITHOUT incrementing the
// catchup-operation counters: a headers batch is one stage of a catchup, not a
// completed catchup. Keeping this credit matters — during a catchup that keeps
// failing at the block-fetch stage (e.g. the serving peer rate-limits subtree
// requests), it offsets the recorded failures so the peer's reputation does not
// collapse below the unhealthy threshold and get the only viable peer excluded.
func (s *Server) ReportValidBlockHeaders(ctx context.Context, req *p2p_api.ReportValidBlockHeadersRequest) (*p2p_api.ReportValidBlockHeadersResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.ReportValidBlockHeadersResponse{
			Success: false,
			Message: errPeerRegistryNotInitialized,
		}, errors.WrapGRPC(errors.NewServiceError(errPeerRegistryNotInitialized))
	}

	if req.PeerId == "" {
		return &p2p_api.ReportValidBlockHeadersResponse{
			Success: false,
			Message: errPeerIDRequired,
		}, errors.WrapGRPC(errors.NewInvalidArgumentError(errPeerIDRequired))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.ReportValidBlockHeadersResponse{
			Success: false,
			Message: errInvalidPeerID,
		}, errors.WrapGRPC(errors.NewProcessingError(errInvalidPeerIDFormat, err))
	}

	if err := s.peerRegistry.UpdatePeerMetrics(ctx, req.PeerId, 0, 0, 0, true, false, false, req.DurationMs); err != nil {
		return &p2p_api.ReportValidBlockHeadersResponse{
			Success: false,
			Message: "failed to record headers success",
		}, errors.WrapGRPC(errors.NewServiceError("update peer metrics", err))
	}

	return &p2p_api.ReportValidBlockHeadersResponse{
		Success: true,
		Message: "headers success recorded",
	}, nil
}

// IsPeerMalicious returns whether a peer is currently considered malicious
// (banned in the centralized registry).
func (s *Server) IsPeerMalicious(ctx context.Context, req *p2p_api.IsPeerMaliciousRequest) (*p2p_api.IsPeerMaliciousResponse, error) {
	if req.PeerId == "" {
		return &p2p_api.IsPeerMaliciousResponse{
			IsMalicious: false,
			Reason:      "empty peer ID",
		}, nil
	}

	banned := false
	if s.peerRegistry != nil {
		var err error
		banned, err = s.peerRegistry.IsPeerBanned(ctx, req.PeerId)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewServiceError("is peer banned", err))
		}
	}

	if banned {
		return &p2p_api.IsPeerMaliciousResponse{
			IsMalicious: true,
			Reason:      "peer is banned",
		}, nil
	}

	return &p2p_api.IsPeerMaliciousResponse{
		IsMalicious: false,
		Reason:      "",
	}, nil
}

// IsPeerUnhealthy returns whether a peer is currently considered unhealthy
// based on reputation and success-rate signals from the centralized registry.
func (s *Server) IsPeerUnhealthy(ctx context.Context, req *p2p_api.IsPeerUnhealthyRequest) (*p2p_api.IsPeerUnhealthyResponse, error) {
	if req.PeerId == "" {
		return &p2p_api.IsPeerUnhealthyResponse{
			IsUnhealthy:     true,
			Reason:          "empty peer ID",
			ReputationScore: 0,
		}, nil
	}

	if s.peerRegistry == nil {
		return &p2p_api.IsPeerUnhealthyResponse{
			IsUnhealthy:     true,
			Reason:          "unable to determine peer health",
			ReputationScore: 0,
		}, nil
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.IsPeerUnhealthyResponse{
			IsUnhealthy:     true,
			Reason:          errInvalidPeerID,
			ReputationScore: 0,
		}, nil
	}

	peerInfo, found, err := s.peerRegistry.GetPeer(ctx, req.PeerId)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewServiceError("get peer", err))
	}
	if !found {
		return &p2p_api.IsPeerUnhealthyResponse{
			IsUnhealthy:     true,
			Reason:          "unknown peer",
			ReputationScore: 0,
		}, nil
	}

	if peerInfo.ReputationScore < 40 {
		return &p2p_api.IsPeerUnhealthyResponse{
			IsUnhealthy:     true,
			Reason:          fmt.Sprintf("low reputation score: %.2f", peerInfo.ReputationScore),
			ReputationScore: float32(peerInfo.ReputationScore),
		}, nil
	}

	totalInteractions := peerInfo.InteractionSuccesses + peerInfo.InteractionFailures
	if totalInteractions > 10 && peerInfo.InteractionSuccesses < totalInteractions/2 {
		successRate := float64(peerInfo.InteractionSuccesses) / float64(totalInteractions)
		return &p2p_api.IsPeerUnhealthyResponse{
			IsUnhealthy:     true,
			Reason:          fmt.Sprintf("low success rate: %.2f%%", successRate*100),
			ReputationScore: float32(peerInfo.ReputationScore),
		}, nil
	}

	return &p2p_api.IsPeerUnhealthyResponse{
		IsUnhealthy:     false,
		Reason:          "",
		ReputationScore: float32(peerInfo.ReputationScore),
	}, nil
}
