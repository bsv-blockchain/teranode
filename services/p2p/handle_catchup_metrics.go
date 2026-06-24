package p2p

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RecordCatchupAttempt records that a catchup attempt was made to a peer.
// Backed by the centralized peer registry's interaction-attempt counter.
func (s *Server) RecordCatchupAttempt(ctx context.Context, req *p2p_api.RecordCatchupAttemptRequest) (*p2p_api.RecordCatchupAttemptResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	// UpdatePeerMetrics with no flags increments LastSeen and BytesReceived; bump
	// the attempt counter via RecordSyncAttempt which sets LastSyncAttempt.
	if err := s.peerRegistry.RecordSyncAttempt(ctx, req.PeerId); err != nil {
		return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("record sync attempt", err))
	}

	return &p2p_api.RecordCatchupAttemptResponse{Ok: true}, nil
}

// RecordCatchupSuccess records a successful catchup from a peer.
func (s *Server) RecordCatchupSuccess(ctx context.Context, req *p2p_api.RecordCatchupSuccessRequest) (*p2p_api.RecordCatchupSuccessResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	if err := s.peerRegistry.UpdatePeerMetrics(ctx, req.PeerId, 0, 0, 0, true, false, false, req.DurationMs); err != nil {
		return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("update peer metrics", err))
	}

	return &p2p_api.RecordCatchupSuccessResponse{Ok: true}, nil
}

// RecordCatchupFailure records a failed catchup attempt from a peer.
func (s *Server) RecordCatchupFailure(ctx context.Context, req *p2p_api.RecordCatchupFailureRequest) (*p2p_api.RecordCatchupFailureResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	if err := s.peerRegistry.UpdatePeerMetrics(ctx, req.PeerId, 0, 0, 0, false, true, false, 0); err != nil {
		return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("update peer metrics", err))
	}

	return &p2p_api.RecordCatchupFailureResponse{Ok: true}, nil
}

// RecordCatchupMalicious records malicious behavior detected during catchup.
func (s *Server) RecordCatchupMalicious(ctx context.Context, req *p2p_api.RecordCatchupMaliciousRequest) (*p2p_api.RecordCatchupMaliciousResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupMaliciousResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.RecordCatchupMaliciousResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
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
		return &p2p_api.UpdateCatchupErrorResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.UpdateCatchupErrorResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
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
		return &p2p_api.GetPeersForCatchupResponse{Peers: []*p2p_api.PeerInfoForCatchup{}}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
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

		totalAttempts := p.InteractionSuccesses + p.InteractionFailures

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
			CatchupAttempts:        totalAttempts,
			CatchupSuccesses:       p.InteractionSuccesses,
			CatchupFailures:        p.InteractionFailures,
		})
	}

	return &p2p_api.GetPeersForCatchupResponse{Peers: protoPeers}, nil
}

// ReportValidSubtree records successful subtree reception against a peer.
func (s *Server) ReportValidSubtree(ctx context.Context, req *p2p_api.ReportValidSubtreeRequest) (*p2p_api.ReportValidSubtreeResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: "peer registry not initialized",
		}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	if req.PeerId == "" {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: "peer ID is required",
		}, errors.WrapGRPC(errors.NewInvalidArgumentError("peer ID is required"))
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
			Message: "invalid peer ID",
		}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
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
			Message: "peer registry not initialized",
		}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	if req.PeerId == "" {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: "peer ID is required",
		}, errors.WrapGRPC(errors.NewInvalidArgumentError("peer ID is required"))
	}

	if req.BlockHash == "" {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: "block hash is required",
		}, errors.WrapGRPC(errors.NewInvalidArgumentError("block hash is required"))
	}

	if _, err := peer.Decode(req.PeerId); err != nil {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: "invalid peer ID",
		}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
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
			Reason:          "invalid peer ID",
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
