package p2p

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RecordCatchupAttempt records that a catchup attempt was made to a peer
func (s *Server) RecordCatchupAttempt(_ context.Context, req *p2p_api.RecordCatchupAttemptRequest) (*p2p_api.RecordCatchupAttemptResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	s.peerRegistry.RecordCatchupAttempt(peerID)

	return &p2p_api.RecordCatchupAttemptResponse{Ok: true}, nil
}

// RecordCatchupSuccess records a successful catchup from a peer
func (s *Server) RecordCatchupSuccess(_ context.Context, req *p2p_api.RecordCatchupSuccessRequest) (*p2p_api.RecordCatchupSuccessResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	duration := time.Duration(req.DurationMs) * time.Millisecond
	s.peerRegistry.RecordCatchupSuccess(peerID, duration)

	return &p2p_api.RecordCatchupSuccessResponse{Ok: true}, nil
}

// RecordCatchupFailure records a failed catchup attempt from a peer
func (s *Server) RecordCatchupFailure(_ context.Context, req *p2p_api.RecordCatchupFailureRequest) (*p2p_api.RecordCatchupFailureResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		return &p2p_api.RecordCatchupFailureResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	s.peerRegistry.RecordCatchupFailure(peerID)

	return &p2p_api.RecordCatchupFailureResponse{Ok: true}, nil
}

// RecordCatchupMalicious records malicious behavior detected during catchup
func (s *Server) RecordCatchupMalicious(_ context.Context, req *p2p_api.RecordCatchupMaliciousRequest) (*p2p_api.RecordCatchupMaliciousResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.RecordCatchupMaliciousResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		return &p2p_api.RecordCatchupMaliciousResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	s.peerRegistry.RecordCatchupMalicious(peerID)

	return &p2p_api.RecordCatchupMaliciousResponse{Ok: true}, nil
}

// UpdateCatchupReputation updates the reputation score for a peer
func (s *Server) UpdateCatchupReputation(_ context.Context, req *p2p_api.UpdateCatchupReputationRequest) (*p2p_api.UpdateCatchupReputationResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.UpdateCatchupReputationResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		return &p2p_api.UpdateCatchupReputationResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	s.peerRegistry.UpdateCatchupReputation(peerID, req.Score)

	return &p2p_api.UpdateCatchupReputationResponse{Ok: true}, nil
}

// UpdateCatchupError updates the last catchup error for a peer
func (s *Server) UpdateCatchupError(_ context.Context, req *p2p_api.UpdateCatchupErrorRequest) (*p2p_api.UpdateCatchupErrorResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.UpdateCatchupErrorResponse{Ok: false}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		return &p2p_api.UpdateCatchupErrorResponse{Ok: false}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	s.peerRegistry.UpdateCatchupError(peerID, req.ErrorMsg)

	return &p2p_api.UpdateCatchupErrorResponse{Ok: true}, nil
}

// GetPeersForCatchup returns peers suitable for catchup operations
func (s *Server) GetPeersForCatchup(_ context.Context, _ *p2p_api.GetPeersForCatchupRequest) (*p2p_api.GetPeersForCatchupResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.GetPeersForCatchupResponse{Peers: []*p2p_api.PeerInfoForCatchup{}}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	peers := s.peerRegistry.GetPeersForCatchup()

	// Convert to proto format
	protoPeers := make([]*p2p_api.PeerInfoForCatchup, 0, len(peers))

	for _, p := range peers {
		// Calculate total attempts as sum of successes and failures
		// InteractionAttempts is a separate counter that may not match
		totalAttempts := p.InteractionSuccesses + p.InteractionFailures

		blockHashStr := ""
		if p.BlockHash != nil {
			blockHashStr = p.BlockHash.String()
		}

		protoPeers = append(protoPeers, &p2p_api.PeerInfoForCatchup{
			Id:                     p.ID.String(),
			Height:                 p.Height,
			BlockHash:              blockHashStr,
			DataHubUrl:             p.DataHubURL,
			CatchupReputationScore: p.ReputationScore,
			CatchupAttempts:        totalAttempts,          // Use calculated total, not InteractionAttempts
			CatchupSuccesses:       p.InteractionSuccesses, // Number of successful interactions
			CatchupFailures:        p.InteractionFailures,  // Number of failed interactions
		})
	}

	return &p2p_api.GetPeersForCatchupResponse{Peers: protoPeers}, nil
}

// ReportValidSubtree is a gRPC handler for reporting valid subtree reception
func (s *Server) ReportValidSubtree(_ context.Context, req *p2p_api.ReportValidSubtreeRequest) (*p2p_api.ReportValidSubtreeResponse, error) {
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

	// Decode peer ID
	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: "invalid peer ID",
		}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	// Record successful subtree reception directly with peer ID
	// Use a nominal duration since we don't have timing info at this level
	s.peerRegistry.RecordSubtreeReceived(peerID, 0)
	s.logger.Debugf("[ReportValidSubtree] Recorded successful subtree %s from peer %s", req.SubtreeHash, req.PeerId)

	return &p2p_api.ReportValidSubtreeResponse{
		Success: true,
		Message: "subtree validation recorded",
	}, nil
}

// ReportValidBlock is a gRPC handler for reporting valid block reception
func (s *Server) ReportValidBlock(_ context.Context, req *p2p_api.ReportValidBlockRequest) (*p2p_api.ReportValidBlockResponse, error) {
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

	// Decode peer ID
	peerID, err := peer.Decode(req.PeerId)
	if err != nil {
		return &p2p_api.ReportValidBlockResponse{
			Success: false,
			Message: "invalid peer ID",
		}, errors.WrapGRPC(errors.NewProcessingError("invalid peer ID: %v", err))
	}

	// Record successful block reception directly with peer ID
	// Use a nominal duration since we don't have timing info at this level
	s.peerRegistry.RecordBlockReceived(peerID, 0)
	s.logger.Debugf("[ReportValidBlock] Recorded successful block %s from peer %s", req.BlockHash, req.PeerId)

	return &p2p_api.ReportValidBlockResponse{
		Success: true,
		Message: "block validation recorded",
	}, nil
}

// IsPeerMalicious checks if a peer is considered malicious based on their behavior
func (s *Server) IsPeerMalicious(_ context.Context, req *p2p_api.IsPeerMaliciousRequest) (*p2p_api.IsPeerMaliciousResponse, error) {
	if req.PeerId == "" {
		return &p2p_api.IsPeerMaliciousResponse{
			IsMalicious: false,
			Reason:      "empty peer ID",
		}, nil
	}

	// Check if peer is in the ban list
	if s.banManager != nil && s.banManager.IsBanned(req.PeerId) {
		return &p2p_api.IsPeerMaliciousResponse{
			IsMalicious: true,
			Reason:      "peer is banned",
		}, nil
	}

	// A peer is ONLY considered malicious if they are explicitly banned.
	// Low reputation scores are handled by IsPeerUnhealthy(), not here.
	// This distinction is critical: malicious = serving invalid data, unhealthy = poor performance.
	// During catchup, we should still try unhealthy peers if they're our only option,
	// but never try truly malicious (banned) peers.

	return &p2p_api.IsPeerMaliciousResponse{
		IsMalicious: false,
		Reason:      "",
	}, nil
}

// IsPeerUnhealthy checks if a peer is considered unhealthy based on their performance
func (s *Server) IsPeerUnhealthy(_ context.Context, req *p2p_api.IsPeerUnhealthyRequest) (*p2p_api.IsPeerUnhealthyResponse, error) {
	if req.PeerId == "" {
		return &p2p_api.IsPeerUnhealthyResponse{
			IsUnhealthy:     true,
			Reason:          "empty peer ID",
			ReputationScore: 0,
		}, nil
	}

	// Check peer registry for health status
	if s.peerRegistry != nil {
		peerId, err := peer.Decode(req.PeerId)
		if err != nil {
			return &p2p_api.IsPeerUnhealthyResponse{
				IsUnhealthy:     true,
				Reason:          "invalid peer ID",
				ReputationScore: 0,
			}, nil
		}
		peerInfo, exists := s.peerRegistry.Get(peerId)
		if !exists {
			// Unknown peer - consider unhealthy since we have no information about them
			return &p2p_api.IsPeerUnhealthyResponse{
				IsUnhealthy:     true,
				Reason:          "unknown peer",
				ReputationScore: 0,
			}, nil
		}

		// A peer is considered unhealthy if:
		// 1. They have a low reputation score (below 40)
		// 2. They have a high failure rate
		if peerInfo.ReputationScore < 40 {
			return &p2p_api.IsPeerUnhealthyResponse{
				IsUnhealthy:     true,
				Reason:          fmt.Sprintf("low reputation score: %.2f", peerInfo.ReputationScore),
				ReputationScore: float32(peerInfo.ReputationScore),
			}, nil
		}

		// Check success rate based on total interactions (successes + failures)
		totalInteractions := peerInfo.InteractionSuccesses + peerInfo.InteractionFailures
		if totalInteractions > 10 && peerInfo.InteractionSuccesses < totalInteractions/2 {
			successRate := float64(peerInfo.InteractionSuccesses) / float64(totalInteractions)
			return &p2p_api.IsPeerUnhealthyResponse{
				IsUnhealthy:     true,
				Reason:          fmt.Sprintf("low success rate: %.2f%%", successRate*100),
				ReputationScore: float32(peerInfo.ReputationScore),
			}, nil
		}

		// Peer is healthy
		return &p2p_api.IsPeerUnhealthyResponse{
			IsUnhealthy:     false,
			Reason:          "",
			ReputationScore: float32(peerInfo.ReputationScore),
		}, nil
	}

	// If we can't determine health, consider unhealthy
	return &p2p_api.IsPeerUnhealthyResponse{
		IsUnhealthy:     true,
		Reason:          "unable to determine peer health",
		ReputationScore: 0,
	}, nil
}
