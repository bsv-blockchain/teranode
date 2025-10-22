package p2p

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RecordCatchupAttempt records that a catchup attempt was made to a peer
func (s *Server) RecordCatchupAttempt(ctx context.Context, req *p2p_api.RecordCatchupAttemptRequest) (*p2p_api.RecordCatchupAttemptResponse, error) {
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
func (s *Server) RecordCatchupSuccess(ctx context.Context, req *p2p_api.RecordCatchupSuccessRequest) (*p2p_api.RecordCatchupSuccessResponse, error) {
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
func (s *Server) RecordCatchupFailure(ctx context.Context, req *p2p_api.RecordCatchupFailureRequest) (*p2p_api.RecordCatchupFailureResponse, error) {
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
func (s *Server) RecordCatchupMalicious(ctx context.Context, req *p2p_api.RecordCatchupMaliciousRequest) (*p2p_api.RecordCatchupMaliciousResponse, error) {
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
func (s *Server) UpdateCatchupReputation(ctx context.Context, req *p2p_api.UpdateCatchupReputationRequest) (*p2p_api.UpdateCatchupReputationResponse, error) {
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

// GetPeersForCatchup returns peers suitable for catchup operations
func (s *Server) GetPeersForCatchup(ctx context.Context, req *p2p_api.GetPeersForCatchupRequest) (*p2p_api.GetPeersForCatchupResponse, error) {
	if s.peerRegistry == nil {
		return &p2p_api.GetPeersForCatchupResponse{Peers: []*p2p_api.PeerInfoForCatchup{}}, errors.WrapGRPC(errors.NewServiceError("peer registry not initialized"))
	}

	peers := s.peerRegistry.GetPeersForCatchup()

	// Convert to proto format
	protoPeers := make([]*p2p_api.PeerInfoForCatchup, 0, len(peers))
	for _, p := range peers {
		protoPeers = append(protoPeers, &p2p_api.PeerInfoForCatchup{
			Id:                      p.ID.String(),
			Height:                  p.Height,
			BlockHash:               p.BlockHash,
			DataHubUrl:              p.DataHubURL,
			IsHealthy:               p.IsHealthy,
			CatchupReputationScore:  p.CatchupReputationScore,
			CatchupAttempts:         p.CatchupAttempts,
			CatchupSuccesses:        p.CatchupSuccesses,
			CatchupFailures:         p.CatchupFailures,
		})
	}

	return &p2p_api.GetPeersForCatchupResponse{Peers: protoPeers}, nil
}
