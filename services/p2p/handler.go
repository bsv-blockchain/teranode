package p2p

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ReportValidSubtreeHandler is a gRPC handler for reporting valid subtree reception
func (s *Server) ReportValidSubtreeHandler(_ context.Context, req *p2p_api.ReportValidSubtreeRequest) (*p2p_api.ReportValidSubtreeResponse, error) {
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
	s.logger.Debugf("[ReportValidSubtreeHandler] Recorded successful subtree %s from peer %s", req.SubtreeHash, req.PeerId)

	return &p2p_api.ReportValidSubtreeResponse{
		Success: true,
		Message: "subtree validation recorded",
	}, nil
}

// ReportInvalidSubtreeHandler is a gRPC handler for reporting invalid subtree reception
func (s *Server) ReportInvalidSubtreeHandler(ctx context.Context, req *p2p_api.ReportInvalidSubtreeRequest) (*p2p_api.ReportValidSubtreeResponse, error) {
	err := s.reportInvalidSubtree(ctx, req.SubtreeHash, req.PeerId, req.Reason)
	if err != nil {
		return &p2p_api.ReportValidSubtreeResponse{
			Success: false,
			Message: fmt.Sprintf("failed to report invalid subtree: %v", err),
		}, errors.WrapGRPC(err)
	}

	return &p2p_api.ReportValidSubtreeResponse{
		Success: true,
		Message: "invalid subtree reported",
	}, nil
}
