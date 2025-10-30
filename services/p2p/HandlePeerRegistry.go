package p2p

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"google.golang.org/protobuf/types/known/emptypb"
)

// GetPeerRegistry returns comprehensive peer registry data with all metadata
func (s *Server) GetPeerRegistry(ctx context.Context, _ *emptypb.Empty) (*p2p_api.GetPeerRegistryResponse, error) {
	s.logger.Debugf("[GetPeerRegistry] called")

	if s.peerRegistry == nil {
		return &p2p_api.GetPeerRegistryResponse{
			Peers: []*p2p_api.PeerRegistryInfo{},
		}, nil
	}

	// Get all peers from the registry
	allPeers := s.peerRegistry.GetAllPeers()

	// Helper function to convert time to Unix timestamp, returning 0 for zero times
	timeToUnix := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}
		return t.Unix()
	}

	// Convert to protobuf format
	peers := make([]*p2p_api.PeerRegistryInfo, 0, len(allPeers))
	for _, peer := range allPeers {
		peers = append(peers, &p2p_api.PeerRegistryInfo{
			Id:               peer.ID.String(),
			Height:           peer.Height,
			BlockHash:        peer.BlockHash,
			DataHubUrl:       peer.DataHubURL,
			IsHealthy:        peer.IsHealthy,
			HealthDurationMs: peer.HealthDuration.Milliseconds(),
			LastHealthCheck:  timeToUnix(peer.LastHealthCheck),
			BanScore:         int32(peer.BanScore),
			IsBanned:         peer.IsBanned,
			IsConnected:      peer.IsConnected,
			ConnectedAt:      timeToUnix(peer.ConnectedAt),
			BytesReceived:    peer.BytesReceived,
			LastBlockTime:    timeToUnix(peer.LastBlockTime),
			LastMessageTime:  timeToUnix(peer.LastMessageTime),
			UrlResponsive:    peer.URLResponsive,
			LastUrlCheck:     timeToUnix(peer.LastURLCheck),

			// Interaction/catchup metrics
			InteractionAttempts:    peer.InteractionAttempts,
			InteractionSuccesses:   peer.InteractionSuccesses,
			InteractionFailures:    peer.InteractionFailures,
			LastInteractionAttempt: timeToUnix(peer.LastInteractionAttempt),
			LastInteractionSuccess: timeToUnix(peer.LastInteractionSuccess),
			LastInteractionFailure: timeToUnix(peer.LastInteractionFailure),
			ReputationScore:        peer.ReputationScore,
			MaliciousCount:         peer.MaliciousCount,
			AvgResponseTimeMs:      peer.AvgResponseTime.Milliseconds(),
			Storage:                peer.Storage,
		})
	}

	return &p2p_api.GetPeerRegistryResponse{
		Peers: peers,
	}, nil
}
