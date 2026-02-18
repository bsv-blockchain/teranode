package peer

import (
	"context"

	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ClientI defines the interface for legacy peer management operations.
// It provides methods for querying connected peers and managing the ban list
// through gRPC calls to the legacy service.
type ClientI interface {
	// GetPeers retrieves information about all currently connected legacy peers.
	GetPeers(ctx context.Context) (*peer_api.GetPeersResponse, error)

	// BanPeer adds a peer to the ban list, preventing future connections.
	BanPeer(ctx context.Context, peer *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error)

	// UnbanPeer removes a peer from the ban list, allowing it to reconnect.
	UnbanPeer(ctx context.Context, peer *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error)

	// IsBanned checks whether a specific peer is currently banned.
	IsBanned(ctx context.Context, peer *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error)

	// ListBanned returns all currently banned peers.
	ListBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ListBannedResponse, error)

	// ClearBanned removes all entries from the ban list.
	ClearBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ClearBannedResponse, error)
}
