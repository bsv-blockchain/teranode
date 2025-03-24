// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"

	"github.com/bitcoin-sv/teranode/services/p2p/p2p_api"
)

// ClientI defines the interface for P2P client operations.
type ClientI interface {
	// GetPeers retrieves a list of connected peers from the P2P network.
	// Returns a GetPeersResponse containing peer information or an error if the operation fails.
	GetPeers(ctx context.Context) (*p2p_api.GetPeersResponse, error)
	BanPeer(ctx context.Context, peer *p2p_api.BanPeerRequest) (*p2p_api.BanPeerResponse, error)
	UnbanPeer(ctx context.Context, peer *p2p_api.UnbanPeerRequest) (*p2p_api.UnbanPeerResponse, error)
	IsBanned(ctx context.Context, peer *p2p_api.IsBannedRequest) (*p2p_api.IsBannedResponse, error)
}
