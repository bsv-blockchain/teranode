package peer

import (
	"context"

	"github.com/bitcoin-sv/teranode/services/legacy/peer_api"
)

type ClientI interface {
	GetPeers(ctx context.Context) (*peer_api.GetPeersResponse, error)
	BanPeer(ctx context.Context, peer *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error)
	UnbanPeer(ctx context.Context, peer *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error)
	IsBanned(ctx context.Context, peer *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error)
}
