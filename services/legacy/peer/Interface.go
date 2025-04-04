package peer

import (
	"context"

	"github.com/bitcoin-sv/teranode/services/legacy/peer_api"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClientI interface {
	GetPeers(ctx context.Context) (*peer_api.GetPeersResponse, error)
	BanPeer(ctx context.Context, peer *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error)
	UnbanPeer(ctx context.Context, peer *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error)
	IsBanned(ctx context.Context, peer *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error)
	ListBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ListBannedResponse, error)
	ClearBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ClearBannedResponse, error)
}
