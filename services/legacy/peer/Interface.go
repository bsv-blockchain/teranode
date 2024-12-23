package peer

import (
	"context"

	"github.com/bitcoin-sv/teranode/services/legacy/peer_api"
)

type ClientI interface {
	GetPeers(ctx context.Context) (*peer_api.GetPeersResponse, error)
}
