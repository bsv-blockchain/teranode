package p2p

import (
	"context"

	"github.com/bitcoin-sv/teranode/services/p2p/p2p_api"
)

type ClientI interface {
	GetPeers(ctx context.Context) (*p2p_api.GetPeersResponse, error)
}
