package p2p

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/p2p/p2p_api"
)

type ClientI interface {
	GetPeers(ctx context.Context) (*p2p_api.GetPeersResponse, error)
}
