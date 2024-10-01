package peer

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/legacy/peer_api"
)

type ClientI interface {
	GetPeers(ctx context.Context) (*peer_api.GetPeersResponse, error)
}
