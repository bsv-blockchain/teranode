package redis

import (
	"context"

	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) { // Remove?
	return s.Get(ctx, hash)
}
