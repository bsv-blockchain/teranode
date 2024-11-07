package redis2

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return s.Get(ctx, hash)
}
