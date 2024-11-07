package redis2

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	return s.client.Del(ctx, hash.String()).Err()
}
