package redis

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

// ProcessConflicting TODO
func (s *Store) ProcessConflicting(ctx context.Context, conflictingTxHashes []chainhash.Hash) (err error) {
	return nil
}
