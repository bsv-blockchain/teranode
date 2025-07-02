package sql

import (
	"context"

	"github.com/bitcoin-sv/teranode/stores/blob/file"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

func (s *SQL) ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error) {
	return nil, nil
}
