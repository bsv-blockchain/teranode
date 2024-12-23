package sql

import (
	"context"

	"github.com/bitcoin-sv/teranode/stores/blob/file"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error) {
	return nil, nil
}
