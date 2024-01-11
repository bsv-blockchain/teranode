package sql

import (
	"context"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetSuitableBlock(ctx context.Context, hash *chainhash.Hash) (*model.SuitableBlock, error) {
	// TODO: implement
	return nil, nil
}
