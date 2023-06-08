package sql

import (
	"context"

	"github.com/libsv/go-bc"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

func (s *SQL) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*bc.BlockHeader, error) {
	//TODO implement me
	panic("implement me")
}
