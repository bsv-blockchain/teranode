package blockassembly

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Store struct {
	db blockassembly_api.BlockAssemblyAPIClient
}

func NewClient(db blockassembly_api.BlockAssemblyAPIClient) (*Store, error) {
	return &Store{
		db: db,
	}, nil
}

func (s Store) Store(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	_, err := s.db.AddTxID(ctx, &blockassembly_api.AddTxIDRequest{
		Txid: hash[:],
	})
	if err != nil {
		return false, err
	}

	return true, nil
}
