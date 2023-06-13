package blockvalidation

import (
	"context"

	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Store struct {
	db blockvalidation_api.BlockValidationAPIClient
}

func NewClient() *Store {
	ctx := context.Background()

	blockAssemblyGrpcAddress, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		panic("no blockvalidation_grpcAddress setting found")
	}
	baConn, err := utils.GetGRPCClient(ctx, blockAssemblyGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	return &Store{
		db: blockvalidation_api.NewBlockValidationAPIClient(baConn),
	}
}

func (s Store) BlockFound(ctx context.Context, hash *chainhash.Hash, utxoHashes []*chainhash.Hash) (bool, error) {
	req := &blockvalidation_api.BlockFoundRequest{
		BlockHeader:   []byte{},
		Coinbase:      []byte{},
		SubtreeHashes: [][]byte{},
	}

	if _, err := s.db.BlockFound(ctx, req); err != nil {
		return false, err
	}

	return true, nil
}
