package blockvalidation

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/model"
	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Store struct {
	client blockvalidation_api.BlockValidationAPIClient
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
		client: blockvalidation_api.NewBlockValidationAPIClient(baConn),
	}
}

func (s Store) BlockFound(ctx context.Context, blockHeader *model.BlockHeader, coinbase *bt.Tx, subtreeHashes []*chainhash.Hash) (bool, error) {
	subtreeBytes := make([][]byte, len(subtreeHashes))
	for i, subtreeHash := range subtreeHashes {
		subtreeBytes[i] = subtreeHash[:]
	}

	req := &blockvalidation_api.BlockFoundRequest{
		BlockHeader:   blockHeader.Bytes(),
		Coinbase:      coinbase.Bytes(),
		SubtreeHashes: subtreeBytes,
	}

	if _, err := s.client.BlockFound(ctx, req); err != nil {
		return false, err
	}

	return true, nil
}
