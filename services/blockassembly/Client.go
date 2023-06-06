package blockassembly

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Store struct {
	db blockassembly_api.BlockAssemblyAPIClient
}

func NewClient() *Store {
	ctx := context.Background()

	blockAssemblyGrpcAddress, ok := gocore.Config().Get("blockassembly_grpcAddress")
	if !ok {
		panic("no blockassembly_grpcAddress setting found")
	}
	baConn, err := utils.GetGRPCClient(ctx, blockAssemblyGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	return &Store{
		db: blockassembly_api.NewBlockAssemblyAPIClient(baConn),
	}
}

func (s Store) Store(ctx context.Context, txid *chainhash.Hash, fees uint64, utxoHashes []*chainhash.Hash) (bool, error) {
	req := &blockassembly_api.AddTxRequest{
		Txid: txid[:],
		Fees: fees,
	}

	for _, hash := range utxoHashes {
		req.UtxoHashes = append(req.UtxoHashes, hash.CloneBytes())
	}

	if _, err := s.db.AddTx(ctx, req); err != nil {
		return false, err
	}

	return true, nil
}
