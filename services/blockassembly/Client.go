package blockassembly

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Client struct {
	db blockassembly_api.BlockAssemblyAPIClient
}

func NewClient() *Client {
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

	return &Client{
		db: blockassembly_api.NewBlockAssemblyAPIClient(baConn),
	}
}

func (s Client) Store(ctx context.Context, txid *chainhash.Hash) (bool, error) {
	req := &blockassembly_api.AddTxRequest{
		Txid: txid[:],
	}

	if _, err := s.db.AddTx(ctx, req); err != nil {
		return false, err
	}

	return true, nil
}
