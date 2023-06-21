package blockassembly

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (s Client) Store(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	req := &blockassembly_api.AddTxRequest{
		Txid: hash[:],
	}

	if _, err := s.db.AddTx(ctx, req); err != nil {
		return false, err
	}

	return true, nil
}

func (s Client) GetMiningCandidate(ctx context.Context) (*model.MiningCandidate, error) {
	req := &emptypb.Empty{}

	res, err := s.db.GetMiningCandidate(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s Client) SubmitMiningSolution(ctx context.Context, id []byte, coinbaseTx []byte, time, nonce, version uint32) error {
	_, err := s.db.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         id,
		Nonce:      nonce,
		CoinbaseTx: coinbaseTx,
		Time:       time,
		Version:    version,
	})
	if err != nil {
		return err
	}

	return nil
}
