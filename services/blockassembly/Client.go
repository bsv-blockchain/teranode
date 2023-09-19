package blockassembly

import (
	"context"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	db blockassembly_api.BlockAssemblyAPIClient
}

func NewClient(ctx context.Context) *Client {
	blockAssemblyGrpcAddress, ok := gocore.Config().Get("blockassembly_grpcAddress")
	if !ok {
		panic("no blockassembly_grpcAddress setting found")
	}
	baConn, err := util.GetGRPCClient(ctx, blockAssemblyGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
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

func (s Client) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	_, err := s.db.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         solution.Id,
		Nonce:      solution.Nonce,
		CoinbaseTx: solution.Coinbase,
		Time:       solution.Time,
		Version:    solution.Version,
	})
	if err != nil {
		return err
	}

	return nil
}
