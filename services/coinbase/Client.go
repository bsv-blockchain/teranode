package coinbase

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client  coinbase_api.CoinbaseAPIClient
	logger  utils.Logger
	running bool
	conn    *grpc.ClientConn
}

func NewClient(ctx context.Context) (*Client, error) {
	coinbaseGrpcAddress, ok := gocore.Config().Get("coinbase_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no coinbase_grpcAddress setting found")
	}
	baConn, err := util.GetGRPCClient(ctx, coinbaseGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client:  coinbase_api.NewCoinbaseAPIClient(baConn),
		logger:  gocore.Log("blkcC"),
		running: true,
		conn:    baConn,
	}, nil
}

func NewClientWithAddress(ctx context.Context, logger utils.Logger, address string) (ClientI, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: coinbase_api.NewCoinbaseAPIClient(baConn),
		logger: logger,
	}, nil
}

func (c Client) Health(ctx context.Context) (*coinbase_api.HealthResponse, error) {
	return c.client.Health(ctx, &emptypb.Empty{})
}

func (c Client) GetUtxo(ctx context.Context, address string) (*bt.UTXO, error) {
	utxo, err := c.client.GetUtxo(ctx, &coinbase_api.GetUtxoRequest{
		Address: address,
	})
	if err != nil {
		return nil, err
	}

	txHash, err := chainhash.NewHash(utxo.GetTxId())
	if err != nil {
		return nil, err
	}

	return &bt.UTXO{
		TxIDHash:       txHash,
		Vout:           utxo.Vout,
		LockingScript:  bscript.NewFromBytes(utxo.Script),
		Satoshis:       utxo.Satoshis,
		SequenceNumber: 0xffffffff,
	}, nil
}

func (c Client) MarkUtxoSpent(ctx context.Context, txId []byte, vout uint32, spentByTxId []byte) error {
	_, err := c.client.MarkUtxoSpent(ctx, &coinbase_api.MarkUtxoSpentRequest{
		TxId:        txId,
		Vout:        vout,
		SpentByTxId: spentByTxId,
	})

	return err
}
