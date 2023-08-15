package coinbasetracker

import (
	"context"
	"fmt"

	coinbasetracker_api "github.com/TAAL-GmbH/ubsv/services/coinbasetracker/coinbasetracker_api"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client coinbasetracker_api.CoinbasetrackerAPIClient
}

func NewClient(ctx context.Context) (*Client, error) {
	coinbaseTrackerGrpcAddress, ok := gocore.Config().Get("coinbasetracker_grpcAddress")
	if !ok {
		panic("no blockvalidation_grpcAddress setting found")
	}
	baConn, err := utils.GetGRPCClient(ctx, coinbaseTrackerGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coinbasetracker: %w", err)
	}

	return &Client{
		client: coinbasetracker_api.NewCoinbasetrackerAPIClient(baConn),
	}, nil
}

func (c Client) Health(ctx context.Context) (*coinbasetracker_api.HealthResponse, error) {
	return c.client.Health(ctx, &emptypb.Empty{})
}

func (c Client) GetUtxo(ctx context.Context, address string) (*bt.UTXO, error) {
	utxo, err := c.client.GetUtxo(ctx, &coinbasetracker_api.GetUtxoRequest{
		Address: address,
	})
	if err != nil {
		return nil, err
	}

	return &bt.UTXO{
		TxID:           utxo.GetTxId(),
		Vout:           utxo.Vout,
		LockingScript:  bscript.NewFromBytes(utxo.Script),
		Satoshis:       utxo.Satoshis,
		SequenceNumber: 0xffffffff,
	}, nil
}

func (c Client) MarkUtxoSpent(ctx context.Context, txId []byte, vout uint32, spentByTxId []byte) error {
	_, err := c.client.MarkUtxoSpent(ctx, &coinbasetracker_api.MarkUtxoSpentRequest{
		TxId:        txId,
		Vout:        vout,
		SpentByTxId: spentByTxId,
	})

	return err
}
