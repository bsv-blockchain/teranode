package txstatus

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/txstatus/txstatus_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client txstatus_api.TxStatusAPIClient
	logger utils.Logger
}

func NewClient(ctx context.Context, logger utils.Logger) (*Client, error) {
	txstatus_grpcAddress, _ := gocore.Config().Get("txstatus_grpcAddress")
	conn, err := utils.GetGRPCClient(ctx, txstatus_grpcAddress, &utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	client := txstatus_api.NewTxStatusAPIClient(conn)

	return &Client{
		client: client,
		logger: logger,
	}, nil
}

func (c *Client) Health(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*txstatus_api.HealthResponse, error) {
	resp, err := c.client.Health(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) Set(ctx context.Context, in *txstatus_api.SetRequest, opts ...grpc.CallOption) (*txstatus_api.SetResponse, error) {
	resp, err := c.client.Set(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) SetMined(ctx context.Context, in *txstatus_api.SetMinedRequest, opts ...grpc.CallOption) (*txstatus_api.SetMinedResponse, error) {
	resp, err := c.client.SetMined(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) Get(ctx context.Context, in *txstatus_api.GetRequest, opts ...grpc.CallOption) (*txstatus_api.GetResponse, error) {
	resp, err := c.client.Get(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
