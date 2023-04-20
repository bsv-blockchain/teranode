package validator

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/services/validator/validator_api"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Client struct {
	client validator_api.ValidatorAPIClient
}

func NewClient() (*Client, error) {
	// opts := []grpc.DialOption{
	// 	grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100 * 1024 * 1024)), // 100MB, TODO make configurable
	// 	grpc.WithTransportCredentials(insecure.NewCredentials()),
	// 	grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
	// }

	validator_grpcAddress, _ := gocore.Config().Get("validator_grpcAddress")
	// conn, err := grpc.Dial(validator_grpcAddress, opts...)
	// if err != nil {
	// 	return nil, err
	// }
	conn, err := utils.GetGRPCClient(context.Background(), validator_grpcAddress, &utils.ConnectionOptions{
		Tracer: gocore.Config().GetBool("tracing_enabled", true),
	})
	if err != nil {
		return nil, err
	}

	client := validator_api.NewValidatorAPIClient(conn)

	return &Client{
		client: client,
	}, nil
}

func (c *Client) Stop() {
	// TODO
}

func (c *Client) Validate(tx *bt.Tx) error {
	resp, err := c.client.ValidateTransaction(context.Background(), &validator_api.ValidateTransactionRequest{
		TransactionData: tx.ExtendedBytes(),
	})
	if err != nil {
		return err
	}

	if !resp.Valid {
		return fmt.Errorf("invalid transaction: %s", resp.Reason)
	}

	return nil
}
