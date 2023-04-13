package validator

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/services/validator/validator_api"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	client validator_api.ValidatorAPIClient
}

func NewClient() (*Client, error) {
    grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"pick_first":{}},{"random":{}}]}`)
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100 * 1024 * 1024)), // 100MB, TODO make configurable
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}},{"random":{}}]}`)
	}

    validator_grpcAddress, _ := gocore.Config().Get("validator_grpcAddress")
    address := fmt.Sprintf("%s:///%s", "dns", validator_grpcAddress)
	conn, err := grpc.Dial(address, opts...)
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
