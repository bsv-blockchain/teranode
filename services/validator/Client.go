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

func NewClient(ctx context.Context) (*Client, error) {
	validator_grpcAddress, _ := gocore.Config().Get("validator_grpcAddress")
	conn, err := utils.GetGRPCClient(ctx, validator_grpcAddress, &utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
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

func (c *Client) Validate(ctx context.Context, tx *bt.Tx) error {
	resp, err := c.client.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
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
