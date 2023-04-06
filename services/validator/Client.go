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
	client         validator_api.ValidatorAPIClient
	validateStream validator_api.ValidatorAPI_ValidateTransactionClient
}

func NewClient() (*Client, error) {
	validator_grpcAddress, _ := gocore.Config().Get("validator_grpcAddress")
	conn, err := grpc.Dial(validator_grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := validator_api.NewValidatorAPIClient(conn)

	validateStream, err := client.ValidateTransaction(context.Background())

	return &Client{
		client:         client,
		validateStream: validateStream,
	}, nil
}

func (c *Client) Stop() {
	_ = c.validateStream.CloseSend()
}

func (c *Client) Validate(tx *bt.Tx) error {
	// TODO is this how this streaming works?
	err := c.validateStream.Send(&validator_api.ValidateTransactionRequest{
		TransactionData: tx.Bytes(),
	})
	if err != nil {
		return err
	}

	resp, err := c.validateStream.CloseAndRecv()
	if err != nil {
		return err
	}
	if !resp.Valid {
		return fmt.Errorf("invalid transaction: %s", resp.Reason)
	}

	return nil
}
