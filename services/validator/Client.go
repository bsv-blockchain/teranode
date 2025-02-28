/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides comprehensive transaction validation for Bitcoin SV nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters (GoBT, GoSDK, GoBDK) and implements the full Bitcoin
transaction validation ruleset.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - Script verification using multiple interpreters
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight)
*/
package validator

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/validator/validator_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	batcher "github.com/bitcoin-sv/teranode/util/batcher_temp"
	"github.com/libsv/go-bt/v2"
	"google.golang.org/grpc"
)

// batchItem represents a single item in a validation batch request
type batchItem struct {
	// req contains the validation request for a single transaction
	req *validator_api.ValidateTransactionRequest

	// done is a channel that receives the validation result
	done chan validateBatchResponse
}

// Client implements a gRPC client for the validator service, providing transaction
// validation capabilities through remote procedure calls
type Client struct {
	// client is the gRPC client implementation for validator API calls
	client validator_api.ValidatorAPIClient

	// running indicates whether the client is currently operational
	running *atomic.Bool

	// conn holds the gRPC connection to the validator service
	conn *grpc.ClientConn

	// logger provides logging functionality for the client
	logger ulogger.Logger

	// batchSize defines the maximum number of transactions to batch together
	// for validation requests
	batchSize int

	// batchTimeout defines the maximum time to wait for batching transactions
	// before sending the batch, in milliseconds
	batchTimeout int

	// batcher handles the batching of transaction validation requests
	batcher batcher.Batcher2[batchItem]
}

// NewClient creates and initializes a new validator client
//
// Parameters:
//   - ctx: Context for client initialization
//   - logger: Logger instance for client operations
//
// Returns:
//   - *Client: Initialized client instance
//   - error: Any error encountered during initialization
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*Client, error) {
	validatorGrpcAddress := tSettings.Validator.GRPCAddress
	if validatorGrpcAddress == "" {
		return nil, errors.NewConfigurationError("missing validator_grpcAddress")
	}

	conn, err := util.GetGRPCClient(ctx, validatorGrpcAddress, &util.ConnectionOptions{
		MaxRetries: 3,
	}, tSettings)

	if err != nil {
		return nil, err
	}

	grpcClient := validator_api.NewValidatorAPIClient(conn)

	sendBatchSize := tSettings.Validator.SendBatchSize
	sendBatchTimeout := tSettings.Validator.SendBatchTimeout

	running := atomic.Bool{}
	running.Store(true)

	client := &Client{
		client:       grpcClient,
		logger:       logger,
		running:      &running,
		conn:         conn,
		batchSize:    sendBatchSize,
		batchTimeout: sendBatchTimeout,
	}

	if sendBatchSize > 0 {
		sendBatch := func(batch []*batchItem) {
			client.sendBatchToValidator(ctx, batch)
		}
		duration := time.Duration(sendBatchTimeout) * time.Millisecond
		client.batcher = *batcher.New[batchItem](sendBatchSize, duration, sendBatch, true)
	}

	return client, nil
}

func (c *Client) Stop() {
	// TODO
}

func (c *Client) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	res, err := c.client.HealthGRPC(ctx, &validator_api.EmptyMessage{})
	if !res.GetOk() || err != nil {
		return http.StatusFailedDependency, res.GetDetails(), errors.UnwrapGRPC(err)
	}

	return http.StatusOK, res.GetDetails(), nil
}

func (c *Client) GetBlockHeight() uint32 {
	resp, err := c.client.GetBlockHeight(context.Background(), &validator_api.EmptyMessage{})
	if err != nil {
		return 0
	}

	return resp.Height
}

func (c *Client) GetMedianBlockTime() uint32 {
	resp, err := c.client.GetMedianBlockTime(context.Background(), &validator_api.EmptyMessage{})
	if err != nil {
		return 0
	}

	return resp.MedianTime
}

func (c *Client) TriggerBatcher() {
	if c.batchSize > 0 {
		c.batcher.Trigger()
	}
}

func (c *Client) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error) {
	validationOptions := NewDefaultOptions()
	for _, opt := range opts {
		opt(validationOptions)
	}

	return c.ValidateWithOptions(ctx, tx, blockHeight, validationOptions)
}

type validateBatchResponse struct {
	metaData []byte
	err      error
}

func (c *Client) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error) {
	if c.batchSize == 0 {
		response, err := c.client.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
			TransactionData:      tx.ExtendedBytes(),
			BlockHeight:          blockHeight,
			SkipUtxoCreation:     &validationOptions.SkipUtxoCreation,
			AddTxToBlockAssembly: &validationOptions.AddTXToBlockAssembly,
			SkipPolicyChecks:     &validationOptions.SkipPolicyChecks,
			CreateConflicting:    &validationOptions.CreateConflicting,
		})
		if err != nil {
			return nil, errors.UnwrapGRPC(err)
		}

		if response.Metadata != nil {
			txMetaData = &meta.Data{}
			meta.NewMetaDataFromBytes(&response.Metadata, txMetaData)
		}
	} else {
		doneCh := make(chan validateBatchResponse)
		/* batch mode */
		c.batcher.Put(&batchItem{
			req: &validator_api.ValidateTransactionRequest{
				TransactionData:      tx.ExtendedBytes(),
				BlockHeight:          blockHeight,
				SkipUtxoCreation:     &validationOptions.SkipUtxoCreation,
				AddTxToBlockAssembly: &validationOptions.AddTXToBlockAssembly,
				SkipPolicyChecks:     &validationOptions.SkipPolicyChecks,
				CreateConflicting:    &validationOptions.CreateConflicting,
			},
			done: doneCh,
		})

		r := <-doneCh

		if r.metaData != nil {
			txMetaData = &meta.Data{}
			meta.NewMetaDataFromBytes(&r.metaData, txMetaData)
		}

		if r.err != nil {
			return nil, errors.UnwrapGRPC(r.err)
		}
	}

	return txMetaData, nil
}

func (c *Client) sendBatchToValidator(ctx context.Context, batch []*batchItem) {
	requests := make([]*validator_api.ValidateTransactionRequest, 0, len(batch))
	for _, item := range batch {
		requests = append(requests, item.req)
	}

	txBatch := &validator_api.ValidateTransactionBatchRequest{
		Transactions: requests,
	}

	resp, err := c.client.ValidateTransactionBatch(ctx, txBatch)
	if err != nil {
		c.logger.Errorf("%v", err)

		for _, item := range batch {
			item.done <- validateBatchResponse{
				metaData: nil,
				err:      err,
			}
		}

		return
	}

	for i, item := range batch {
		if !resp.Errors[i].IsNil() {
			item.done <- validateBatchResponse{
				metaData: nil,
				err:      resp.Errors[i],
			}
		} else {
			item.done <- validateBatchResponse{
				metaData: resp.Metadata[i],
				err:      nil,
			}
		}
	}
}
