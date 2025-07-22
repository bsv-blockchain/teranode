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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/validator/validator_api"
	"github.com/bitcoin-sv/teranode/settings"
	utxometa "github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bsv-blockchain/go-batcher"
	"github.com/bsv-blockchain/go-bt/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	batcher batcher.Batcher[batchItem]

	// validatorHTTPAddr holds the HTTP endpoint address for validator fallback
	validatorHTTPAddr *url.URL
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
		client:            grpcClient,
		logger:            logger,
		running:           &running,
		conn:              conn,
		batchSize:         sendBatchSize,
		batchTimeout:      sendBatchTimeout,
		validatorHTTPAddr: tSettings.Validator.HTTPAddress,
	}

	if sendBatchSize > 0 {
		sendBatch := func(batch []*batchItem) {
			client.sendBatchToValidator(ctx, batch)
		}
		duration := time.Duration(sendBatchTimeout) * time.Millisecond
		client.batcher = *batcher.New(sendBatchSize, duration, sendBatch, true)
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

func (c *Client) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*utxometa.Data, error) {
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

func (c *Client) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *utxometa.Data, err error) {
	if c.batchSize == 0 {
		// Non-batch mode: direct validation
		response, err := c.client.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
			TransactionData:      tx.SerializeBytes(),
			BlockHeight:          blockHeight,
			SkipUtxoCreation:     &validationOptions.SkipUtxoCreation,
			AddTxToBlockAssembly: &validationOptions.AddTXToBlockAssembly,
			SkipPolicyChecks:     &validationOptions.SkipPolicyChecks,
			CreateConflicting:    &validationOptions.CreateConflicting,
		})
		if err != nil {
			c.logger.Errorf("[ValidateWithOptions] failed to validate non-batched transaction: %v", err)
			return nil, c.handleValidationError(ctx, tx, blockHeight, validationOptions, err)
		}

		result := &utxometa.Data{}

		if err = utxometa.NewMetaDataFromBytes(response.Metadata, result); err != nil {
			c.logger.Errorf("[ValidateWithOptions] failed to parse metadata: %v", err)
			return nil, err
		}

		return result, nil
	}

	// Batch mode
	doneCh := make(chan validateBatchResponse)
	c.batcher.Put(&batchItem{
		req: &validator_api.ValidateTransactionRequest{
			TransactionData:      tx.SerializeBytes(),
			BlockHeight:          blockHeight,
			SkipUtxoCreation:     &validationOptions.SkipUtxoCreation,
			AddTxToBlockAssembly: &validationOptions.AddTXToBlockAssembly,
			SkipPolicyChecks:     &validationOptions.SkipPolicyChecks,
			CreateConflicting:    &validationOptions.CreateConflicting,
		},
		done: doneCh,
	})

	r := <-doneCh

	if r.err != nil {
		c.logger.Errorf("[ValidateWithOptions] failed to validate batched transaction: %v", r.err)
		return nil, r.err
	}

	result := &utxometa.Data{}

	if err = utxometa.NewMetaDataFromBytes(r.metaData, result); err != nil {
		c.logger.Errorf("[ValidateWithOptions] failed to parse metadata: %v", err)
		return nil, err
	}

	return result, nil
}

// handleValidationError processes validation errors and attempts HTTP fallback if appropriate
func (c *Client) handleValidationError(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options, err error) error {
	// Check if the error is related to message size (ResourceExhausted)
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted || c.validatorHTTPAddr == nil {
		return errors.UnwrapGRPC(err)
	}

	// Try HTTP fallback
	c.logger.Warnf("[ValidateWithOptions][%s] Transaction exceeds gRPC message limit, falling back to validator /tx endpoint: %s",
		tx.TxID(), st.Message())

	httpErr := c.validateTransactionViaHTTP(ctx, tx, blockHeight, validationOptions)
	if httpErr == nil {
		// HTTP validation succeeded, but we don't have metadata
		c.logger.Debugf("[ValidateWithOptions][%s] Successfully validated via HTTP fallback", tx.TxID())
		return nil
	}

	c.logger.Errorf("[ValidateWithOptions][%s] HTTP fallback also failed: %v", tx.TxID(), httpErr)

	return errors.UnwrapGRPC(err)
}

func (c *Client) sendBatchToValidator(ctx context.Context, batch []*batchItem) {
	// Prepare batch request
	requests := make([]*validator_api.ValidateTransactionRequest, 0, len(batch))
	for _, item := range batch {
		requests = append(requests, item.req)
	}

	txBatch := &validator_api.ValidateTransactionBatchRequest{
		Transactions: requests,
	}

	// Try gRPC validation first
	resp, err := c.client.ValidateTransactionBatch(ctx, txBatch)
	if err != nil {
		c.logger.Errorf("Failed to validate transaction batch: %v", err)

		// Check if the error is related to message size (ResourceExhausted)
		if c.shouldAttemptHTTPFallback(err) {
			c.handleBatchHTTPFallback(ctx, batch)
			return
		}

		// For any other error, notify all transactions with the unwrapped error
		c.notifyAllBatchItems(batch, nil, errors.UnwrapGRPC(err))

		return
	}

	// Process successful responses
	c.processBatchResponse(batch, resp)
}

// shouldAttemptHTTPFallback determines if HTTP fallback should be attempted based on the error
func (c *Client) shouldAttemptHTTPFallback(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.ResourceExhausted && c.validatorHTTPAddr != nil
}

// handleBatchHTTPFallback attempts to validate each transaction individually via HTTP
func (c *Client) handleBatchHTTPFallback(ctx context.Context, batch []*batchItem) {
	c.logger.Warnf("Batch transaction exceeds gRPC message limit, trying HTTP fallback for individual transactions")

	for _, item := range batch {
		// Extract transaction data and options from the request
		txReq := item.req

		tx, err := bt.NewTxFromBytes(txReq.TransactionData)
		if err != nil {
			item.done <- validateBatchResponse{
				metaData: nil,
				err:      errors.NewServiceError("Failed to parse transaction for HTTP fallback: %v", err),
			}

			continue
		}

		// Create options from the request
		options := &Options{
			SkipUtxoCreation:     *txReq.SkipUtxoCreation,
			AddTXToBlockAssembly: *txReq.AddTxToBlockAssembly,
			SkipPolicyChecks:     *txReq.SkipPolicyChecks,
			CreateConflicting:    *txReq.CreateConflicting,
		}

		// Try HTTP fallback for this individual transaction
		httpErr := c.validateTransactionViaHTTP(ctx, tx, txReq.BlockHeight, options)

		if httpErr == nil {
			c.logger.Debugf("[%s] Successfully validated via HTTP fallback", tx.TxID())
			item.done <- validateBatchResponse{metaData: nil, err: nil}
		} else {
			c.logger.Errorf("[%s] HTTP fallback failed: %v", tx.TxID(), httpErr)
			item.done <- validateBatchResponse{metaData: nil, err: httpErr}
		}
	}
}

// processBatchResponse handles successful batch responses
func (c *Client) processBatchResponse(batch []*batchItem, resp *validator_api.ValidateTransactionBatchResponse) {
	for i, item := range batch {
		if !resp.Errors[i].IsNil() {
			item.done <- validateBatchResponse{metaData: nil, err: resp.Errors[i]}
		} else {
			item.done <- validateBatchResponse{metaData: resp.Metadata[i], err: nil}
		}
	}
}

// notifyAllBatchItems notifies all items in a batch with the same response
func (c *Client) notifyAllBatchItems(batch []*batchItem, metadata []byte, err error) {
	for _, item := range batch {
		item.done <- validateBatchResponse{metaData: metadata, err: err}
	}
}

// validateTransactionViaHTTP sends a transaction to the validator's HTTP endpoint
// This is used as a fallback when gRPC message size limits are exceeded
func (c *Client) validateTransactionViaHTTP(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) error {
	if c.validatorHTTPAddr == nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] Transaction exceeds gRPC message limit, but no HTTP endpoint configured for validator", tx.TxID())
	}

	// Create an HTTP client with a timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Prepare request to validator /tx endpoint
	endpoint, err := url.Parse("/tx")
	if err != nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] error parsing endpoint /tx: %v", tx.TxID(), err)
	}

	// Add validation options as query parameters
	queryParams := url.Values{}
	if validationOptions.SkipUtxoCreation {
		queryParams.Add("skipUtxoCreation", "true")
	}

	if validationOptions.AddTXToBlockAssembly {
		queryParams.Add("addTxToBlockAssembly", "true")
	}

	if validationOptions.SkipPolicyChecks {
		queryParams.Add("skipPolicyChecks", "true")
	}

	if validationOptions.CreateConflicting {
		queryParams.Add("createConflicting", "true")
	}

	if blockHeight > 0 {
		queryParams.Add("blockHeight", fmt.Sprintf("%d", blockHeight))
	}

	endpoint.RawQuery = queryParams.Encode()

	fullURL := c.validatorHTTPAddr.ResolveReference(endpoint)

	// Create the HTTP request with the transaction data
	req, err := http.NewRequestWithContext(ctx, "POST", fullURL.String(), bytes.NewReader(tx.SerializeBytes()))
	if err != nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] error creating request to validator /tx endpoint: %v", tx.TxID(), err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return errors.NewServiceError("[ValidateWithOptions][%s] error sending transaction to validator /tx endpoint: %v", tx.TxID(), err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.NewServiceError("[ValidateWithOptions][%s] validator /tx endpoint returned non-OK status: %d, body: %s",
			tx.TxID(), resp.StatusCode, string(body))
	}

	c.logger.Debugf("[ValidateWithOptions][%s] successfully validated using validator /tx endpoint", tx.TxID())

	return nil
}
