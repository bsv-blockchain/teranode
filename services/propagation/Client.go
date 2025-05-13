// Package propagation provides Bitcoin SV transaction propagation functionality.
// This file implements a batching client for efficient transaction processing.
package propagation

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/propagation/propagation_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	batcher "github.com/bitcoin-sv/teranode/util/batcher"
	"github.com/libsv/go-bt/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// batchItem represents a single transaction in a batch along with its completion channel.
type batchItem struct {
	tx   *bt.Tx     // Bitcoin transaction to process
	done chan error // Channel to signal completion and return any error
}

// Client implements a batching transaction client for the BSV propagation service.
// It supports both individual transaction processing and efficient batch processing
// with configurable batch sizes and timeouts.
type Client struct {
	client              propagation_api.PropagationAPIClient
	conn                *grpc.ClientConn
	batchSize           int
	batchCh             chan []*batchItem
	batcher             batcher.Batcher2[batchItem]
	logger              ulogger.Logger
	settings            *settings.Settings
	propagationHTTPAddr *url.URL
}

// NewClient creates a new propagation client with optional connection configuration.
// It initializes the client with batch processing capabilities based on configuration:
//   - propagation_sendBatchSize: Maximum transactions per batch (default: 100)
//   - propagation_sendBatchTimeout: Batch timeout in milliseconds (default: 5)
//
// Parameters:
//   - ctx: Context for client operations
//   - logger: Logger instance for client operations
//   - tSettings: Settings instance for client operations
//   - conn: Optional pre-configured gRPC connection
//
// Returns:
//   - *Client: New client instance
//   - error: Error if client creation fails
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, conn ...*grpc.ClientConn) (*Client, error) {
	var useConn *grpc.ClientConn
	if len(conn) > 0 {
		useConn = conn[0]
	} else {
		localConn, err := getClientConn(ctx, tSettings.Propagation.GRPCAddresses, tSettings)
		if err != nil {
			return nil, err
		}

		useConn = localConn
	}

	client := propagation_api.NewPropagationAPIClient(useConn)

	batchSize := tSettings.Propagation.SendBatchSize
	sendBatchTimeout := tSettings.Propagation.SendBatchTimeout

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %d", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	// Determine validator HTTP address from settings
	httpAddresses := tSettings.Propagation.HTTPAddresses
	if len(httpAddresses) == 0 {
		return nil, errors.NewServiceError("no propagation HTTP address configured")
	}

	propagationHTTPAddr, err := url.Parse(httpAddresses[0])
	if err != nil {
		return nil, errors.NewServiceError("invalid propagation HTTP address configured")
	}

	logger.Infof("Using propagation HTTP address: %s", propagationHTTPAddr)

	c := &Client{
		client:              client,
		conn:                useConn,
		batchSize:           batchSize,
		batchCh:             make(chan []*batchItem),
		logger:              logger,
		settings:            tSettings,
		propagationHTTPAddr: propagationHTTPAddr,
	}

	sendBatch := func(batch []*batchItem) {
		if err := c.ProcessTransactionBatch(ctx, batch); err != nil {
			logger.Errorf("Error sending batch: %s", err)
		}
	}
	c.batcher = *batcher.New(batchSize, duration, sendBatch, true)

	return c, nil
}

// Stop gracefully shuts down the client and closes the gRPC connection.
// This should be called when the client is no longer needed to clean up resources.

func (c *Client) Stop() {
	if c.client != nil && c.conn != nil {
		_ = c.conn.Close()
	}
}

// ProcessTransaction submits a single BSV transaction for processing.
// If batching is enabled (batchSize > 0), the transaction is added to the current
// batch. Otherwise, it is processed immediately.
//
// Parameters:
//   - ctx: Context for transaction processing
//   - tx: Bitcoin transaction to process
//
// Returns:
//   - error: Error if transaction processing fails
func (c *Client) ProcessTransaction(ctx context.Context, tx *bt.Tx) error {
	if c.settings.Propagation.AlwaysUseHTTP {
		return c.sendTransactionViaHTTP(ctx, tx)
	}

	if c.batchSize > 0 {
		done := make(chan error)
		c.batcher.Put(&batchItem{tx: tx, done: done})
		err := <-done

		return err
	}

	// Try gRPC first
	_, err := c.client.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.ExtendedBytes(),
	})

	// If successful, return nil
	if err == nil {
		return nil
	}

	// Check if the error is related to message size (ResourceExhausted)
	st, ok := status.FromError(err)
	if ok && st.Code() == codes.ResourceExhausted {
		// Log the fallback
		c.logger.Warnf("[ProcessTransaction][%s] Transaction exceeds gRPC message limit, falling back to validator /tx endpoint: %s",
			tx.TxID(), st.Message())

		return c.sendTransactionViaHTTP(ctx, tx)
	}

	// For any other types of errors, return the unwrapped gRPC error
	return errors.UnwrapGRPC(err)
}

func (c *Client) sendTransactionViaHTTP(ctx context.Context, tx *bt.Tx) error {
	// Create an HTTP client with a timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Prepare request to validator /tx endpoint
	req, err := http.NewRequestWithContext(ctx, "POST", c.propagationHTTPAddr.String()+"/tx", bytes.NewBuffer(tx.ExtendedBytes()))
	if err != nil {
		return errors.NewServiceError("[ProcessTransaction][%s] error creating request to validator /tx endpoint", tx.TxID(), err)
	}

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return errors.NewServiceError("[ProcessTransaction][%s] error sending transaction to validator /tx endpoint", tx.TxID(), err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		httpErr := errors.NewServiceError("HTTP status %d: %s", resp.StatusCode, string(body))

		return errors.NewServiceError("[ProcessTransaction][%s] validator /tx endpoint returned non-OK status", tx.TxID(), httpErr)
	}

	c.logger.Debugf("[ProcessTransaction][%s] successfully validated using validator /tx endpoint", tx.TxID())

	return nil
}

// TriggerBatcher forces the current batch to be processed immediately,
// regardless of whether it has reached the configured batch size or timeout.
func (c *Client) TriggerBatcher() {
	c.batcher.Trigger()
}

// handleBatchError sends the given error to all transactions in the batch
func (c *Client) handleBatchError(batch []*batchItem, err error, format string, args ...interface{}) error {
	wrappedErr := errors.NewServiceError(format, append(args, err)...)
	c.logger.Errorf(wrappedErr.Error())

	for _, tx := range batch {
		tx.done <- wrappedErr
	}

	return wrappedErr
}

// handleBatchResponse processes the gRPC response for a batch and notifies each transaction
func (c *Client) handleBatchResponse(batch []*batchItem, response *propagation_api.ProcessTransactionBatchResponse) {
	for i, err := range response.Errors {
		if !err.IsNil() { // don't do err != nil, proto can't return nil TError
			batch[i].done <- err
		} else {
			batch[i].done <- nil
		}
	}
}

// processBatchViaHTTP sends transaction batch via HTTP fallback when gRPC fails
func (c *Client) processBatchViaHTTP(ctx context.Context, batch []*batchItem, txs [][]byte) error {
	// Create HTTP client for large batch
	client := &http.Client{
		Timeout: 60 * time.Second, // Longer timeout for batch
	}

	// Combine all transactions into a single payload for the /txs endpoint
	var combinedTxs bytes.Buffer
	for _, txBytes := range txs {
		combinedTxs.Write(txBytes)
	}

	// Send batch to validator /txs endpoint
	endpoint, err := url.Parse("/txs")
	if err != nil {
		return c.handleBatchError(batch, err, "[processBatchViaHTTP] Failed to parse endpoint /txs")
	}

	fullURL := c.propagationHTTPAddr.ResolveReference(endpoint)

	req, err := http.NewRequestWithContext(ctx, "POST", fullURL.String(), &combinedTxs)
	if err != nil {
		return c.handleBatchError(batch, err, "[processBatchViaHTTP] Failed to create HTTP request for batch")
	}

	resp, err := client.Do(req)
	if err != nil {
		return c.handleBatchError(batch, err, "[processBatchViaHTTP] Failed to send HTTP request for batch")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		httpErr := errors.NewServiceError("HTTP status %d: %s", resp.StatusCode, string(body))

		return c.handleBatchError(batch, httpErr, "[processBatchViaHTTP] Propagation HTTP endpoint returned error")
	}

	// Success - notify all transactions
	c.logger.Debugf("[processBatchViaHTTP] Successfully processed %d transactions via HTTP fallback", len(batch))

	for _, tx := range batch {
		tx.done <- nil
	}

	return nil
}

// ProcessTransactionBatch sends multiple transactions to the validator for processing.
// It attempts to process them via gRPC first, falling back to HTTP if the message size is too large.
func (c *Client) ProcessTransactionBatch(ctx context.Context, batch []*batchItem) error {
	// Create a slice of raw transaction bytes for the gRPC request
	txs := make([][]byte, len(batch))
	for i, item := range batch {
		txs[i] = item.tx.ExtendedBytes()
	}

	if c.settings.Propagation.AlwaysUseHTTP {
		return c.processBatchViaHTTP(ctx, batch, txs)
	}

	// First, try to process the batch using gRPC
	response, err := c.client.ProcessTransactionBatch(ctx, &propagation_api.ProcessTransactionBatchRequest{
		Tx: txs,
	})

	// If successful, handle the response and return
	if err == nil {
		c.handleBatchResponse(batch, response)
		return nil
	}

	// Check if the error is related to message size (ResourceExhausted)
	st, ok := status.FromError(err)
	if ok && st.Code() == codes.ResourceExhausted {
		// Log the fallback
		c.logger.Warnf("[ProcessTransactionBatch] Batch exceeds gRPC message limit, falling back to validator /txs endpoint: %s",
			st.Message())

		// Process the batch via HTTP
		return c.processBatchViaHTTP(ctx, batch, txs)
	}

	// Any other gRPC error - unwrap it according to our error handling rule
	// and propagate to all transactions in batch
	unwrappedErr := errors.UnwrapGRPC(err)

	return c.handleBatchError(batch, unwrappedErr, "[ProcessTransactionBatch] Failed to process transaction batch")
}

// getClientConn establishes a gRPC connection to the propagation service.
// It uses the configured service addresses and connection options.
//
// Parameters:
//   - ctx: Context for connection establishment
//
// Returns:
//   - *grpc.ClientConn: Established gRPC connection
//   - error: Error if connection fails
func getClientConn(ctx context.Context, propagationGrpcAddresses []string, tSettings *settings.Settings) (*grpc.ClientConn, error) {
	conn, err := util.GetGRPCClient(ctx, propagationGrpcAddresses[0], &util.ConnectionOptions{
		MaxRetries:   tSettings.GRPCMaxRetries,
		RetryBackoff: tSettings.GRPCRetryBackoff,
	}, tSettings)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
