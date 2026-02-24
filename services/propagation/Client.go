// Package propagation provides Bitcoin SV transaction propagation functionality for the Teranode system.
// This package implements efficient transaction processing and distribution across the Bitcoin SV network,
// supporting both individual transaction handling and high-throughput batch processing operations.
//
// Key Features:
//   - Batching client for efficient transaction processing with configurable batch sizes
//   - HTTP and gRPC interfaces for transaction submission and status queries
//   - Large transaction fallback mechanisms for handling oversized transactions
//   - Comprehensive error handling and retry logic for network resilience
//   - Integration with OpenTelemetry for distributed tracing and monitoring
//   - Support for both synchronous and asynchronous transaction processing
//
// Architecture:
// The propagation service acts as a bridge between transaction producers (such as mining pools,
// wallets, and applications) and the Bitcoin SV network. It optimizes transaction throughput
// through intelligent batching while maintaining reliability through robust error handling.
//
// The service provides multiple interfaces:
//   - gRPC API for high-performance programmatic access
//   - HTTP REST API for web-based integrations
//   - Batch processing for high-volume transaction scenarios
//
// Integration:
// This package integrates with other Teranode services including the mempool, validator,
// and P2P services to ensure transactions are properly validated and distributed across
// the network according to Bitcoin SV protocol specifications.
package propagation

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/bsv-blockchain/go-batcher"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// batchItem represents a single transaction in a batch along with its completion channel.
// This type serves as the fundamental unit in the transaction batch processing system:
//
// - tx: Contains the parsed Bitcoin transaction (bt.Tx) ready for submission
// - done: Channel used for asynchronous completion notification and error reporting
//
// When a transaction is submitted through the batching system, it's wrapped in a batchItem
// and placed in the batch queue. Once the transaction is processed (either successfully
// or with an error), the result is sent through the done channel, allowing the original
// caller to continue execution with proper error handling.
type batchItem struct {
	ctx  context.Context
	tx   *bt.Tx     // Bitcoin transaction to process
	done chan error // Channel to signal completion and return any error
}

// Client implements a batching transaction client for the BSV propagation service.
// This client provides efficient transaction processing through intelligent batching mechanisms,
// supporting both individual transaction submission and high-throughput batch operations.
//
// The Client maintains persistent gRPC connections to the propagation service and manages
// transaction batching according to configurable size and timeout parameters. It handles
// both synchronous and asynchronous transaction processing patterns.
//
// Key capabilities:
//   - Automatic transaction batching for improved throughput
//   - Configurable batch sizes and timeout intervals
//   - Fallback mechanisms for large transactions that exceed batch limits
//   - HTTP endpoint support for REST API compatibility
//   - Comprehensive error handling and retry logic
//   - Integration with distributed tracing for monitoring and debugging
//
// Thread Safety:
// The Client is designed for concurrent use and maintains internal synchronization
// for batch processing operations. Multiple goroutines can safely submit transactions
// simultaneously through the same Client instance.
//
// Fields:
//   - client: gRPC client for communicating with the propagation service
//   - conn: Persistent gRPC connection for efficient communication
//   - batchSize: Maximum number of transactions per batch
//   - batchCh: Channel for coordinating batch processing operations
//   - batcher: Batching utility for managing transaction grouping and timing
//   - logger: Structured logger for debugging and monitoring
//   - settings: Global configuration settings for the client
//   - propagationHTTPAddr: HTTP endpoint URL for REST API fallback operations
type Client struct {
	client              propagation_api.PropagationAPIClient
	conn                *grpc.ClientConn
	batchSize           int
	batchCh             chan []*batchItem
	batcher             batcher.Batcher[batchItem]
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
// This method performs proper cleanup of resources used by the propagation client:
//
// 1. Verifies that both client and connection are initialized before attempting cleanup
// 2. Safely closes the gRPC connection to the propagation service
// 3. Silently handles any close errors to prevent propagation of shutdown errors
//
// This method should be called when the client is no longer needed to prevent
// resource leaks, particularly long-lived gRPC connections. It's commonly used
// during service shutdown or when switching connection configurations.
//
// Important: After Stop is called, the client instance should not be reused.
// A new client instance should be created if needed.

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
	ctx, _, endSpan := tracing.Tracer("PropagationClient").Start(ctx, "ProcessTransaction", tracing.WithTag("tx", tx.TxID()))
	defer endSpan()

	txBytes := tx.SerializeBytes()

	if c.settings.Propagation.AlwaysUseHTTP {
		return c.sendTransactionViaHTTP(ctx, txBytes, *tx.TxIDChainHash())
	}

	if c.batchSize > 0 {
		done := make(chan error)

		c.batcher.Put(&batchItem{
			ctx:  ctx,
			tx:   tx,
			done: done,
		})

		err := <-done

		return err
	}

	// Try gRPC first
	_, err := c.client.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.SerializeBytes(),
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

		return c.sendTransactionViaHTTP(ctx, txBytes, *tx.TxIDChainHash())
	}

	// For any other types of errors, return the unwrapped gRPC error
	return errors.UnwrapGRPC(err)
}

// sendTransactionViaHTTP sends a single transaction to the propagation service via HTTP.
// This method implements the HTTP fallback path for transaction submission when:
// 1. The transaction exceeds gRPC message size limits
// 2. The client is configured to always use HTTP (AlwaysUseHTTP=true)
//
// The implementation follows these steps:
// 1. Creates an HTTP client with appropriate timeout settings
// 2. Prepares a POST request to the /tx endpoint with the transaction's extended bytes
// 3. Sends the request with proper context propagation for cancellation
// 4. Processes the response with thorough error handling
// 5. Logs success or detailed error information
//
// Parameters:
//   - ctx: Context for HTTP request with cancellation support
//   - tx: Bitcoin transaction to submit
//
// Returns:
//   - error: Error if HTTP transaction submission fails
func (c *Client) sendTransactionViaHTTP(ctx context.Context, txBytes []byte, txHash chainhash.Hash) error {
	// Create an HTTP client with a timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Prepare request to validator /tx endpoint
	req, err := http.NewRequestWithContext(ctx, "POST", c.propagationHTTPAddr.String()+"/tx", bytes.NewBuffer(txBytes))
	if err != nil {
		return errors.NewServiceError("[ProcessTransaction][%s] error creating request to validator /tx endpoint", txHash, err)
	}

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return errors.NewServiceError("[ProcessTransaction][%s] error sending transaction to validator /tx endpoint", txHash, err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		httpErr := errors.NewServiceError("HTTP status %d: %s", resp.StatusCode, string(body))

		return errors.NewServiceError("[ProcessTransaction][%s] validator /tx endpoint returned non-OK status", txHash, httpErr)
	}

	c.logger.Debugf("[ProcessTransaction][%s] successfully validated using validator /tx endpoint", txHash)

	return nil
}

// TriggerBatcher forces the current batch to be processed immediately,
// regardless of whether it has reached the configured batch size or timeout.
// This method provides a mechanism to flush any pending transactions in the
// current batch, which is particularly useful in scenarios such as:
//
// 1. Service shutdown where pending transactions should be processed
// 2. Low-volume periods where transactions might wait too long
// 3. Testing scenarios where immediate processing is desired
// 4. Manual intervention points in transaction processing flow
//
// The method delegates to the underlying batcher's Trigger method to initiate
// the immediate batch processing.
func (c *Client) TriggerBatcher() {
	c.batcher.Trigger()
}

// handleBatchError sends the given error to all transactions in the batch.
// This method provides uniform error handling for batch processing failures:
//
//  1. Logs the error with the provided format string and additional context
//  2. Distributes the error to all transaction items in the batch through their
//     individual completion channels
//  3. Ensures all goroutines waiting on transaction completion are unblocked
//
// This centralized error handling ensures consistent behavior across different
// batch processing failure scenarios.
//
// Parameters:
//   - batch: Slice of transaction items with completion channels
//   - err: Error to propagate to all transactions
//   - format: Log message format string
//   - args: Additional arguments for the format string
//
// Returns:
//   - error: Returns the input error for convenience in call chaining
func (c *Client) handleBatchError(batch []*batchItem, err error, format string, args ...interface{}) error {
	wrappedErr := errors.NewServiceError(format, append(args, err)...)
	c.logger.Errorf("%s", wrappedErr.Error())

	for _, tx := range batch {
		tx.done <- wrappedErr
	}

	return wrappedErr
}

// handleBatchResponse processes the gRPC response for a batch and notifies each transaction.
// This method handles the successful gRPC batch processing response by:
//
// 1. Iterating through each transaction in the batch
// 2. Mapping response errors to the corresponding transactions
// 3. Sending appropriate errors or success notifications via completion channels
// 4. Ensuring all waiting goroutines are unblocked with the correct status
//
// The method ensures that even within a successful batch, individual transaction
// errors are properly captured and reported.
//
// Parameters:
//   - batch: Slice of transaction items with completion channels
//   - response: gRPC response containing per-transaction error status
func (c *Client) handleBatchResponse(batch []*batchItem, response *propagation_api.ProcessTransactionBatchResponse) {
	for i, err := range response.Errors {
		if !err.IsNil() { // don't do err != nil, proto can't return nil TError
			batch[i].done <- err
		} else {
			batch[i].done <- nil
		}
	}
}

// processBatchViaHTTP sends transaction batch via HTTP fallback when gRPC fails.
// This method implements the HTTP transport fallback for batch processing:
//
// 1. Prepares a properly formatted multipart form request with all transactions
// 2. Sets appropriate HTTP headers and connection parameters
// 3. Submits the batch to the propagation service's /txs endpoint
// 4. Processes the response, analyzing status codes and error conditions
// 5. Reports any errors back to the original transaction channels
//
// The HTTP fallback path is critical for resilient operation when transactions
// exceed the gRPC message size limits or when configured to always use HTTP.
//
// Parameters:
//   - ctx: Context for HTTP request processing
//   - batch: Slice of transaction items with completion channels
//   - txs: Raw transaction bytes for each transaction in the batch
//
// Returns:
//   - error: Error if the HTTP batch processing fails
func (c *Client) processBatchViaHTTP(ctx context.Context, batch []*batchItem, items []*propagation_api.BatchTransactionItem) error {
	// Create HTTP client for large batch
	client := &http.Client{
		Timeout: 60 * time.Second, // Longer timeout for batch
	}

	// Combine all transactions into a single payload for the /txs endpoint
	var combinedTxs bytes.Buffer
	for _, item := range items {
		combinedTxs.Write(item.Tx)
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
// This method implements a sophisticated multi-transport strategy with automatic fallback:
//
// 1. If AlwaysUseHTTP is configured, it directly uses the HTTP endpoint
// 2. Otherwise, it attempts to use gRPC for better performance:
//   - Prepares a batch request with extended transaction bytes
//   - Sends the request to the propagation service
//   - Processes the response with per-transaction error handling
//
// 3. If the gRPC request fails due to message size limits:
//   - Automatically falls back to HTTP transport
//   - Logs the transport fallback event
//   - Preserves the original batch processing semantics
//
// 4. For other types of errors, properly unwraps and propagates them
//
// The method ensures all transactions in the batch receive appropriate error
// feedback through their individual completion channels.
//
// Parameters:
//   - ctx: Context for batch processing with timeout/cancellation
//   - batch: Slice of transaction items with completion channels
//
// Returns:
//   - error: Error if batch processing fails at the transport level
func (c *Client) ProcessTransactionBatch(ctx context.Context, batch []*batchItem) error {
	// Create a slice of raw transaction bytes for the gRPC request
	items := make([]*propagation_api.BatchTransactionItem, len(batch))

	for i, item := range batch {
		// Serialize the trace context for this transaction
		traceContext := make(map[string]string)

		prop := otel.GetTextMapPropagator()
		prop.Inject(item.ctx, propagation.MapCarrier(traceContext))

		items[i] = &propagation_api.BatchTransactionItem{
			Tx:           item.tx.SerializeBytes(),
			TraceContext: traceContext,
		}
	}

	if c.settings.Propagation.AlwaysUseHTTP {
		return c.processBatchViaHTTP(ctx, batch, items)
	}

	// First, try to process the batch using gRPC
	response, err := c.client.ProcessTransactionBatch(ctx, &propagation_api.ProcessTransactionBatchRequest{
		Items: items,
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

		return c.processBatchViaHTTP(ctx, batch, items)
	}

	// Any other gRPC error - unwrap it according to our error handling rule
	// and propagate to all transactions in batch
	unwrappedErr := errors.UnwrapGRPC(err)

	return c.handleBatchError(batch, unwrappedErr, "[ProcessTransactionBatch] Failed to process transaction batch")
}

// getClientConn establishes a gRPC connection to the propagation service.
// This method handles the critical task of establishing a reliable connection
// to the propagation service with proper configuration:
//
// 1. Uses the configured list of propagation service addresses
// 2. Configures connection with appropriate gRPC options:
//   - WithInsecure for non-TLS connections (configurable)
//   - WithBlock for synchronous connection establishment
//   - WithDefaultCallOptions for appropriate message size limits
//
// 3. Establishes the connection with proper timeout handling
// 4. Incorporates any user-defined dial options from settings
//
// The connection created by this method serves as the foundation for all
// gRPC-based transaction propagation communications.
//
// Parameters:
//   - ctx: Context for connection establishment with timeout control
//   - propagationGrpcAddresses: List of propagation service endpoints
//   - tSettings: Settings containing connection configuration
//
// Returns:
//   - *grpc.ClientConn: Established gRPC connection ready for client creation
//   - error: Error if connection establishment fails
func getClientConn(ctx context.Context, propagationGrpcAddresses []string, tSettings *settings.Settings) (*grpc.ClientConn, error) {
	if len(propagationGrpcAddresses) == 0 {
		return nil, errors.NewServiceError("no gRPC addresses provided")
	}

	conn, err := util.GetGRPCClient(ctx, propagationGrpcAddresses[0], &util.ConnectionOptions{
		MaxRetries:   tSettings.GRPCMaxRetries,
		RetryBackoff: tSettings.GRPCRetryBackoff,
	}, tSettings)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
