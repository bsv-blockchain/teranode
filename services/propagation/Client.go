// Package propagation provides Bitcoin SV transaction propagation functionality.
// This file implements a batching client for efficient transaction processing.
package propagation

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
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
	client    propagation_api.PropagationAPIClient
	conn      *grpc.ClientConn
	batchSize int
	batchCh   chan []*batchItem
	batcher   batcher.Batcher2[batchItem]
}

// NewClient creates a new propagation client with optional connection configuration.
// It initializes the client with batch processing capabilities based on configuration:
//   - propagation_sendBatchSize: Maximum transactions per batch (default: 100)
//   - propagation_sendBatchTimeout: Batch timeout in milliseconds (default: 5)
//
// Parameters:
//   - ctx: Context for client operations
//   - logger: Logger instance for client operations
//   - conn: Optional pre-configured gRPC connection
//
// Returns:
//   - *Client: New client instance
//   - error: Error if client creation fails
func NewClient(ctx context.Context, logger ulogger.Logger, conn ...*grpc.ClientConn) (*Client, error) {

	initResolver(logger)

	var useConn *grpc.ClientConn
	if len(conn) > 0 {
		useConn = conn[0]
	} else {
		localConn, err := getClientConn(ctx)
		if err != nil {
			return nil, err
		}
		useConn = localConn
	}

	client := propagation_api.NewPropagationAPIClient(useConn)

	batchSize, _ := gocore.Config().GetInt("propagation_sendBatchSize", 100)
	sendBatchTimeout, _ := gocore.Config().GetInt("propagation_sendBatchTimeout", 5)

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %d", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	c := &Client{
		client:    client,
		conn:      useConn,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	sendBatch := func(batch []*batchItem) {
		if err := c.ProcessTransactionBatch(ctx, batch); err != nil {
			logger.Errorf("Error sending batch: %s", err)
		}
	}
	c.batcher = *batcher.New[batchItem](batchSize, duration, sendBatch, true)

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
	if c.batchSize > 0 {
		done := make(chan error)
		c.batcher.Put(&batchItem{tx: tx, done: done})
		err := <-done
		return err
	}

	_, err := c.client.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.ExtendedBytes(),
	})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// TriggerBatcher forces the current batch to be processed immediately,
// regardless of whether it has reached the configured batch size or timeout.
func (c *Client) TriggerBatcher() {
	c.batcher.Trigger()
}

// ProcessTransactionBatch processes a batch of transactions in a single request.
// It converts the transactions to their extended byte format and handles error
// responses for each transaction in the batch.
//
// Parameters:
//   - ctx: Context for batch processing
//   - batch: Slice of batch items containing transactions
//
// Returns:
//   - error: Error if batch processing fails
func (c *Client) ProcessTransactionBatch(ctx context.Context, batch []*batchItem) error {
	txs := make([][]byte, 0, len(batch))
	for _, tx := range batch {
		txs = append(txs, tx.tx.ExtendedBytes())
	}

	response, err := c.client.ProcessTransactionBatch(ctx, &propagation_api.ProcessTransactionBatchRequest{
		Tx: txs,
	})
	if err != nil {
		for _, tx := range batch {
			tx.done <- err
		}
		return err
	}

	for i, errStr := range response.Error {
		if errStr != "" {
			batch[i].done <- errors.NewError(errStr)
		} else {
			batch[i].done <- nil
		}
	}

	return nil
}

// initResolver configures the gRPC resolver based on the environment.
// It supports different resolver configurations:
//   - k8s: Kubernetes resolver using default scheme
//   - kubernetes: In-cluster Kubernetes resolver
//   - default: Standard gRPC resolver
//
// Parameters:
//   - logger: Logger for resolver configuration messages
func initResolver(logger ulogger.Logger) {
	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	switch grpcResolver {
	case "k8s":
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	case "kubernetes":
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	default:
		logger.Infof("[VALIDATOR] Using default resolver for clients")
	}
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
func getClientConn(ctx context.Context) (*grpc.ClientConn, error) {
	propagation_grpcAddresses, _ := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
	conn, err := util.GetGRPCClient(ctx, propagation_grpcAddresses[0], &util.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, err
	}

	return conn, nil
}
