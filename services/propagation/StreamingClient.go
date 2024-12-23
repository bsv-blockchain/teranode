// Package propagation provides Bitcoin SV transaction propagation functionality.
package propagation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/services/propagation/propagation_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"google.golang.org/grpc"
)

// Package propagation provides Bitcoin SV transaction propagation functionality.
type timing struct {
	total time.Duration // Total time spent processing transactions
	count int           // Number of transactions processed
}

// transaction represents a transaction to be processed along with its error channel
type transaction struct {
	txBytes []byte     // Raw transaction bytes
	errCh   chan error // Channel for receiving processing errors
}

// StreamingClient handles streaming transaction processing over gRPC.
// It maintains a persistent connection to the propagation server and
// provides methods for submitting transactions and tracking metrics.
type StreamingClient struct {
	logger        ulogger.Logger
	settings      *settings.Settings
	transactionCh chan *transaction
	timingsCh     chan chan timing
	conn          *grpc.ClientConn
	stream        propagation_api.PropagationAPI_ProcessTransactionStreamClient
	totalTime     time.Duration
	count         int  // Total transactions processed
	testMode      bool // Flag for test mode operation
}

// NewStreamingClient creates a new StreamingClient instance with the specified buffer size.
// It initializes channels and starts the background handler routine.
//
// Parameters:
//   - ctx: Context for client operations
//   - logger: Logger instance for client operations
//   - bufferSize: Size of the transaction buffer channel
//   - testMode: Optional boolean flag for test mode operation
//
// Returns:
//   - *StreamingClient: New client instance
//   - error: Error if client creation fails
func NewStreamingClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, bufferSize int, testMode ...bool) (*StreamingClient, error) {
	sc := &StreamingClient{
		logger:        logger,
		settings:      tSettings,
		transactionCh: make(chan *transaction, bufferSize),
		timingsCh:     make(chan chan timing),
	}

	if len(testMode) > 0 {
		sc.testMode = testMode[0]
	}

	initResolver(logger, tSettings.Propagation.GRPCResolver)

	go sc.handler(ctx)

	return sc, nil
}

// handler is the main event loop for the StreamingClient.
// It processes incoming transactions, manages timing requests, and handles
// connection cleanup. The handler automatically closes idle connections
// after 10 seconds of inactivity.
//
// Parameters:
//   - ctx: Context for cancellation
func (sc *StreamingClient) handler(ctx context.Context) {
	defer sc.closeResources()

	for {
		select {
		case <-ctx.Done():
			return

		case getTimeCh := <-sc.timingsCh:
			getTimeCh <- timing{
				total: sc.totalTime,
				count: sc.count,
			}

		case tx := <-sc.transactionCh:
			if sc.testMode {
				tx.errCh <- nil
				continue
			}

			if sc.stream == nil {
				if err := sc.initStream(ctx); err != nil {
					tx.errCh <- err
					return
				}
			}

			if err := sc.stream.Send(&propagation_api.ProcessTransactionRequest{
				Tx: tx.txBytes,
			}); err != nil {
				tx.errCh <- err
				return
			}

			if _, err := sc.stream.Recv(); err != nil {
				tx.errCh <- err
				return
			}

			tx.errCh <- nil

		case <-time.After(10 * time.Second):
			sc.closeResources()
		}
	}
}

// closeResources cleans up client resources including the gRPC stream and connection.
// This is called automatically when the handler exits or the connection is idle.
func (sc *StreamingClient) closeResources() {
	if sc.stream != nil {
		_ = sc.stream.CloseSend()
	}
	if sc.conn != nil {
		_ = sc.conn.Close()
	}
	sc.stream = nil
	sc.conn = nil
}

// ProcessTransaction submits a single transaction for processing.
// It tracks timing information and waits for completion or error.
//
// Parameters:
//   - txBytes: Raw transaction bytes to process
//
// Returns:
//   - error: Error if transaction processing fails
func (sc *StreamingClient) ProcessTransaction(txBytes []byte) error {
	start := time.Now()

	errCh := make(chan error)

	sc.transactionCh <- &transaction{
		txBytes: txBytes,
		errCh:   errCh,
	}

	err := <-errCh

	sc.totalTime += time.Since(start)
	sc.count++

	return err
}

// ProcessTransactionBatch processes multiple transactions sequentially.
// It stops and returns on the first error encountered.
//
// Parameters:
//   - txs: Slice of raw transaction bytes to process
//
// Returns:
//   - error: Error if any transaction fails to process
func (sc *StreamingClient) ProcessTransactionBatch(txs [][]byte) error {
	for _, tx := range txs {
		if err := sc.ProcessTransaction(tx); err != nil {
			return err
		}
	}

	return nil
}

// GetTimings returns the total processing time and count of transactions.
// This provides metrics about the client's operation since creation.
//
// Returns:
//   - time.Duration: Total time spent processing transactions
//   - int: Total number of transactions processed
func (sc *StreamingClient) GetTimings() (time.Duration, int) {
	ch := make(chan timing)
	sc.timingsCh <- ch

	timings := <-ch

	return timings.total, timings.count
}

// initStream initializes the gRPC stream connection to the propagation server.
// This is called automatically when needed and after connection timeouts.
//
// Parameters:
//   - ctx: Context for stream initialization
//
// Returns:
//   - error: Error if stream initialization fails
func (sc *StreamingClient) initStream(ctx context.Context) error {
	var err error

	conn, err := getClientConn(ctx, sc.settings.Propagation.GRPCAddresses)
	if err != nil {
		return err
	}

	client := propagation_api.NewPropagationAPIClient(conn)

	sc.stream, err = client.ProcessTransactionStream(ctx)
	if err != nil {
		return err
	}
	return nil
}
