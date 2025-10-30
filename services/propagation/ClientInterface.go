/*
Package propagation provides Bitcoin SV transaction propagation functionality for the Teranode system.

This file defines the core interface for the propagation client, providing the contract
that all propagation client implementations must fulfill. The ClientInterface type establishes
the required methods for any propagation client implementation, ensuring consistent behavior
across different implementations or testing scenarios.

The propagation client interface enables loose coupling between components while enforcing
the necessary contract for transaction submission to the propagation service.
*/
package propagation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
)

// ClientInterface defines the core functionality required for propagation client operations.
// Any implementation of this interface must provide transaction processing capabilities
// along with batch management functionality.
type ClientInterface interface {
	// ProcessTransaction submits a single BSV transaction for processing through the propagation service.
	// The method blocks until the transaction has been validated and accepted or rejected.
	//
	// If batching is enabled (batchSize > 0), the transaction is added to the current batch
	// and the method blocks until the batch is processed. Otherwise, it is processed immediately
	// via gRPC or HTTP depending on configuration and message size constraints.
	//
	// Parameters:
	//   - ctx: Context for transaction processing, supports cancellation and timeouts
	//   - tx: Bitcoin transaction to process, must be properly formatted
	//
	// Returns:
	//   - error: Processing errors if transaction is rejected or validation fails
	ProcessTransaction(ctx context.Context, tx *bt.Tx) error

	// TriggerBatcher forces the current batch to be processed immediately, regardless of
	// whether it has reached the configured batch size or timeout. This method provides
	// a mechanism to flush any pending transactions in the current batch, which is useful
	// for testing scenarios, service shutdown, or manual intervention points.
	//
	// This method does not return any values and executes asynchronously.
	TriggerBatcher()
}
