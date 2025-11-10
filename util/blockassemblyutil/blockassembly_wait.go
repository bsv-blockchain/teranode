// Package blockassemblyutil provides utility functions for block assembly coordination.
package blockassemblyutil

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/retry"
)

// WaitForBlockAssemblyReady waits for the block assembly service to be ready to process
// a block at the given height. This ensures that all necessary data (such as coinbase
// transactions) has been processed before allowing block validation to proceed.
//
// The function implements a retry mechanism with linear backoff, checking if the
// block assembly service is not too far behind the target height. This prevents the
// blockchain state from running too far ahead of block assembly, which would cause
// coinbase maturity checks to fail incorrectly in the UTXO store.
//
// Parameters:
//   - ctx: Context for cancellation
//   - logger: Logger for recording operations
//   - blockAssemblyClient: Client interface to the block assembly service
//   - blockHeight: The height of the block to be processed
//   - maxBlocksBehind: Maximum number of blocks block assembly can be behind
//
// Returns:
//   - error: nil if block assembly is ready, error if timeout or other failure
func WaitForBlockAssemblyReady(
	ctx context.Context,
	logger ulogger.Logger,
	blockAssemblyClient blockassembly.ClientI,
	blockHeight uint32,
	maxBlocksBehind int,
) error {
	// Skip if block assembly client is not available (e.g., in tests)
	if blockAssemblyClient == nil {
		return nil
	}

	// Check that block assembly is not more than maxBlocksBehind blocks behind
	// This is to make sure all the coinbases have been processed in the block assembly
	_, err := retry.Retry(ctx, logger, func() (uint32, error) {
		blockAssemblyStatus, err := blockAssemblyClient.GetBlockAssemblyState(ctx)
		if err != nil {
			return 0, errors.NewProcessingError("failed to get block assembly state", err)
		}

		if blockAssemblyStatus.CurrentHeight+uint32(maxBlocksBehind) < blockHeight {
			return 0, errors.NewProcessingError("block assembly is behind, block height %d, block assembly height %d", blockHeight, blockAssemblyStatus.CurrentHeight)
		}

		return blockAssemblyStatus.CurrentHeight, nil
	},
		retry.WithRetryCount(100),
		retry.WithBackoffDurationType(20*time.Millisecond),
		retry.WithBackoffMultiplier(4),
		retry.WithMessage(fmt.Sprintf("[WaitForBlockAssemblyReady] block assembly block height %d is behind, waiting", blockHeight)),
	)

	if err != nil {
		// block-assembly is still behind, so we cannot process this block
		return err
	}

	return nil
}
