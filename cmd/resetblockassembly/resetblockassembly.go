// Package resetblockassembly provides functionality to reset the block assembly process
package resetblockassembly

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// ResetBlockAssembly resets the block assembly process by rescanning unmined transactions into a new block template.
//
// Parameters:
//   - logger: A ulogger.Logger instance for logging purposes.
//   - settings: A pointer to settings.Settings containing configuration details.
//   - fullReset: If true, performs a full scan of all unmined transactions.
//   - validateInputs: If true, validates that each unmined tx's inputs are still spent by this tx.
//
// Returns:
//   - An error if any step in the process fails.
func ResetBlockAssembly(logger ulogger.Logger, settings *settings.Settings, fullReset bool, validateInputs bool) error {
	// Set the context for the operation
	ctx := context.Background()

	// Initialize the block assembly service
	ba, err := blockassembly.NewClient(ctx, logger, settings)
	if err != nil {
		return errors.NewConfigurationError("failed to create block assembly client: %w", err)
	}

	if validateInputs {
		if err = ba.ResetBlockAssemblyValidateInputs(ctx); err != nil {
			return errors.NewProcessingError("failed to reset block assembly with input validation: %w", err)
		}
	} else if fullReset {
		if err = ba.ResetBlockAssemblyFully(ctx); err != nil {
			return errors.NewProcessingError("failed to reset block assembly: %w", err)
		}
	} else {
		if err = ba.ResetBlockAssembly(ctx); err != nil {
			return errors.NewProcessingError("failed to reset block assembly: %w", err)
		}
	}

	return nil
}
