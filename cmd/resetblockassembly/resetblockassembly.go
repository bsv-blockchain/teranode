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
	// Initialize the block assembly service
	ba, err := blockassembly.NewClient(context.Background(), logger, settings)
	if err != nil {
		return errors.NewConfigurationError("failed to create block assembly client", err)
	}
	defer func() { _ = ba.Close() }()

	return operatorReset(ba, fullReset, validateInputs)
}

// resetter is the part of the block assembly client the command calls.
type resetter interface {
	ResetBlockAssembly(ctx context.Context) error
	ResetBlockAssemblyFully(ctx context.Context) error
	ResetBlockAssemblyValidateInputs(ctx context.Context) error
}

// operatorReset makes the reset call the flags ask for. The call is marked as an operator's, the
// one reset that runs while block assembly is halted on a deep reorg and lifts the halt.
func operatorReset(ba resetter, fullReset bool, validateInputs bool) error {
	ctx := blockassembly.WithOperatorReset(context.Background())

	if validateInputs {
		if err := ba.ResetBlockAssemblyValidateInputs(ctx); err != nil {
			return errors.NewProcessingError("failed to reset block assembly with input validation", err)
		}
	} else if fullReset {
		if err := ba.ResetBlockAssemblyFully(ctx); err != nil {
			return errors.NewProcessingError("failed to reset block assembly", err)
		}
	} else {
		if err := ba.ResetBlockAssembly(ctx); err != nil {
			return errors.NewProcessingError("failed to reset block assembly", err)
		}
	}

	return nil
}
