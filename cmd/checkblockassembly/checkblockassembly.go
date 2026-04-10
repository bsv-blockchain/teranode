// Package checkblockassembly checks unmined transactions for input validity
// without modifying block assembly state.
package checkblockassembly

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// CheckBlockAssembly validates that all unmined transactions in block assembly have
// inputs that are still validly spent by those transactions. It does not modify any state.
// Returns an error if any unmined transaction is found with invalid inputs, or if
// the block assembly service cannot be reached.
func CheckBlockAssembly(logger ulogger.Logger, settings *settings.Settings) error {
	ctx := context.Background()

	ba, err := blockassembly.NewClient(ctx, logger, settings)
	if err != nil {
		return errors.NewConfigurationError("failed to create block assembly client", err)
	}

	if err = ba.CheckBlockAssemblyValidateInputs(ctx); err != nil {
		return errors.NewProcessingError("block assembly validation failed", err)
	}

	return nil
}
