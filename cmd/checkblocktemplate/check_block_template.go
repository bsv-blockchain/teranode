// Package checkblocktemplate provides functionality for validating block templates in the block assembly process.
// It is designed to ensure that block templates meet the required validation criteria before being used in the blockchain.
//
// Usage:
//
// This package is typically used as part of the block validation pipeline to fetch and validate block templates
// using the provided settings and services.
//
// Functions:
//   - ValidateBlockTemplate: Fetches a block template and validates it using the block validation service.
//
// Side effects:
//
// Functions in this package may log messages and return errors if validation fails.
package checkblocktemplate

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// ValidateBlockTemplate fetches a block template and validates it using the block validation service.
//
// This function is part of the block validation pipeline and is used to ensure that block templates
// meet the required criteria before being used in the blockchain. It interacts with the block assembly
// and block validation services to perform its operations.
//
// Parameters:
//   - logger: An instance of ulogger.Logger used for logging messages during the validation process.
//   - settings: A pointer to settings.Settings containing configuration details for the services.
//
// Returns:
//   - *model.Block: The validated block template if the validation is successful.
//   - error: An error object if any issues occur during the fetching or validation process.
//
// Side effects:
//   - Logs messages to the provided logger.
//   - May return errors if the block template cannot be fetched or fails validation.
func ValidateBlockTemplate(logger ulogger.Logger, settings *settings.Settings) (*model.Block, error) {
	// Print a blank line for better readability in logs
	fmt.Println()

	// Set the context for the operation
	ctx := context.Background()

	// Create a new block assembly client
	blockAssemblyClient, err := blockassembly.NewClient(ctx, logger, settings)
	if err != nil {
		return nil, err
	}

	// create a new blockchain validation client
	var blockValidationClient *blockvalidation.Client

	blockValidationClient, err = blockvalidation.NewClient(ctx, logger, settings, "ValidateBlockTemplate Command")
	if err != nil {
		return nil, err
	}

	// create a new block template
	var blockTemplate *model.Block

	blockTemplate, err = blockAssemblyClient.GetBlockAssemblyBlockCandidate(ctx)
	if err != nil {
		return nil, err
	}

	// validate the block template
	err = blockValidationClient.ValidateBlock(ctx, blockTemplate)
	if err != nil {
		return blockTemplate, err
	}

	return blockTemplate, nil
}
