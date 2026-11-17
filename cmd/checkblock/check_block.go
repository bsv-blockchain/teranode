// Package checkblock provides functionality for validating blocks that have already been added to the blockchain.
// This can be used to ensure the integrity and validity of blocks after they have been processed.
package checkblock

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// CheckBlock fetches a block and validates it using the block validation service.
//
// Parameters:
//   - logger: An instance of ulogger.Logger used for logging messages during the validation process.
//   - settings: A pointer to settings.Settings containing configuration details for the services.
//
// Returns:
//   - *model.Block: The validated block if the validation is successful.
//   - error: An error object if any issues occur during the fetching or validation process.
//
// Side effects:
//   - Logs messages to the provided logger.
//   - May return errors if the block cannot be fetched or fails validation.
func CheckBlock(logger ulogger.Logger, settings *settings.Settings, blockStr string) (*model.Block, error) {
	// Print a blank line for better readability in logs
	fmt.Println()

	if blockStr == "" {
		return nil, errors.NewProcessingError("empty block string")
	}

	blockHash, err := chainhash.NewHashFromStr(blockStr)
	if err != nil {
		return nil, errors.NewProcessingError("invalid block hash")
	}

	// Set the context for the operation
	ctx := context.Background()

	// Create a new block assembly client
	blockchainClient, err := blockchain.NewClient(ctx, logger, settings, "")
	if err != nil {
		return nil, err
	}
	// ClientI does not declare Close; the concrete client gained it in DC7.
	if c, ok := blockchainClient.(interface{ Close() error }); ok {
		defer func() { _ = c.Close() }()
	}

	blockValidationClient, err := blockvalidation.NewClient(ctx, logger, settings, "ValidateBlockTemplate Command")
	if err != nil {
		return nil, err
	}
	defer func() { _ = blockValidationClient.Close() }()

	block, err := blockchainClient.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	// validate the block
	err = blockValidationClient.ValidateBlock(ctx, block, nil)
	if err != nil {
		return block, err
	}

	return block, nil
}
