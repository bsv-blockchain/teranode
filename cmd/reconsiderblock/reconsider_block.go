// functionality for reconsidering blocks that were
// previously marked as invalid. This allows the block to be re-validated and potentially
// accepted into the blockchain if it passes validation.
package reconsiderblock

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// reconsiderBlock removes the invalid status from a previously invalidated block
// and triggers revalidation.
func ReconsiderBlock(logger ulogger.Logger, settings *settings.Settings, blockHashStr string) error {
	fmt.Println()

	if blockHashStr == "" {
		return errors.NewProcessingError("block hash is required")
	}

	if len(blockHashStr) != 64 {
		return errors.NewProcessingError("invalid block hash: must be 64 hex characters")
	}

	blockHash, err := chainhash.NewHashFromStr(blockHashStr)
	if err != nil {
		return errors.NewProcessingError("invalid block hash: %v", err)
	}

	ctx := context.Background()

	blockValidationClient, err := blockvalidation.NewClient(ctx, logger, settings, "reconsiderblock")
	if err != nil {
		return errors.NewProcessingError("failed to create block validation client: %v", err)
	}

	logger.Infof("[reconsiderblock] Reconsidering block %s", blockHash.String())

	err = blockValidationClient.RevalidateBlock(ctx, *blockHash)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully reconsidered block %s\n", blockHash.String())

	return nil
}
