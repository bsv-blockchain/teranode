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

// CheckBlockTemplate handles checking the block template in block assembly
func CheckBlockTemplate(logger ulogger.Logger, tSettings *settings.Settings) (*model.Block, error) {
	fmt.Println()

	ctx := context.Background()

	blockAssemblyClient, err := blockassembly.NewClient(ctx, logger, tSettings)
	if err != nil {
		return nil, err
	}

	// create a new blockchain client
	blockValidationClient, err := blockvalidation.NewClient(ctx, logger, tSettings, "CheckBlockTemplate Command")
	if err != nil {
		return nil, err
	}

	blockTemplate, err := blockAssemblyClient.GetBlockAssemblyBlockCandidate(ctx)
	if err != nil {
		return nil, err
	}

	err = blockValidationClient.ValidateBlock(ctx, blockTemplate)
	if err != nil {
		return blockTemplate, err
	}

	return blockTemplate, nil
}
