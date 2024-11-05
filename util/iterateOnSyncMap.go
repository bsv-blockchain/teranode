package util

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"sync"
)

func IterateOnSyncMap(ctx context.Context, oldBlockIDsMap *sync.Map, blockchainClient blockchain.ClientI, logger ulogger.Logger, blockStr string) error {
	var iterationError error
	// range over the oldBlockIDsMap to get txID - oldBlockID pairs
	oldBlockIDsMap.Range(func(txID, blockIDs interface{}) bool {
		txHash, ok := txID.(chainhash.Hash)
		if !ok {
			logger.Errorf("[Block Validation][IterateOnSyncMap] failed to assert txID to chainhash.Hash for txID: %v", txID)
		}

		parentTransactionsBlockIDs, ok := blockIDs.([]uint32)
		if !ok {
			logger.Errorf("[Block Validation][IterateOnSyncMap] failed to assert blockIDs to []uint32 for txID: %v", txID)
		}

		// Flag to check if the old blocks are part of the current chain
		var blocksPartOfCurrentChain bool
		blocksPartOfCurrentChain, err := blockchainClient.CheckBlockIsInCurrentChain(ctx, parentTransactionsBlockIDs)
		// if err is not nil, log the error and continue iterating for the next transaction
		if err != nil {
			logger.Errorf("[Block Validation][%s] failed to check if old blocks are part of the current chain: %v", blockStr, err)
		}

		// if the blocks are not part of the current chain, stop iteration, set the iterationError and return false
		if !blocksPartOfCurrentChain {
			iterationError = errors.NewBlockInvalidError("[Block Validation][%s] block is not invalid. Transaction's (%v) parent blocks (%v) are not from current chain", txHash, blockStr)
			return false
		}

		return true
	})

	return iterationError
}
