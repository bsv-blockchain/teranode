package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/errgroup"
)

// SetMinedMultiChunked merges mined-block info into pre-existing transactions in
// bounded chunks across a small worker pool. A single SetMinedMulti call with every
// existing tx in a block overruns the aerospike client connection pool on fat blocks
// (e.g. mainnet 755880 = 2.87M txs, issue 936). Pattern mirrors
// stores/utxo/aerospike/longest_chain.go:MarkTransactionsOnLongestChain.
//
// Extracted from services/legacy/netsync createUtxos so the legacy and
// quick-validation paths share one implementation. batchSize and maxWorkers are
// floored to 1; info is passed through verbatim (callers differ deliberately in
// SubtreeIdx/OnLongestChain — do not normalise here).
func SetMinedMultiChunked(ctx context.Context, logger ulogger.Logger, store Store, hashes []*chainhash.Hash, info MinedBlockInfo, batchSize, maxWorkers int) error {
	if len(hashes) == 0 {
		return nil
	}

	logger.Debugf("[SetMinedMultiChunked] merging blockID %d into %d pre-existing tx(s)", info.BlockID, len(hashes))

	if batchSize < 1 {
		batchSize = 1
	}

	numChunks := (len(hashes) + batchSize - 1) / batchSize

	numWorkers := min(maxWorkers, numChunks)
	if numWorkers < 1 {
		numWorkers = 1
	}

	rangeSize := (len(hashes) + numWorkers - 1) / numWorkers

	mergeG, mergeCtx := errgroup.WithContext(ctx)

	for w := 0; w < numWorkers && w*rangeSize < len(hashes); w++ {
		workerStart := w * rangeSize
		workerEnd := min(workerStart+rangeSize, len(hashes))
		workerHashes := hashes[workerStart:workerEnd]

		mergeG.Go(func() error {
			for i := 0; i < len(workerHashes); i += batchSize {
				if mergeCtx.Err() != nil {
					return mergeCtx.Err()
				}
				chunkEnd := min(i+batchSize, len(workerHashes))
				chunk := workerHashes[i:chunkEnd]
				if _, err := store.SetMinedMulti(mergeCtx, chunk, info); err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := mergeG.Wait(); err != nil {
		return errors.NewProcessingError("failed to merge blockID into %d pre-existing txs", len(hashes), err)
	}

	return nil
}
