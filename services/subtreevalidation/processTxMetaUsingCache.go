// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// processTxMetaUsingCache attempts to retrieve transaction metadata from the cache for a batch
// of transactions. It processes transactions in parallel batches for efficiency.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - txHashes: Slice of transaction hashes to process
//   - txMetaSlice: Pre-allocated slice to store retrieved metadata
//   - failFast: If true, fails quickly when missing transaction threshold is exceeded
//
// Returns:
//   - int: Number of transactions missing from cache
//   - error: Any error encountered during processing
//
// The function will return a ThresholdExceededError if failFast is true and
// the number of missing transactions exceeds the configured threshold.
func (u *Server) processTxMetaUsingCache(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem, failFast bool) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewProcessingError("txHashes and txMetaSlice must be the same length")
	}

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTxMetaUsingCache")
	defer deferFn()

	batchSize := u.settings.SubtreeValidation.ProcessTxMetaUsingCacheBatchSize
	validateSubtreeInternalConcurrency := u.settings.SubtreeValidation.ProcessTxMetaUsingCacheConcurrency
	missingTxThreshold := u.settings.SubtreeValidation.ProcessTxMetaUsingCacheMissingTxThreshold

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, validateSubtreeInternalConcurrency)

	cache, ok := u.utxoStore.(*txmetacache.TxMetaCache)
	if !ok {
		u.logger.Warnf("[processTxMetaUsingCache] txMetaStore is not a cached implementation")
		return len(txHashes), nil // As there was no cache, we "missed" all the txHashes
	}

	p := &TxMetaProcessor{
		ctx:                                gCtx,
		logger:                             u.logger,
		batchSize:                          batchSize,
		validateSubtreeInternalConcurrency: validateSubtreeInternalConcurrency,
		missingTxThreshold:                 uint32(missingTxThreshold), // nolint: gosec // G601: Integer overflow (gosec)
		cache:                              cache,
		txHashes:                           txHashes,
		txMetaSlice:                        txMetaSlice,
		failFast:                           failFast,
	}

	// cycle through batches of 1024 txHashes at a time
	for i := 0; i < len(txHashes); i += batchSize {
		i := i

		g.Go(func() error {
			return p.processTxMetaUsingCache(i)
		})
	}

	if err := g.Wait(); err != nil {
		return int(p.missed.Load()), errors.NewProcessingError("error processing txMeta using cache", err)
	}

	return int(p.missed.Load()), nil
}

type TxMetaProcessor struct {
	ctx                                context.Context
	logger                             ulogger.Logger
	batchSize                          int
	validateSubtreeInternalConcurrency int
	missingTxThreshold                 uint32
	cache                              *txmetacache.TxMetaCache
	txHashes                           []chainhash.Hash
	txMetaSlice                        []metaSliceItem
	failFast                           bool
	missed                             atomic.Uint32
}

func (p *TxMetaProcessor) processTxMetaUsingCache(i int) error {
	var (
		err   error
		found bool
	)

	txMeta := meta.Data{}

	for j := 0; j < subtree.Min(p.batchSize, len(p.txHashes)-i); j++ {
		if p.txMetaSlice[i+j].isSet {
			continue
		}

		select {
		case <-p.ctx.Done():
			return errors.NewContextCanceledError("[processTxMetaUsingCache context cancelled]", p.ctx.Err())
		default:
		}

		txHash := p.txHashes[i+j]

		if txHash.Equal(*subtree.CoinbasePlaceholderHash) {
			continue
		}

		found, err = p.cache.GetMetaCached(p.ctx, txHash, &txMeta)
		if err != nil && !errors.Is(err, errors.ErrNotFound) {
			p.logger.Warnf("[processTxMetaUsingCache] error retrieving txMeta for %s: %v", txHash.String(), err)
		}

		if found {
			p.txMetaSlice[i+j] = metaSliceItem{
				fee:         txMeta.Fee,
				sizeInBytes: txMeta.SizeInBytes,
				coinbase:    txMeta.IsCoinbase,
				conflicting: txMeta.Conflicting,
				creating:    txMeta.Creating,
				isSet:       true,
				txInpoints:  txMeta.TxInpoints,
			}

			continue
		}

		newMissed := p.missed.Add(1)

		if p.failFast && p.missingTxThreshold > 0 && newMissed > p.missingTxThreshold {
			return errors.NewThresholdExceededError("threshold exceeded for missing txs: %d > %d", newMissed, p.missingTxThreshold)
		}
	}

	return nil
}
