package rewindblockchain

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// phase1Unmined sweeps the UTXO store in two passes:
//  1. Iterate non-conflicting unmined transactions and purge each via
//     deleteTxWithParents.
//  2. Iterate conflicting transactions (GetConflictingTxIterator) and do
//     the same.
//
// Both iterators are Aerospike-partition-parallel. Accumulated batch
// operations (RemoveFromConflictingChildren, RemoveBlockIDs) are flushed
// once per batch for Aerospike throughput.
func (e *env) phase1Unmined(ctx context.Context, pf *preflightResult) error {
	if err := e.runCleanupIterator(ctx, pf, iteratorModeUnmined); err != nil {
		return err
	}
	return e.runCleanupIterator(ctx, pf, iteratorModeConflicting)
}

type iteratorMode int

const (
	iteratorModeUnmined iteratorMode = iota
	iteratorModeConflicting
)

func (e *env) runCleanupIterator(ctx context.Context, pf *preflightResult, mode iteratorMode) error {
	var (
		it  utxo.UnminedTxIterator
		err error
	)

	switch mode {
	case iteratorModeUnmined:
		it, err = e.utxoStore.GetUnminedTxIterator()
	case iteratorModeConflicting:
		it, err = e.utxoStore.GetConflictingTxIterator()
	default:
		return errors.NewProcessingError("unknown iterator mode %d", mode)
	}
	if err != nil {
		return errors.NewStorageError("failed to open iterator: %w", err)
	}
	if it == nil {
		return nil
	}

	defer func() {
		if cerr := it.Close(); cerr != nil {
			e.logger.Warnf("iterator close: %v", cerr)
		}
	}()

	for {
		batch, nextErr := it.Next(ctx)
		if nextErr != nil {
			return errors.NewStorageError("iterator Next: %w", nextErr)
		}
		if len(batch) == 0 {
			return nil
		}

		if err = e.processCleanupBatch(ctx, pf, batch, mode); err != nil {
			return err
		}
	}
}

// processCleanupBatch handles one batch from either unmined or conflicting
// iterator, deleting each tx and collecting cross-record cleanups that are
// then flushed with the store's batch APIs.
func (e *env) processCleanupBatch(ctx context.Context, pf *preflightResult, batch []*utxo.UnminedTransaction, mode iteratorMode) error {
	collector := newRemovalCollector()

	for _, rec := range batch {
		if rec == nil || rec.Skip {
			continue
		}

		acted, err := e.deleteTxWithParents(ctx, &rec.Hash, pf, collector)
		if err != nil {
			return err
		}
		if acted {
			switch mode {
			case iteratorModeUnmined:
				e.stats.UnminedPurged++
			case iteratorModeConflicting:
				e.stats.ConflictingPurged++
			}
		}
	}

	return e.flushCollector(ctx, collector)
}
