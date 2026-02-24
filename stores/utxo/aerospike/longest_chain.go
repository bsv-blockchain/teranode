package aerospike

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// MarkTransactionsOnLongestChain updates unmined_since for transactions based on their chain status.
//
// This function is critical for maintaining data integrity during blockchain reorganizations.
// Uses a worker-pool pattern matching SetTxMined's proven production approach: a fixed number
// of worker goroutines each process sequential batches with pooled memory, avoiding the
// goroutine churn and allocation overhead of per-chunk goroutines.
//
// Behavior:
//   - onLongestChain=true: Clears unmined_since (transaction is mined on main chain)
//   - onLongestChain=false: Sets unmined_since to current height (transaction is unmined)
//
// CRITICAL - Resilient Error Handling (Must Not Fail Fast):
// This function attempts to update ALL transactions even if some fail.
//
// Error handling strategy:
//   - Processes ALL transactions concurrently (does not stop on first error)
//   - Collects up to 10 errors for logging/debugging (prevents log spam)
//   - Logs summary: attempted, succeeded, failed counts
//   - Returns aggregated errors after attempting all transactions
//   - Missing transactions trigger FATAL error (data corruption - unrecoverable)
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	allErrors := make([]error, 0, 10)
	missingTxErrors := make([]error, 0, 10)
	var errorCount int64
	var mu sync.Mutex

	currentBlockHeight := s.GetBlockHeight()
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchWritePolicy := aerospike.NewBatchWritePolicy()
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchSize := s.settings.UtxoStore.MaxMinedBatchSize
	numChunks := (len(txHashes) + batchSize - 1) / batchSize
	numWorkers := min(s.settings.UtxoStore.MaxMinedRoutines, numChunks)

	// Pre-create the shared operation — identical for every transaction (same bin name, same value).
	// The aerospike *Operation struct is immutable after creation and only read during serialization,
	// so sharing one instance across all batch writes is safe and eliminates 2 allocs per tx.
	var binValue any
	if !onLongestChain {
		binValue = currentBlockHeight
	}
	op := aerospike.PutOp(aerospike.NewBin(fields.UnminedSince.String(), binValue))

	// Divide work evenly across workers (fewer goroutines, sequential batches within each)
	rangeSize := (len(txHashes) + numWorkers - 1) / numWorkers

	g, gCtx := errgroup.WithContext(ctx)

	for w := 0; w < numWorkers && w*rangeSize < len(txHashes); w++ {
		workerStart := w * rangeSize
		workerEnd := min(workerStart+rangeSize, len(txHashes))
		workerHashes := txHashes[workerStart:workerEnd]

		g.Go(func() error {
			// Get pooled slice, reuse across sequential batches
			batchRecordsPtr := getBatchRecordsSlice(batchSize)
			defer putBatchRecordsSlice(batchRecordsPtr)

			for i := 0; i < len(workerHashes); i += batchSize {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}

				batchEnd := min(i+batchSize, len(workerHashes))
				chunk := workerHashes[i:batchEnd]

				// Reuse pooled slice, sized to this chunk
				batchRecords := (*batchRecordsPtr)[:0]

				for _, txHash := range chunk {
					key, err := aerospike.NewKey(s.namespace, s.setName, txHash[:])
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						mu.Lock()
						if len(allErrors) < 10 {
							allErrors = append(allErrors, err)
						}
						mu.Unlock()
						continue
					}

					batchRecords = append(batchRecords, aerospike.NewBatchWrite(
						batchWritePolicy,
						key,
						op,
					))
				}

				if len(batchRecords) == 0 {
					continue
				}

				if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
					count := int64(len(batchRecords))
					atomic.AddInt64(&errorCount, count)
					mu.Lock()
					if len(allErrors) < 10 {
						allErrors = append(allErrors, errors.NewProcessingError("could not batch operate longest chain flag", err))
					}
					mu.Unlock()
					continue // Don't fail fast — continue processing other batches
				}

				// Check individual record results
				for j, batchRecord := range batchRecords {
					if recErr := batchRecord.BatchRec().Err; recErr != nil {
						atomic.AddInt64(&errorCount, 1)
						mu.Lock()
						if len(allErrors) < 10 {
							s.logger.Errorf("[MarkTransactionsOnLongestChain] error %d: transaction %s: %v",
								atomic.LoadInt64(&errorCount), chunk[j], recErr)
							allErrors = append(allErrors, recErr)

							if errors.Is(recErr, aerospike.ErrKeyNotFound) {
								missingTxErrors = append(missingTxErrors, errors.NewStorageError("MISSING transaction %s", chunk[j], recErr))
							}
						}
						mu.Unlock()
					}
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	attempted := len(txHashes)
	finalErrorCount := int(atomic.LoadInt64(&errorCount))
	succeeded := attempted - finalErrorCount

	s.logger.Infof("[MarkTransactionsOnLongestChain] completed: attempted=%d, succeeded=%d, failed=%d, onLongestChain=%t",
		attempted, succeeded, finalErrorCount, onLongestChain)

	if len(missingTxErrors) > 0 {
		s.logger.Fatalf("CRITICAL: %d missing transactions during MarkTransactionsOnLongestChain - data integrity compromised. First errors: %v",
			len(missingTxErrors), errors.Join(missingTxErrors...))
	}

	if len(allErrors) > 0 {
		if finalErrorCount > 10 {
			s.logger.Errorf("[MarkTransactionsOnLongestChain] only returned first 10 of %d errors", finalErrorCount)
		}
		return errors.Join(allErrors...)
	}

	return nil
}
