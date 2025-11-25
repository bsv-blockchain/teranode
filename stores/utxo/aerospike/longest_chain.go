package aerospike

import (
	"context"
	"sync"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"golang.org/x/sync/errgroup"
)

// batchLongestChain represents a batch operation to mark transactions as on/not on the longest chain
type batchLongestChain struct {
	ctx            context.Context
	txHash         chainhash.Hash
	onLongestChain bool
	errCh          chan error // Channel for completion notification
}

// MarkTransactionsOnLongestChain updates unmined_since for transactions based on their chain status.
//
// This function is critical for maintaining data integrity during blockchain reorganizations.
// Uses Aerospike batch operations for efficient updates of potentially millions of transactions.
//
// Behavior:
//   - onLongestChain=true: Clears unmined_since (transaction is mined on main chain)
//   - onLongestChain=false: Sets unmined_since to current height (transaction is unmined)
//
// CRITICAL - Resilient Error Handling (Must Not Fail Fast):
// This function attempts to update ALL transactions even if some fail. Uses concurrent
// goroutines with batching for performance while ensuring all transactions are attempted.
//
// Error handling strategy:
//   - Processes ALL transactions concurrently (does not stop on first error)
//   - Collects up to 10 errors for logging/debugging (prevents log spam)
//   - Logs summary: attempted, succeeded, failed counts
//   - Returns aggregated errors after attempting all transactions
//   - Missing transactions trigger FATAL error (data corruption - unrecoverable)
//
// Why resilient processing is critical:
//   - Large reorgs can affect millions of transactions
//   - Transient errors shouldn't prevent updating other transactions
//   - Maximizes data integrity by updating as many as possible
//   - Process halts only for unrecoverable errors (missing transactions)
//
// Timing guarantee:
// This function is called synchronously from reset/reorg operations. Cleanup operations
// (preserve parents, DAH cleanup) only run AFTER this completes via setBestBlockHeader,
// ensuring they see consistent transaction state.
//
// Called from:
//   - BlockAssembler.reset() - marks moveBack transactions during large reorgs
//   - SubtreeProcessor.reorgBlocks() - marks transactions during small/medium reorgs
//   - loadUnminedTransactions() - fixes data inconsistencies
//
// Parameters:
//   - ctx: Context for cancellation
//   - txHashes: Transactions to update (can be millions during large reorgs)
//   - onLongestChain: true = clear unmined_since (mined), false = set unmined_since (unmined)
//
// Returns:
//   - error: Aggregated errors (up to 10) if any failures occurred
//   - Note: Function calls logger.Fatalf for missing transactions before returning
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	allErrors := make([]error, 0, 10) // Pre-allocate capacity for up to 10 errors
	missingTxErrors := make([]error, 0, 10)
	var errorCount int
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)

	for _, txHash := range txHashes {
		txHash := txHash

		g.Go(func() error {
			errCh := make(chan error, 1)

			s.longestChainBatcher.Put(&batchLongestChain{
				ctx:            ctx,
				txHash:         txHash,
				onLongestChain: onLongestChain,
				errCh:          errCh,
			})

			err := <-errCh
			if err != nil {
				mu.Lock()
				errorCount++
				// Only log and collect first 10 errors to avoid spam (could be millions of transactions)
				if len(allErrors) < 10 {
					s.logger.Errorf("[MarkTransactionsOnLongestChain] error %d: transaction %s: %v", errorCount, txHash, err)
					allErrors = append(allErrors, err)

					// Track missing transaction errors separately (these are fatal)
					if errors.Is(err, aerospike.ErrKeyNotFound) {
						missingTxErrors = append(missingTxErrors, errors.NewStorageError("MISSING transaction %s", txHash, err))
					}
				}
				mu.Unlock()
			}
			// Don't return error - continue processing all transactions
			return nil
		})
	}

	_ = g.Wait() // Ignore error since we're collecting them manually

	// Log summary
	mu.Lock()
	attempted := len(txHashes)
	succeeded := attempted - errorCount
	mu.Unlock()

	s.logger.Infof("[MarkTransactionsOnLongestChain] completed: attempted=%d, succeeded=%d, failed=%d, onLongestChain=%t",
		attempted, succeeded, errorCount, onLongestChain)

	// FATAL if we have missing transactions - this indicates data corruption
	if len(missingTxErrors) > 0 {
		s.logger.Fatalf("CRITICAL: %d missing transactions during MarkTransactionsOnLongestChain - data integrity compromised. First errors: %v",
			len(missingTxErrors), errors.Join(missingTxErrors...))
	}

	// Return aggregated errors (up to 10) for other error types
	if len(allErrors) > 0 {
		if errorCount > 10 {
			s.logger.Errorf("[MarkTransactionsOnLongestChain] only returned first 10 of %d errors", errorCount)
		}
		return errors.Join(allErrors...)
	}

	return nil
}

// setLongestChainBatch marks transactions as on/not on the longest chain in a batch using direct Aerospike expressions
func (s *Store) setLongestChainBatch(batch []*batchLongestChain) {
	var (
		batchWritePolicy   = aerospike.NewBatchWritePolicy()
		batchRecords       = make([]aerospike.BatchRecordIfc, 0, len(batch))
		currentBlockHeight = s.GetBlockHeight()
	)

	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	// Go through each batch item and mark the transaction as on/not on longest chain
	for _, batchItem := range batch {
		// Calculate key for the master record (childIndex = 0)
		keySource := uaerospike.CalculateKeySource(&batchItem.txHash, 0, 1)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			batchItem.errCh <- err
			continue
		}

		if batchItem.onLongestChain {
			// Transaction is on longest chain - unset unminedSince field (set to nil)
			batchRecords = append(batchRecords, aerospike.NewBatchWrite(
				batchWritePolicy,
				key,
				aerospike.PutOp(aerospike.NewBin(fields.UnminedSince.String(), nil)),
			))
		} else {
			// Transaction is not on longest chain - set unminedSince to current block height
			batchRecords = append(batchRecords, aerospike.NewBatchWrite(
				batchWritePolicy,
				key,
				aerospike.PutOp(aerospike.NewBin(fields.UnminedSince.String(), currentBlockHeight)),
			))
		}
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		for _, batchItem := range batch {
			batchItem.errCh <- errors.NewProcessingError("could not batch operate longest chain flag", err)
		}

		return
	}

	// Process results
	for idx, batchRecord := range batchRecords {
		if batchRecord.BatchRec().Err != nil {
			batch[idx].errCh <- errors.NewProcessingError("could not batch write longest chain flag", batchRecord.BatchRec().Err)
		} else {
			batch[idx].errCh <- nil
		}
	}
}
