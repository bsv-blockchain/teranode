package aerospike

import (
	"context"
	"time"

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

func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
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

			// Sleep a percentage of the batch duration before waiting for response to reduce CPU contention.
			// Configurable via batchResponseWaitPercent (default 0 = disabled).
			time.Sleep(time.Duration(s.settings.UtxoStore.LongestChainBatcherDurationMillis) * time.Millisecond * time.Duration(s.batchResponseWaitPercent) / 100)
			return <-errCh
		})
	}

	return g.Wait()
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
