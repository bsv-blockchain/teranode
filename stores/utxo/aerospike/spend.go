// //go:build aerospike

// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through TTL
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - nrUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"context"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// Spend operations in the Aerospike UTXO store handle spending UTXOs through
// batched Lua operations with automatic TTL management and error handling.
//
// # Architecture
//
// The spend process uses a multi-layered approach:
//   1. Batch collection of spend requests
//   2. Grouping of spends by transaction
//   3. Atomic Lua scripts for spending
//   4. TTL management for cleanup
//   5. External storage synchronization
//
// # Main Types

// batchSpend represents a single UTXO spend request in a batch
type batchSpend struct {
	spend *utxo.Spend // UTXO to spend
	done  chan error  // Channel for completion notification
}

// batchIncrement handles record count updates for paginated transactions
type batchIncrement struct {
	txID      *chainhash.Hash            // Transaction hash
	increment int                        // Count adjustment
	res       chan incrementNrRecordsRes // Result channel
}

// Spend marks UTXOs as spent in a batch operation.
// The function:
//  1. Validates inputs
//  2. Batches spend requests
//  3. Handles responses
//  4. Manages rollback on failure
//
// Parameters:
//   - ctx: Context for cancellation
//   - spends: Array of UTXOs to spend
//   - blockHeight: Current block height (unused, derived from store)
//
// Error handling:
//   - Rolls back successful spends on partial failure
//   - Handles panic recovery
//   - Reports metrics for failures
//
// Example:
//
//	spends := []*utxo.Spend{
//	    {
//	        TxID: txHash,
//	        Vout: 0,
//	        UTXOHash: utxoHash,
//	        SpendingTxID: spendingTxHash,
//	    },
//	}
//	err := store.Spend(ctx, spends, blockHeight)
func (s *Store) Spend(ctx context.Context, spends []*utxo.Spend, _ uint32) (err error) {
	return s.spend(ctx, spends)
}

func (s *Store) spend(ctx context.Context, spends []*utxo.Spend) (err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoMapErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in aerospike Spend: %v\n", recoverErr)
		}
	}()

	spentSpends := make([]*utxo.Spend, 0, len(spends))

	var mu sync.Mutex

	g := errgroup.Group{}

	for _, spend := range spends {
		if spend == nil {
			return errors.NewProcessingError("spend should not be nil")
		}

		spend := spend

		g.Go(func() error {
			done := make(chan error)
			s.spendBatcher.Put(&batchSpend{
				spend: spend,
				done:  done,
			})

			// this waits for the batch to be sent and the response to be received from the batch operation
			batchErr := <-done
			if batchErr != nil {
				// just return the raw error, should already be wrapped
				return batchErr
			}

			mu.Lock()
			spentSpends = append(spentSpends, spend)
			mu.Unlock()

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		s.logger.Errorf("error in aerospike spend (batched mode): %v", err)

		// revert the successfully spent utxos
		unspendErr := s.UnSpend(ctx, spentSpends)
		if unspendErr != nil {
			err = errors.Join(err, unspendErr)
		}

		return errors.NewError("error in aerospike spend (batched mode)", err)
	}

	prometheusUtxoMapSpend.Add(float64(len(spends)))

	return nil
}

// sendSpendBatchLua processes a batch of spend requests via Lua scripts.
// The function:
//  1. Groups spends by transaction
//  2. Creates batch UDF operations
//  3. Executes Lua scripts
//  4. Handles responses and errors
//  5. Manages TTL settings
//  6. Updates external storage
func (s *Store) sendSpendBatchLua(batch []*batchSpend) {
	start := time.Now()
	ctx, stat, deferFn := tracing.StartTracing(s.ctx, "sendSpendBatchLua",
		tracing.WithParentStat(gocoreStat),
		tracing.WithHistogram(prometheusUtxoSpendBatch),
	)

	defer func() {
		prometheusUtxoSpendBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchID := s.batchID.Add(1)

	if gocore.Config().GetBool("utxostore_verbose_debug") {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends", batchID, len(batch))

		defer func() {
			s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends DONE", batchID, len(batch))
		}()
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))

	// s.blockHeight is the last mined block, but for the LUA script we are telling it to
	// evaluate this spend in this block height (i.e. 1 greater)
	thisBlockHeight := s.blockHeight.Load() + 1

	var (
		key *aerospike.Key
		err error
		ok  bool
	)

	// create batches of spends, each batch is a record in aerospike
	// we batch the spends to the same record (tx) to the same call in LUA
	// this is to avoid the LUA script being called multiple times for the same transaction
	// we calculate the key source based on the txid and the vout divided by the utxoBatchSize
	aeroKeyMap := make(map[string]*aerospike.Key)
	batchesByKey := make(map[*aerospike.Key][]aerospike.MapValue, len(batch))

	for idx, bItem := range batch {
		keySource := uaerospike.CalculateKeySource(bItem.spend.TxID, bItem.spend.Vout/uint32(s.utxoBatchSize)) //nolint:gosec
		keySourceStr := string(keySource)

		if key, ok = aeroKeyMap[keySourceStr]; !ok {
			key, err = aerospike.NewKey(s.namespace, s.setName, keySource)
			if err != nil {
				// we just return the error on the channel, we cannot process this utxo any further
				bItem.done <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] failed to init new aerospike key for spend", bItem.spend.TxID.String(), err)
				continue
			}

			aeroKeyMap[keySourceStr] = key
		}

		// we need to check if the spending tx id is nil, if it is we cannot proceed
		if bItem.spend.SpendingTxID == nil {
			bItem.done <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] spending tx id is nil", bItem.spend.TxID.String())
			continue
		}

		newMapValue := aerospike.NewMapValue(map[interface{}]interface{}{
			"idx":          idx,
			"offset":       s.calculateOffsetForOutput(bItem.spend.Vout),
			"vOut":         bItem.spend.Vout,
			"utxoHash":     bItem.spend.UTXOHash[:],
			"spendingTxID": bItem.spend.SpendingTxID[:],
		})

		if _, ok = batchesByKey[key]; !ok {
			batchesByKey[key] = []aerospike.MapValue{newMapValue}
		} else {
			batchesByKey[key] = append(batchesByKey[key], newMapValue)
		}
	}

	// TODO #1035 group all spends to the same record (tx) to the same call in LUA and change the LUA script to handle multiple spends
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()
	for aeroKey, batchItems := range batchesByKey {
		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, LuaPackage, "spendMulti",
			aerospike.NewValue(batchItems),
			aerospike.NewValue(thisBlockHeight),
			aerospike.NewValue(s.expiration), // ttl
		))
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[SPEND_BATCH_LUA][%d] failed to batch spend aerospike map utxos in batchId %d: %v", batchID, len(batch), err)

		for idx, bItem := range batch {
			bItem.done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] failed to batch spend aerospike map utxo in batchId %d: %d - %w", bItem.spend.TxID.String(), batchID, idx, err)
		} // TODO should we return here?
	}

	start = stat.NewStat("BatchOperate").AddTime(start)

	// batchOperate may have no errors, but some of the records may have failed
	for _, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		aeroKey := batchRecord.BatchRec().Key // will this be the same memory address as the key in the loop above?

		batchByKey := batchesByKey[aeroKey]
		txID := batch[batchByKey[0]["idx"].(int)].spend.TxID // all the same ...

		if err != nil {
			// error occurred, we need to send the error to the done channel for each spend in this batch
			for _, batchItem := range batchByKey {
				idx := batchItem["idx"].(int)
				batch[idx].done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, blockHeight %d: %d - %w", batch[idx].spend.TxID.String(), thisBlockHeight, batchID, err)
			}
		} else {
			response := batchRecord.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
				responseMsg, ok := response.Bins["SUCCESS"].(string)
				if ok {
					res, err := s.ParseLuaReturnValue(responseMsg)
					if err != nil {
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String(), err)
						}
					}

					switch res.ReturnValue {
					case LuaOk:
						if res.Signal == LuaAllSpent {
							// all utxos in this record are spent so we decrement the nrRecords in the master record
							// we do this in a separate go routine to avoid blocking the batcher
							go s.handleAllSpent(ctx, txID)
						} else if res.Signal == LuaTTLSet {
							// record has been set to expire, we need to set the TTL on the external transaction file
							if res.External {
								// add ttl to the externally stored transaction, if applicable
								go s.setTTLExternalTransaction(ctx, txID, time.Duration(s.expiration)*time.Second)
							}
						}

						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- nil
						}

					case LuaFrozen:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] transaction is frozen, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}

					case LuaConflicting:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- errors.NewTxConflictingError("[SPEND_BATCH_LUA][%s] transaction is conflicting, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}

					case LuaSpent:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- errors.NewUtxoSpentError(*batch[idx].spend.TxID, batch[idx].spend.Vout, *batch[idx].spend.UTXOHash, *res.SpendingTxID)
						}
					case LuaError:
						if res.Signal == LuaTxNotFound {
							for _, batchItem := range batchByKey {
								idx := batchItem["idx"].(int)
								batch[idx].done <- errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] transaction not found, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
							}
						} else {
							for _, batchItem := range batchByKey {
								idx := batchItem["idx"].(int)
								batch[idx].done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, res.Signal)
							}
						}
					default:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}
					}
				}
			} else {
				for _, batchItem := range batchByKey {
					idx := batchItem["idx"].(int)
					batch[idx].done <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String())
				}
			}
		}
	}

	stat.NewStat("postBatchOperate").AddTime(start)
}

// handleAllSpent manages cleanup when all UTXOs in a transaction are spent:
//  1. Decrements record count for pagination
//  2. Sets TTL for cleanup
//  3. Updates external storage TTL
func (s *Store) handleAllSpent(ctx context.Context, txID *chainhash.Hash) {
	res, err := s.IncrementNrRecords(txID, -1)
	if err != nil {
		// TODO if this goes wrong, we never decrement the nrRecords and the record will never be deleted
		s.logger.Errorf("[SPEND_BATCH_LUA][%s] failed to decrement nrRecords: %v", txID.String(), err)
	}

	if r, ok := res.(string); ok {
		ret, err := s.ParseLuaReturnValue(r) // always returns len 3
		if err != nil {
			s.logger.Errorf("[SPEND_BATCH_LUA][%s] failed to parse LUA return value: %v", txID.String(), err)
		} else if ret.ReturnValue == LuaOk {
			if ret.Signal == LuaTTLSet {
				// TODO - we should TTL all the pagination records for this TX
				_ = ret.Signal

				if ret.External {
					// add ttl to the externally stored transaction, if applicable
					s.setTTLExternalTransaction(ctx, txID, time.Duration(s.expiration)*time.Second)
				}
			}
		}
	}
}

type incrementNrRecordsRes struct {
	res interface{}
	err error
}

// IncrementNrRecords updates the record count for paginated transactions.
// Used for cleanup management of large transactions.
func (s *Store) IncrementNrRecords(txid *chainhash.Hash, increment int) (interface{}, error) {
	res := make(chan incrementNrRecordsRes)

	go func() {
		s.incrementBatcher.Put(&batchIncrement{
			txID:      txid,
			increment: increment,
			res:       res,
		})
	}()

	response := <-res

	return response.res, response.err
}

func (s *Store) sendIncrementBatch(batch []*batchIncrement) {
	var err error

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	// Create a batch of records to read, with a max size of the batch
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))

	// Create a batch of records to read from the txHashes
	for _, item := range batch {
		aeroKey, err := aerospike.NewKey(s.namespace, s.setName, item.txID[:])
		if err != nil {
			item.res <- incrementNrRecordsRes{
				res: nil,
				err: errors.NewProcessingError("failed to init new aerospike key for txMeta", err),
			}

			continue
		}

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, LuaPackage, "incrementNrRecords",
			aerospike.NewIntegerValue(item.increment),
			aerospike.NewValue(s.expiration), // ttl
		))
	}

	// send the batch to aerospike
	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		for _, item := range batch {
			item.res <- incrementNrRecordsRes{
				res: nil,
				err: errors.NewStorageError("error in aerospike send outpoint batch records", err),
			}
		}

		return
	}

	// Process the batch records
	for idx, batchRecordIfc := range batchRecords {
		batchRecord := batchRecordIfc.BatchRec()
		if batchRecord.Err != nil {
			batch[idx].res <- incrementNrRecordsRes{
				res: nil,
				err: errors.NewStorageError("error in aerospike send outpoint batch records", err),
			}

			continue
		}

		res, ok := batchRecord.Record.Bins["SUCCESS"].(string)
		if !ok {
			batch[idx].res <- incrementNrRecordsRes{
				res: nil,
				err: errors.NewProcessingError("failed to parse response"),
			}

			continue
		}

		batch[idx].res <- incrementNrRecordsRes{
			res: res,
			err: nil,
		}
	}
}

func (s *Store) setTTLExternalTransaction(ctx context.Context, txid *chainhash.Hash, newTTL time.Duration) {
	if err := s.externalStore.SetTTL(ctx,
		txid[:],
		newTTL,
		options.WithFileExtension("tx"),
	); err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			// did not find the tx, try the outputs
			if err = s.externalStore.SetTTL(ctx,
				txid[:],
				newTTL,
				options.WithFileExtension("outputs"),
			); err != nil {
				s.logger.Errorf("[ttlExternalTransaction][%s] failed to set TTL for external transaction outputs: %v", txid, err)
			}
		} else {
			s.logger.Errorf("[ttlExternalTransaction][%s] failed to set TTL for external transaction: %v", txid, err)
		}
	}
}
