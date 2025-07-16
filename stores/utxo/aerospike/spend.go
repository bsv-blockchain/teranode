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
//   - Automatic cleanup of spent UTXOs through DAH
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
//   - totalUtxos: Total number of UTXOs in the transaction
//   - recordUtxos: Total number of UTXO in this record
//   - spentUtxos: Number of spent UTXOs in this record
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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// Spend operations in the Aerospike UTXO store handle spending UTXOs through
// batched Lua operations with automatic DAH management and error handling.
//
// # Architecture
//
// The spend process uses a multi-layered approach:
//   1. Batch collection of spend requests
//   2. Grouping of spends by transaction
//   3. Atomic Lua scripts for spending
//   4. DAH management for cleanup
//   5. External storage synchronization
//
// # Main Types

// batchSpend represents a single UTXO spend request in a batch
type batchSpend struct {
	spend             *utxo.Spend // UTXO to spend
	errCh             chan error  // Channel for completion notification
	ignoreConflicting bool
	ignoreUnspendable bool
}

// batchIncrement handles record count updates for paginated transactions
type batchIncrement struct {
	txID      *chainhash.Hash               // Transaction hash
	increment int                           // Count adjustment
	res       chan incrementSpentRecordsRes // Result channel
}

type batchDAH struct {
	txID           *chainhash.Hash // Transaction hash
	childIdx       uint32          // Child record index
	deleteAtHeight uint32          // DeleteAtHeight (0 = no delete)
	errCh          chan error      // Error Result channel
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
//   - tx: tx to spend
//
// Error handling:
//   - Rolls back successful spends on partial failure
//   - Handles panic recovery
//   - Reports metrics for failures
//
// Example return value:
//
//	spends := []*utxo.Spend{
//	    {
//	        TxID: txHash,
//	        Vout: 0,
//	        UTXOHash: utxoHash,
//	        SpendingTxID: spendingTxHash,
//	    },
//	}
//
//	doubleSpendConflicts := []*chainhash.Hash{
//	    &spendingTxHash,
//	}
//
//	err := store.Spend(ctx, tx)
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoMapErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in aerospike Spend: %v\n", recoverErr)
		}
	}()

	useIgnoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	useIgnoreUnspendable := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreUnspendable

	spends, err := utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	var (
		mu sync.Mutex
		g  = errgroup.Group{}

		spentSpends     = make([]*utxo.Spend, 0, len(spends))
		errSpent        *errors.UtxoSpentErrData
		txAlreadyExists bool
	)

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		idx := idx
		spend := spend

		g.Go(func() error {
			errCh := make(chan error)
			s.spendBatcher.Put(&batchSpend{
				spend:             spend,
				errCh:             errCh,
				ignoreConflicting: useIgnoreConflicting,
				ignoreUnspendable: useIgnoreUnspendable,
			})

			// this waits for the batch to be sent and the response to be received from the batch operation
			batchErr := <-errCh

			if batchErr != nil && errors.Is(batchErr, errors.ErrTxNotFound) {
				mu.Lock()
				exists := txAlreadyExists
				mu.Unlock()
				// the parent transaction was not found, this can happen when the parent tx has been DAH'd and removed from
				// the utxo store. We can check whether the tx already exists, which means it has been validated and
				// blessed. In this case we can just return early.
				if exists {
					// we've previously validated that this tx already exists, no point doing a lookup again or logging anything
					batchErr = nil
				} else if _, batchErr = s.Get(ctx, tx.TxIDChainHash()); batchErr == nil {
					s.logger.Warnf("[Validate][%s] parent tx not found, but tx already exists in store, assuming already blessed", tx.TxID())

					batchErr = nil

					mu.Lock()
					txAlreadyExists = true
					mu.Unlock()
				}
			}

			if batchErr != nil {
				spends[idx].Err = batchErr

				s.logger.Debugf("[SPEND][%s:%d] error in aerospike spend: %+v", spend.TxID.String(), spend.Vout, spend.Err)

				if errors.AsData(batchErr, &errSpent) {
					spends[idx].ConflictingTxID = errSpent.SpendingData.TxID
				}

				// s.logger.Errorf("error in aerospike spend (batched mode) %s: %v\n", spends[idx].TxID.String(), spends[idx].Err)

				// don't stop processing the rest of the batch, we want to see all errors
				return nil
			}

			mu.Lock()
			spentSpends = append(spentSpends, spend)
			mu.Unlock()

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.NewError("error in aerospike spend (batched mode)", err)
	}

	if len(spends) != len(spentSpends) { // there must have been failures
		// s.logger.Errorf("len(spends) != len(spentSpends): %d != %d", len(spends), len(spentSpends))

		// for i, spend := range spends {
		// 	s.logger.Errorf("spend: %d: %v", i, spend.Err)
		// }
		// revert the successfully spent utxos
		unspendErr := s.Unspend(ctx, spentSpends)
		if unspendErr != nil {
			s.logger.Errorf("error in aerospike unspend (batched mode): %v", unspendErr)
		}

		var firstError error

		for _, spend := range spends {
			if spend.Err != nil {
				firstError = spend.Err
				break
			}
		}

		// return the first error found
		return spends, errors.NewTxInvalidError("error in aerospike spend (batched mode) - first error", firstError)
	}

	prometheusUtxoMapSpend.Add(float64(len(spends)))

	return spends, nil
}

type keyIgnoreUnspendable struct {
	key               *aerospike.Key
	ignoreConflicting bool
	ignoreUnspendable bool
}

// sendSpendBatchLua processes a batch of spend requests via Lua scripts.
// The function:
//  1. Groups spends by transaction
//  2. Creates batch UDF operations
//  3. Executes Lua scripts
//  4. Handles responses and errors
//  5. Manages DAH settings
//  6. Updates external storage
func (s *Store) sendSpendBatchLua(batch []*batchSpend) {
	start := time.Now()

	stat := gocore.NewStat("sendSpendBatchLua")

	ctx, _, deferFn := tracing.Tracer("aerospike").Start(s.ctx, "sendSpendBatchLua",
		tracing.WithParentStat(gocoreStat),
		tracing.WithHistogram(prometheusUtxoSpendBatch),
	)

	defer func() {
		prometheusUtxoSpendBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchID := s.batchID.Add(1)

	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends", batchID, len(batch))

		defer func() {
			s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends DONE", batchID, len(batch))
		}()
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	batchRecordKeys := make([]keyIgnoreUnspendable, 0, len(batch))

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
	batchesByKey := make(map[keyIgnoreUnspendable][]aerospike.MapValue, len(batch))

	sUtxoBatchSizeUint32, err := safeconversion.IntToUint32(s.utxoBatchSize)
	if err != nil {
		s.logger.Errorf("Could not convert utxoBatchSize (%d) to uint32", s.utxoBatchSize)
	}

	for idx, bItem := range batch {
		keySource := uaerospike.CalculateKeySource(bItem.spend.TxID, bItem.spend.Vout/sUtxoBatchSizeUint32)
		keySourceStr := string(keySource)

		if key, ok = aeroKeyMap[keySourceStr]; !ok {
			key, err = aerospike.NewKey(s.namespace, s.setName, keySource)
			if err != nil {
				// we just return the error on the channel, we cannot process this utxo any further
				bItem.errCh <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] failed to init new aerospike key for spend", bItem.spend.TxID.String(), err)
				continue
			}

			aeroKeyMap[keySourceStr] = key
		}

		// we need to check if the spending data is nil, if it is we cannot proceed
		if bItem.spend.SpendingData == nil {
			bItem.errCh <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] spending data is nil", bItem.spend.TxID.String())
			continue
		}

		newMapValue := aerospike.NewMapValue(map[any]any{
			"idx":          idx,
			"offset":       s.calculateOffsetForOutput(bItem.spend.Vout),
			"vOut":         bItem.spend.Vout,
			"utxoHash":     bItem.spend.UTXOHash[:],
			"spendingData": bItem.spend.SpendingData.Bytes(),
		})

		// we need to group the spends by key and ignoreUnspendable flag
		useKey := keyIgnoreUnspendable{
			key:               key,
			ignoreConflicting: bItem.ignoreConflicting,
			ignoreUnspendable: bItem.ignoreUnspendable,
		}

		if _, ok = batchesByKey[useKey]; !ok {
			batchesByKey[useKey] = []aerospike.MapValue{newMapValue}
		} else {
			batchesByKey[useKey] = append(batchesByKey[useKey], newMapValue)
		}
	}

	// TODO #1035 group all spends to the same record (tx) to the same call in LUA and change the LUA script to handle multiple spends
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	for batchKey, batchItems := range batchesByKey {
		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, batchKey.key, LuaPackage, "spendMulti",
			aerospike.NewValue(batchItems),
			aerospike.NewValue(batchKey.ignoreConflicting),
			aerospike.NewValue(batchKey.ignoreUnspendable),
			aerospike.NewValue(thisBlockHeight),
			aerospike.NewValue(s.settings.GetUtxoStoreBlockHeightRetention()),
		))

		batchRecordKeys = append(batchRecordKeys, batchKey)
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		for idx, bItem := range batch {
			bItem.errCh <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] failed to batch spend aerospike map utxo in batchId %d: %d - %w", bItem.spend.TxID.String(), batchID, idx, err)
		}

		return
	}

	var errs error

	start = stat.NewStat("BatchOperate").AddTime(start)

	// batchOperate may have no errors, but some of the records may have failed
	for batchIdx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err

		batchByKey, ok := batchesByKey[batchRecordKeys[batchIdx]]
		if !ok {
			s.logger.Errorf("[SPEND_BATCH_LUA] could not find batch key for batchIdx %d", batchIdx)
			continue
		}

		txID := batch[batchByKey[0]["idx"].(int)].spend.TxID // all the same ...

		if err != nil {
			// error occurred, we need to send the error to the done channel for each spend in this batch
			for _, batchItem := range batchByKey {
				idx := batchItem["idx"].(int)
				batch[idx].errCh <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, blockHeight %d: %d", batch[idx].spend.TxID.String(), thisBlockHeight, batchID, err)
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
							batch[idx].errCh <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String(), err)

							continue
						}
					}

					switch res.ReturnValue {
					case LuaOk:
						switch res.Signal {
						case LuaAllSpent:
							if err := errors.Join(errs, s.handleExtraRecords(ctx, txID, 1)); err != nil {
								errs = errors.Join(errs, err)
							}

						case LuaDAHSet:
							if err := s.SetDAHForChildRecords(txID, res.ChildCount, thisBlockHeight+s.settings.GetUtxoStoreBlockHeightRetention()); err != nil {
								errs = errors.Join(errs, err)
							}

							if err := s.setDAHExternalTransaction(ctx, txID, thisBlockHeight+s.settings.GetUtxoStoreBlockHeightRetention()); err != nil {
								errs = errors.Join(errs, err)
							}

						case LuaDAHUnset:
							if err := s.SetDAHForChildRecords(txID, res.ChildCount, aerospike.TTLDontExpire); err != nil {
								errs = errors.Join(errs, err)
							}

							if err := s.setDAHExternalTransaction(ctx, txID, 0); err != nil {
								errs = errors.Join(errs, err)
							}
						}

						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].errCh <- nil
						}

					case LuaFrozen:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].errCh <- errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] transaction is frozen, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}

					case LuaConflicting:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].errCh <- errors.NewTxConflictingError("[SPEND_BATCH_LUA][%s] transaction is conflicting, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}

					case LuaUnspendable:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].errCh <- errors.NewTxUnspendableError("[SPEND_BATCH_LUA][%s] transaction is unspendable, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}

					case LuaCoinbaseImmature:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].errCh <- errors.NewTxCoinbaseImmatureError("[SPEND_BATCH_LUA][%s] coinbase is locked, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}

					case LuaSpent:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)

							if res.SpendingData == nil {
								batch[idx].errCh <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] missing spending data in response", txID.String())
								continue
							}

							batch[idx].errCh <- errors.NewUtxoSpentError(*batch[idx].spend.TxID, batch[idx].spend.Vout, *batch[idx].spend.UTXOHash, res.SpendingData)
						}
					case LuaError:
						if res.Signal == LuaTxNotFound {
							for _, batchItem := range batchByKey {
								idx := batchItem["idx"].(int)
								batch[idx].errCh <- errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] transaction not found, blockHeight %d: %d - %v", txID.String(), thisBlockHeight, batchID, res)
							}
						} else {
							for _, batchItem := range batchByKey {
								idx := batchItem["idx"].(int)
								batch[idx].errCh <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %v", txID.String(), thisBlockHeight, batchID, res)
							}
						}
					default:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].errCh <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %v", txID.String(), thisBlockHeight, batchID, res)
						}
					}
				}
			} else {
				for _, batchItem := range batchByKey {
					idx := batchItem["idx"].(int)
					batch[idx].errCh <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String())
				}
			}
		}
	}

	stat.NewStat("postBatchOperate").AddTime(start)
}

// SetDAHForChildRecords sets DAH for all child records of a transaction
func (s *Store) SetDAHForChildRecords(txID *chainhash.Hash, childCount int, dah uint32) error {
	errs := make([]error, childCount)

	for i := uint32(0); i < uint32(childCount); i++ { // nolint: gosec
		errCh := make(chan error)

		go func() {
			s.setDAHBatcher.Put(&batchDAH{
				txID:           txID,
				childIdx:       i + 1, // We want to set DAH for child record i+1
				deleteAtHeight: dah,
				errCh:          errCh,
			})
		}()

		errs[i] = <-errCh
		if errs[i] != nil {
			s.logger.Errorf("[setDAHForChildRecords][%s] failed to set DAH for child record %d: %v", txID.String(), i, errs[i])
		}
	}

	var errorsFound bool

	for _, err := range errs {
		if err != nil {
			errorsFound = true
			break
		}
	}

	if errorsFound {
		return errors.NewStorageError("[setDAHForChildRecords][%s] failed to set DAH for one or more child records", txID.String())
	}

	return nil
}

// handleExtraRecords manages the record count for paginated transactions when UTXOs are spent.
// This function is called when spending operations affect transactions with multiple records
// to maintain accurate pagination counts for cleanup operations.
//
// Parameters:
//   - ctx: Context for cancellation
//   - txID: Transaction ID whose record count needs updating
//   - increment: Amount to increment (can be negative for decrement)
//
// Returns:
//   - error: Any error encountered during the record count update
func (s *Store) handleExtraRecords(ctx context.Context, txID *chainhash.Hash, increment int) error {
	res, err := s.IncrementSpentRecords(txID, increment) // This is a batch operation
	if err != nil {
		return err
	}

	if r, ok := res.(string); ok {
		ret, err := s.ParseLuaReturnValue(r) // always returns len 3
		if err != nil {
			s.logger.Errorf("[SPEND_BATCH_LUA][%s] failed to parse LUA return value: %v", txID.String(), err)
		} else if ret.ReturnValue == LuaOk {
			switch ret.Signal {
			case LuaDAHSet:
				thisBlockHeight := s.blockHeight.Load()
				dah := thisBlockHeight + s.settings.GetUtxoStoreBlockHeightRetention()

				if err := s.SetDAHForChildRecords(txID, ret.ChildCount, dah); err != nil {
					return err
				}

				if err := s.setDAHExternalTransaction(ctx, txID, dah); err != nil {
					return err
				}

			case LuaDAHUnset:
				if err := s.SetDAHForChildRecords(txID, ret.ChildCount, 0); err != nil {
					return err
				}

				if err := s.setDAHExternalTransaction(ctx, txID, 0); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// setDAHExternalTransaction sets the Delete-At-Height (DAH) for a transaction stored in external storage.
// This is used to schedule cleanup of large transactions that are stored in blob storage
// rather than directly in Aerospike records.
//
// Parameters:
//   - ctx: Context for cancellation
//   - txid: Transaction ID to set DAH for
//   - newDAH: Block height at which the transaction should be deleted
//
// Returns:
//   - error: Any error encountered, or nil if successful or transaction not found
func (s *Store) setDAHExternalTransaction(ctx context.Context, txid *chainhash.Hash, newDAH uint32) error {
	if err := s.externalStore.SetDAH(ctx,
		txid[:],
		fileformat.FileTypeTx,
		newDAH,
	); err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			// did not find the tx, try the outputs
			if err := s.externalStore.SetDAH(ctx,
				txid[:],
				fileformat.FileTypeOutputs,
				newDAH,
			); err != nil {
				return errors.NewStorageError("[ttlExternalTransaction][%s] failed to %s DAH for external transaction outputs",
					txid,
					dahOperation(newDAH),
					err)
			}
		} else {
			return errors.NewStorageError("[ttlExternalTransaction][%s] failed to %s DAH for external transaction",
				txid,
				dahOperation(newDAH),
				err)
		}
	}

	return nil
}

// dahOperation returns a human-readable string describing the DAH operation.
// This is used for logging and error messages to indicate whether DAH is being
// set to a specific block height or unset (cleared).
//
// Parameters:
//   - dah: Delete-At-Height value (0 means unset, >0 means set to that height)
//
// Returns:
//   - string: "set at <height>" if dah > 0, "unset" if dah == 0
func dahOperation(dah uint32) string {
	if dah > 0 {
		return fmt.Sprintf("set at %d", dah)
	}

	return "unset"
}

type incrementSpentRecordsRes struct {
	res interface{}
	err error
}

// IncrementSpentRecords updates the record count for paginated transactions.
// Used for cleanup management of large transactions.
func (s *Store) IncrementSpentRecords(txid *chainhash.Hash, increment int) (interface{}, error) {
	res := make(chan incrementSpentRecordsRes)

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

	currentBlockHeight := s.blockHeight.Load()

	// Create a batch of records to read from the txHashes
	for _, item := range batch {
		aeroKey, err := aerospike.NewKey(s.namespace, s.setName, item.txID[:])
		if err != nil {
			item.res <- incrementSpentRecordsRes{
				res: nil,
				err: errors.NewProcessingError("failed to init new aerospike key for txMeta", err),
			}

			continue
		}

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, LuaPackage, "incrementSpentExtraRecs",
			aerospike.NewIntegerValue(item.increment),
			aerospike.NewIntegerValue(int(currentBlockHeight)),
			aerospike.NewValue(s.settings.GetUtxoStoreBlockHeightRetention()),
		))
	}

	// send the batch to aerospike
	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		for _, item := range batch {
			item.res <- incrementSpentRecordsRes{
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
			batch[idx].res <- incrementSpentRecordsRes{
				res: nil,
				err: errors.NewStorageError("error in aerospike send outpoint batch records", err),
			}

			continue
		}

		res, ok := batchRecord.Record.Bins["SUCCESS"].(string)
		if !ok {
			batch[idx].res <- incrementSpentRecordsRes{
				res: nil,
				err: errors.NewProcessingError("failed to parse response"),
			}

			continue
		}

		if strings.HasPrefix(res, "ERROR:") {
			batch[idx].res <- incrementSpentRecordsRes{
				res: nil,
				err: errors.NewProcessingError(res),
			}

			continue
		}

		batch[idx].res <- incrementSpentRecordsRes{
			res: res,
			err: nil,
		}
	}
}

func (s *Store) sendSetDAHBatch(batch []*batchDAH) {
	var err error

	// Create batch records with individual TTLs
	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	for i, b := range batch {
		keySource := uaerospike.CalculateKeySource(b.txID, b.childIdx)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			s.logger.Errorf("[SetDAHBatch][%s] failed to create key for pagination record %d: %v", b.txID.String(), b.childIdx, err)
			continue
		}

		batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)

		if b.deleteAtHeight > 0 {
			batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), b.deleteAtHeight)))
		} else {
			batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), nil)))
		}
	}

	// Execute batch operation
	err = s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords)
	if err != nil {
		for _, bItem := range batch {
			bItem.errCh <- errors.NewStorageError("[SetDAHBatch][%s] failed to set DAH", err)
		}

		return
	}

	// batchOperate may have no errors, but some of the records may have failed
	for batchIdx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err

		if err != nil {
			batch[batchIdx].errCh <- err
			continue
		}

		batch[batchIdx].errCh <- nil
	}
}
