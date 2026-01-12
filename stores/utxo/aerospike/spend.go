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
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
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
	blockHeight       uint32      // Current block height
	errCh             chan error  // Channel for completion notification
	ignoreConflicting bool
	ignoreLocked      bool
}

// IncrementSpentRecordsMulti performs a single BatchOperate to increment spent-extra-records for many txids.
// This avoids enqueueing each increment through the batcher and waiting per-item.
func (s *Store) IncrementSpentRecordsMulti(txids []*chainhash.Hash, increment int) error {
	if len(txids) == 0 {
		return nil
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	currentBlockHeight := s.blockHeight.Load()

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(txids))

	for _, txid := range txids {
		key, err := aerospike.NewKey(s.namespace, s.setName, txid[:])
		if err != nil {
			return errors.NewProcessingError("failed to init new aerospike key for txMeta", err)
		}

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, key, LuaPackage, "incrementSpentExtraRecs",
			aerospike.NewIntegerValue(increment),
			aerospike.NewIntegerValue(int(currentBlockHeight)),
			aerospike.NewValue(s.settings.GetUtxoStoreBlockHeightRetention()),
		))
	}

	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[IncrementSpentRecordsMulti] error in aerospike batch", err)
	}

	// Inspect per-record errors
	var aggErr error
	for i := range batchRecords {
		if recErr := batchRecords[i].BatchRec().Err; recErr != nil {
			if aggErr == nil {
				aggErr = recErr
			} else {
				aggErr = errors.Join(aggErr, recErr)
			}
		}

		response := batchRecords[i].BatchRec().Record
		if response != nil && response.Bins != nil {
			successMap := response.Bins[LuaSuccess.String()].(map[interface{}]interface{})
			status, ok := successMap["status"].(string)
			if !ok || status != "OK" {
				aggErr = errors.Join(aggErr, errors.NewProcessingError(successMap["message"].(string)))
			}
		}
	}

	return aggErr
}

// SetDAHForChildRecordsMulti expands childCount per tx and performs a single BatchOperate
// to set/unset DeleteAtHeight across all child pagination records.
func (s *Store) SetDAHForChildRecordsMulti(items []struct {
	TxID           *chainhash.Hash
	ChildCount     int
	DeleteAtHeight uint32
}) error {
	// Expand into individual child records
	total := 0
	for _, it := range items {
		if it.ChildCount > 0 {
			total += it.ChildCount
		}
	}
	if total == 0 {
		return nil
	}

	batchRecords := make([]aerospike.BatchRecordIfc, 0, total)

	for _, it := range items {
		for i := uint32(1); i <= uint32(it.ChildCount); i++ { // nolint: gosec
			keySource := uaerospike.CalculateKeySourceInternal(it.TxID, i) // children start at 1
			key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
			if err != nil {
				return errors.NewProcessingError("[SetDAHForChildRecordsMulti][%s] failed to create key for pagination record %d: %v", it.TxID.String(), i, err)
			}

			batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
			if it.DeleteAtHeight > 0 {
				batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), it.DeleteAtHeight))))
			} else {
				batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), nil))))
			}
		}
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return errors.NewStorageError("[SetDAHForChildRecordsMulti] failed to set DAH", err)
	}

	var aggErr error
	for _, br := range batchRecords {
		if recErr := br.BatchRec().Err; recErr != nil {
			if aggErr == nil {
				aggErr = recErr
			} else {
				aggErr = errors.Join(aggErr, recErr)
			}
		}
	}

	return aggErr
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
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoMapErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in aerospike Spend: %v\n", recoverErr)
		}
	}()

	if blockHeight == 0 {
		return nil, errors.NewProcessingError("blockHeight must be greater than zero")
	}

	useIgnoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	useIgnoreLocked := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreLocked

	spends, err := utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	var (
		mu sync.Mutex
		g  = errgroup.Group{}

		spentSpends     = make([]*utxo.Spend, 0, len(spends))
		txAlreadyExists bool
	)

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		idx := idx
		spend := spend

		g.Go(func() error {
			// Fast-fail check: if circuit breaker is already open, reject immediately
			if s.spendCircuitBreaker != nil && !s.spendCircuitBreaker.Allow() {
				spends[idx].Err = errors.NewServiceUnavailableError("[SPEND] circuit breaker open, rejecting request")
				return nil
			}

			errCh := make(chan error, 1)
			s.spendBatcher.Put(&batchSpend{
				spend:             spend,
				blockHeight:       blockHeight,
				errCh:             errCh,
				ignoreConflicting: useIgnoreConflicting,
				ignoreLocked:      useIgnoreLocked,
			})

			// Wait for batch response with timeout to prevent indefinite blocking
			var batchErr error
			spendTimeout := s.settings.UtxoStore.SpendWaitTimeout
			if spendTimeout <= 0 {
				spendTimeout = 30 * time.Second
			}

			timer := time.NewTimer(spendTimeout)
			defer timer.Stop()

			select {
			case batchErr = <-errCh:
				// Batch completed successfully or with error
			case <-ctx.Done():
				spends[idx].Err = errors.NewContextCanceledError("[SPEND][%s:%d] context canceled while waiting for batch response", spend.TxID.String(), spend.Vout)
				return nil
			case <-timer.C:
				if prometheusUtxoMapErrors != nil {
					prometheusUtxoMapErrors.WithLabelValues("Spend", "BatchTimeout").Inc()
				}
				spends[idx].Err = errors.NewServiceUnavailableError("[SPEND][%s:%d] batch operation timed out after %s", spend.TxID.String(), spend.Vout, spendTimeout)
				return nil
			}

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

				var errSpent *errors.UtxoSpentErrData
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
		unspendErr := s.Unspend(ctx, spentSpends)
		if unspendErr != nil {
			s.logger.Errorf("error in aerospike unspend (batched mode): %v", unspendErr)
		}

		var spendErrors error

		for _, spend := range spends {
			if spend.Err != nil {
				if spendErrors != nil {
					spendErrors = errors.Join(spendErrors, spend.Err)
				} else {
					spendErrors = spend.Err
				}
			}
		}

		// return the errors found
		return spends, errors.NewUtxoError("error in aerospike spend (batched mode) - errors", spendErrors)
	}

	prometheusUtxoMapSpend.Add(float64(len(spends)))

	return spends, nil
}

type keyIgnoreLocked struct {
	key               *aerospike.Key
	blockHeight       uint32
	ignoreConflicting bool
	ignoreLocked      bool
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
		tracing.WithParentStat(stat),
		tracing.WithHistogram(prometheusUtxoSpendBatch),
	)

	defer func() {
		prometheusUtxoSpendBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchID := s.batchID.Add(1)
	s.logSpendBatchStart(batchID, len(batch))

	// Prepare and execute batch
	batchesByKey, err := s.prepareSpendBatches(batch, batchID)
	if err != nil {
		return
	}

	batchRecords, batchRecordKeys := s.createBatchRecords(batchesByKey)

	if err := s.executeSpendBatch(batchRecords, batch, batchID); err != nil {
		return
	}

	// Process results
	s.processSpendBatchResults(ctx, batchRecords, batchRecordKeys, batchesByKey, batch, batchID)
	stat.NewStat("postBatchOperate").AddTime(start)
}

// logSpendBatchStart logs the start of a spend batch if verbose debug is enabled
func (s *Store) logSpendBatchStart(batchID uint64, batchSize int) {
	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends", batchID, batchSize)
	}
}

// prepareSpendBatches groups spends by key and validates them
func (s *Store) prepareSpendBatches(batch []*batchSpend, batchID uint64) (map[keyIgnoreLocked][]aerospike.MapValue, error) {
	aeroKeyMap := make(map[string]*aerospike.Key)
	batchesByKey := make(map[keyIgnoreLocked][]aerospike.MapValue, len(batch))

	for idx, bItem := range batch {
		key, err := s.getOrCreateAerospikeKey(bItem, s.utxoBatchSize, aeroKeyMap)
		if err != nil {
			bItem.errCh <- err
			continue
		}

		if err := s.validateSpendItem(bItem); err != nil {
			bItem.errCh <- err
			continue
		}

		mapValue := s.createSpendMapValue(idx, bItem)
		useKey := keyIgnoreLocked{
			key:               key,
			blockHeight:       bItem.blockHeight,
			ignoreConflicting: bItem.ignoreConflicting,
			ignoreLocked:      bItem.ignoreLocked,
		}

		batchesByKey[useKey] = append(batchesByKey[useKey], mapValue)
	}

	return batchesByKey, nil
}

// getOrCreateAerospikeKey gets or creates an Aerospike key for the spend
func (s *Store) getOrCreateAerospikeKey(bItem *batchSpend, utxoBatchSize int, keyMap map[string]*aerospike.Key) (*aerospike.Key, error) {
	keySource := uaerospike.CalculateKeySource(bItem.spend.TxID, bItem.spend.Vout, utxoBatchSize)
	keySourceStr := string(keySource)

	if key, ok := keyMap[keySourceStr]; ok {
		return key, nil
	}

	key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
	if err != nil {
		return nil, errors.NewProcessingError("[SPEND_BATCH_LUA][%s] failed to init new aerospike key for spend", bItem.spend.TxID.String(), err)
	}

	keyMap[keySourceStr] = key
	return key, nil
}

// validateSpendItem validates that the spend item has all required data
func (s *Store) validateSpendItem(bItem *batchSpend) error {
	if bItem.spend.SpendingData == nil {
		return errors.NewProcessingError("[SPEND_BATCH_LUA][%s] spending data is nil", bItem.spend.TxID.String())
	}
	return nil
}

// createSpendMapValue creates the map value for a spend item
func (s *Store) createSpendMapValue(idx int, bItem *batchSpend) aerospike.MapValue {
	return aerospike.NewMapValue(map[any]any{
		"idx":          idx,
		"offset":       s.calculateOffsetForOutput(bItem.spend.Vout),
		"vOut":         bItem.spend.Vout,
		"utxoHash":     bItem.spend.UTXOHash[:],
		"spendingData": bItem.spend.SpendingData.Bytes(),
	})
}

// createBatchRecords creates the batch records for Aerospike operations
func (s *Store) createBatchRecords(batchesByKey map[keyIgnoreLocked][]aerospike.MapValue) ([]aerospike.BatchRecordIfc, []keyIgnoreLocked) {
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batchesByKey))
	batchRecordKeys := make([]keyIgnoreLocked, 0, len(batchesByKey))
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	for batchKey, batchItems := range batchesByKey {
		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, batchKey.key, LuaPackage, "spendMulti",
			aerospike.NewValue(batchItems),
			aerospike.NewValue(batchKey.ignoreConflicting),
			aerospike.NewValue(batchKey.ignoreLocked),
			aerospike.NewValue(batchKey.blockHeight),
			aerospike.NewValue(s.settings.GetUtxoStoreBlockHeightRetention()),
		))
		batchRecordKeys = append(batchRecordKeys, batchKey)
	}

	return batchRecords, batchRecordKeys
}

// executeSpendBatch executes the batch operation
func (s *Store) executeSpendBatch(batchRecords []aerospike.BatchRecordIfc, batch []*batchSpend, batchID uint64) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		for idx, bItem := range batch {
			bItem.errCh <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] failed to batch spend aerospike map utxo in batchId %d: %d - %w", bItem.spend.TxID.String(), batchID, idx, err)
		}
		return err
	}
	return nil
}

// processSpendBatchResults processes the results of the batch operation
func (s *Store) processSpendBatchResults(ctx context.Context, batchRecords []aerospike.BatchRecordIfc, batchRecordKeys []keyIgnoreLocked, batchesByKey map[keyIgnoreLocked][]aerospike.MapValue, batch []*batchSpend, batchID uint64) {
	for batchIdx, batchRecord := range batchRecords {
		key := batchRecordKeys[batchIdx]
		batchByKey, ok := batchesByKey[key]
		if !ok {
			s.logger.Errorf("[SPEND_BATCH_LUA] could not find batch key for batchIdx %d", batchIdx)
			continue
		}

		txID := batch[batchByKey[0]["idx"].(int)].spend.TxID
		s.processSingleBatchResult(ctx, batchRecord, batchByKey, batch, txID, key.blockHeight, batchID)
	}

	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends DONE", batchID, len(batch))
	}
}

// processSingleBatchResult processes a single batch record result
func (s *Store) processSingleBatchResult(ctx context.Context, batchRecord aerospike.BatchRecordIfc, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64) {
	batchErr := batchRecord.BatchRec().Err
	if batchErr != nil {
		s.handleBatchError(batchByKey, batch, thisBlockHeight, batchID, batchErr)
		return
	}

	response := batchRecord.BatchRec().Record
	if response == nil || response.Bins == nil || response.Bins[LuaSuccess.String()] == nil {
		s.handleMissingResponse(batchByKey, batch, txID)
		return
	}

	res, parseErr := s.ParseLuaMapResponse(response.Bins[LuaSuccess.String()])
	if parseErr != nil {
		s.handleParseError(batchByKey, batch, txID, parseErr)
		return
	}

	// Handle signals
	if res.Signal != "" {
		s.handleSpendSignal(ctx, res.Signal, txID, res.ChildCount, thisBlockHeight)
	}

	// Process based on status
	switch res.Status {
	case LuaStatusOK:
		s.handleSuccessfulSpends(batchByKey, batch)
	case LuaStatusError:
		s.handleErrorSpends(res, batchByKey, batch, txID, thisBlockHeight, batchID)
	}
}

// handleBatchError handles errors from batch operations
func (s *Store) handleBatchError(batchByKey []aerospike.MapValue, batch []*batchSpend, thisBlockHeight uint32, batchID uint64, err error) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)
		batch[idx].errCh <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, blockHeight %d: %d", batch[idx].spend.TxID.String(), thisBlockHeight, batchID, err)
	}
	// Record batch-level failure for circuit breaker
	if s.spendCircuitBreaker != nil {
		s.spendCircuitBreaker.RecordFailure()
	}
}

// handleMissingResponse handles missing response from batch operation
func (s *Store) handleMissingResponse(batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)
		batch[idx].errCh <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String())
	}
}

// handleParseError handles parse errors from response
func (s *Store) handleParseError(batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, err error) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)
		batch[idx].errCh <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String(), err)
	}
}

// handleSpendSignal handles signals from spend operations
func (s *Store) handleSpendSignal(ctx context.Context, signal LuaSignal, txID *chainhash.Hash, childCount int, thisBlockHeight uint32) {
	switch signal {
	case LuaSignalAllSpent:
		if err := s.handleExtraRecords(ctx, txID, 1); err != nil {
			s.logger.Errorf("Failed to handle extra records: %v", err)
		}

	case LuaSignalDAHSet:
		// Only set DAH if BlockHeightRetention is configured (> 0)
		// When retention is 0, it means "don't use automatic retention"
		if retention := s.settings.GetUtxoStoreBlockHeightRetention(); retention > 0 {
			dahHeight := thisBlockHeight + retention

			if err := s.SetDAHForChildRecords(txID, childCount, dahHeight); err != nil {
				s.logger.Errorf("Failed to set DAH for child records: %v", err)
			}
			// External store DAH is disabled - lifecycle managed by pruner service
		}

	case LuaSignalDAHUnset:
		if err := s.SetDAHForChildRecords(txID, childCount, aerospike.TTLDontExpire); err != nil {
			s.logger.Errorf("Failed to unset DAH for child records: %v", err)
		}
		// External store DAH is disabled - lifecycle managed by pruner service
	}
}

// handleSuccessfulSpends handles successful spend operations
func (s *Store) handleSuccessfulSpends(batchByKey []aerospike.MapValue, batch []*batchSpend) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)
		batch[idx].errCh <- nil
	}
	// Record successful batch operation for circuit breaker
	if s.spendCircuitBreaker != nil {
		s.spendCircuitBreaker.RecordSuccess()
	}
}

// handleErrorSpends handles error responses from spend operations
func (s *Store) handleErrorSpends(res *LuaMapResponse, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64) {
	if res.Message != "" {
		// General error for all spends
		generalErr := s.createGeneralError(res.ErrorCode, txID, thisBlockHeight, batchID, res.Message)
		for _, batchItem := range batchByKey {
			idx := batchItem["idx"].(int)
			batch[idx].errCh <- generalErr
		}
	} else if res.Errors != nil {
		// Individual errors for specific spends
		s.handleIndividualErrors(res.Errors, batchByKey, batch, txID)
	} else {
		// ERROR status but no message or errors
		for _, batchItem := range batchByKey {
			idx := batchItem["idx"].(int)
			batch[idx].errCh <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %v", txID.String(), thisBlockHeight, batchID, res)
		}
	}
}

// createGeneralError creates a general error based on error code
func (s *Store) createGeneralError(errorCode LuaErrorCode, txID *chainhash.Hash, thisBlockHeight uint32, batchID uint64, message string) error {
	switch errorCode {
	case LuaErrorCodeFrozen:
		return errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] transaction is frozen, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	case LuaErrorCodeConflicting:
		return errors.NewTxConflictingError("[SPEND_BATCH_LUA][%s] transaction is conflicting, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	case LuaErrorCodeLocked:
		return errors.NewTxLockedError("[SPEND_BATCH_LUA][%s] transaction is locked, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	case LuaErrorCodeCreating:
		return errors.NewTxCreatingError("[SPEND_BATCH_LUA][%s] transaction is creating, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	case LuaErrorCodeCoinbaseImmature:
		return errors.NewTxCoinbaseImmatureError("[SPEND_BATCH_LUA][%s] coinbase is locked, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	case LuaErrorCodeTxNotFound:
		return errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] transaction not found, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	default:
		return errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, message)
	}
}

// handleIndividualErrors handles individual errors for specific spends
func (s *Store) handleIndividualErrors(errors map[int]LuaErrorInfo, batchByKey []aerospike.MapValue, batch []*batchSpend, txID *chainhash.Hash) {
	for _, batchItem := range batchByKey {
		idx := batchItem["idx"].(int)

		if errMsg, hasError := errors[idx]; hasError {
			batch[idx].errCh <- s.createSpendError(errMsg, batch[idx], txID)
		} else {
			batch[idx].errCh <- nil
		}
	}
}

// createSpendError creates an error for a specific spend
func (s *Store) createSpendError(errMsg LuaErrorInfo, batchItem *batchSpend, txID *chainhash.Hash) error {
	switch errMsg.ErrorCode {
	case LuaErrorCodeSpent:
		if errMsg.SpendingData != "" {
			spendingData, parseErr := spendpkg.NewSpendingDataFromString(errMsg.SpendingData)
			if parseErr != nil {
				return errors.NewStorageError("[SPEND_BATCH_LUA][%s] invalid spending data in error: %s", txID.String(), errMsg.SpendingData)
			}

			return errors.NewUtxoSpentError(*batchItem.spend.TxID, batchItem.spend.Vout, *batchItem.spend.UTXOHash, spendingData)
		}

		return errors.NewStorageError("[SPEND_BATCH_LUA][%s] UTXO already spent but no spending data provided", txID.String())

	case LuaErrorCodeInvalidSpend:
		return errors.NewUtxoError("[SPEND_BATCH_LUA][%s] invalid spend for vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeFrozen:
		return errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] UTXO is frozen, vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeFrozenUntil:
		return errors.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] UTXO frozen until block, vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoNotFound:
		return errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] UTXO not found for vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoHashMismatch:
		return errors.NewUtxoHashMismatchError("[SPEND_BATCH_LUA][%s] UTXO hash mismatch for vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	case LuaErrorCodeUtxoInvalidSize:
		return errors.NewUtxoInvalidSize("[SPEND_BATCH_LUA][%s] UTXO invalid size for vout %d: %s", txID.String(), batchItem.spend.Vout, errMsg.Message)

	default:
		return errors.NewStorageError("[SPEND_BATCH_LUA][%s] error for vout %d (code: %s): %s", txID.String(), batchItem.spend.Vout, errMsg.ErrorCode, errMsg.Message)
	}
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

	// Parse the map response
	ret, err := s.ParseLuaMapResponse(res)
	if err != nil {
		s.logger.Errorf("[SPEND_BATCH_LUA][%s] failed to parse LUA return value: %v", txID.String(), err)
		return err
	}

	if ret.Status == LuaStatusOK {
		if ret.Signal != "" {
			switch ret.Signal {
			case LuaSignalDAHSet:
				// Only set DAH if BlockHeightRetention is configured (> 0)
				// When retention is 0, it means "don't use automatic retention"
				if retention := s.settings.GetUtxoStoreBlockHeightRetention(); retention > 0 {
					thisBlockHeight := s.blockHeight.Load()
					dah := thisBlockHeight + retention

					if err := s.SetDAHForChildRecords(txID, ret.ChildCount, dah); err != nil {
						return err
					}
					// External store DAH is disabled - lifecycle managed by pruner service
				}

			case LuaSignalDAHUnset:
				if err := s.SetDAHForChildRecords(txID, ret.ChildCount, 0); err != nil {
					return err
				}
				// External store DAH is disabled - lifecycle managed by pruner service
			}
		}
	} else if ret.Status == LuaStatusError {
		return errors.NewStorageError("[SPEND_BATCH_LUA][%s] failed to handleExtraRecords: %v", txID.String(), ret.Message)
	}

	return nil
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

		// Get the raw response from Lua
		rawResponse := batchRecord.Record.Bins[LuaSuccess.String()]
		if rawResponse == nil {
			batch[idx].res <- incrementSpentRecordsRes{
				res: nil,
				err: errors.NewProcessingError("no response from Lua"),
			}
			continue
		}

		// Pass through the raw response - let the caller handle parsing
		batch[idx].res <- incrementSpentRecordsRes{
			res: rawResponse,
			err: nil,
		}
	}
}

func (s *Store) sendSetDAHBatch(batch []*batchDAH) {
	var err error

	// Create batch records with individual TTLs
	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	for i, b := range batch {
		keySource := uaerospike.CalculateKeySourceInternal(b.txID, b.childIdx)

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
