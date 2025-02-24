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
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

// Used for NOOP batch operations
var placeholderKey *aerospike.Key

// BatchStoreItem represents a transaction to be stored in a batch operation.
type BatchStoreItem struct {
	// TxHash is the transaction ID
	txHash *chainhash.Hash

	// IsCoinbase indicates if this is a coinbase transaction
	isCoinbase bool

	// Tx contains the full transaction data
	tx *bt.Tx

	// BlockHeight is the height where this transaction appears
	blockHeight uint32

	// BlockIDs contains all blocks where this transaction appears
	blockIDs []uint32

	// BlockHeights contains all blocks where this transaction appears
	blockHeights []uint32

	// subtreeIdxs contains all subtree indexes where this transaction appears
	subtreeIdxs []int

	// LockTime is the transaction's lock time
	lockTime uint32

	// Conflicting indicates if this transaction is conflicting with another transaction
	conflicting bool

	// Done is used to signal completion and return errors
	done chan error
}

// Create stores a new transaction's outputs as UTXOs.
// It queues the transaction for batch processing.
//
// The function:
//  1. Creates metadata
//  2. Prepares a BatchStoreItem
//  3. Queues for batch processing
//  4. Waits for completion
//
// Parameters:
//   - ctx: Context for cancellation
//   - tx: Transaction to store
//   - blockHeight: Current block height
//   - opts: Additional creation options
//
// Returns:
//   - Transaction metadata
//   - Any error that occurred
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	createOptions := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(createOptions)
	}

	_, _, deferFn := tracing.StartTracing(ctx, "aerospike:Create")
	defer deferFn()

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get tx meta data", err)
	}

	txMeta.Conflicting = createOptions.Conflicting

	// when creating conflicting transactions, we must set the conflictingChildren in all the parents
	// we should do this before we store the transaction, so we are sure the parents have been updated properly
	if txMeta.Conflicting {
		if err = s.updateParentConflictingChildren(tx); err != nil {
			return nil, errors.NewProcessingError("failed to update parent conflicting children", err)
		}
	}

	errCh := make(chan error)
	defer close(errCh)

	var txHash *chainhash.Hash
	if createOptions.TxID != nil {
		txHash = createOptions.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	isCoinbase := txMeta.IsCoinbase

	if createOptions.IsCoinbase != nil {
		isCoinbase = *createOptions.IsCoinbase
	}

	blockIds := make([]uint32, 0)
	blockHeights := make([]uint32, 0)
	subtreeIdxs := make([]int, 0)

	if len(createOptions.MinedBlockInfos) > 0 {
		for _, blockMeta := range createOptions.MinedBlockInfos {
			blockIds = append(blockIds, blockMeta.BlockID)
			blockHeights = append(blockHeights, blockMeta.BlockHeight)
			subtreeIdxs = append(subtreeIdxs, blockMeta.SubtreeIdx)
		}
	}

	item := &BatchStoreItem{
		txHash:       txHash,
		isCoinbase:   isCoinbase,
		tx:           tx,
		blockHeight:  blockHeight,
		lockTime:     tx.LockTime,
		blockIDs:     blockIds,
		blockHeights: blockHeights,
		subtreeIdxs:  subtreeIdxs,
		conflicting:  createOptions.Conflicting,
		done:         errCh,
	}

	if s.storeBatcher != nil {
		s.storeBatcher.Put(item)
	} else {
		// if the batcher is disabled, we still want to process the request in a go routine
		go func() {
			s.sendStoreBatch([]*BatchStoreItem{item})
		}()
	}

	err = <-errCh
	if err != nil {
		// return raw err, should already be wrapped
		return nil, err
	}

	prometheusUtxostoreCreate.Inc()

	return txMeta, nil
}

// sendStoreBatch processes a batch of transaction storage requests.
// It handles automatic switching between in-database and external storage
// based on transaction size and configuration.
//
// The process flow:
//  1. For each transaction in the batch:
//     - Create Aerospike key
//     - Check if external storage is needed
//     - Prepare Aerospike bins
//     - Handle pagination if needed
//  2. Execute batch operation
//  3. Process results and handle errors
//  4. Signal completion to callers
//
// Flow diagram for each transaction:
//
//	Check Size ──┬──> Small ──> Store in Aerospike
//	             │
//	             └──> Large ──> Store in External Blob
//	                         ├─> Full Transaction (.tx)
//	                         └─> Partial Transaction (.outputs)
//
// Parameters:
//   - batch: Array of BatchStoreItems to process
func (s *Store) sendStoreBatch(batch []*BatchStoreItem) {
	start := time.Now()
	ctx, stat, deferFn := tracing.StartTracing(s.ctx, "sendStoreBatch",
		tracing.WithParentStat(gocoreStat),
		tracing.WithHistogram(prometheusUtxoCreateBatch),
	)

	defer func() {
		prometheusUtxoCreateBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings, 0, aerospike.TTLDontExpire)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchWritePolicyWithTTL := util.GetAerospikeBatchWritePolicy(s.settings, 0, uint32(s.expiration.Seconds()))
	batchWritePolicyWithTTL.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[STORE_BATCH] sending batch of %d txMetas", len(batch))
	}

	var (
		key         *aerospike.Key
		binsToStore [][]*aerospike.Bin
		hasUtxos    bool
		err         error
	)

	for idx, bItem := range batch {
		key, err = aerospike.NewKey(s.namespace, s.setName, bItem.txHash[:])
		if err != nil {
			utils.SafeSend(bItem.done, err)

			// NOOP for this record
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			continue
		}

		// We calculate the bin that we want to store, but we may get back lots of bin batches
		// because we have had to split the UTXOs into multiple records

		external := s.settings.UtxoStore.ExternalizeAllTransactions

		// also check whether the tx is too big and needs to be stored externally
		var extendedSize int

		if len(batch[idx].tx.Inputs) == 0 {
			// This is a partial transaction, and we calculate the size of the outputs only
			for _, output := range batch[idx].tx.Outputs {
				if output != nil {
					extendedSize += len(output.Bytes())
				}
			}
		} else {
			// we cannot use tx.Size() here, because it doesn't include the extended data for the inputs
			extendedSize = len(batch[idx].tx.ExtendedBytes())
		}

		if extendedSize > MaxTxSizeInStoreInBytes {
			external = true
		}

		binsToStore, hasUtxos, err = s.GetBinsToStore(bItem.tx, bItem.blockHeight, bItem.blockIDs, bItem.blockHeights, bItem.subtreeIdxs, external, bItem.txHash, bItem.isCoinbase, bItem.conflicting) // false is to say this is a normal record, not external.
		if err != nil {
			utils.SafeSend[error](bItem.done, errors.NewProcessingError("could not get bins to store", err))

			// NOOP for this record
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			continue
		}

		start = stat.NewStat("GetBinsToStore").AddTime(start)

		if len(binsToStore) > 1 {
			// Make this batch item a NOOP and persist all of these to be written via a queue
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			if len(batch[idx].tx.Inputs) == 0 {
				// This will also create the aerospike records
				go s.StorePartialTransactionExternally(ctx, batch[idx], binsToStore, hasUtxos)
			} else {
				// This will also create the aerospike records
				go s.StoreTransactionExternally(ctx, batch[idx], binsToStore, hasUtxos)
			}

			continue
		} else if external {
			if len(batch[idx].tx.Inputs) == 0 {
				nonNilOutputs := utxopersister.UnpadSlice(bItem.tx.Outputs)

				wrapper := utxopersister.UTXOWrapper{
					TxID:     *bItem.txHash,
					Height:   bItem.blockHeight,
					Coinbase: bItem.isCoinbase,
					UTXOs:    make([]*utxopersister.UTXO, 0, len(nonNilOutputs)),
				}

				for i, output := range bItem.tx.Outputs {
					if output == nil {
						continue
					}

					// nolint: gosec
					wrapper.UTXOs = append(wrapper.UTXOs, &utxopersister.UTXO{
						Index:  uint32(i),
						Value:  output.Satoshis,
						Script: *output.LockingScript,
					})
				}

				timeStart := time.Now()

				setOptions := []options.FileOption{options.WithFileExtension("outputs")}

				if !hasUtxos {
					// add a TTL to the external file, since there were no spendable utxos in the transaction
					setOptions = append(setOptions, options.WithTTL(s.expiration))
				}

				if err = s.externalStore.Set(
					ctx,
					bItem.txHash[:],
					wrapper.Bytes(),
					setOptions...,
				); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
					utils.SafeSend[error](bItem.done, errors.NewStorageError("error writing outputs to external store [%s]", bItem.txHash.String(), err))
					// NOOP for this record
					batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

					continue
				}

				prometheusTxMetaAerospikeMapSetExternal.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
			} else {
				timeStart := time.Now()

				setOptions := []options.FileOption{
					options.WithFileExtension("tx"),
					// options.WithAllowOverwrite(true),
				}

				if !hasUtxos {
					// add a TTL to the external file, since there were no spendable utxos in the transaction
					setOptions = append(setOptions, options.WithTTL(s.expiration))
				}

				// store the tx data externally, it is not in our aerospike record
				if err = s.externalStore.Set(
					ctx,
					bItem.txHash[:],
					bItem.tx.ExtendedBytes(),
					setOptions...,
				); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
					utils.SafeSend[error](bItem.done, errors.NewStorageError("[sendStoreBatch] error batch writing transaction to external store [%s]", bItem.txHash.String(), err))
					// NOOP for this record
					batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

					continue
				}

				prometheusTxMetaAerospikeMapSetExternal.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
			}
		}

		putOps := make([]*aerospike.Operation, len(binsToStore[0]))
		for i, bin := range binsToStore[0] {
			putOps[i] = aerospike.PutOp(bin)
		}

		if bItem.conflicting {
			// set the TTL on conflicting records
			batchRecords[idx] = aerospike.NewBatchWrite(batchWritePolicyWithTTL, key, putOps...)
		} else {
			batchRecords[idx] = aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
		}
	}

	batchID := s.batchID.Add(1)

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		var aErr *aerospike.AerospikeError

		ok := errors.As(err, &aErr)
		if ok {
			if aErr.ResultCode == types.KEY_EXISTS_ERROR {
				// we want to return a tx already exists error on this case
				// this should only be called with 1 record
				err = errors.NewTxExistsError("[sendStoreBatch-1] %v already exists in store", batch[0].txHash)
				for _, bItem := range batch {
					utils.SafeSend(bItem.done, err)
				}

				return
			}
		}

		s.logger.Errorf("[STORE_BATCH][batch:%d] error in aerospike map store batch records: %v", batchID, err)

		for _, bItem := range batch {
			utils.SafeSend(bItem.done, err)
		}
	}

	start = stat.NewStat("BatchOperate").AddTime(start)

	// batchOperate may have no errors, but some of the records may have failed
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			aErr, ok := err.(*aerospike.AerospikeError)
			if ok {
				if aErr.ResultCode == types.KEY_EXISTS_ERROR {
					utils.SafeSend[error](batch[idx].done, errors.NewTxExistsError("[sendStoreBatch-2] %v already exists in store", batch[idx].txHash))
					continue
				}

				if aErr.ResultCode == types.RECORD_TOO_BIG {
					binsToStore, hasUtxos, err = s.GetBinsToStore(batch[idx].tx, batch[idx].blockHeight, batch[idx].blockIDs, batch[idx].blockHeights, batch[idx].subtreeIdxs, true, batch[idx].txHash, batch[idx].isCoinbase, batch[idx].conflicting) // true is to say this is a big record
					if err != nil {
						utils.SafeSend[error](batch[idx].done, errors.NewProcessingError("could not get bins to store", err))
						continue
					}

					if len(batch[idx].tx.Inputs) == 0 {
						go s.StorePartialTransactionExternally(ctx, batch[idx], binsToStore, hasUtxos)
					} else {
						go s.StoreTransactionExternally(ctx, batch[idx], binsToStore, hasUtxos)
					}

					continue
				}

				if aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
					// This is a NOOP record and the done channel will be called by the external process
					continue
				}

				utils.SafeSend[error](batch[idx].done, errors.NewStorageError("[STORE_BATCH][%s:%d] error in aerospike store batch record for tx (will retry): %d - %w", batch[idx].txHash.String(), idx, batchID, err))
			}
		} else if len(batch[idx].tx.Outputs) <= s.utxoBatchSize {
			// We notify the done channel that the operation was successful, except
			// if this item was offloaded to the multi-record queue
			utils.SafeSend(batch[idx].done, nil)
		}
	}

	stat.NewStat("postBatchOperate").AddTime(start)
}

// splitIntoBatches splits a set of UTXOs into batches of the configured size.
// Each batch includes common metadata bins plus the UTXO-specific data.
//
// This is used to handle transactions with large numbers of outputs
// by splitting them into multiple records to stay within Aerospike size limits.
//
// Parameters:
//   - utxos: Array of UTXO data to split
//   - commonBins: Metadata bins shared across batches
//
// Returns:
//   - Array of bin batches, where each batch contains:
//   - Common metadata (version, locktime, etc)
//   - UTXOs for that batch
//   - Count of non-nil UTXOs in batch
func (s *Store) splitIntoBatches(utxos []interface{}, commonBins []*aerospike.Bin) [][]*aerospike.Bin {
	// Pre-calculate number of batches to avoid reallocation
	numBatches := (len(utxos) + s.utxoBatchSize - 1) / s.utxoBatchSize
	batches := make([][]*aerospike.Bin, 0, numBatches)

	// Pre-allocate the batch slice to avoid reallocation during append
	batchCap := len(commonBins) + 2 // +2 for utxos and nrUtxos bins

	for start := 0; start < len(utxos); start += s.utxoBatchSize {
		end := start + s.utxoBatchSize
		if end > len(utxos) {
			end = len(utxos)
		}

		// Count non-nil UTXOs while creating the batch slice
		nrUtxos := 0
		batchUtxos := utxos[start:end]
		for _, utxo := range batchUtxos {
			if utxo != nil {
				nrUtxos++
			}
		}

		// Pre-allocate the batch with exact capacity needed
		batch := make([]*aerospike.Bin, 0, batchCap)
		batch = append(batch, commonBins...)
		batch = append(batch,
			aerospike.NewBin(utxo.FieldUtxos.String(), aerospike.NewListValue(batchUtxos)),
			aerospike.NewBin(utxo.FieldNrUtxos.String(), aerospike.NewIntegerValue(nrUtxos)),
		)
		batches = append(batches, batch)
	}

	return batches
}

// GetBinsToStore prepares Aerospike bins for storage, handling transaction data
// and UTXO organization.
//
// The function:
//  1. Calculates fees and UTXO hashes
//  2. Prepares transaction data
//  3. Organizes UTXOs
//  4. Splits into batches if needed
//  5. Handles external storage decisions
//
// Parameters:
//   - tx: Transaction to process
//   - blockHeight: Current block height
//   - blockIDs: Blocks containing this transaction
//   - external: Whether to use external storage
//   - txHash: Transaction ID
//   - isCoinbase: Whether this is a coinbase transaction
//
// Returns:
//   - Array of bin batches
//   - Whether the transaction has UTXOs
//   - Any error that occurred
func (s *Store) GetBinsToStore(tx *bt.Tx, blockHeight uint32, blockIDs, blockHeights []uint32, subtreeIdxs []int, external bool, txHash *chainhash.Hash, isCoinbase bool, isConflicting bool) ([][]*aerospike.Bin, bool, error) {
	var (
		fee          uint64
		utxoHashes   []*chainhash.Hash
		err          error
		size         int
		extendedSize int
		hasUtxos     bool
	)

	if len(tx.Outputs) == 0 {
		return nil, hasUtxos, errors.NewProcessingError("tx %s has no outputs", txHash)
	}

	if len(tx.Inputs) == 0 {
		fee = 0
		utxoHashes, err = utxo.GetUtxoHashes(tx, txHash)
	} else {
		size = tx.Size()
		extendedSize = len(tx.ExtendedBytes())
		fee, utxoHashes, err = utxo.GetFeesAndUtxoHashes(context.Background(), tx, blockHeight)
	}

	if err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", err.Error()).Inc()
		return nil, hasUtxos, errors.NewProcessingError("failed to get fees and utxo hashes for %s", txHash, err)
	}

	var inputs []interface{}

	if !external {
		// create a tx interface[] map
		inputs = make([]interface{}, len(tx.Inputs))

		for i, input := range tx.Inputs {
			h := input.Bytes(false)

			// this is needed for extended txs, go-bt does not do this itself
			h = append(h, []byte{
				byte(input.PreviousTxSatoshis),
				byte(input.PreviousTxSatoshis >> 8),
				byte(input.PreviousTxSatoshis >> 16),
				byte(input.PreviousTxSatoshis >> 24),
				byte(input.PreviousTxSatoshis >> 32),
				byte(input.PreviousTxSatoshis >> 40),
				byte(input.PreviousTxSatoshis >> 48),
				byte(input.PreviousTxSatoshis >> 56),
			}...)

			if input.PreviousTxScript == nil {
				h = append(h, bt.VarInt(0).Bytes()...)
			} else {
				l := uint64(len(*input.PreviousTxScript))
				h = append(h, bt.VarInt(l).Bytes()...)
				h = append(h, *input.PreviousTxScript...)
			}

			inputs[i] = h
		}
	}

	outputs := make([]interface{}, len(tx.Outputs))
	utxos := make([]interface{}, len(tx.Outputs))

	for i, output := range tx.Outputs {
		if output != nil {
			outputs[i] = output.Bytes()

			// store all coinbases, non-zero utxos and exceptions from pre-genesis
			if utxo.ShouldStoreOutputAsUTXO(isCoinbase, output, blockHeight) {
				utxos[i] = aerospike.NewBytesValue(utxoHashes[i][:])
			}
		}
	}

	for _, u := range utxos {
		// check whether at least one utxo is not nil
		if u != nil {
			hasUtxos = true
			break
		}
	}

	commonBins := []*aerospike.Bin{
		aerospike.NewBin(utxo.FieldVersion.String(), aerospike.NewIntegerValue(int(tx.Version))),
		aerospike.NewBin(utxo.FieldLockTime.String(), aerospike.NewIntegerValue(int(tx.LockTime))),
		// nolint: gosec
		aerospike.NewBin(utxo.FieldFee.String(), aerospike.NewIntegerValue(int(fee))),
		aerospike.NewBin(utxo.FieldSizeInBytes.String(), aerospike.NewIntegerValue(size)),
		aerospike.NewBin(utxo.FieldExtendedSize.String(), aerospike.NewIntegerValue(extendedSize)),
		aerospike.NewBin(utxo.FieldSpentUtxos.String(), aerospike.NewIntegerValue(0)),
		aerospike.NewBin(utxo.FieldIsCoinbase.String(), isCoinbase),
	}

	if isCoinbase {
		// TODO - verify this is correct.  You cannot spend outputs that were created in a coinbase transaction
		// until 100 blocks have been mined on top of the block containing the coinbase transaction.
		// Bitcoin has a 100 block coinbase maturity period and the block in which the coinbase transaction is included is block 0.
		// counts as the 1st confirmation, so we need to wait for 99 more blocks to be mined before the coinbase outputs can be spent.
		// So, for instance an output from the coinbase transaction in block 9 can be spent in block 109.
		commonBins = append(commonBins, aerospike.NewBin(utxo.FieldSpendingHeight.String(), aerospike.NewIntegerValue(int(blockHeight+uint32(s.settings.ChainCfgParams.CoinbaseMaturity)))))
	}

	if isConflicting {
		commonBins = append(commonBins, aerospike.NewBin(utxo.FieldConflicting.String(), true))
	}

	// Split utxos into batches
	batches := s.splitIntoBatches(utxos, commonBins)

	batches[0] = append(batches[0], aerospike.NewBin(utxo.FieldNrRecords.String(), aerospike.NewIntegerValue(len(batches))))
	batches[0] = append(batches[0], aerospike.NewBin(utxo.FieldBlockIDs.String(), blockIDs))
	batches[0] = append(batches[0], aerospike.NewBin(utxo.FieldBlockHeights.String(), blockHeights))
	batches[0] = append(batches[0], aerospike.NewBin(utxo.FieldSubtreeIdxs.String(), subtreeIdxs))

	if len(batches) > 1 {
		// if we have more than one batch, we opt to store the transaction externally
		external = true
	}

	if external {
		batches[0] = append(batches[0], aerospike.NewBin(utxo.FieldExternal.String(), true))
	} else {
		batches[0] = append(batches[0], aerospike.NewBin(utxo.FieldInputs.String(), inputs))
		batches[0] = append(batches[0], aerospike.NewBin(utxo.FieldOutputs.String(), outputs))
	}

	return batches, hasUtxos, nil
}

// StoreTransactionExternally handles storage of large transactions in external blob storage.
// This is used when transactions exceed the Aerospike record size limit.
//
// The process:
//  1. Stores transaction data in blob storage
//  2. Creates Aerospike records with metadata
//  3. Links records to external data
//  4. Handles pagination if needed
func (s *Store) StoreTransactionExternally(ctx context.Context, bItem *BatchStoreItem, binsToStore [][]*aerospike.Bin, hasUtxos bool) {
	timeStart := time.Now()

	opts := []options.FileOption{
		options.WithFileExtension("tx"),
		// options.WithAllowOverwrite(true),
	}

	if !hasUtxos {
		// add a TTL to the external file, since there were no spendable utxos in the transaction
		opts = append(opts, options.WithTTL(s.expiration))
	}

	if err := s.externalStore.Set(
		ctx,
		bItem.txHash[:],
		bItem.tx.ExtendedBytes(),
		opts...,
	); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
		utils.SafeSend[error](bItem.done, errors.NewStorageError("[GetBinsToStore] error writing transaction to external store [%s]", bItem.txHash.String(), err))
		return
	}

	prometheusTxMetaAerospikeMapSetExternal.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	// Get a new write policy which will allow CREATE or UPDATE
	wPolicy := util.GetAerospikeWritePolicy(s.settings, 0, aerospike.TTLDontExpire)

	// For all records, set the write policy to CREATE_ONLY
	wPolicy.RecordExistsAction = aerospike.CREATE_ONLY

	for binIdx := len(binsToStore) - 1; binIdx >= 0; binIdx-- {
		bins := binsToStore[binIdx]

		// nolint: gosec
		keySource := uaerospike.CalculateKeySource(bItem.txHash, uint32(binIdx))

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			utils.SafeSend[error](bItem.done, err)
			return
		}

		putOps := make([]*aerospike.Operation, len(bins))
		for i, bin := range bins {
			putOps[i] = aerospike.PutOp(bin)
		}

		if err = s.client.PutBins(wPolicy, key, bins...); err != nil {
			var aErr *aerospike.AerospikeError

			ok := errors.As(err, &aErr)
			if ok {
				if aErr.ResultCode == types.KEY_EXISTS_ERROR {
					s.logger.Warnf("[StoreTransactionExternally][%s] bin %d already exists in store", bItem.txHash, binIdx)
					continue
				}
			}

			utils.SafeSend[error](bItem.done, errors.NewProcessingError("[StoreTransactionExternally][%s] could not put bins (extended mode) to store", bItem.txHash, err))

			return
		}
	}

	utils.SafeSend(bItem.done, nil)
}

// StorePartialTransactionExternally handles storage of partial transactions
// (typically just outputs) in external storage.
//
// Used for:
//   - Transaction outputs received before inputs
//   - Very large output sets
//   - Special transaction types
func (s *Store) StorePartialTransactionExternally(ctx context.Context, bItem *BatchStoreItem, binsToStore [][]*aerospike.Bin, hasUtxos bool) {
	nonNilOutputs := utxopersister.UnpadSlice(bItem.tx.Outputs)

	wrapper := utxopersister.UTXOWrapper{
		TxID:     *bItem.txHash,
		Height:   bItem.blockHeight,
		Coinbase: bItem.isCoinbase,
		UTXOs:    make([]*utxopersister.UTXO, 0, len(nonNilOutputs)),
	}

	for i, output := range bItem.tx.Outputs {
		if output == nil {
			continue
		}

		wrapper.UTXOs = append(wrapper.UTXOs, &utxopersister.UTXO{
			// nolint: gosec
			Index:  uint32(i),
			Value:  output.Satoshis,
			Script: *output.LockingScript,
		})
	}

	timeStart := time.Now()

	opts := []options.FileOption{
		options.WithFileExtension("outputs"),
		// options.WithAllowOverwrite(true),
	}

	if !hasUtxos {
		// add a TTL to the external file, since there were no spendable utxos in the transaction
		opts = append(opts, options.WithTTL(s.expiration))
	}

	if err := s.externalStore.Set(
		ctx,
		bItem.txHash[:],
		wrapper.Bytes(),
		opts...,
	); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
		utils.SafeSend[error](bItem.done, errors.NewStorageError("[StorePartialTransactionExternally] error writing output to external store [%s]", bItem.txHash.String(), err))
		return
	}

	prometheusTxMetaAerospikeMapSetExternal.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	// Get a new write policy which will allow CREATE or UPDATE
	wPolicy := util.GetAerospikeWritePolicy(s.settings, 0, aerospike.TTLDontExpire)

	for i := len(binsToStore) - 1; i >= 0; i-- {
		bins := binsToStore[i]

		if i == 0 {
			// For the "master" record, set the write policy to CREATE_ONLY
			wPolicy.RecordExistsAction = aerospike.CREATE_ONLY
		}

		// nolint: gosec
		keySource := uaerospike.CalculateKeySource(bItem.txHash, uint32(i))

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			utils.SafeSend[error](bItem.done, err)
			return
		}

		putOps := make([]*aerospike.Operation, len(bins))
		for i, bin := range bins {
			putOps[i] = aerospike.PutOp(bin)
		}

		if err := s.client.PutBins(wPolicy, key, bins...); err != nil {
			aErr, ok := err.(*aerospike.AerospikeError)
			if ok {
				if aErr.ResultCode == types.KEY_EXISTS_ERROR {
					utils.SafeSend[error](bItem.done, errors.NewTxExistsError("[StorePartialTransactionExternally] %v already exists in store", bItem.txHash))

					return
				}
			}

			utils.SafeSend[error](bItem.done, errors.NewProcessingError("could not put partial bins (extended mode) to store", err))

			return
		}
	}

	utils.SafeSend(bItem.done, nil)
}
