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
//   - totalUtxos: Total number of UTXOs
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
	"os"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
)

// Used for NOOP batch operations
var placeholderKey *aerospike.Key

// LockRecordIndex is a special index value for lock records
// Uses high uint32 values to avoid conflict with actual sub-records (0, 1, 2, ...)
// Version history:
//   - v1: 0xFFFFFFFF (had TTL bug - locks never expired)
//   - v2: 0xFFFFFFFE (TTL fix applied)
const LockRecordIndex = uint32(0xFFFFFFFE)

// LockRecordBaseTTL is the minimum time-to-live for lock records in seconds
const LockRecordBaseTTL = uint32(30)

// LockRecordPerRecordTTL is the additional TTL per record
const LockRecordPerRecordTTL = uint32(2)

// LockRecordMaxTTL is the maximum time-to-live for lock records in seconds
const LockRecordMaxTTL = uint32(300)

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

	// Locked indicates if this transaction is locked for spending
	locked bool

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

	_, _, deferFn := tracing.Tracer("aerospike").Start(ctx, "aerospike:Create")
	defer deferFn()

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get tx meta data", err)
	}

	txMeta.Conflicting = createOptions.Conflicting

	txMeta.Locked = createOptions.Locked

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
		locked:       createOptions.Locked,
		done:         errCh,
	}

	s.storeBatcher.Put(item)

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

	stat := gocore.NewStat("sendStoreBatch")

	ctx, _, deferFn := tracing.Tracer("aerospike").Start(s.ctx, "sendStoreBatch",
		tracing.WithParentStat(gocoreStat),
		tracing.WithHistogram(prometheusUtxoCreateBatch),
	)

	defer func() {
		prometheusUtxoCreateBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	if s.settings.UtxoStore.VerboseDebug {
		s.logger.Debugf("[STORE_BATCH] sending batch of %d txMetas", len(batch))
	}

	var (
		key         *aerospike.Key
		binsToStore [][]*aerospike.Bin
		err         error
	)

	for idx, bItem := range batch {
		key, err = aerospike.NewKey(s.namespace, s.setName, bItem.txHash[:])
		if err != nil {
			util.SafeSend(bItem.done, err)

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

		binsToStore, err = s.GetBinsToStore(bItem.tx, bItem.blockHeight, bItem.blockIDs, bItem.blockHeights, bItem.subtreeIdxs, external, bItem.txHash, bItem.isCoinbase, bItem.conflicting, bItem.locked) // false is to say this is a normal record, not external.
		if err != nil {
			util.SafeSend[error](bItem.done, errors.NewProcessingError("could not get bins to store", err))

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
				go s.StorePartialTransactionExternally(ctx, batch[idx], binsToStore)
			} else {
				// This will also create the aerospike records
				go s.StoreTransactionExternally(ctx, batch[idx], binsToStore)
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

					iUint32, err := safeconversion.IntToUint32(i)
					if err != nil {
						s.logger.Errorf("Could not convert i (%d) to uint32", i)
					}

					wrapper.UTXOs = append(wrapper.UTXOs, &utxopersister.UTXO{
						Index:  iUint32,
						Value:  output.Satoshis,
						Script: *output.LockingScript,
					})
				}

				timeStart := time.Now()

				setOptions := []options.FileOption{}

				if err = s.externalStore.Set(
					ctx,
					bItem.txHash[:],
					fileformat.FileTypeOutputs,
					wrapper.Bytes(),
					setOptions...,
				); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
					util.SafeSend[error](bItem.done, errors.NewStorageError("error writing outputs to external store [%s]", bItem.txHash.String()))
					// NOOP for this record
					batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

					continue
				}

				prometheusTxMetaAerospikeMapSetExternal.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
			} else {
				timeStart := time.Now()

				// store the tx data externally, it is not in our aerospike record
				if err = s.externalStore.Set(
					ctx,
					bItem.txHash[:],
					fileformat.FileTypeTx,
					bItem.tx.ExtendedBytes(),
				); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
					util.SafeSend[error](bItem.done, errors.NewStorageError("[sendStoreBatch] error batch writing transaction to external store [%s]", bItem.txHash.String()))
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
			dah := bItem.blockHeight + s.settings.GetUtxoStoreBlockHeightRetention()
			putOps = append(putOps, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), dah)))
		}

		batchRecords[idx] = aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)

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
					util.SafeSend(bItem.done, err)
				}

				return
			}
		}

		s.logger.Errorf("[STORE_BATCH][batch:%d] error in aerospike map store batch records: %v", batchID, err)

		for _, bItem := range batch {
			util.SafeSend(bItem.done, err)
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
					util.SafeSend[error](batch[idx].done, errors.NewTxExistsError("[sendStoreBatch-2] %v already exists in store", batch[idx].txHash))
					continue
				}

				if aErr.ResultCode == types.RECORD_TOO_BIG {
					binsToStore, err = s.GetBinsToStore(batch[idx].tx, batch[idx].blockHeight, batch[idx].blockIDs, batch[idx].blockHeights, batch[idx].subtreeIdxs, true, batch[idx].txHash, batch[idx].isCoinbase, batch[idx].conflicting, batch[idx].locked) // true is to say this is a big record
					if err != nil {
						util.SafeSend[error](batch[idx].done, errors.NewProcessingError("could not get bins to store", err))
						continue
					}

					if len(batch[idx].tx.Inputs) == 0 {
						go s.StorePartialTransactionExternally(ctx, batch[idx], binsToStore)
					} else {
						go s.StoreTransactionExternally(ctx, batch[idx], binsToStore)
					}

					continue
				}

				if aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
					// This is a NOOP record and the done channel will be called by the external process
					continue
				}

				util.SafeSend[error](batch[idx].done, errors.NewStorageError("[STORE_BATCH][%s:%d] error in aerospike store batch record for tx (will retry): %d", batch[idx].txHash.String(), idx, batchID, err))
			}
		} else if len(batch[idx].tx.Outputs) <= s.utxoBatchSize {
			// We notify the done channel that the operation was successful, except
			// if this item was offloaded to the multi-record queue
			util.SafeSend(batch[idx].done, nil)
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
	batchCap := len(commonBins) + 2 // +2 for utxos and totalUtxos bins

	for start := 0; start < len(utxos); start += s.utxoBatchSize {
		end := start + s.utxoBatchSize
		if end > len(utxos) {
			end = len(utxos)
		}

		// Count non-nil UTXOs while creating the batch slice
		totalUtxos := 0
		batchUtxos := utxos[start:end]

		for _, utxo := range batchUtxos {
			if utxo != nil {
				totalUtxos++
			}
		}

		// Pre-allocate the batch with exact capacity needed
		batch := make([]*aerospike.Bin, 0, batchCap)
		batch = append(batch, commonBins...)
		batch = append(batch,
			aerospike.NewBin(fields.Utxos.String(), aerospike.NewListValue(batchUtxos)),
			aerospike.NewBin(fields.RecordUtxos.String(), aerospike.NewIntegerValue(totalUtxos)),
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
func (s *Store) GetBinsToStore(tx *bt.Tx, blockHeight uint32, blockIDs, blockHeights []uint32, subtreeIdxs []int, external bool,
	txHash *chainhash.Hash, isCoinbase bool, isConflicting bool, isLocked bool) ([][]*aerospike.Bin, error) {
	var (
		fee          uint64
		utxoHashes   []*chainhash.Hash
		err          error
		size         int
		extendedSize int
	)

	if len(tx.Outputs) == 0 {
		return nil, errors.NewProcessingError("tx %s has no outputs", txHash)
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
		if e, ok := err.(*errors.Error); ok {
			prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", e.Code().Enum().String()).Inc()
		} else if e, ok := err.(*aerospike.AerospikeError); ok {
			prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", e.ResultCode.String()).Inc()
		} else {
			prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", "unknown").Inc()
		}
		return nil, errors.NewProcessingError("failed to get fees and utxo hashes for %s", txHash, err)
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

	feeInt, err := safeconversion.Uint64ToInt(fee)
	if err != nil {
		return nil, err
	}

	commonBins := []*aerospike.Bin{
		aerospike.NewBin(fields.TxID.String(), aerospike.NewBytesValue(txHash[:])),
		aerospike.NewBin(fields.Version.String(), aerospike.NewIntegerValue(int(tx.Version))),
		aerospike.NewBin(fields.LockTime.String(), aerospike.NewIntegerValue(int(tx.LockTime))),
		aerospike.NewBin(fields.Fee.String(), aerospike.NewIntegerValue(feeInt)),
		aerospike.NewBin(fields.SizeInBytes.String(), aerospike.NewIntegerValue(size)),
		aerospike.NewBin(fields.ExtendedSize.String(), aerospike.NewIntegerValue(extendedSize)),
		aerospike.NewBin(fields.SpentUtxos.String(), aerospike.NewIntegerValue(0)),
		aerospike.NewBin(fields.IsCoinbase.String(), isCoinbase),
	}

	if isCoinbase {
		// TODO - verify this is correct.  You cannot spend outputs that were created in a coinbase transaction
		// until 100 blocks have been mined on top of the block containing the coinbase transaction.
		// Bitcoin has a 100 block coinbase maturity period and the block in which the coinbase transaction is included is block 0.
		// counts as the 1st confirmation, so we need to wait for 99 more blocks to be mined before the coinbase outputs can be spent.
		// So, for instance an output from the coinbase transaction in block 9 can be spent in block 109.
		commonBins = append(commonBins, aerospike.NewBin(fields.SpendingHeight.String(), aerospike.NewIntegerValue(int(blockHeight+uint32(s.settings.ChainCfgParams.CoinbaseMaturity)))))
	}

	// add the conflicting bin to all the records
	commonBins = append(commonBins, aerospike.NewBin(fields.Conflicting.String(), isConflicting))

	// add the locked bin to all the records
	commonBins = append(commonBins, aerospike.NewBin(fields.Locked.String(), isLocked))

	// Split utxos into batches
	batches := s.splitIntoBatches(utxos, commonBins)

	batches[0] = append(batches[0], aerospike.NewBin(fields.TotalExtraRecs.String(), aerospike.NewIntegerValue(len(batches)-1)))
	batches[0] = append(batches[0], aerospike.NewBin(fields.BlockIDs.String(), blockIDs))
	batches[0] = append(batches[0], aerospike.NewBin(fields.BlockHeights.String(), blockHeights))
	batches[0] = append(batches[0], aerospike.NewBin(fields.SubtreeIdxs.String(), subtreeIdxs))
	batches[0] = append(batches[0], aerospike.NewBin(fields.TotalUtxos.String(), len(utxos)))

	// Set UnminedSince for unmined transactions (when no blockIDs/blockHeights)
	if len(blockIDs) == 0 && len(blockHeights) == 0 && len(subtreeIdxs) == 0 {
		batches[0] = append(batches[0], aerospike.NewBin(fields.UnminedSince.String(), aerospike.NewIntegerValue(int(blockHeight))))
	}

	// add the created at bin in milliseconds to the first record
	batches[0] = append(batches[0], aerospike.NewBin(fields.CreatedAt.String(), aerospike.NewIntegerValue(int(time.Now().UnixMilli()))))

	if len(batches) > 1 {
		// if we have more than one batch, we opt to store the transaction externally
		external = true
	}

	if external {
		batches[0] = append(batches[0], aerospike.NewBin(fields.External.String(), true))
	} else {
		batches[0] = append(batches[0], aerospike.NewBin(fields.Inputs.String(), inputs))
		batches[0] = append(batches[0], aerospike.NewBin(fields.Outputs.String(), outputs))
	}

	return batches, nil
}

// StoreTransactionExternally handles storage of large transactions in external blob storage.
// This is used when transactions exceed the Aerospike record size limit.
//
// The process:
//  1. Acquires lock record
//  2. Stores transaction data in blob storage
//  3. Creates all Aerospike records in batch with creating=true
//  4. Clears creating flag for all records
//  5. Releases lock
func (s *Store) StoreTransactionExternally(ctx context.Context, bItem *BatchStoreItem, binsToStore [][]*aerospike.Bin) {
	s.storeExternallyWithLock(
		ctx,
		bItem,
		binsToStore,
		bItem.tx.ExtendedBytes(),
		fileformat.FileTypeTx,
		"StoreTransactionExternally",
	)
}

// StorePartialTransactionExternally handles storage of partial transactions
// (typically just outputs) in external storage.
//
// Used for:
//   - Transaction outputs received before inputs
//   - Very large output sets
//   - Special transaction types
func (s *Store) StorePartialTransactionExternally(ctx context.Context, bItem *BatchStoreItem, binsToStore [][]*aerospike.Bin) {
	// Prepare output wrapper for blob storage
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

		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			s.logger.Errorf("Could not convert i (%d) to uint32", i)
		}

		wrapper.UTXOs = append(wrapper.UTXOs, &utxopersister.UTXO{
			Index:  iUint32,
			Value:  output.Satoshis,
			Script: *output.LockingScript,
		})
	}

	// Delegate to shared implementation
	s.storeExternallyWithLock(
		ctx,
		bItem,
		binsToStore,
		wrapper.Bytes(),
		fileformat.FileTypeOutputs,
		"StorePartialTransactionExternally",
	)
}

// storeExternallyWithLock is the shared implementation for external transaction storage
// Both StoreTransactionExternally and StorePartialTransactionExternally delegate to this
//
// TWO-PHASE COMMIT PROTOCOL FOR MULTI-RECORD TRANSACTIONS:
//
// Phase 1: Create all records with creating=true flag
//   - Acquires lock to prevent duplicate work
//   - Stores transaction data in blob storage
//   - Creates all Aerospike records with creating=true
//   - Notifies block assembly ONCE (only when records are newly created)
//   - Lock is released
//
// Phase 2: Clear creating flags (children first, then master)
//   - Children records cleared first (indices 1, 2, ..., N-1)
//   - Master record cleared last (index 0)
//   - Master's creating flag absence = atomic completion indicator
//
// ERROR HANDLING PHILOSOPHY:
//
// The function returns success (nil error) as long as Phase 1 completes, even if Phase 2
// fails. This is intentional and correct because:
//
// 1. TRANSACTION IS PERSISTED: Phase 1 success means all records exist with complete data
// 2. SPEND PROTECTION: creating=true flags prevent premature UTXO spending (per-record Lua checks)
// 3. AUTO-RECOVERY: System self-heals through multiple paths (see line 911 for details)
// 4. ATOMICITY: Returning error would break atomicity (Phase 1 done, but system thinks it failed)
// 5. BLOCK ASSEMBLY: Notification is about existence, not spendability
//
// RECOVERY SCENARIOS:
// - Retry attempts complete Phase 2 via "All exist" path (line 888)
// - Auto-recovery triggers when transaction is re-encountered (processTxMetaUsingStore.go:112-122)
// - Mining operation clears flags via setMined
func (s *Store) storeExternallyWithLock(
	ctx context.Context,
	bItem *BatchStoreItem,
	binsToStore [][]*aerospike.Bin,
	blobData []byte,
	fileType fileformat.FileType,
	funcName string,
) {
	// Acquire semaphore to limit concurrent external storage operations
	if s.externalStoreSem != nil {
		s.externalStoreSem <- struct{}{}
		defer func() { <-s.externalStoreSem }()
	}

	// Acquire lock FIRST to prevent duplicate work
	lockKey, err := s.acquireLock(bItem.txHash, len(binsToStore))
	if err != nil {
		util.SafeSend(bItem.done, err)
		return
	}

	// Always release the lock when done (success or failure)
	// The creating bin in each record prevents UTXO spending until cleared
	// Failed creations leave partial records for the next attempt to "finish off"
	defer func() {
		if releaseErr := s.releaseLock(lockKey); releaseErr != nil {
			s.logger.Warnf("[%s] Failed to release lock: %v", funcName, releaseErr)
		}
	}()

	// Pre-create all record keys to fail fast on key creation errors
	recordKeys, err := s.prepareRecordKeys(bItem.txHash, len(binsToStore))
	if err != nil {
		util.SafeSend(bItem.done, err)
		return
	}

	// Write to external blob storage (now protected by lock - no duplicate work)
	// NOTE: Pass WithDeleteAt(0) to prevent DAH file creation. The pruner service will manage
	// deletion of external files directly when pruning Aerospike records.
	timeStart := time.Now()
	if err := s.externalStore.Set(ctx, bItem.txHash[:], fileType, blobData, options.WithDeleteAt(0)); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
		util.SafeSend[error](bItem.done, errors.NewStorageError("[%s] error writing to external store [%s]", funcName, bItem.txHash.String()))
		return
	}

	prometheusTxMetaAerospikeMapSetExternal.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	// Create Aerospike records
	batchRecords := make([]aerospike.BatchRecordIfc, len(binsToStore))
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	for idx, bins := range binsToStore {
		binsWithCreating := s.ensureCreatingBin(bins, true)
		key := recordKeys[idx]

		putOps := make([]*aerospike.Operation, len(binsWithCreating))
		for i, bin := range binsWithCreating {
			putOps[i] = aerospike.PutOp(bin)
		}

		if idx == 0 && bItem.conflicting {
			dah := bItem.blockHeight + s.settings.GetUtxoStoreBlockHeightRetention()
			putOps = append(putOps, aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), dah)))
		}

		batchRecords[idx] = aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	_ = s.client.BatchOperate(batchPolicy, batchRecords)

	// Check results - KEY_EXISTS_ERROR means recovery (completing previous attempt)
	hasFailures := false
	createdAny := false
	for idx, record := range batchRecords {
		if err := record.BatchRec().Err; err != nil {
			aErr, ok := err.(*aerospike.AerospikeError)
			if ok && aErr.ResultCode == types.KEY_EXISTS_ERROR {
				s.logger.Debugf("[%s] Record %d already exists for tx %s (completing previous attempt)", funcName, idx, bItem.txHash)
				continue
			}
			s.logger.Errorf("[%s] Failed to create record %d for tx %s: %v", funcName, idx, bItem.txHash, err)
			hasFailures = true
		} else {
			// No error - this record was created successfully
			createdAny = true
		}
	}

	if hasFailures {
		// Do NOT clean up partial records - leave them for the next attempt to complete
		// The creating bin in each record prevents UTXO spending until all records exist
		// The defer will release the lock, allowing another process to finish the creation
		util.SafeSend[error](bItem.done, errors.NewProcessingError("failed to create all records for tx %s - partial records remain for next attempt to complete", bItem.txHash))
		return
	}

	// If we didn't create any new records, all already existed - transaction is complete
	if !createdAny {
		// RECOVERY PATH: All records already exist from previous attempt
		//
		// We don't notify block assembly (transaction already processed) but we still attempt
		// Phase 2 cleanup to handle the case where a previous attempt completed Phase 1 but
		// failed during Phase 2 (creating flag cleanup).
		//
		// This is a key part of the self-healing architecture:
		// - First attempt: Creates records → Tries to clear flags → Fails → Returns success → Validator notifies BlockAssembly
		// - Retry attempt: Finds all records exist → Returns ErrTxExists → Skips BlockAssembly → Completes flag cleanup
		//
		// Without this cleanup attempt, creating flags would remain set indefinitely, requiring
		// manual intervention. This ensures eventual consistency through automatic recovery.
		clearErr := s.clearCreatingFlag(bItem.txHash, len(binsToStore))
		if clearErr != nil {
			s.logger.Warnf("[%s] Transaction %s exists but creating flag cleanup failed: %v", funcName, bItem.txHash, clearErr)
		}
		util.SafeSend[error](bItem.done, errors.NewTxExistsError("transaction already exists: %s", bItem.txHash))
		return
	}

	clearErr := s.clearCreatingFlag(bItem.txHash, len(binsToStore))
	if clearErr != nil {
		// PARTIAL SUCCESS: Transaction records created successfully, but creating flag cleanup failed
		//
		// WHY WE RETURN SUCCESS DESPITE INCOMPLETE PHASE 2:
		//
		// 1. TRANSACTION IS PERSISTED: All records exist in Aerospike with complete data.
		//    Returning error would falsely indicate the transaction doesn't exist.
		//
		// 2. SPENDING IS SAFELY PROTECTED: Each UTXO spend operation checks the creating flag
		//    per-record via Lua script (teranode.lua:288). UTXOs remain unspendable until flags
		//    are cleared, preventing premature spending.
		//
		// 3. AUTO-RECOVERY IS SELF-HEALING: The system automatically recovers via multiple paths:
		//    a) When transaction is re-encountered (propagation/subtree), subtreevalidation checks
		//       the Creating flag (processTxMetaUsingStore.go:112-122) and triggers re-processing
		//    b) When setMined is called, it clears creating flags as part of mining
		//    c) Retry attempts from other sources will complete Phase 2 via "All exist" path (line 890)
		//
		// 4. RETURNING ERROR CREATES WORSE PROBLEMS:
		//    - Caller would assume transaction failed and retry creation
		//    - Retry would hit KEY_EXISTS_ERROR, creating confusion
		//    - Block assembly wouldn't be notified of the transaction's existence
		//    - Loss of atomicity: Phase 1 complete but system thinks it failed
		//
		// 5. BLOCK ASSEMBLY NOTIFICATION IS CORRECT: Block assembly needs to track transaction
		//    existence for fee calculation and block template building. The creating flag doesn't
		//    affect this - it only affects individual UTXO spendability.
		//
		// RECOVERY GUARANTEES:
		// - Next transaction encounter triggers auto-recovery and cleanup
		// - Manual retry completes Phase 2 via "All exist" path
		// - Mining operation clears flags via setMined
		// - No manual intervention required
		s.logger.Errorf("[%s] Transaction %s created but creating flag not cleared: %v", funcName, bItem.txHash, clearErr)
		s.logger.Errorf("[%s] Records remain with creating=true, preventing UTXO spending until auto-recovery completes", funcName)
	}

	util.SafeSend(bItem.done, nil)
}

// calculateLockKey generates the key for a lock record using the special LockRecordIndex
func calculateLockKey(txHash *chainhash.Hash) []byte {
	return uaerospike.CalculateKeySourceInternal(txHash, LockRecordIndex)
}

// calculateLockTTL dynamically calculates the lock TTL based on the number of records
func calculateLockTTL(numRecords int) uint32 {
	ttl := LockRecordBaseTTL + (LockRecordPerRecordTTL * uint32(numRecords))
	if ttl > LockRecordMaxTTL {
		return LockRecordMaxTTL
	}
	return ttl
}

// acquireLock creates and acquires the lock record for transaction creation
// Returns the lock key on success, or error if lock acquisition fails
func (s *Store) acquireLock(txHash *chainhash.Hash, numRecords int) (*aerospike.Key, error) {
	lockKey, err := aerospike.NewKey(s.namespace, s.setName, calculateLockKey(txHash))
	if err != nil {
		return nil, errors.NewProcessingError("failed to create lock key", err)
	}

	lockTTL := calculateLockTTL(numRecords)

	lockPolicy := util.GetAerospikeWritePolicy(s.settings, 0, util.WithExpiration(lockTTL))
	lockPolicy.RecordExistsAction = aerospike.CREATE_ONLY

	hostname, _ := os.Hostname()

	lockBins := []*aerospike.Bin{
		aerospike.NewBin("created_at", time.Now().Unix()),
		aerospike.NewBin("lock_type", "tx_creation"),
		aerospike.NewBin("process_id", os.Getpid()),
		aerospike.NewBin("hostname", hostname),
		aerospike.NewBin("expected_recs", numRecords),
	}

	err = s.client.PutBins(lockPolicy, lockKey, lockBins...)
	if err != nil {
		aErr, ok := err.(*aerospike.AerospikeError)
		if ok && aErr.ResultCode == types.KEY_EXISTS_ERROR {
			return nil, errors.NewTxExistsError("transaction creation in progress or already exists: %s", txHash)
		}

		return nil, errors.NewProcessingError("failed to acquire lock", err)
	}

	return lockKey, nil
}

// releaseLock deletes the lock record
func (s *Store) releaseLock(lockKey *aerospike.Key) error {
	policy := util.GetAerospikeWritePolicy(s.settings, 0)

	_, err := s.client.Delete(policy, lockKey)
	if err != nil {
		aErr, ok := err.(*aerospike.AerospikeError)
		if ok && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
			return nil
		}

		return err
	}

	return nil
}

// prepareRecordKeys pre-creates all record keys for transaction storage.
// This is done BEFORE writing anything to the database to fail fast if key creation fails.
func (s *Store) prepareRecordKeys(txHash *chainhash.Hash, numRecords int) ([]*aerospike.Key, error) {
	recordKeys := make([]*aerospike.Key, numRecords)
	for idx := range numRecords {
		keySource := uaerospike.CalculateKeySourceInternal(txHash, uint32(idx))
		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return nil, errors.NewProcessingError("failed to create record key %d", idx, err)
		}
		recordKeys[idx] = key
	}

	return recordKeys, nil
}

// ensureCreatingBin ensures the creating bin is set to the specified value
// The creating bin is used for multi-record 2-phase commit to prevent UTXO spending during creation
func (s *Store) ensureCreatingBin(bins []*aerospike.Bin, creating bool) []*aerospike.Bin {
	for i, bin := range bins {
		if bin.Name == fields.Creating.String() {
			newBins := make([]*aerospike.Bin, len(bins))
			copy(newBins, bins)
			newBins[i] = aerospike.NewBin(fields.Creating.String(), creating)
			return newBins
		}
	}

	newBins := make([]*aerospike.Bin, len(bins)+1)
	copy(newBins, bins)
	newBins[len(bins)] = aerospike.NewBin(fields.Creating.String(), creating)
	return newBins
}

// clearCreatingFlag removes the creating flag from all records for a transaction
// This is called after all records have been successfully created to allow UTXO spending
// Uses expression filtering to only clear the bin on records that have it set
func (s *Store) clearCreatingFlag(txHash *chainhash.Hash, numRecords int) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	// Expression filter: only update records where creating bin exists
	filterExp := aerospike.ExpBinExists(fields.Creating.String())

	// Separate master record (index 0) from children (indices 1+)
	// Children will be cleared first, then master last
	// This makes master's creating flag an atomic completion indicator
	var masterWrite aerospike.BatchRecordIfc
	childWrites := make([]aerospike.BatchRecordIfc, 0, numRecords-1)

	for i := range numRecords {
		keySource := uaerospike.CalculateKeySourceInternal(txHash, uint32(i))
		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			return err
		}

		writePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
		writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
		writePolicy.FilterExpression = filterExp // Only update if creating bin exists

		// Delete the creating bin entirely by setting to nil
		// This saves storage space and makes absence of bin = not creating
		op := aerospike.PutOp(aerospike.NewBin(fields.Creating.String(), nil))
		writeOp := aerospike.NewBatchWrite(writePolicy, key, op)

		if i == 0 {
			masterWrite = writeOp
		} else {
			childWrites = append(childWrites, writeOp)
		}
	}

	// Phase 1: Clear child records first (indices 1, 2, ..., N-1)
	if len(childWrites) > 0 {
		err := s.client.BatchOperate(batchPolicy, childWrites)
		if err != nil {
			return errors.NewProcessingError("failed to unlock child records", err)
		}

		// Check results - FILTERED_OUT means bin didn't exist (success case)
		failedCount := 0
		for idx, record := range childWrites {
			if record.BatchRec().Err != nil {
				aErr, ok := record.BatchRec().Err.(*aerospike.AerospikeError)
				// FILTERED_OUT is success - bin didn't exist, nothing to clear
				if ok && aErr.ResultCode == types.FILTERED_OUT {
					continue
				}
				failedCount++
				s.logger.Errorf("[clearCreatingFlag] Failed to clear creating flag for child record %d for tx %s: %v", idx+1, txHash, record.BatchRec().Err)
			}
		}

		if failedCount > 0 {
			return errors.NewProcessingError("failed to unlock %d of %d child records for tx %s", failedCount, len(childWrites), txHash)
		}
	}

	// Phase 2: Clear master record last (index 0)
	// Only executed if children succeeded - master's creating flag becomes atomic completion indicator
	if masterWrite != nil {
		err := s.client.BatchOperate(batchPolicy, []aerospike.BatchRecordIfc{masterWrite})
		if err != nil {
			return errors.NewProcessingError("failed to unlock master record", err)
		}

		if masterWrite.BatchRec().Err != nil {
			aErr, ok := masterWrite.BatchRec().Err.(*aerospike.AerospikeError)
			// FILTERED_OUT is success - bin didn't exist, nothing to clear
			if ok && aErr.ResultCode == types.FILTERED_OUT {
				return nil
			}
			s.logger.Errorf("[clearCreatingFlag] Failed to clear creating flag for master record for tx %s: %v", txHash, masterWrite.BatchRec().Err)
			return errors.NewProcessingError("failed to unlock master record for tx %s", txHash)
		}
	}

	return nil
}
