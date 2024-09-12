// //go:build aerospike

package aerospike

import (
	"context"
	"math"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

// Used for NOOP batch operations
var placeholderKey *aerospike.Key

type batchStoreItem struct {
	txHash      *chainhash.Hash
	isCoinbase  bool
	tx          *bt.Tx
	blockHeight uint32
	blockIDs    []uint32
	lockTime    uint32
	done        chan error
}

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

	errCh := make(chan error)
	defer close(errCh)

	var txHash *chainhash.Hash
	if createOptions.TxID != nil {
		txHash = createOptions.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	isCoinbase := tx.IsCoinbase()
	if createOptions.IsCoinbase != nil {
		isCoinbase = *createOptions.IsCoinbase
	}

	item := &batchStoreItem{
		txHash:      txHash,
		isCoinbase:  isCoinbase,
		tx:          tx,
		blockHeight: blockHeight,
		lockTime:    tx.LockTime,
		blockIDs:    createOptions.BlockIDs,
		done:        errCh,
	}

	if s.storeBatcher != nil {
		s.storeBatcher.Put(item)
	} else {
		// if the batcher is disabled, we still want to process the request in a go routine
		go func() {
			s.sendStoreBatch([]*batchStoreItem{item})
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

func (s *Store) sendStoreBatch(batch []*batchStoreItem) {
	start := time.Now()
	_, stat, deferFn := tracing.StartTracing(context.Background(), "sendStoreBatch",
		tracing.WithParentStat(gocoreStat),
		tracing.WithHistogram(prometheusUtxoCreateBatch),
	)

	defer func() {
		prometheusUtxoCreateBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchPolicy := util.GetAerospikeBatchPolicy()

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	s.logger.Debugf("[STORE_BATCH] sending batch of %d txMetas", len(batch))

	var (
		key         *aerospike.Key
		binsToStore [][]*aerospike.Bin
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

		external := s.externalizeAllTransactions

		// also check whether the tx is too big and needs to be stored externally
		var size int

		if len(batch[idx].tx.Inputs) == 0 {
			// This is a partial transaction, and we calculate the size of the outputs only
			for _, output := range batch[idx].tx.Outputs {
				if output != nil {
					size += len(output.Bytes())
				}
			}
		} else {
			size = batch[idx].tx.Size()
		}

		if size > MaxTxSizeInStoreInBytes {
			external = true
		}

		binsToStore, err = s.getBinsToStore(bItem.tx, bItem.blockHeight, bItem.blockIDs, external, bItem.txHash, bItem.isCoinbase) // false is to say this is a normal record, not external.
		if err != nil {
			utils.SafeSend[error](bItem.done, errors.NewProcessingError("could not get bins to store", err))

			// NOOP for this record
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			continue
		}

		start = stat.NewStat("getBinsToStore").AddTime(start)

		if len(binsToStore) > 1 {
			// Make this batch item a NOOP and persist all of these to be written via a queue
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			if len(batch[idx].tx.Inputs) == 0 {
				go s.storePartialTransactionExternally(batch[idx], binsToStore)
			} else {
				go s.storeTransactionExternally(batch[idx], binsToStore)
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

				if err := s.externalStore.Set(
					context.TODO(),
					bItem.txHash[:],
					wrapper.Bytes(),
					options.WithFileExtension("outputs"),
				); err != nil {
					if errors.Is(err, errors.ErrBlobAlreadyExists) {
						utils.SafeSend[error](bItem.done, errors.NewTxAlreadyExistsError("error writing output to external store [%s]", bItem.txHash.String(), err))
					} else {
						utils.SafeSend[error](bItem.done, errors.NewStorageError("error writing output to external store [%s]", bItem.txHash.String(), err))
					}

					batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

					continue
				}
			} else {
				// store the tx data externally, it is not in our aerospike record
				if err = s.externalStore.Set(
					context.Background(),
					bItem.txHash[:],
					bItem.tx.ExtendedBytes(),
					options.WithFileExtension("tx"),
				); err != nil {
					if errors.Is(err, errors.ErrBlobAlreadyExists) {
						utils.SafeSend[error](bItem.done, errors.NewTxAlreadyExistsError("error writing output to external store [%s]", bItem.txHash.String(), err))
					} else {
						utils.SafeSend[error](bItem.done, errors.NewStorageError("error writing transaction to external store [%s]", bItem.txHash.String(), err))
					}

					batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

					continue
				}
			}
		}

		putOps := make([]*aerospike.Operation, len(binsToStore[0]))
		for i, bin := range binsToStore[0] {
			putOps[i] = aerospike.PutOp(bin)
		}

		record := aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
		batchRecords[idx] = record
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
				err = errors.NewTxAlreadyExistsError("[sendStoreBatch-1] %v already exists in store", batch[0].txHash)
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
					utils.SafeSend[error](batch[idx].done, errors.NewTxAlreadyExistsError("[sendStoreBatch-2] %v already exists in store", batch[idx].txHash))
					continue
				}

				if aErr.ResultCode == types.RECORD_TOO_BIG {
					binsToStore, err = s.getBinsToStore(batch[idx].tx, batch[idx].blockHeight, batch[idx].blockIDs, true, batch[idx].txHash, batch[idx].isCoinbase) // true is to say this is a big record
					if err != nil {
						utils.SafeSend[error](batch[idx].done, errors.NewProcessingError("could not get bins to store", err))
						continue
					}

					if len(batch[idx].tx.Inputs) == 0 {
						go s.storePartialTransactionExternally(batch[idx], binsToStore)
					} else {
						go s.storeTransactionExternally(batch[idx], binsToStore)
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

func (s *Store) splitIntoBatches(utxos []interface{}, commonBins []*aerospike.Bin) [][]*aerospike.Bin {
	var batches [][]*aerospike.Bin

	for start := 0; start < len(utxos); start += s.utxoBatchSize {
		end := start + s.utxoBatchSize
		if end > len(utxos) {
			end = len(utxos)
		}

		batchUtxos := utxos[start:end]

		// Count the number of non-nil utxos in this batch

		nrUtxos := 0

		for _, utxo := range batchUtxos {
			if utxo != nil {
				nrUtxos++
			}
		}

		batch := append([]*aerospike.Bin(nil), commonBins...)
		batch = append(batch, aerospike.NewBin("utxos", aerospike.NewListValue(batchUtxos)))
		batch = append(batch, aerospike.NewBin("nrUtxos", aerospike.NewIntegerValue(nrUtxos)))
		batches = append(batches, batch)
	}

	return batches
}

func (s *Store) getBinsToStore(tx *bt.Tx, blockHeight uint32, blockIDs []uint32, external bool, txHash *chainhash.Hash, isCoinbase bool) ([][]*aerospike.Bin, error) {
	var (
		fee        uint64
		utxoHashes []*chainhash.Hash
		err        error
		size       int
	)

	if len(tx.Outputs) == 0 {
		return nil, errors.NewProcessingError("tx %s has no outputs", txHash)
	}

	if len(tx.Inputs) == 0 {
		fee = 0
		utxoHashes, err = utxo.GetUtxoHashes(tx, txHash)
	} else {
		size = tx.Size()
		fee, utxoHashes, err = utxo.GetFeesAndUtxoHashes(context.Background(), tx, blockHeight)
	}

	if err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", err.Error()).Inc()
		return nil, errors.NewProcessingError("failed to get fees and utxo hashes for %s: %v", txHash, err)
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

			// store all non-zero utxos and exceptions from pre-genesis
			if utxo.ShouldStoreOutputAsUTXO(output, blockHeight) {
				utxos[i] = aerospike.NewBytesValue(utxoHashes[i][:])
			}
		}
	}

	commonBins := []*aerospike.Bin{
		aerospike.NewBin("version", aerospike.NewIntegerValue(int(tx.Version))),
		aerospike.NewBin("locktime", aerospike.NewIntegerValue(int(tx.LockTime))),
		// nolint: gosec
		aerospike.NewBin("fee", aerospike.NewIntegerValue(int(fee))),
		aerospike.NewBin("sizeInBytes", aerospike.NewIntegerValue(size)),
		aerospike.NewBin("spentUtxos", aerospike.NewIntegerValue(0)),
		aerospike.NewBin("isCoinbase", tx.IsCoinbase()),
	}

	if isCoinbase {
		// TODO - verify this is correct.  You cannot spend outputs that were created in a coinbase transaction
		// until 100 blocks have been mined on top of the block containing the coinbase transaction.
		// Bitcoin has a 100 block coinbase maturity period and the block in which the coinbase transaction is included is block 0.
		// counts as the 1st confirmation, so we need to wait for 99 more blocks to be mined before the coinbase outputs can be spent.
		// So, for instance an output from the coinbase transaction in block 9 can be spent in block 109.
		commonBins = append(commonBins, aerospike.NewBin("spendingHeight", aerospike.NewIntegerValue(int(blockHeight+100))))
	}

	// Split utxos into batches
	batches := s.splitIntoBatches(utxos, commonBins)

	batches[0] = append(batches[0], aerospike.NewBin("nrRecords", aerospike.NewIntegerValue(len(batches))))
	batches[0] = append(batches[0], aerospike.NewBin("blockIDs", blockIDs))

	if len(batches) > 1 {
		// if we have more than one batch, we opt to store the transaction externally
		external = true
	}

	if external {
		batches[0] = append(batches[0], aerospike.NewBin("external", true))
	} else {
		batches[0] = append(batches[0], aerospike.NewBin("inputs", inputs))
		batches[0] = append(batches[0], aerospike.NewBin("outputs", outputs))
	}

	return batches, nil
}

func (s *Store) storeTransactionExternally(bItem *batchStoreItem, binsToStore [][]*aerospike.Bin) {
	if err := s.externalStore.Set(
		context.TODO(),
		bItem.txHash[:],
		bItem.tx.ExtendedBytes(),
		options.WithFileExtension("tx"),
	); err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			utils.SafeSend[error](bItem.done, errors.NewTxAlreadyExistsError("error writing transaction to external store [%s]", bItem.txHash.String(), err))
		} else {
			utils.SafeSend[error](bItem.done, errors.NewStorageError("error writing transaction to external store [%s]", bItem.txHash.String(), err))

			return
		}
	}

	// Get a new write policy which will allow CREATE or UPDATE
	wPolicy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

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
					utils.SafeSend[error](bItem.done, errors.NewTxAlreadyExistsError("[storeTransactionExternally] %v already exists in store", bItem.txHash))

					return
				}
			}

			utils.SafeSend[error](bItem.done, errors.NewProcessingError("could not put bins (extended mode) to store", err))

			return
		}
	}

	utils.SafeSend(bItem.done, nil)
}

func (s *Store) storePartialTransactionExternally(bItem *batchStoreItem, binsToStore [][]*aerospike.Bin) {
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

	if err := s.externalStore.Set(
		context.TODO(),
		bItem.txHash[:],
		wrapper.Bytes(),
		options.WithFileExtension("outputs"),
	); err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			utils.SafeSend[error](bItem.done, errors.NewTxAlreadyExistsError("error writing output to external store [%s]", bItem.txHash.String(), err))
		} else {
			utils.SafeSend[error](bItem.done, errors.NewStorageError("error writing output to external store [%s]", bItem.txHash.String(), err))

			return
		}
	}

	// Get a new write policy which will allow CREATE or UPDATE
	wPolicy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

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
					utils.SafeSend[error](bItem.done, errors.NewTxAlreadyExistsError("[storePartialTransactionExternally] %v already exists in store", bItem.txHash))

					return
				}
			}

			utils.SafeSend[error](bItem.done, errors.NewProcessingError("could not put bins (extended mode) to store", err))

			return
		}
	}

	utils.SafeSend(bItem.done, nil)
}
