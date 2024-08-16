// //go:build aerospike

package aerospike

import (
	"context"
	"math"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

// Used for NOOP batch operations
var placeholderKey *aerospike.Key

type batchStoreItem struct {
	tx          *bt.Tx
	blockHeight uint32
	blockIDs    []uint32
	lockTime    uint32
	done        chan error
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, blockIDs ...uint32) (*meta.Data, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "aerospike:Create")
	defer deferFn()

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get tx meta data", err)
	}

	errCh := make(chan error)
	defer close(errCh)

	item := &batchStoreItem{
		tx:          tx,
		blockHeight: blockHeight,
		lockTime:    tx.LockTime,
		blockIDs:    blockIDs,
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
	start := gocore.CurrentTime()
	stat := gocore.NewStat("sendStoreBatch")

	if s.utxoBatchSize == 0 {
		s.utxoBatchSize = defaultUxtoBatchSize
	}

	batchPolicy := util.GetAerospikeBatchPolicy()

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	s.logger.Debugf("[STORE_BATCH] sending batch of %d txMetas", len(batch))

	var hash *chainhash.Hash
	var key *aerospike.Key
	var binsToStore [][]*aerospike.Bin
	var err error

	for idx, bItem := range batch {
		hash = bItem.tx.TxIDChainHash()
		key, err = aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			utils.SafeSend(bItem.done, err)
			//NOOP for this record
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		// We calculate the bin that we want to store, but we may get back lots of bin batches
		// because we have had to split the UTXOs into multiple records

		binsToStore, err = s.getBinsToStore(bItem.tx, bItem.blockHeight, bItem.blockIDs, s.externalizeAllTransactions) // false is to say this is a normal record, not external.
		if err != nil {
			utils.SafeSend[error](bItem.done, errors.NewProcessingError("could not get bins to store", err))
			//NOOP for this record
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		start = stat.NewStat("getBinsToStore").AddTime(start)

		if len(binsToStore) > 1 {
			// Make this batch item a NOOP and persist all of these to be written via a queue
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			go s.storeTransactionExternally(batch[idx], binsToStore)

			continue
		} else if s.externalizeAllTransactions {
			// store the tx data externally, it is not in our aerospike record
			if err = s.externalStore.Set(
				context.Background(),
				bItem.tx.TxIDChainHash()[:],
				bItem.tx.Bytes(),
				options.WithFileExtension("tx"),
			); err != nil {
				utils.SafeSend[error](bItem.done, errors.NewStorageError("error writing transaction to external store [%s]: %v", bItem.tx.TxIDChainHash().String(), err))
				continue
			}
		}

		putOps := make([]*aerospike.Operation, len(binsToStore[0]))
		for i, bin := range binsToStore[0] {
			putOps[i] = aerospike.PutOp(bin)
		}

		record := aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
		batchRecords[idx] = record
	}

	batchId := s.batchId.Add(1)

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[STORE_BATCH][batch:%d] error in aerospike map store batch records: %w", batchId, err)
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
					s.logger.Warnf("[STORE_BATCH][%s:%d] tx already exists in batch %d, skipping", batch[idx].tx.TxIDChainHash().String(), idx, batchId)
					utils.SafeSend[error](batch[idx].done, errors.NewTxAlreadyExistsError("%v already exists in store", batch[idx].tx.TxIDChainHash()))
					continue
				}

				if aErr.ResultCode == types.RECORD_TOO_BIG {
					binsToStore, err = s.getBinsToStore(batch[idx].tx, batch[idx].blockHeight, batch[idx].blockIDs, true) // true is to say this is a big record
					if err != nil {
						utils.SafeSend[error](batch[idx].done, errors.NewProcessingError("could not get bins to store", err))
						continue
					}

					go s.storeTransactionExternally(batch[idx], binsToStore)
					continue
				}

				if aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
					// This is a NOOP record and the done channel will be called by the external process
					continue
				}

				utils.SafeSend[error](batch[idx].done, errors.NewStorageError("[STORE_BATCH][%s:%d] error in aerospike store batch record for tx (will retry): %d - %w", batch[idx].tx.TxIDChainHash().String(), idx, batchId, err))
			}
		} else {
			if len(batch[idx].tx.Outputs) <= s.utxoBatchSize {
				// We notify the done channel that the operation was successful, except
				// if this item was offloaded to the multi-record queue
				utils.SafeSend(batch[idx].done, nil)
			}
		}
	}

	stat.NewStat("postBatchOperate").AddTime(start)
}

func (s *Store) splitIntoBatches(utxos []interface{}, commonBins []*aerospike.Bin) [][]*aerospike.Bin {
	if s.utxoBatchSize == 0 {
		s.utxoBatchSize = defaultUxtoBatchSize
	}

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

func (s *Store) getBinsToStore(tx *bt.Tx, blockHeight uint32, blockIDs []uint32, external bool) ([][]*aerospike.Bin, error) {
	fee, utxoHashes, err := utxo.GetFeesAndUtxoHashes(context.Background(), tx, blockHeight)
	if err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", err.Error()).Inc()
		return nil, errors.NewProcessingError("failed to get fees and utxo hashes for %s: %v", tx.TxIDChainHash(), err)
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
		outputs[i] = output.Bytes()

		// store all non-zero utxos and exceptions from pre-genesis
		if utxo.ShouldStoreOutputAsUTXO(output, blockHeight) {
			utxos[i] = aerospike.NewBytesValue(utxoHashes[i][:])
		}
	}

	commonBins := []*aerospike.Bin{
		aerospike.NewBin("version", aerospike.NewIntegerValue(int(tx.Version))),
		aerospike.NewBin("locktime", aerospike.NewIntegerValue(int(tx.LockTime))),
		aerospike.NewBin("fee", aerospike.NewIntegerValue(int(fee))),
		aerospike.NewBin("sizeInBytes", aerospike.NewIntegerValue(tx.Size())),
		aerospike.NewBin("spentUtxos", aerospike.NewIntegerValue(0)),
		aerospike.NewBin("isCoinbase", tx.IsCoinbase()),
	}

	if tx.IsCoinbase() {
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
		bItem.tx.TxIDChainHash()[:],
		bItem.tx.Bytes(),
		options.WithFileExtension("tx"),
	); err != nil {
		utils.SafeSend[error](bItem.done, errors.NewStorageError("error writing transaction to external store [%s]: %v", bItem.tx.TxIDChainHash().String(), err))
		return
	}

	// Get a new write policy which will allow CREATE or UPDATE
	wPolicy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

	txid := bItem.tx.TxIDChainHash()

	for i := len(binsToStore) - 1; i >= 0; i-- {
		bins := binsToStore[i]

		if i == 0 {
			// For the "master" record, set the write policy to CREATE_ONLY
			wPolicy.RecordExistsAction = aerospike.CREATE_ONLY
		}

		keySource := uaerospike.CalculateKeySource(txid, uint32(i))

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
			utils.SafeSend[error](bItem.done, errors.NewProcessingError("could not put bins (extended mode) to store", err))
			return
		}
	}

	utils.SafeSend(bItem.done, nil)
}
