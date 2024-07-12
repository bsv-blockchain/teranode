// //go:build aerospike

package aerospike2

import (
	"context"
	"math"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

// Used for NOOP batch operations
var placeholderKey *aerospike.Key

type batchStoreItem struct {
	tx       *bt.Tx
	lockTime uint32
	done     chan error
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockIDs ...uint32) (*meta.Data, error) {
	startTotal, stat, _ := util.StartStatFromContext(ctx, "Create")

	defer func() {
		stat.AddTime(startTotal)
	}()

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "failed to get tx meta data", err)
	}

	errCh := make(chan error)
	defer close(errCh)

	item := &batchStoreItem{tx: tx, lockTime: tx.LockTime, done: errCh}

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
	batchPolicy := util.GetAerospikeBatchPolicy()

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	s.logger.Debugf("[STORE_BATCH] sending batch of %d txMetas", len(batch))

	var hash *chainhash.Hash
	var key *aerospike.Key
	var binsToStore [][]*aerospike.Bin
	var err error

	blockHeight, err := s.GetBlockHeight()
	if err != nil {
		s.logger.Warnf("Could not get block height, using Genesis activation height")
		blockHeight = util.GenesisActivationHeight
	}

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

		binsToStore, err = getBinsToStore(bItem.tx, blockHeight, false) // false is to say this is a normal record, not external.
		if err != nil {
			utils.SafeSend[error](bItem.done, errors.New(errors.ERR_PROCESSING, "could not get bins to store", err))
			//NOOP for this record
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)
			continue
		}

		if len(binsToStore) > 1 {
			// Make this batch item a NOOP and persist all of these to be written via a queue
			batchRecords[idx] = aerospike.NewBatchRead(nil, placeholderKey, nil)

			go func(binsToStore [][]*aerospike.Bin) {
				if err := s.externalStore.Set(
					context.TODO(),
					batch[idx].tx.TxIDChainHash()[:],
					batch[idx].tx.Bytes(),
					options.WithSubDirectory("legacy"),
					options.WithFileExtension("tx"),
				); err != nil {
					utils.SafeSend[error](batch[idx].done, errors.New(errors.ERR_STORAGE_ERROR, "[STORE_BATCH][%s:%d] error in external store batch record for tx: %w", batch[idx].tx.TxIDChainHash().String(), idx, err))
					return
				}

				for i, bins := range binsToStore {
					keySource := calculateKeySource(bItem.tx.TxIDChainHash(), uint32(i))

					extendedKey, err := aerospike.NewKey(s.namespace, s.setName, keySource)
					if err != nil {
						utils.SafeSend[error](bItem.done, err)
						return
					}

					if err := s.client.PutBins(nil, extendedKey, bins...); err != nil {
						utils.SafeSend[error](bItem.done, errors.New(errors.ERR_STORAGE_ERROR, "could not put bins to store", err))
						return
					}
				}

				utils.SafeSend(bItem.done, nil)
			}(binsToStore)

			continue
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

	utxoBatchSize, _ := gocore.Config().GetInt("utxoBatchSize", 20_000)

	// batchOperate may have no errors, but some of the records may have failed
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			aErr, ok := err.(*aerospike.AerospikeError)
			if ok {
				if aErr.ResultCode == types.KEY_EXISTS_ERROR {
					s.logger.Warnf("[STORE_BATCH][%s:%d] tx already exists in batch %d, skipping", batch[idx].tx.TxIDChainHash().String(), idx, batchId)
					utils.SafeSend[error](batch[idx].done, errors.New(errors.ERR_TX_ALREADY_EXISTS, "%v already exists in store", batch[idx].tx.TxIDChainHash()))
					continue
				}

				if aErr.ResultCode == types.RECORD_TOO_BIG {
					if err := s.externalStore.Set(
						context.TODO(),
						batch[idx].tx.TxIDChainHash()[:],
						batch[idx].tx.Bytes(),
						options.WithSubDirectory("legacy"),
						options.WithFileExtension("tx"),
					); err != nil {
						utils.SafeSend[error](batch[idx].done, errors.New(errors.ERR_STORAGE_ERROR, "[STORE_BATCH][%s:%d] error in aerospike store batch record for tx: %d - %w", batch[idx].tx.TxIDChainHash().String(), idx, batchId, err))
						continue
					}

					binsToStore, err = getBinsToStore(batch[idx].tx, blockHeight, true) // true is to say this is a big record
					if err != nil {
						utils.SafeSend[error](batch[idx].done, errors.New(errors.ERR_PROCESSING, "could not get bins to store", err))
						continue
					}

				OUTER:
					for i, bins := range binsToStore {
						keySource := calculateKeySource(batch[idx].tx.TxIDChainHash(), uint32(i))

						extendedKey, err := aerospike.NewKey(s.namespace, s.setName, keySource)
						if err != nil {
							utils.SafeSend[error](batch[idx].done, err)
							continue OUTER
						}

						putOps := make([]*aerospike.Operation, len(bins))
						for i, bin := range bins {
							putOps[i] = aerospike.PutOp(bin)
						}

						if err := s.client.PutBins(nil, extendedKey, bins...); err != nil {
							utils.SafeSend[error](batch[idx].done, errors.New(errors.ERR_PROCESSING, "could not put bins (big mode) to store", err))
							continue OUTER
						}
					}

					utils.SafeSend(batch[idx].done, nil)

					continue
				}

				if aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
					// This is a NOOP record and the done channel will be called by the external process
					continue
				}

				utils.SafeSend[error](batch[idx].done, errors.New(errors.ERR_STORAGE_ERROR, "[STORE_BATCH][%s:%d] error in aerospike store batch record for tx (will retry): %d - %w", batch[idx].tx.TxIDChainHash().String(), idx, batchId, err))
			}
		} else {
			if len(batch[idx].tx.Outputs) <= utxoBatchSize {
				// We notify the done channel that the operation was successful, except
				// if this item was offloaded to the multi-record queue
				utils.SafeSend(batch[idx].done, nil)
			}
		}
	}
}

func splitIntoBatches(utxos []interface{}, batchSize int, commonBins []*aerospike.Bin) [][]*aerospike.Bin {
	var batches [][]*aerospike.Bin
	for start := 0; start < len(utxos); start += batchSize {
		end := start + batchSize
		if end > len(utxos) {
			end = len(utxos)
		}
		batchUtxos := utxos[start:end]
		batch := append([]*aerospike.Bin(nil), commonBins...)
		batch = append(batch, aerospike.NewBin("utxos", aerospike.NewListValue(batchUtxos)))
		batch = append(batch, aerospike.NewBin("nrUtxos", aerospike.NewIntegerValue(len(batchUtxos))))
		batches = append(batches, batch)
	}
	return batches
}

func getBinsToStore(tx *bt.Tx, blockHeight uint32, external bool, blockIDs ...uint32) ([][]*aerospike.Bin, error) {
	fee, utxoHashes, err := utxo.GetFeesAndUtxoHashes(context.Background(), tx, blockHeight)
	if err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", err.Error()).Inc()
		return nil, errors.New(errors.ERR_PROCESSING, "failed to get fees and utxo hashes for %s: %v", tx.TxIDChainHash(), err)
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

		if !output.LockingScript.IsData() {
			utxos[i] = aerospike.NewBytesValue(utxoHashes[i][:])
		}
	}

	if len(blockIDs) == 0 {
		blockIDs = make([]uint32, 0)
	}

	commonBins := []*aerospike.Bin{
		aerospike.NewBin("version", aerospike.NewIntegerValue(int(tx.Version))),
		aerospike.NewBin("locktime", aerospike.NewIntegerValue(int(tx.LockTime))),
		aerospike.NewBin("fee", aerospike.NewIntegerValue(int(fee))),
		aerospike.NewBin("sizeInBytes", aerospike.NewIntegerValue(tx.Size())),
		aerospike.NewBin("spentUtxos", aerospike.NewIntegerValue(0)),
		aerospike.NewBin("blockIDs", blockIDs),
		aerospike.NewBin("isCoinbase", tx.IsCoinbase()),
	}

	if tx.IsCoinbase() {
		commonBins = append(commonBins, aerospike.NewBin("spendingHeight", aerospike.NewIntegerValue(int(blockHeight+100))))
	}

	// Split utxos into batches
	utxoBatchSize, _ := gocore.Config().GetInt("utxoBatchSize", 20_000) // This should never be overwritten in settings.  It is a setting to allow testing to set a different value
	batches := splitIntoBatches(utxos, utxoBatchSize, commonBins)

	batches[0] = append(batches[0], aerospike.NewBin("nrRecords", aerospike.NewIntegerValue(len(batches))))

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
