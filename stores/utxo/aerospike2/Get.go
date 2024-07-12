// //go:build aerospike

package aerospike2

import (
	"bytes"
	"context"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/exp/slices"
)

type batchGetItemData struct {
	Data *meta.Data
	Err  error
}

type batchGetItem struct {
	hash   chainhash.Hash
	fields []string
	done   chan batchGetItemData
}

func (s *Store) GetSpend(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	if s.utxoBatchSize == 0 {
		panic("aerospike utxo store initialised without specifying a non-zero utxoBatchSize")
	}

	prometheusUtxoMapGet.Inc()

	keySource := calculateKeySource(spend.TxID, spend.Vout/uint32(s.utxoBatchSize))

	key, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Get", aErr.Error()).Inc()
		s.logger.Errorf("Failed to init new aerospike key: %v\n", aErr)
		return nil, aErr
	}

	policy := util.GetAerospikeReadPolicy()
	policy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	value, aErr := s.client.Get(policy, key, binNames...)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Get", aErr.Error()).Inc()
		if errors.Is(aErr, aerospike.ErrKeyNotFound) {
			return &utxo.SpendResponse{
				Status: int(utxostore.Status_NOT_FOUND),
			}, nil
		}
		s.logger.Errorf("Failed to get aerospike key: %v\n", aErr)
		return nil, aErr
	}

	var err error
	var spendingTxId *chainhash.Hash

	if value != nil {
		utxos, ok := value.Bins["utxos"].([]interface{})
		if ok {
			b, ok := utxos[spend.Vout].([]byte)
			if ok && len(b) == 64 {
				spendingTxId, err = chainhash.NewHash(b[32:])
				if err != nil {
					return nil, errors.New(errors.ERR_PROCESSING, "chain hash error", err)
				}
			}
		}
	}

	return &utxo.SpendResponse{
		Status:       int(utxostore.CalculateUtxoStatus2(spendingTxId)),
		SpendingTxID: spendingTxId,
	}, nil
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return s.get(ctx, hash, utxo.MetaFields)
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(fields) > 0 {
		bins = fields[0]
	}
	return s.get(ctx, hash, bins)
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []string) (*meta.Data, error) {

	bins = s.addAbstractedBins(bins)

	done := make(chan batchGetItemData)
	item := &batchGetItem{hash: *hash, fields: bins, done: done}

	if s.getBatcher != nil {
		s.getBatcher.Put(item)
	} else {
		// if the batcher is disabled, we still want to process the request in a go routine
		go func() {
			s.sendGetBatch([]*batchGetItem{item})
		}()
	}

	data := <-done
	if data.Err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Get", data.Err.Error()).Inc()
	} else {
		prometheusTxMetaAerospikeMapGet.Inc()
	}
	return data.Data, data.Err
}

func (s *Store) getTxFromBins(bins aerospike.BinMap) (*bt.Tx, error) {
	tx := &bt.Tx{
		Version:  uint32(bins["version"].(int)),
		LockTime: uint32(bins["locktime"].(int)),
	}
	inputInterfaces, ok := bins["inputs"].([]interface{})
	if ok {
		tx.Inputs = make([]*bt.Input, len(inputInterfaces))
		for i, inputInterface := range inputInterfaces {
			input := inputInterface.([]byte)
			tx.Inputs[i] = &bt.Input{}
			_, err := tx.Inputs[i].ReadFromExtended(bytes.NewReader(input))
			if err != nil {
				return nil, errors.New(errors.ERR_TX_INVALID, "could not read input: %v", err)
			}
		}
	}

	outputInterfaces, ok := bins["outputs"].([]interface{})
	if ok {
		tx.Outputs = make([]*bt.Output, len(outputInterfaces))
		for i, outputInterface := range outputInterfaces {
			output := outputInterface.([]byte)
			tx.Outputs[i] = &bt.Output{}
			_, err := tx.Outputs[i].ReadFrom(bytes.NewReader(output))
			if err != nil {
				return nil, errors.New(errors.ERR_TX_INVALID, "could not read output: %v", err)
			}
		}
	}

	return tx, nil
}

func (s *Store) addAbstractedBins(bins []string) []string {
	// add missing bins
	if slices.Contains(bins, "parentTxHashes") {
		if !slices.Contains(bins, "inputs") {
			bins = append(bins, "inputs")
			bins = append(bins, "external")
		}
	}
	if slices.Contains(bins, "tx") {
		if !slices.Contains(bins, "inputs") {
			bins = append(bins, "inputs")
		}
		if !slices.Contains(bins, "outputs") {
			bins = append(bins, "outputs")
		}
		if !slices.Contains(bins, "version") {
			bins = append(bins, "version")
		}
		if !slices.Contains(bins, "locktime") {
			bins = append(bins, "locktime")
		}
		if !slices.Contains(bins, "external") {
			bins = append(bins, "external")
		}
	}
	return bins
}

func (s *Store) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, fields ...string) error {
	batchPolicy := util.GetAerospikeBatchPolicy()
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(items))

	for idx, item := range items {
		key, err := aerospike.NewKey(s.namespace, s.setName, item.Hash[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "failed to init new aerospike key for txMeta", err)
		}

		bins := []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"}
		if len(item.Fields) > 0 {
			bins = item.Fields
		} else if len(fields) > 0 {
			bins = fields
		}

		item.Fields = s.addAbstractedBins(bins)

		record := aerospike.NewBatchRead(policy, key, item.Fields)
		// Add to batch
		batchRecords[idx] = record
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike map store batch records: %w", err)
	}

	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			items[idx].Data = nil
			if !model.CoinbasePlaceholderHash.Equal(items[idx].Hash) {
				if errors.Is(err, aerospike.ErrKeyNotFound) {
					items[idx].Err = errors.New(errors.ERR_TX_NOT_FOUND, "%v not found", items[idx].Hash)
				} else {
					items[idx].Err = err
				}
			}
		} else {
			bins := batchRecord.BatchRec().Record.Bins

			items[idx].Data = &meta.Data{}

			externalTx := &bt.Tx{}

			external, ok := bins["external"].(bool)
			if ok && external {
				// Get the raw transaction from the externalStore...
				reader, err := s.externalStore.GetIoReader(
					context.TODO(),
					items[idx].Hash[:],
					options.WithSubDirectory("legacy"),
					options.WithFileExtension("tx"),
				)
				if err != nil {
					items[idx].Err = errors.New(errors.ERR_STORAGE_ERROR, "could not get tx from external store", err)
					continue
				}

				_, err = externalTx.ReadFrom(reader)
				if err != nil {
					items[idx].Err = errors.New(errors.ERR_TX_INVALID, "could not read tx from reader", err)
					continue
				}
			}

			for _, key := range items[idx].Fields {
				value := bins[key]
				switch key {
				case "tx":
					if external {
						items[idx].Data.Tx = externalTx
					} else {
						tx, txErr := s.getTxFromBins(bins)
						if txErr != nil {
							return errors.New(errors.ERR_TX_INVALID, "invalid tx: %v", txErr)
						}
						items[idx].Data.Tx = tx
					}
				case "fee":
					fee, ok := value.(int)
					if ok {
						items[idx].Data.Fee = uint64(fee)
					}
				case "sizeInBytes":
					sizeInBytes, ok := value.(int)
					if ok {
						items[idx].Data.SizeInBytes = uint64(sizeInBytes)
					}
				case "parentTxHashes":
					if external {
						items[idx].Data.ParentTxHashes = make([]chainhash.Hash, len(externalTx.Inputs))
						for i, input := range externalTx.Inputs {
							items[idx].Data.ParentTxHashes[i] = *input.PreviousTxIDChainHash()
						}
					} else {
						inputInterfaces, ok := bins["inputs"].([]interface{})
						if ok {
							items[idx].Data.ParentTxHashes = make([]chainhash.Hash, len(inputInterfaces))
							for i, inputInterface := range inputInterfaces {
								input := inputInterface.([]byte)
								items[idx].Data.ParentTxHashes[i] = chainhash.Hash(input[:32])
							}
						}
					}
				case "blockIDs":
					temp := value.([]interface{})
					var blockIDs []uint32
					for _, val := range temp {
						blockIDs = append(blockIDs, uint32(val.(int)))
					}
					items[idx].Data.BlockIDs = blockIDs
				case "isCoinbase":
					coinbaseBool, ok := value.(bool)
					if ok {
						items[idx].Data.IsCoinbase = coinbaseBool
					}
				}
			}
		}
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))

	return nil
}

func (s *Store) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	var err error

	batchPolicy := util.GetAerospikeBatchPolicy()
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(outpoints))

	for idx, item := range outpoints {
		key, err := aerospike.NewKey(s.namespace, s.setName, item.PreviousTxID[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "failed to init new aerospike key for txMeta: %w", err)
		}

		bins := []string{"version", "locktime", "inputs", "outputs", "external"}
		record := aerospike.NewBatchRead(policy, key, bins)
		// Add to batch
		batchRecords[idx] = record
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike map store batch records: %w", err)
	}

	for idx, batchRecordIfc := range batchRecords {
		batchRecord := batchRecordIfc.BatchRec()
		if batchRecord.Err != nil {
			return errors.New(errors.ERR_PROCESSING, "error in aerospike map store batch record: %w", batchRecord.Err)
		}

		bins := batchRecord.Record.Bins

		previousTx := &bt.Tx{}

		external, ok := bins["external"].(bool)
		if ok && external {
			// Get the raw transaction from the externalStore...
			reader, err := s.externalStore.GetIoReader(
				context.TODO(),
				outpoints[idx].PreviousTxID[:],
				options.WithSubDirectory("legacy"),
				options.WithFileExtension("tx"),
			)
			if err != nil {
				return errors.New(errors.ERR_STORAGE_ERROR, "could not get tx from external store", err)
			}

			_, err = previousTx.ReadFrom(reader)
			if err != nil {
				return errors.New(errors.ERR_TX_INVALID, "could not read tx from reader", err)
			}

		} else {
			previousTx, err = s.getTxFromBins(bins)
			if err != nil {
				return errors.New(errors.ERR_TX_INVALID, "invalid tx: %v", err)
			}
		}

		outpoints[idx].Satoshis = previousTx.Outputs[outpoints[idx].Vout].Satoshis
		outpoints[idx].LockingScript = *previousTx.Outputs[outpoints[idx].Vout].LockingScript
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))

	return nil
}

func (s *Store) sendGetBatch(batch []*batchGetItem) {
	items := make([]*utxo.UnresolvedMetaData, 0, len(batch))
	for idx, item := range batch {
		items = append(items, &utxo.UnresolvedMetaData{
			Hash:   item.hash,
			Idx:    idx,
			Fields: item.fields,
		})
	}

	retries := 0
	for {
		if err := s.BatchDecorate(context.Background(), items); err != nil {
			if retries < 3 {
				retries++
				s.logger.Errorf("failed to get batch of txmeta: %v", err)
				time.Sleep(time.Duration(retries) * time.Second)
				continue
			}

			// mark all items as errored
			for _, bItem := range batch {
				bItem.done <- batchGetItemData{
					Err: err,
				}
			}
			return
		}

		break
	}

	for _, item := range items {
		// send the data back to the original caller
		batch[item.Idx].done <- batchGetItemData{
			Data: item.Data,
			Err:  item.Err,
		}
	}
}
