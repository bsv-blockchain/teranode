// //go:build aerospike

package aerospike

import (
	"bytes"
	"context"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"golang.org/x/exp/slices"
)

var (
	gocoreStat                  = gocore.NewStat("Aerospike")
	externalTxCache             = expiringmap.New[chainhash.Hash, *bt.Tx](1 * time.Minute)
	previousOutputsDecorateStat = gocoreStat.NewStat("PreviousOutputsDecorate").AddRanges(0, 1, 100, 1_000, 10_000, 100_000)
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

type batchOutpoint struct {
	outpoint *meta.PreviousOutput
	errCh    chan error
}

func (s *Store) GetSpend(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	prometheusUtxoMapGet.Inc()

	// nolint: gosec
	keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/uint32(s.utxoBatchSize))

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
				Status: int(utxo.Status_NOT_FOUND),
			}, nil
		}

		s.logger.Errorf("Failed to get aerospike key: %v\n", aErr)

		return nil, aErr
	}

	var (
		err          error
		spendingTxID *chainhash.Hash
	)

	if value != nil {
		utxos, ok := value.Bins["utxos"].([]interface{})
		if ok {
			b, ok := utxos[spend.Vout].([]byte)
			if ok {
				if len(b) < 32 {
					return nil, errors.NewProcessingError("invalid utxo hash length", nil)
				}

				// check utxoHash is the same as the one we expect
				utxoHash := chainhash.Hash(b[:32])
				if !utxoHash.IsEqual(spend.UTXOHash) {
					return nil, errors.NewProcessingError("utxo hash mismatch", nil)
				}

				if len(b) == 64 {
					spendingTxID, err = chainhash.NewHash(b[32:])
					if err != nil {
						return nil, errors.NewProcessingError("chain hash error", err)
					}
				}
			}
		}
	}

	utxoStatus := utxo.CalculateUtxoStatus2(spendingTxID)

	// check if frozen
	if spendingTxID != nil && spendingTxID.IsEqual((*chainhash.Hash)(frozenUTXOBytes)) {
		utxoStatus = utxo.Status_FROZEN
		spendingTxID = nil
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingTxID: spendingTxID,
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
	// nolint: gosec
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
				return nil, errors.NewTxInvalidError("could not read input: %v", err)
			}
		}
	}

	outputInterfaces, ok := bins["outputs"].([]interface{})
	if ok {
		tx.Outputs = make([]*bt.Output, len(outputInterfaces))

		for i, outputInterface := range outputInterfaces {
			if outputInterface == nil {
				continue
			}

			output := outputInterface.([]byte)
			tx.Outputs[i] = &bt.Output{}

			_, err := tx.Outputs[i].ReadFrom(bytes.NewReader(output))
			if err != nil {
				return nil, errors.NewTxInvalidError("could not read output: %v", err)
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
	var err error

	batchPolicy := util.GetAerospikeBatchPolicy()
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(items))

	for idx, item := range items {
		key, err := aerospike.NewKey(s.namespace, s.setName, item.Hash[:])
		if err != nil {
			return errors.NewProcessingError("failed to init new aerospike key for txMeta", err)
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

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.NewStorageError("error in aerospike map store batch records: %w", err)
	}

	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			items[idx].Data = nil
			if !model.CoinbasePlaceholderHash.Equal(items[idx].Hash) {
				if errors.Is(err, aerospike.ErrKeyNotFound) {
					items[idx].Err = errors.NewTxNotFoundError("%v not found", items[idx].Hash)
				} else {
					items[idx].Err = err
				}
			}
		} else {
			bins := batchRecord.BatchRec().Record.Bins

			items[idx].Data = &meta.Data{}

			var externalTx *bt.Tx

			external, ok := bins["external"].(bool)
			if ok && external {
				if externalTx, err = s.getTxFromExternalStore(items[idx].Hash); err != nil {
					items[idx].Err = err

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
							return errors.NewTxInvalidError("invalid tx: %v", txErr)
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
						// nolint: gosec
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

func (s *Store) PreviousOutputsDecorate(_ context.Context, outpoints []*meta.PreviousOutput) error {
	errChans := make([]chan error, len(outpoints))

	for i, outpoint := range outpoints {
		errChan := make(chan error, 1)
		errChans[i] = errChan

		// Wrap the outpoint in OutpointRequest and put it in the batcher
		s.outpointBatcher.Put(&batchOutpoint{
			outpoint: outpoint,
			errCh:    errChan,
		})
	}

	// Wait for all error channels to receive a result
	for _, errChan := range errChans {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) sendOutpointBatch(batch []*batchOutpoint) {
	start := gocore.CurrentTime()
	defer func() {
		previousOutputsDecorateStat.AddTimeForRange(start, len(batch))
	}()

	var err error

	batchPolicy := util.GetAerospikeBatchPolicy()
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy()

	// Create a batch of records to read, with a max size of the batch
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	batchRecordHashes := make([]chainhash.Hash, 0, len(batch))

	// we de-dupe the txs we need to lookup, since we may have multiple outpoints for the same tx
	// this is done by using a map of txHashes
	uniqueTxHashes := make(map[chainhash.Hash]struct{})
	for _, item := range batch {
		uniqueTxHashes[item.outpoint.PreviousTxID] = struct{}{}
	}

	// Create a batch of records to read from the txHashes
	for txHash := range uniqueTxHashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, txHash[:])
		if err != nil {
			for _, item := range batch {
				sendErrorAndClose(item.errCh, errors.NewProcessingError("failed to init new aerospike key for txMeta: %w", err))
			}

			return
		}

		bins := []string{"version", "locktime", "inputs", "outputs", "external"}
		record := aerospike.NewBatchRead(policy, key, bins)

		// Add to batch records
		batchRecords = append(batchRecords, record)
		batchRecordHashes = append(batchRecordHashes, txHash)
	}

	// send the batch to aerospike
	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		for _, item := range batch {
			sendErrorAndClose(item.errCh, errors.NewStorageError("error in aerospike send outpoint batch records: %w", err))
		}

		return
	}

	txs := make(map[chainhash.Hash]*bt.Tx, len(batchRecords))
	txErrors := make(map[chainhash.Hash]error)

	// Process the batch records
	for idx, batchRecordIfc := range batchRecords {
		previousTxHash := batchRecordHashes[idx]

		batchRecord := batchRecordIfc.BatchRec()
		if batchRecord.Err != nil {
			if errors.Is(batchRecord.Err, aerospike.ErrKeyNotFound) {
				txErrors[previousTxHash] = errors.NewTxNotFoundError("could not find transaction in aerospike: %w", batchRecord.Err)
			} else {
				txErrors[previousTxHash] = errors.NewProcessingError("error in aerospike get outpoint batch record: %w", batchRecord.Err)
			}

			continue
		}

		bins := batchRecord.Record.Bins

		var previousTx *bt.Tx

		external, ok := bins["external"].(bool)
		if ok && external {
			if previousTx, err = s.getTxFromExternalStore(previousTxHash); err != nil {
				txErrors[previousTxHash] = err

				continue
			}
		} else {
			previousTx, err = s.getTxFromBins(bins)
			if err != nil {
				txErrors[previousTxHash] = errors.NewTxInvalidError("invalid tx: %v", err)

				continue
			}
		}

		txs[previousTxHash] = previousTx
	}

	// Now we have all the txs, we can decorate the outpoints
	for _, batchItem := range batch {
		previousTx := txs[batchItem.outpoint.PreviousTxID]
		if previousTx == nil {
			if err, ok := txErrors[batchItem.outpoint.PreviousTxID]; ok {
				sendErrorAndClose(batchItem.errCh, err)
			} else {
				sendErrorAndClose(batchItem.errCh, errors.NewTxNotFoundError("previous tx not found: %v", batchItem.outpoint.PreviousTxID))
			}

			continue
		}

		batchItem.outpoint.Satoshis = previousTx.Outputs[batchItem.outpoint.Vout].Satoshis
		batchItem.outpoint.LockingScript = *previousTx.Outputs[batchItem.outpoint.Vout].LockingScript
		batchItem.errCh <- nil
		close(batchItem.errCh)
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))
}

func sendErrorAndClose(errCh chan error, err error) {
	select {
	case errCh <- err:
	default:
	}
	close(errCh)
}

func (s *Store) getTxFromExternalStore(previousTxHash chainhash.Hash) (*bt.Tx, error) {
	// Check the cache first
	if tx, ok := externalTxCache.Get(previousTxHash); ok {
		return tx, nil
	}

	ext := "tx"

	// Get the raw transaction from the externalStore...
	reader, err := s.externalStore.GetIoReader(
		context.TODO(),
		previousTxHash[:],
		options.WithFileExtension(ext),
	)
	if err != nil {
		// Try to get the data from an output file instead
		ext = "outputs"

		reader, err = s.externalStore.GetIoReader(
			context.TODO(),
			previousTxHash[:],
			options.WithFileExtension(ext),
		)
		if err != nil {
			return nil, errors.NewStorageError("could not get tx from external store", err)
		}
	}

	tx := &bt.Tx{}

	if ext == "tx" {
		if _, err = tx.ReadFrom(reader); err != nil {
			return nil, errors.NewTxInvalidError("could not read tx from reader: %w", err)
		}
	} else {
		var uw *utxopersister.UTXOWrapper

		uw, err := utxopersister.NewUTXOWrapperFromReader(context.Background(), reader)
		if err != nil {
			return nil, errors.NewTxInvalidError("could not read outputs from reader: %w", err)
		}

		utxos := utxopersister.PadUTXOsWithNil(uw.UTXOs)

		tx.Outputs = make([]*bt.Output, len(utxos))

		for _, u := range uw.UTXOs {
			s := bscript.NewFromBytes(u.Script)

			tx.Outputs[u.Index] = &bt.Output{
				Satoshis:      u.Value,
				LockingScript: s,
			}
		}
	}

	// Cache the tx
	externalTxCache.Set(previousTxHash, tx)

	return tx, nil
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
