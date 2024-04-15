// //go:build aerospike

package aerospikemap

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/ubsv/model"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusTxMetaAerospikeMapGet       prometheus.Counter
	prometheusTxMetaAerospikeMapStore     prometheus.Counter
	prometheusTxMetaAerospikeMapSetMined  prometheus.Counter
	prometheusTxMetaAerospikeMapDelete    prometheus.Counter
	prometheusTxMetaAerospikeMapErrors    *prometheus.CounterVec
	prometheusTxMetaAerospikeMapGetMulti  prometheus.Counter
	prometheusTxMetaAerospikeMapGetMultiN prometheus.Counter

	prometheusTxMetaAerospikeMapSetMinedBatch     prometheus.Counter
	prometheusTxMetaAerospikeMapSetMinedBatchN    prometheus.Counter
	prometheusTxMetaAerospikeMapSetMinedBatchErrN prometheus.Counter
)

func init() {
	prometheusTxMetaAerospikeMapGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_get",
			Help: "Number of txmeta get calls done to aerospike",
		},
	)
	prometheusTxMetaAerospikeMapStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_store",
			Help: "Number of txmeta set calls done to aerospike",
		},
	)
	prometheusTxMetaAerospikeMapSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined",
			Help: "Number of txmeta set_mined calls done to aerospike",
		},
	)
	prometheusTxMetaAerospikeMapDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_delete",
			Help: "Number of txmeta delete calls done to aerospike",
		},
	)
	prometheusTxMetaAerospikeMapErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_errors",
			Help: "Number of txmeta map errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)
	prometheusTxMetaAerospikeMapGetMulti = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_get_multi",
			Help: "Number of txmeta get_multi calls done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapGetMultiN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_get_multi_n",
			Help: "Number of txmeta get_multi txs done to aerospike map",
		},
	)

	prometheusTxMetaAerospikeMapSetMinedBatch = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined_batch",
			Help: "Number of txmeta set_mined_batch calls done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapSetMinedBatchN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined_batch_n",
			Help: "Number of txmeta set_mined_batch txs done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapSetMinedBatchErrN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined_batch_err_n",
			Help: "Number of txmeta set_mined_batch txs errors to aerospike map",
		},
	)

	if gocore.Config().GetBool("aerospike_debug", true) {
		asl.Logger.SetLevel(asl.DEBUG)
	}

}

type batchStoreItem struct {
	tx     *bt.Tx
	txMeta *txmeta.Data
	done   chan error
}

type batchGetItemData struct {
	Data *txmeta.Data
	Err  error
}

type batchGetItem struct {
	hash   chainhash.Hash
	fields []string
	done   chan batchGetItemData
}

type Store struct {
	client       *uaerospike.Client
	namespace    string
	setName      string
	expiration   uint32
	logger       ulogger.Logger
	batchId      atomic.Uint64
	storeBatcher *batcher.Batcher2[batchStoreItem]
	getBatcher   *batcher.Batcher2[batchGetItem]
}

func New(logger ulogger.Logger, u *url.URL) (*Store, error) {
	namespace := u.Path[1:]

	client, err := util.GetAerospikeClient(logger, u)
	if err != nil {
		return nil, err
	}

	expiration := uint32(0)
	expirationValue := u.Query().Get("expiration")
	if expirationValue != "" {
		expiration64, err := strconv.ParseUint(expirationValue, 10, 64)
		if err != nil {
			logger.Fatalf("could not parse expiration %s: %v", expirationValue, err)
		}
		expiration = uint32(expiration64)
	}

	setName := u.Query().Get("set")
	if setName == "" {
		setName = "txmeta"
	}

	s := &Store{
		client:     client,
		namespace:  namespace,
		setName:    setName,
		expiration: expiration,
		logger:     logger,
	}

	storeBatcherEnabled := gocore.Config().GetBool("txmeta_store_storeBatcherEnabled", true)
	if storeBatcherEnabled {
		batchSize, _ := gocore.Config().GetInt("txmeta_store_storeBatcherSize", 256)
		batchDuration, _ := gocore.Config().GetInt("txmeta_store_storeBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.storeBatcher = batcher.New[batchStoreItem](batchSize, duration, s.sendStoreBatch, true)
	}

	getBatcherEnabled := gocore.Config().GetBool("txmeta_store_getBatcherEnabled", true)
	if getBatcherEnabled {
		batchSize, _ := gocore.Config().GetInt("txmeta_store_getBatcherSize", 1024)
		batchDuration, _ := gocore.Config().GetInt("txmeta_store_getBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.getBatcher = batcher.New[batchGetItem](batchSize, duration, s.sendGetBatch, true)
	}

	logger.Infof("[Aerospike] map txmeta store initialised with namespace: %s, set: %s", namespace, setName)

	return s, nil
}

func (s *Store) sendStoreBatch(batch []*batchStoreItem) {
	batchPolicy := util.GetAerospikeBatchPolicy()

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	s.logger.Debugf("[STORE_BATCH] sending batch of %d txMetas", len(batch))

	var hash *chainhash.Hash
	var key *aerospike.Key
	var err error
	var utxoHashes []chainhash.Hash
	for idx, bItem := range batch {
		hash = bItem.tx.TxIDChainHash()
		key, err = aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			bItem.done <- err
			continue
		}

		if bItem.txMeta == nil {
			bItem.txMeta, err = util.TxMetaDataFromTx(bItem.tx)
			if err != nil {
				bItem.done <- err
				continue
			}
		}

		utxoHashes, err = utxostore.GetUtxoHashes(bItem.tx)
		if err != nil {
			prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", err.Error()).Inc()
			s.logger.Errorf("failed to get utxo hashes: %v", err)
			continue
		}

		utxos := make(map[interface{}]interface{})
		for _, utxoHash := range utxoHashes {
			utxos[utxoHash.String()] = aerospike.NewNullValue()
		}

		parentTxHashesInterface := make([]byte, 0, 32*len(bItem.txMeta.ParentTxHashes))
		for _, v := range bItem.txMeta.ParentTxHashes {
			parentTxHashesInterface = append(parentTxHashesInterface, v[:]...)
		}

		blockIDs := make([]uint32, 0)

		putOps := []*aerospike.Operation{
			aerospike.PutOp(aerospike.NewBin("tx", bItem.tx.ExtendedBytes())),
			aerospike.PutOp(aerospike.NewBin("fee", int(bItem.txMeta.Fee))),
			aerospike.PutOp(aerospike.NewBin("sizeInBytes", int(bItem.txMeta.SizeInBytes))),
			aerospike.PutOp(aerospike.NewBin("locktime", int(bItem.tx.LockTime))),
			aerospike.PutOp(aerospike.NewBin("utxos", aerospike.NewMapValue(utxos))),
			aerospike.PutOp(aerospike.NewBin("parentTxHashes", parentTxHashesInterface)),
			aerospike.PutOp(aerospike.NewBin("blockIDs", blockIDs)),
			aerospike.PutOp(aerospike.NewBin("isCoinbase", bItem.tx.IsCoinbase())),
		}

		record := aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
		batchRecords[idx] = record
	}

	batchId := s.batchId.Add(1)

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[STORE_BATCH][batch:%d] error in aerospike map store batch records: %v", batchId, err)
		for _, bItem := range batch {
			bItem.done <- err
		}
	}

	// batchOperate may have no errors, but some of the records may have failed
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_EXISTS_ERROR {
				s.logger.Warnf("[STORE_BATCH][%s:%d] txMeta already exists in batch %d, skipping", batch[idx].tx.TxIDChainHash().String(), idx, batchId)
				batch[idx].done <- txmeta.NewErrTxmetaAlreadyExists(hash)
				continue
			}

			batch[idx].done <- fmt.Errorf("[STORE_BATCH][%s:%d] error in aerospike store batch record for txMeta (will retry): %d - %w", batch[idx].tx.TxIDChainHash().String(), idx, batchId, err)
		} else {
			batch[idx].done <- nil
		}
	}
}

func (s *Store) sendGetBatch(batch []*batchGetItem) {
	items := make([]*txmeta.MissingTxHash, 0, len(batch))
	for idx, item := range batch {
		items = append(items, &txmeta.MissingTxHash{
			Hash:   item.hash,
			Idx:    idx,
			Fields: item.fields,
		})
	}

	retries := 0
	for {
		if err := s.MetaBatchDecorate(context.Background(), items); err != nil {
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

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"})
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"})
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []string) (*txmeta.Data, error) {

	if s.getBatcher != nil {
		done := make(chan batchGetItemData)
		s.getBatcher.Put(&batchGetItem{hash: *hash, fields: bins, done: done})

		data := <-done
		if data.Err != nil {
			prometheusTxMetaAerospikeMapErrors.WithLabelValues("Get", data.Err.Error()).Inc()
		} else {
			prometheusTxMetaAerospikeMapGet.Inc()
		}
		return data.Data, data.Err
	}

	// in the map implementation, we are using the utxo store for the data
	key, aeroErr := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if aeroErr != nil {
		return nil, aeroErr
	}

	var value *aerospike.Record

	readPolicy := util.GetAerospikeReadPolicy()
	readPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	value, aeroErr = s.client.Get(readPolicy, key, bins...)
	if aeroErr != nil {
		return nil, aeroErr
	}

	if value == nil {
		return nil, nil
	}

	var parentTxHashes []chainhash.Hash
	if value.Bins["parentTxHashes"] != nil {
		parentTxHashesInterface, ok := value.Bins["parentTxHashes"].([]byte)
		if ok {
			parentTxHashes = make([]chainhash.Hash, len(parentTxHashesInterface)/32)
			for i := 0; i < len(parentTxHashesInterface); i += 32 {
				parentTxHashes[i] = chainhash.Hash(parentTxHashesInterface[i : i+32])
			}
		}
	}

	var blockIDs []uint32
	if value.Bins["blockIDs"] != nil {
		// convert from []interface{} to []uint32
		for _, v := range value.Bins["blockIDs"].([]interface{}) {
			blockIDs = append(blockIDs, uint32(v.(int)))
		}
	}

	var tx *bt.Tx
	var err error
	if value.Bins["tx"] != nil {
		tx, err = bt.NewTxFromBytes(value.Bins["tx"].([]byte))
		if err != nil {
			return nil, fmt.Errorf("invalid tx: %w", err)
		}
	}

	// transform the aerospike interface{} into the correct types
	status := &txmeta.Data{
		Tx:             tx,
		Fee:            uint64(value.Bins["fee"].(int)),
		SizeInBytes:    uint64(value.Bins["sizeInBytes"].(int)),
		ParentTxHashes: parentTxHashes,
		BlockIDs:       blockIDs,
	}

	if value.Bins["isCoinbase"] != nil {
		status.IsCoinbase = value.Bins["isCoinbase"].(bool)
	}

	return status, nil
}

func (s *Store) MetaBatchDecorate(_ context.Context, items []*txmeta.MissingTxHash, fields ...string) error {
	batchPolicy := util.GetAerospikeBatchPolicy()
	batchPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	policy := util.GetAerospikeBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(items))

	for idx, item := range items {
		key, err := aerospike.NewKey(s.namespace, s.setName, item.Hash[:])
		if err != nil {
			return err
		}

		bins := []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"}
		if len(item.Fields) > 0 {
			bins = item.Fields
		} else if len(fields) > 0 {
			bins = fields
		}

		record := aerospike.NewBatchRead(policy, key, bins)
		// Add to batch
		batchRecords[idx] = record
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return err
	}

	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			items[idx].Data = nil
			if !model.CoinbasePlaceholderHash.Equal(items[idx].Hash) {
				if errors.Is(err, aerospike.ErrKeyNotFound) {
					items[idx].Err = txmeta.NewErrTxmetaNotFound(&items[idx].Hash)
				} else {
					items[idx].Err = err
				}
			}
		} else {
			bins := batchRecord.BatchRec().Record.Bins

			items[idx].Data = &txmeta.Data{}

			for key, value := range bins {
				switch key {
				case "tx":
					txBytes, ok := value.([]byte)
					if ok {
						tx, err := bt.NewTxFromBytes(txBytes)
						if err != nil {
							return fmt.Errorf("could not convert tx bytes to bt.Tx: %w", err)
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
					parentTxHashesInterface, ok := value.([]byte)
					if ok {
						parentTxHashes := make([]chainhash.Hash, len(parentTxHashesInterface)/32)
						for i := 0; i < len(parentTxHashesInterface); i += 32 {
							parentTxHashes[i/32] = chainhash.Hash(parentTxHashesInterface[i : i+32])
						}
						items[idx].Data.ParentTxHashes = parentTxHashes
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

func (s *Store) Create(ctx context.Context, tx *bt.Tx, lockTime ...uint32) (*txmeta.Data, error) {
	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	txMeta, bins, err := getBinsToStore(ctx, tx, storeLockTime)
	if err != nil {
		return nil, err
	}

	if s.storeBatcher != nil {
		done := make(chan error)
		s.storeBatcher.Put(&batchStoreItem{tx: tx, txMeta: txMeta, done: done})

		err = <-done
		if err != nil {
			return nil, err
		}

		prometheusTxMetaAerospikeMapStore.Inc()
		return txMeta, nil
	}

	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.CREATE_ONLY

	key, aeroErr := aerospike.NewKey(s.namespace, s.setName, tx.TxIDChainHash().CloneBytes())
	if aeroErr != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", aeroErr.Error()).Inc()
		s.logger.Errorf("Failed to store new aerospike key: %v\n", aeroErr)
		return nil, aeroErr
	}

	aeroErr = s.client.PutBins(policy, key, bins...)
	if aeroErr != nil {
		var aErr *aerospike.AerospikeError
		if errors.As(aeroErr, &aErr) && aErr.ResultCode == types.KEY_EXISTS_ERROR {
			return nil, txmeta.NewErrTxmetaAlreadyExists(tx.TxIDChainHash())
		}

		return nil, aeroErr
	}

	prometheusTxMetaAerospikeMapStore.Inc()

	return txMeta, nil
}

func (s *Store) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	batchPolicy := util.GetAerospikeBatchPolicy()

	// math.MaxUint32 - 1 does not update expiration of the record
	policy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32-1)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(hashes))

	for idx, hash := range hashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			return fmt.Errorf("aerospike NewKey error: %w", err)
		}
		op := aerospike.ListAppendOp("blockIDs", blockID)
		record := aerospike.NewBatchWrite(policy, key, op)
		// Add to batch
		batchRecords[idx] = record
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return fmt.Errorf("aerospike BatchOperate error: %w", err)
	}

	prometheusTxMetaAerospikeMapSetMinedBatch.Inc()

	okUpdates := 0
	errs := make([]error, 0, len(hashes))
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
				// the tx Meta does not exist anymore, so we do not have to set the mined status
				continue
			}
			errs = append(errs, fmt.Errorf("%s - %v", hashes[idx].String(), err))
		} else {
			okUpdates++
		}
	}

	prometheusTxMetaAerospikeMapSetMinedBatchN.Add(float64(okUpdates))

	if len(errs) > 0 {
		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(len(errs)))
		return fmt.Errorf("aerospike batchRecord errors: %v", errs)
	}

	return nil
}

func (s *Store) SetMined(_ context.Context, hash *chainhash.Hash, blockID uint32) error {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return err
	}

	readPolicy := util.GetAerospikeReadPolicy()
	readPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

	record, err := s.client.Get(readPolicy, key, "blockIDs")
	if err != nil {
		return err
	}

	blockIDs, ok := record.Bins["blockIDs"].([]interface{})
	if !ok {
		blockIDs = make([]interface{}, 0)
	}
	blockIDs = append(blockIDs, blockID)

	bin := aerospike.NewBin("blockIDs", blockIDs)

	err = s.client.PutBins(policy, key, bin)
	if err != nil {
		return err
	}

	prometheusTxMetaAerospikeMapSetMined.Inc()

	return nil
}

func (s *Store) Delete(_ context.Context, _ *chainhash.Hash) error {
	prometheusTxMetaAerospikeMapDelete.Inc()

	// this is a no-op for the map implementation - it is deleted in the utxo store
	return nil
}

func getBinsToStore(ctx context.Context, tx *bt.Tx, lockTime uint32) (*txmeta.Data, []*aerospike.Bin, error) {
	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, nil, err
	}

	utxoHashes, err := utxostore.GetUtxoHashes(tx)
	if err != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", err.Error()).Inc()
		return nil, nil, fmt.Errorf("failed to get fees and utxo hashes: %v", err)
	}

	utxos := make(map[interface{}]interface{})
	for i, utxoHash := range utxoHashes {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, nil, fmt.Errorf("timeout getBinsToStore#1 %d of %d utxos", i, len(utxoHashes))
			}
			return nil, nil, fmt.Errorf("context cancelled getBinsToStore#1 %d of %d utxos", i, len(utxoHashes))
		default:
			utxos[utxoHash.String()] = aerospike.NewNullValue()
		}
	}

	parentTxHashesInterface := make([]byte, 0, 32*len(txMeta.ParentTxHashes))
	for _, v := range txMeta.ParentTxHashes {
		parentTxHashesInterface = append(parentTxHashesInterface, v[:]...)
	}

	blockIDs := make([]uint32, 0)

	bins := []*aerospike.Bin{
		aerospike.NewBin("tx", tx.ExtendedBytes()),
		aerospike.NewBin("fee", aerospike.NewIntegerValue(int(txMeta.Fee))),
		aerospike.NewBin("sizeInBytes", aerospike.NewIntegerValue(int(txMeta.SizeInBytes))),
		aerospike.NewBin("locktime", aerospike.NewIntegerValue(int(lockTime))),
		aerospike.NewBin("utxos", aerospike.NewMapValue(utxos)),
		aerospike.NewBin("parentTxHashes", parentTxHashesInterface),
		aerospike.NewBin("blockIDs", blockIDs),
		aerospike.NewBin("isCoinbase", tx.IsCoinbase()),
	}

	return txMeta, bins, nil
}
