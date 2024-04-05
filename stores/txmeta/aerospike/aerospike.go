// //go:build aerospike

package aerospike

import (
	"context"
	"errors"
	"fmt"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"math"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

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
	initPrometheusMetrics()

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

	return s, nil
}

func (s *Store) sendStoreBatch(batch []*batchStoreItem) {
	batchPolicy := util.GetAerospikeBatchPolicy()

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, 0)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	s.logger.Debugf("[STORE_BATCH] sending batch of %d txMetas", len(batch))

	var hash *chainhash.Hash
	var key *aerospike.Key
	var err error
	for idx, bItem := range batch {
		hash = bItem.tx.TxIDChainHash()
		key, err = aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			bItem.done <- err
			continue
		}

		parentTxHashesInterface := make([]byte, 0, 32*len(bItem.txMeta.ParentTxHashes))
		for _, v := range bItem.txMeta.ParentTxHashes {
			parentTxHashesInterface = append(parentTxHashesInterface, v[:]...)
		}

		putOps := []*aerospike.Operation{
			aerospike.PutOp(aerospike.NewBin("tx", bItem.tx.ExtendedBytes())),
			aerospike.PutOp(aerospike.NewBin("fee", int(bItem.txMeta.Fee))),
			aerospike.PutOp(aerospike.NewBin("sizeInBytes", int(bItem.txMeta.SizeInBytes))),
			aerospike.PutOp(aerospike.NewBin("parentTxHashes", parentTxHashesInterface)),
			aerospike.PutOp(aerospike.NewBin("firstSeen", time.Now().Unix())),
			aerospike.PutOp(aerospike.NewBin("lockTime", int(bItem.tx.LockTime))),
			aerospike.PutOp(aerospike.NewBin("isCoinbase", bItem.tx.IsCoinbase())),
		}

		record := aerospike.NewBatchWrite(batchWritePolicy, key, putOps...)
		batchRecords[idx] = record
	}

	batchId := s.batchId.Add(1)

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[STORE_BATCH][%s] Failed to batch store aerospike txMeta in batchId %d: %v\n", len(batch), batchId, err)
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

	err := s.MetaBatchDecorate(context.Background(), items)
	if err != nil {
		// mark all items as errored
		for _, bItem := range batch {
			bItem.done <- batchGetItemData{
				Err: err,
			}
		}
		return
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
	startTime := time.Now()
	defer func() {
		prometheusTxMetaGetDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()
	fields := []string{"fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"}

	if s.getBatcher != nil {
		done := make(chan batchGetItemData)
		s.getBatcher.Put(&batchGetItem{hash: *hash, fields: fields, done: done})

		data := <-done
		if data.Err != nil {
			prometheusTxMetaGetErr.Inc()
		} else {
			prometheusTxMetaGet.Inc()
		}
		return data.Data, data.Err
	}

	return s.get(ctx, hash, fields)
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	startTime := time.Now()
	defer func() {
		prometheusTxMetaGetDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()
	fields := []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"}

	if s.getBatcher != nil {
		done := make(chan batchGetItemData)
		s.getBatcher.Put(&batchGetItem{hash: *hash, fields: fields, done: done})

		data := <-done
		if data.Err != nil {
			prometheusTxMetaGetErr.Inc()
		} else {
			prometheusTxMetaGet.Inc()
		}
		return data.Data, data.Err
	}

	return s.get(ctx, hash, fields)
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []string) (*txmeta.Data, error) {
	prometheusTxMetaGet.Inc()

	key, aeroErr := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if aeroErr != nil {
		return nil, aeroErr
	}

	var value *aerospike.Record

	readPolicy := util.GetAerospikeReadPolicy()
	start := time.Now()
	value, aeroErr = s.client.Get(readPolicy, key, bins...)
	if aeroErr != nil {
		prometheusTxMetaGetErr.Inc()
		if errors.Is(aeroErr, aerospike.ErrKeyNotFound) {
			return nil, txmeta.NewErrTxmetaNotFound(hash)
		}
		return nil, fmt.Errorf("aerospike get error (time taken: %s) : %w", time.Since(start).String(), aeroErr)
	}

	if value == nil {
		return nil, nil
	}

	status := &txmeta.Data{}

	if fee, ok := value.Bins["fee"].(int); ok {
		status.Fee = uint64(fee)
	}

	if sb, ok := value.Bins["sizeInBytes"].(int); ok {
		status.SizeInBytes = uint64(sb)
	}

	var parentTxHashes []chainhash.Hash
	if value.Bins["parentTxHashes"] != nil {
		parentTxHashesInterface, ok := value.Bins["parentTxHashes"].([]byte)
		if ok {
			parentTxHashes = make([]chainhash.Hash, 0, len(parentTxHashesInterface)/32)
			for i := 0; i < len(parentTxHashesInterface); i += 32 {
				parentTxHashes = append(parentTxHashes, chainhash.Hash(parentTxHashesInterface[i:i+32]))
			}

			status.ParentTxHashes = parentTxHashes
		}
	}

	if value.Bins["blockIDs"] != nil {
		temp := value.Bins["blockIDs"].([]interface{})
		var blockIDs []uint32
		for _, val := range temp {
			blockIDs = append(blockIDs, uint32(val.(int)))
		}
		status.BlockIDs = blockIDs
	}

	// transform the aerospike interface{} into the correct types
	if value.Bins["tx"] != nil {
		b, ok := value.Bins["tx"].([]byte)
		if !ok {
			return nil, errors.New("could not convert tx to []byte")
		}

		tx, err := bt.NewTxFromBytes(b)
		if err != nil {
			return nil, errors.Join(errors.New("could not convert tx bytes to bt.Tx"), err)
		}
		status.Tx = tx
	}

	if value.Bins["isCoinbase"] != nil {
		status.IsCoinbase = value.Bins["isCoinbase"].(bool)
	}

	return status, nil
}

func (s *Store) MetaBatchDecorate(_ context.Context, items []*txmeta.MissingTxHash, fields ...string) error {
	batchPolicy := util.GetAerospikeBatchPolicy()
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
						parentTxHashes := make([]chainhash.Hash, 0, len(parentTxHashesInterface)/32)
						for i := 0; i < len(parentTxHashesInterface); i += 32 {
							parentTxHashes = append(parentTxHashes, chainhash.Hash(parentTxHashesInterface[i:i+32]))
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

	prometheusTxMetaGetMulti.Inc()
	prometheusTxMetaGetMultiN.Add(float64(len(batchRecords)))

	return nil
}

func (s *Store) Create(_ context.Context, tx *bt.Tx, lockTime ...uint32) (*txmeta.Data, error) {
	start := time.Now()
	var e error
	defer func() {
		if e != nil {
			s.logger.Errorf("txmeta Create error for %s (time taken: %s) : %v", tx.TxIDChainHash().String(), time.Since(start).String(), e)
		}
	}()

	hash := tx.TxIDChainHash()
	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		e = err
		return nil, err
	}

	if s.storeBatcher != nil {
		done := make(chan error)
		s.storeBatcher.Put(&batchStoreItem{tx: tx, txMeta: txMeta, done: done})

		err = <-done
		if err != nil {
			return nil, err
		}

		prometheusTxMetaSet.Inc()
		return txMeta, nil
	}

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		e = err
		return nil, err
	}

	parentTxHashesInterface := make([]byte, 0, 32*len(txMeta.ParentTxHashes))
	for _, v := range txMeta.ParentTxHashes {
		parentTxHashesInterface = append(parentTxHashesInterface, v[:]...)
	}

	bins := []*aerospike.Bin{
		aerospike.NewBin("tx", tx.ExtendedBytes()),
		aerospike.NewBin("fee", int(txMeta.Fee)),
		aerospike.NewBin("sizeInBytes", int(txMeta.SizeInBytes)),
		aerospike.NewBin("parentTxHashes", parentTxHashesInterface),
		aerospike.NewBin("firstSeen", time.Now().Unix()),
		aerospike.NewBin("lockTime", int(tx.LockTime)),
		aerospike.NewBin("isCoinbase", tx.IsCoinbase()),
	}

	// s.expiration - expiration is set in SetMined and SetMinedMulti
	policy := util.GetAerospikeWritePolicy(0, 0)
	policy.RecordExistsAction = aerospike.CREATE_ONLY

	maxRetries := 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = s.client.PutBins(policy, key, bins...)

		if err == nil {
			prometheusTxMetaSet.Inc()
			return txMeta, nil
		}

		aeroErr := &aerospike.AerospikeError{}
		ok := errors.As(err, &aeroErr)
		if !ok {
			e = err
			break
		}

		if aeroErr.ResultCode == types.KEY_EXISTS_ERROR {
			return txMeta, txmeta.NewErrTxmetaAlreadyExists(hash)
		}
		switch aeroErr.ResultCode {
		case types.NETWORK_ERROR, types.TIMEOUT, types.MAX_ERROR_RATE, types.COMMAND_REJECTED, types.INVALID_NODE_ERROR, types.MAX_RETRIES_EXCEEDED, types.SERVER_ERROR, types.SERVER_NOT_AVAILABLE, types.LOST_CONFLICT:
			s.logger.Errorf("Aerospike error for %s on attempt %d: %v", tx.TxIDChainHash().String(), attempt+1, err)
			duration := time.Duration(math.Pow(2, float64(attempt))) * time.Millisecond
			if attempt < maxRetries-1 { // don't sleep on last attempt
				time.Sleep(duration)
			}
		default:
			e = err
		}
	}

	return txMeta, err
}

func (s *Store) SetMined(_ context.Context, hash *chainhash.Hash, blockID uint32) error {
	var e error
	defer func() {
		if e != nil {
			s.logger.Errorf("txmeta SetMined error for %s: %v", hash.String(), e)
		}
	}()

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		e = err
		return err
	}

	writePolicy := util.GetAerospikeWritePolicy(0, s.expiration)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	op := aerospike.ListAppendOp("blockIDs", blockID)
	_, err = s.client.Operate(writePolicy, key, op)
	if err != nil {
		return err
	}

	prometheusTxMetaSetMined.Inc()

	return nil
}

func (s *Store) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	batchPolicy := util.GetAerospikeBatchPolicy()

	policy := util.GetAerospikeBatchWritePolicy(0, s.expiration)
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

	prometheusTxMetaSetMinedBatch.Inc()

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

	prometheusTxMetaSetMinedBatchN.Add(float64(okUpdates))

	if len(errs) > 0 {
		prometheusTxMetaSetMinedBatchErrN.Add(float64(len(errs)))
		return fmt.Errorf("aerospike batchRecord errors: %v", errs)
	}

	return nil
}

func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	var e error
	defer func() {
		if e != nil {
			s.logger.Errorf("txmeta Delete error for %s: %v", hash.String(), e)
		} else {
			s.logger.Warnf("txmeta Delete success for %s", hash.String())
		}
	}()

	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		e = err
		return err
	}

	start := time.Now()
	_, err = s.client.Delete(policy, key)
	if err != nil {
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return txmeta.NewErrTxmetaNotFound(hash)
		}

		e = fmt.Errorf("aerospike delete error (time taken: %s) : %w", time.Since(start).String(), err)
		return err
	}

	prometheusTxMetaDelete.Inc()

	return nil
}
