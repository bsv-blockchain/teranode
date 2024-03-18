// //go:build aerospike

package aerospike

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/aerospike/aerospike-client-go/v6"
	asl "github.com/aerospike/aerospike-client-go/v6/logger"
	"github.com/aerospike/aerospike-client-go/v6/types"
	"github.com/bitcoin-sv/ubsv/model"
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
	prometheusTxMetaGet            prometheus.Counter
	prometheusTxMetaSet            prometheus.Counter
	prometheusTxMetaSetMined       prometheus.Counter
	prometheusTxMetaSetMinedBatch  prometheus.Counter
	prometheusTxMetaSetMinedBatchN prometheus.Counter
	prometheusTxMetaGetMulti       prometheus.Counter
	prometheusTxMetaGetMultiN      prometheus.Counter
	prometheusTxMetaDelete         prometheus.Counter
)

func init() {
	prometheusTxMetaGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_get",
			Help: "Number of txmeta get calls done to aerospike",
		},
	)
	prometheusTxMetaSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set",
			Help: "Number of txmeta set calls done to aerospike",
		},
	)
	prometheusTxMetaSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set_mined",
			Help: "Number of txmeta set_mined calls done to aerospike",
		},
	)
	prometheusTxMetaSetMinedBatch = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set_mined_batch",
			Help: "Number of txmeta set_mined_batch calls done to aerospike",
		},
	)
	prometheusTxMetaSetMinedBatchN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set_mined_batch_n",
			Help: "Number of txmeta set_mined_batch txs done to aerospike",
		},
	)
	prometheusTxMetaGetMulti = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_get_multi",
			Help: "Number of txmeta get_multi calls done to aerospike",
		},
	)
	prometheusTxMetaGetMultiN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_get_multi_n",
			Help: "Number of txmeta get_multi txs done to aerospike",
		},
	)
	prometheusTxMetaDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_delete",
			Help: "Number of txmeta delete calls done to aerospike",
		},
	)

	if gocore.Config().GetBool("aerospike_debug", true) {
		asl.Logger.SetLevel(asl.DEBUG)
	}

}

type Store struct {
	client     *uaerospike.Client
	namespace  string
	expiration uint32
	logger     ulogger.Logger
}

func New(logger ulogger.Logger, u *url.URL) (*Store, error) {
	logger = logger.New("aero_store")

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

	return &Store{
		client:     client,
		namespace:  namespace,
		expiration: expiration,
		logger:     logger,
	}, nil
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"fee", "sizeInBytes", "parentTxHashes", "blockIDs"})
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "blockIDs"})
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []string) (*txmeta.Data, error) {
	prometheusTxMetaGet.Inc()

	key, aeroErr := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if aeroErr != nil {
		return nil, aeroErr
	}

	var value *aerospike.Record

	readPolicy := util.GetAerospikeReadPolicy()
	start := time.Now()
	value, aeroErr = s.client.Get(readPolicy, key, bins...)
	if aeroErr != nil {
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

	return status, nil
}

func (s *Store) MetaBatchDecorate(ctx context.Context, items []*txmeta.MissingTxHash, fields ...string) error {
	batchPolicy := util.GetAerospikeBatchPolicy()

	//policy := util.GetAerospikeBatchReadPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(items))

	for idx, item := range items {
		key, err := aerospike.NewKey(s.namespace, "txmeta", item.Hash[:])
		if err != nil {
			return err
		}

		bins := []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "blockIDs"}
		if len(fields) > 0 {
			bins = fields
		}

		record := aerospike.NewBatchRead(key, bins)
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
			if !model.CoinbasePlaceholderHash.IsEqual(items[idx].Hash) {
				s.logger.Errorf("batchRecord SetMinedMulti: %s - %v", items[idx].Hash.String(), err)
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
				}
			}
		}
	}

	prometheusTxMetaGetMulti.Inc()
	prometheusTxMetaGetMultiN.Add(float64(len(batchRecords)))

	return nil
}

func (s *Store) Create(_ context.Context, tx *bt.Tx) (*txmeta.Data, error) {
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

	key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
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

	key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
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
		key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
		if err != nil {
			return err
		}
		op := aerospike.ListAppendOp("blockIDs", blockID)
		record := aerospike.NewBatchWrite(policy, key, op)
		// Add to batch
		batchRecords[idx] = record
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return err
	}

	prometheusTxMetaSetMinedBatch.Inc()

	okUpdates := 0
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			// TODO what to do here?
			hash := hashes[idx]
			s.logger.Errorf("batchRecord SetMinedMulti: %s - %v", hash.String(), err)
		} else {
			okUpdates++
		}
	}

	prometheusTxMetaSetMinedBatchN.Add(float64(okUpdates))

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

	key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if err != nil {
		e = err
		return err
	}

	start := time.Now()
	_, err = s.client.Delete(policy, key)
	if err != nil {
		e = fmt.Errorf("aerospike delete error (time taken: %s) : %w", time.Since(start).String(), err)
		return err
	}

	prometheusTxMetaDelete.Inc()

	return nil
}
