// //go:build aerospike

package aerospikemap

import (
	"context"
	"errors"
	"fmt"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/ubsv/model"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"math"
	"net/url"

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

	if gocore.Config().GetBool("aerospike_debug", true) {
		asl.Logger.SetLevel(asl.DEBUG)
	}

}

type Store struct {
	client    *uaerospike.Client
	namespace string
	setName   string
	logger    ulogger.Logger
}

func New(logger ulogger.Logger, u *url.URL) (*Store, error) {
	namespace := u.Path[1:]

	client, err := util.GetAerospikeClient(logger, u)
	if err != nil {
		return nil, err
	}

	setName := u.Query().Get("set")
	if setName == "" {
		setName = "txmeta"
	}

	logger.Infof("[Aerospike] map txmeta store initialised with namespace: %s, set: %s", namespace, setName)
	return &Store{
		client:    client,
		namespace: namespace,
		setName:   setName,
		logger:    logger,
	}, nil
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"fee", "size", "parentTxHashes", "blockIDs"})
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"tx", "fee", "size", "parentTxHashes", "blockIDs"})
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []string) (*txmeta.Data, error) {
	prometheusTxMetaAerospikeMapGet.Inc()

	// in the map implementation, we are using the utxo store for the data
	key, aeroErr := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if aeroErr != nil {
		return nil, aeroErr
	}

	var value *aerospike.Record

	readPolicy := util.GetAerospikeReadPolicy()
	value, aeroErr = s.client.Get(readPolicy, key, bins...)
	if aeroErr != nil {
		return nil, aeroErr
	}

	if value == nil {
		return nil, nil
	}

	var parentTxHashes []chainhash.Hash
	if value.Bins["parentTxHashes"] != nil {
		parentTxHashesInterface, ok := value.Bins["parentTxHashes"].([]interface{})
		if ok {
			parentTxHashes = make([]chainhash.Hash, len(parentTxHashesInterface))
			for i, v := range parentTxHashesInterface {
				if len(v.([]byte)) != 32 {
					return nil, fmt.Errorf("invalid parentTxHashes length")
				}
				parentTxHashes[i] = chainhash.Hash(v.([]byte))
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
		SizeInBytes:    uint64(value.Bins["size"].(int)),
		ParentTxHashes: parentTxHashes,
		BlockIDs:       blockIDs,
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

		bins := []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "blockIDs"}
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
				}
			}
		}
	}

	prometheusTxMetaAerospikeMapGetMulti.Inc()
	prometheusTxMetaAerospikeMapGetMultiN.Add(float64(len(batchRecords)))

	return nil
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx, lockTime ...uint32) (*txmeta.Data, error) {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.CREATE_ONLY

	key, aeroErr := aerospike.NewKey(s.namespace, s.setName, tx.TxIDChainHash().CloneBytes())
	if aeroErr != nil {
		prometheusTxMetaAerospikeMapErrors.WithLabelValues("Store", aeroErr.Error()).Inc()
		s.logger.Errorf("Failed to store new aerospike key: %v\n", aeroErr)
		return nil, aeroErr
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	txMeta, bins, err := getBinsToStore(ctx, tx, storeLockTime)
	if err != nil {
		return nil, err
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

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	for _, hash := range hashes {
		if err = s.SetMined(ctx, hash, blockID); err != nil {
			return err
		}
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

	parentTxHashes := make([][]byte, 0, len(txMeta.ParentTxHashes))
	for _, parenTxHash := range txMeta.ParentTxHashes {
		parentTxHashes = append(parentTxHashes, parenTxHash.CloneBytes())
	}

	blockIDs := make([]uint32, 0)

	bins := []*aerospike.Bin{
		aerospike.NewBin("tx", tx.ExtendedBytes()),
		aerospike.NewBin("fee", aerospike.NewIntegerValue(int(txMeta.Fee))),
		aerospike.NewBin("size", aerospike.NewIntegerValue(int(txMeta.SizeInBytes))),
		aerospike.NewBin("locktime", aerospike.NewIntegerValue(int(lockTime))),
		aerospike.NewBin("utxos", aerospike.NewMapValue(utxos)),
		aerospike.NewBin("parentTxHashes", parentTxHashes),
		aerospike.NewBin("blockIDs", blockIDs),
	}

	return txMeta, bins, nil
}
