//go:build aerospike

package aerospike

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v6"
	asl "github.com/aerospike/aerospike-client-go/v6/logger"
	"github.com/aerospike/aerospike-client-go/v6/types"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
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
	prometheusTxMetaDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_delete",
			Help: "Number of txmeta delete calls done to aerospike",
		},
	)
}

type Store struct {
	client     *aerospike.Client
	namespace  string
	expiration uint32
	logger     utils.Logger
}

var initMu sync.Mutex

func New(u *url.URL) (*Store, error) {
	// this is weird, but if we are starting 2 connections (utxo, txmeta) at the same time, this fails
	initMu.Lock()
	asl.Logger.SetLevel(asl.DEBUG)
	initMu.Unlock()

	logger := gocore.Log("aero_store")

	namespace := u.Path[1:]

	client, err := util.GetAerospikeClient(u)
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
	return s.get(ctx, hash, []string{"fee", "sizeInBytes", "locktime"})
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"tx", "fee", "sizeInBytes", "parentTxHashes", "firstSeen", "blockHashes", "lockTime"})
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []string) (*txmeta.Data, error) {
	var e error
	defer func() {
		if e != nil {
			s.logger.Errorf("txmeta get error for %s: %v", hash.String(), e)
		} else {
			s.logger.Warnf("txmeta get success for %s", hash.String())
		}
	}()

	prometheusTxMetaGet.Inc()

	key, aeroErr := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if aeroErr != nil {
		e = aeroErr
		return nil, aeroErr
	}

	var value *aerospike.Record

	readPolicy := util.GetAerospikeReadPolicy()
	start := time.Now()
	value, aeroErr = s.client.Get(readPolicy, key, bins...)
	if aeroErr != nil {
		e = aeroErr
		if errors.Is(aeroErr, aerospike.ErrKeyNotFound) {
			return nil, txmeta.ErrNotFound
		}
		return nil, fmt.Errorf("aerospike get error (time taken: %s) : %w", time.Since(start).String(), aeroErr)
	}

	if value == nil {
		e = errors.New("value is nil")
		return nil, nil
	}

	var err error

	status := &txmeta.Data{}

	if fee, ok := value.Bins["fee"].(int); ok {
		status.Fee = uint64(fee)
	}

	if sb, ok := value.Bins["sizeInBytes"].(int); ok {
		status.SizeInBytes = uint64(sb)
	}

	if ls, ok := value.Bins["lockTime"].(int); ok {
		status.LockTime = uint32(ls)
	}

	if fs, ok := value.Bins["firstSeen"].(int); ok {
		status.FirstSeen = uint32(fs)
	}

	var cHash *chainhash.Hash

	var parentTxHashes []*chainhash.Hash
	if value.Bins["parentTxHashes"] != nil {
		parentTxHashesInterface, ok := value.Bins["parentTxHashes"].([]byte)
		if ok {
			parentTxHashes = make([]*chainhash.Hash, 0, len(parentTxHashesInterface)/32)
			for i := 0; i < len(parentTxHashesInterface); i += 32 {
				cHash, err = chainhash.NewHash(parentTxHashesInterface[i : i+32])
				if err != nil {
					e = err
					return nil, err
				}
				parentTxHashes = append(parentTxHashes, cHash)
			}

			status.ParentTxHashes = parentTxHashes
		}
	}

	var blockHashes []*chainhash.Hash
	if value.Bins["blockHashes"] != nil {
		blockHashesInterface, ok := value.Bins["blockHashes"].([]byte)
		if ok {
			blockHashes = make([]*chainhash.Hash, 0, len(blockHashesInterface)/32)
			for i := 0; i < len(blockHashesInterface); i += 32 {
				cHash, err = chainhash.NewHash(blockHashesInterface[i : i+32])
				if err != nil {
					e = err
					return nil, err
				}
				blockHashes = append(blockHashes, cHash)
			}
			status.BlockHashes = blockHashes
		}
	}

	// transform the aerospike interface{} into the correct types
	if value.Bins["tx"] != nil {
		b, ok := value.Bins["tx"].([]byte)
		if !ok {
			e = errors.New("could not convert tx to []byte")
			return nil, e
		}

		tx, err := bt.NewTxFromBytes(b)
		if err != nil {
			e = errors.New("could not convert tx bytes to bt.Tx")
			return nil, e
		}
		status.Tx = tx
	}

	return status, nil
}

func (s *Store) Create(_ context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	var e error
	defer func() {
		if e != nil {
			s.logger.Errorf("txmeta Create error for %s: %v", tx.TxIDChainHash().String(), e)
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

	policy := util.GetAerospikeWritePolicy(0, 0)
	policy.RecordExistsAction = aerospike.CREATE_ONLY

	start := time.Now()
	err = s.client.PutBins(policy, key, bins...)
	if err != nil {
		aeroErr := &aerospike.AerospikeError{}
		if ok := errors.As(err, &aeroErr); ok {
			if aeroErr.ResultCode == types.KEY_EXISTS_ERROR {
				return txMeta, txmeta.ErrAlreadyExists
			}
		}

		err = fmt.Errorf("aerospike put error (time taken: %s) : %w", time.Since(start).String(), err)
		e = err
		return txMeta, err
	}

	prometheusTxMetaSet.Inc()

	return txMeta, nil
}

func (s *Store) SetMined(_ context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
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

	bin := aerospike.NewBin("blockHashes", blockHash[:])
	start := time.Now()
	err = s.client.AppendBins(writePolicy, key, bin)
	if err != nil {
		e = fmt.Errorf("aerospike put error (time taken: %s) : %w", time.Since(start).String(), err)
		return err
	}

	prometheusTxMetaSetMined.Inc()

	return nil
}

func (s *Store) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, blockHash *chainhash.Hash) error {
	batchPolicy := util.GetAerospikeBatchPolicy()

	policy := util.GetAerospikeBatchWritePolicy(0, s.expiration)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(hashes))

	for idx, hash := range hashes {
		key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
		if err != nil {
			return err
		}
		bin := aerospike.NewBin("blockHashes", blockHash[:])
		record := aerospike.NewBatchWrite(policy, key, aerospike.AppendOp(bin))
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
