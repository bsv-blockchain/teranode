// //go:build aerospike

package aerospike

import (
	"context"
	"errors"
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
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusTxMetaGet      prometheus.Counter
	prometheusTxMetaSet      prometheus.Counter
	prometheusTxMetaSetMined prometheus.Counter
	prometheusTxMetaDelete   prometheus.Counter
	logger                   = gocore.Log("aero_store")
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
	prometheusTxMetaDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_delete",
			Help: "Number of txmeta delete calls done to aerospike",
		},
	)
}

type Store struct {
	client     *aerospike.Client
	expiration uint32
	timeout    time.Duration
	namespace  string
}

var initMu sync.Mutex

func New(u *url.URL) (*Store, error) {
	// this is weird, but if we are starting 2 connections (utxo, txmeta) at the same time, this fails
	initMu.Lock()
	asl.Logger.SetLevel(asl.DEBUG)
	initMu.Unlock()

	namespace := u.Path[1:]

	client, err := util.GetAerospikeClient(u)
	if err != nil {
		return nil, err
	}

	var timeout time.Duration

	timeoutValue := u.Query().Get("timeout")
	if timeoutValue != "" {
		var err error
		if timeout, err = time.ParseDuration(timeoutValue); err != nil {
			timeout = 0
		}
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
		timeout:    timeout,
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
			logger.Errorf("txmeta get error for %s: %v", hash.String(), e)
		} else {
			logger.Warnf("txmeta get success for %s", hash.String())
		}
	}()

	prometheusTxMetaGet.Inc()

	key, aeroErr := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if aeroErr != nil {
		e = aeroErr
		return nil, aeroErr
	}

	var value *aerospike.Record

	options := make([]util.AerospikeReadPolicyOptions, 0)

	if s.timeout > 0 {
		options = append(options, util.WithTotalTimeout(s.timeout))
	}

	readPolicy := util.GetAerospikeReadPolicy(options...)
	value, aeroErr = s.client.Get(readPolicy, key, bins...)
	if aeroErr != nil {
		e = aeroErr
		if errors.Is(aeroErr, aerospike.ErrKeyNotFound) {
			return nil, txmeta.ErrNotFound
		}
		return nil, aeroErr
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
			logger.Errorf("txmeta Create error for %s: %v", tx.TxIDChainHash().String(), e)
		} else {
			logger.Warnf("txmeta Create success for %s", tx.TxIDChainHash().String())
		}
	}()

	options := make([]util.AerospikeWritePolicyOptions, 0)

	if s.timeout > 0 {
		options = append(options, util.WithTotalTimeoutWrite(s.timeout))
	}

	policy := util.GetAerospikeWritePolicy(0, s.expiration, options...)
	policy.RecordExistsAction = aerospike.CREATE_ONLY
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

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

	err = s.client.PutBins(policy, key, bins...)
	if err != nil {
		aeroErr := &aerospike.AerospikeError{}
		if ok := errors.As(err, &aeroErr); ok {
			if aeroErr.ResultCode == types.KEY_EXISTS_ERROR {
				return txMeta, txmeta.ErrAlreadyExists
			}
		}

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
			logger.Errorf("txmeta SetMined error for %s: %v", hash.String(), e)
		} else {
			logger.Warnf("txmeta SetMined success for %s", hash.String())
		}
	}()

	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32-1)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency
	//policy.Expiration = uint32(time.Now().Add(24 * time.Hour).Unix())

	key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if err != nil {
		e = err
		return err
	}

	readPolicy := util.GetAerospikeReadPolicy()
	record, err := s.client.Get(readPolicy, key, "blockHashes")
	if err != nil {
		e = err
		return err
	}

	blockHashes, ok := record.Bins["blockHashes"].([]byte)
	if !ok {
		blockHashes = make([]byte, 0, 32)
	}
	blockHashes = append(blockHashes, blockHash[:]...)

	bin := aerospike.NewBin("blockHashes", blockHashes)

	err = s.client.PutBins(policy, key, bin)
	if err != nil {
		e = err
		return err
	}

	prometheusTxMetaSetMined.Inc()

	return nil
}

func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	var e error
	defer func() {
		if e != nil {
			logger.Errorf("txmeta Delete error for %s: %v", hash.String(), e)
		} else {
			logger.Warnf("txmeta Delete success for %s", hash.String())
		}
	}()

	policy := util.GetAerospikeWritePolicy(0, 0)
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if err != nil {
		e = err
		return err
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		e = err
		return err
	}

	prometheusTxMetaDelete.Inc()

	return nil
}
