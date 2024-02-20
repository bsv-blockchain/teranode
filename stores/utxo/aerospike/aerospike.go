// //go:build aerospike

package aerospike

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v6"
	"github.com/aerospike/aerospike-client-go/v6/types"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
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
	prometheusUtxoGet       prometheus.Counter
	prometheusUtxoStore     prometheus.Counter
	prometheusUtxoStoreFail prometheus.Counter
	//prometheusUtxoReStore        prometheus.Counter
	prometheusUtxoRetryStore     prometheus.Counter
	prometheusUtxoRetryStoreFail prometheus.Counter
	//prometheusUtxoStoreSpent     prometheus.Counter
	prometheusUtxoSpend      prometheus.Counter
	prometheusUtxoReSpend    prometheus.Counter
	prometheusUtxoSpendSpent prometheus.Counter
	prometheusUtxoReset      prometheus.Counter
	prometheusUtxoDelete     prometheus.Counter
	prometheusUtxoErrors     *prometheus.CounterVec
)

func init() {
	prometheusUtxoGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_get",
			Help: "Number of utxo get calls done to aerospike",
		},
	)
	prometheusUtxoStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_store",
			Help: "Number of utxo store calls done to aerospike",
		},
	)
	prometheusUtxoStoreFail = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_store_fail",
			Help: "Number of utxo store failed calls done to aerospike",
		},
	)
	//prometheusUtxoStoreSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "aerospike_utxo_store_spent",
	//		Help: "Number of utxo store calls that were already spent to aerospike",
	//	},
	//)
	//prometheusUtxoReStore = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "aerospike_utxo_restore",
	//		Help: "Number of utxo restore calls done to aerospike",
	//	},
	//)
	prometheusUtxoRetryStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_retry_store",
			Help: "Number of utxo retry store calls done to aerospike",
		},
	)
	prometheusUtxoRetryStoreFail = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_retry_store_fail",
			Help: "Number of utxo retry store failed calls done to aerospike",
		},
	)
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_spend",
			Help: "Number of utxo spend calls done to aerospike",
		},
	)
	prometheusUtxoReSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_respend",
			Help: "Number of utxo respend calls done to aerospike",
		},
	)
	prometheusUtxoSpendSpent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_spend_spent",
			Help: "Number of utxo spend calls that were already spent done to aerospike",
		},
	)
	prometheusUtxoReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_reset",
			Help: "Number of utxo reset calls done to aerospike",
		},
	)
	prometheusUtxoDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_delete",
			Help: "Number of utxo delete calls done to aerospike",
		},
	)
	prometheusUtxoErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_errors",
			Help: "Number of utxo errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)
}

type storeUtxo struct {
	idx        int
	utxoHash   chainhash.Hash
	txHash     chainhash.Hash
	lockTime   uint32
	retryCount int
}

type Store struct {
	u               *url.URL
	client          *uaerospike.Client
	namespace       string
	logger          ulogger.Logger
	blockHeight     atomic.Uint32
	expiration      uint32
	dbTimeout       time.Duration
	storeRetryCh    chan *storeUtxo
	filterEnabled   bool
	batchId         atomic.Uint64
	batchingEnabled bool
}

func New(logger ulogger.Logger, u *url.URL) (*Store, error) {
	//asl.Logger.SetLevel(asl.DEBUG)

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger = logger.New("aero", ulogger.WithLevel(logLevelStr))

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

	dbTimeoutMillis, _ := gocore.Config().GetInt("utxostore_dbTimeoutMillis", 5000)
	filterEnabled := gocore.Config().GetBool("utxostore_filterEnabled", true)
	batchingEnabled := gocore.Config().GetBool("utxostore_batchingEnabled", true)

	s := &Store{
		u:               u,
		client:          client,
		namespace:       namespace,
		logger:          logger,
		blockHeight:     atomic.Uint32{},
		expiration:      expiration,
		dbTimeout:       time.Duration(dbTimeoutMillis) * time.Millisecond,
		storeRetryCh:    make(chan *storeUtxo, 1_000_000), // buffer needs to be big enough to never fail
		filterEnabled:   filterEnabled,
		batchingEnabled: batchingEnabled,
	}

	s.logger.Infof("[UTXO] filter expressions enabled: %t", s.filterEnabled)

	go func() {
		defer func() {
			s.logger.Infof("[UTXO] stopping storeRetryCh utxo goroutine")
		}()

		s.logger.Infof("[UTXO] starting storeRetryCh utxo goroutine")
		policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
		policy.RecordExistsAction = aerospike.CREATE_ONLY

		// retry storing utxos that failed
		for storeRetryUtxo := range s.storeRetryCh {
			prometheusUtxoRetryStore.Inc()

			bins := []*aerospike.Bin{
				aerospike.NewBin("locktime", storeRetryUtxo.lockTime),
			}

			key, err := aerospike.NewKey(s.namespace, "utxo", storeRetryUtxo.utxoHash[:])
			if err != nil {
				s.logger.Errorf("[UTXO] failed to init new aerospike key in storeRetryCh: %v", err)
				continue
			}

			if err = s.client.PutBins(policy, key, bins...); err != nil {
				var aErr *aerospike.AerospikeError
				if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_EXISTS_ERROR {
					continue
				}
				prometheusUtxoRetryStoreFail.Inc()

				s.logger.Errorf("[UTXO][%s] failed to store utxo %d in aerospike in storeRetryCh for txid %s: %v", storeRetryUtxo.utxoHash.String(), storeRetryUtxo.idx, storeRetryUtxo.txHash.String(), err)

				// requeue for retry
				storeRetryUtxo.retryCount++
				if storeRetryUtxo.retryCount < 3 {
					// backoff
					time.Sleep(time.Duration(storeRetryUtxo.retryCount) * time.Second)
					s.storeRetryCh <- storeRetryUtxo
				}
			} else {
				s.logger.Warnf("[UTXO][%s] successfully stored utxo %d in aerospike in storeRetryCh for txid %s", storeRetryUtxo.utxoHash.String(), storeRetryUtxo.idx, storeRetryUtxo.txHash.String())
			}
		}
	}()

	return s, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.logger.Debugf("setting block height to %d", blockHeight)
	s.blockHeight.Store(blockHeight)
	return nil
}

func (s *Store) GetBlockHeight() (uint32, error) {
	return s.blockHeight.Load(), nil
}

func (s *Store) Health(ctx context.Context) (int, string, error) {
	/* As written by one of the Aerospike developers, Go contexts are not supported:

	The Aerospike Go Client is a high performance library that supports hundreds of thousands
	of transactions per second per instance. Context support would require us to spawn a new
	goroutine for every request, adding significant overhead to the scheduler and GC.

	I am convinced that most users would benchmark their code with the context support and
	decide against using it after noticing the incurred penalties.

	Therefore we will extract the Deadline from the context and use it as a timeout for the
	operation.
	*/

	var timeout time.Duration

	deadline, ok := ctx.Deadline()
	if ok {
		timeout = time.Until(deadline)
	}

	writePolicy := aerospike.NewWritePolicy(0, 0)
	if timeout > 0 {
		writePolicy.TotalTimeout = timeout
	}

	details := fmt.Sprintf("url: %s, namespace: %s", s.u.String(), s.namespace)

	// Trying to put and get a record to test the connection
	key, err := aerospike.NewKey(s.namespace, "set", "key")
	if err != nil {
		return -1, details, err
	}

	bin := aerospike.NewBin("bin", "value")
	err = s.client.PutBins(util.GetAerospikeWritePolicy(0, math.MaxUint32), key, bin)
	if err != nil {
		return -2, details, err
	}

	policy := aerospike.NewPolicy()
	if timeout > 0 {
		policy.TotalTimeout = timeout
	}

	_, err = s.client.Get(policy, key)
	if err != nil {
		return -3, details, err
	}

	return 0, details, nil
}

func (s *Store) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	prometheusUtxoGet.Inc()

	key, aErr := aerospike.NewKey(s.namespace, "utxo", spend.Hash[:])
	if aErr != nil {
		prometheusUtxoErrors.WithLabelValues("Get", aErr.Error()).Inc()
		s.logger.Errorf("Failed to init new aerospike key: %v\n", aErr)
		return nil, aErr
	}

	policy := util.GetAerospikeReadPolicy()

	start := time.Now()
	value, aErr := s.client.Get(policy, key, "txid", "locktime")
	if aErr != nil {
		prometheusUtxoErrors.WithLabelValues("Get", aErr.Error()).Inc()
		if errors.Is(aErr, aerospike.ErrKeyNotFound) {
			return &utxostore.Response{
				Status: int(utxostore.Status_NOT_FOUND),
			}, fmt.Errorf("%v: %w", aErr, utxostore.ErrNotFound)
		}

		s.logger.Errorf("Failed to get aerospike key (time taken: %s) : %v\n", time.Since(start).String(), aErr)
		return nil, fmt.Errorf("%v: %w", aErr, utxostore.ErrNotFound)
	}

	var err error
	var spendingTxId *chainhash.Hash
	lockTime := uint32(0)
	if value != nil {
		spendingTxIdBytes, _ := value.Bins["txid"].([]byte)
		if spendingTxIdBytes != nil {
			spendingTxId, err = chainhash.NewHash(spendingTxIdBytes)
			if err != nil {
				return nil, fmt.Errorf("chainhash error: %w", err)
			}
		}

		iVal := value.Bins["locktime"]
		if iVal != nil {
			lockTimeInt, ok := iVal.(int)
			if ok {
				lockTime = uint32(lockTimeInt)
			}
		}
	}

	return &utxostore.Response{
		Status:       int(utxostore.CalculateUtxoStatus(spendingTxId, lockTime, s.blockHeight.Load())),
		SpendingTxID: spendingTxId,
		LockTime:     lockTime,
	}, nil
}

// Store stores the utxos of the tx in aerospike
// the lockTime optional argument is needed for coinbase transactions that do not contain the lock time
func (s *Store) Store(_ context.Context, tx *bt.Tx, lockTime ...uint32) error {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.CREATE_ONLY

	utxoHashes, err := utxostore.GetUtxoHashes(tx)
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Store", err.Error()).Inc()
		return fmt.Errorf("failed to get fees and utxo hashes %s: %v", tx.TxIDChainHash().String(), err)
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	// just store it normally if it is only 1 utxo
	if len(utxoHashes) == 1 {
		return s.storeUtxo(policy, utxoHashes[0], storeLockTime)
	}

	if !s.batchingEnabled {
		// TODO this as temporary fix for testing whether batching is causing a problem
		for _, hash := range utxoHashes {
			if err = s.storeUtxo(policy, hash, storeLockTime); err != nil {
				// storeUtxo will retry if it fails
				s.logger.Errorf("[UTXO] failed to store utxo %s: %v", hash.String(), err)
			}
		}

		return nil
	}

	return s.storeUtxosInternal(*tx.TxIDChainHash(), utxoHashes, storeLockTime)
}

func (s *Store) storeUtxosInternal(txID chainhash.Hash, utxoHashes []chainhash.Hash, storeLockTime uint32) (err error) {
	batchPolicy := util.GetAerospikeBatchPolicy()

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, 0)
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(utxoHashes))

	var key *aerospike.Key
	var bin *aerospike.Bin
	for idx, hash := range utxoHashes {
		key, err = aerospike.NewKey(s.namespace, "utxo", hash[:])
		if err != nil {
			prometheusUtxoErrors.WithLabelValues("Store", err.Error()).Inc()
			return err
		}

		bin = aerospike.NewBin("locktime", storeLockTime)
		record := aerospike.NewBatchWrite(batchWritePolicy, key, aerospike.PutOp(bin))
		batchRecords[idx] = record
	}

	batchId := s.batchId.Add(1)

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Warnf("[BATCH_ERR][%d] Failed to batch store aerospike utxos, adding to retry queue: %v\n", batchId, err)

		// don't return, check each record in the batch for errors and process accordingly
	}

	// batchOperate may have no errors, but some of the records may have failed
	errorsThrown := make([]error, 0)
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			var aErr *aerospike.AerospikeError
			// TODO check if this is the correct handling of this
			// we assume because it exists, it is OK
			if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_EXISTS_ERROR {
				prometheusUtxoErrors.WithLabelValues("Store", err.Error()).Inc()
				s.logger.Warnf("[BATCH_ERR][%d] %s already exists, skipping", batchId, utxoHashes[idx].String())
				continue
			}

			prometheusUtxoErrors.WithLabelValues("Store", err.Error()).Inc()
			e := fmt.Errorf("[BATCH_ERR][%d] error in aerospike store batch record (will retry): %s - %w", batchId, utxoHashes[idx].String(), err)
			errorsThrown = append(errorsThrown, e)

			s.logger.Errorf("%s", e.Error())

			s.storeRetryCh <- &storeUtxo{
				idx:      idx,
				utxoHash: utxoHashes[idx],
				txHash:   txID,
				lockTime: storeLockTime,
			}
		}
	}

	if len(errorsThrown) > 0 {
		prometheusUtxoStoreFail.Add(float64(len(errorsThrown)))
		return fmt.Errorf("[BATCH_ERR][%d] error in aerospike store batch records: %d of %d failed", batchId, len(errorsThrown), len(batchRecords))
	}

	prometheusUtxoStore.Add(float64(len(utxoHashes)))

	return nil
}

func (s *Store) StoreFromHashes(_ context.Context, txID chainhash.Hash, utxoHashes []chainhash.Hash, lockTime uint32) error {
	return s.storeUtxosInternal(txID, utxoHashes, lockTime)
}

func (s *Store) storeUtxo(policy *aerospike.WritePolicy, hash chainhash.Hash, nLockTime uint32) error {
	key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Store", err.Error()).Inc()
		return err
	}

	bins := []*aerospike.Bin{
		aerospike.NewBin("locktime", nLockTime),
	}

	start := time.Now()
	err = s.client.PutBins(policy, key, bins...)
	if err != nil {
		var aErr *aerospike.AerospikeError
		if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_EXISTS_ERROR {
			s.logger.Warnf("%s Key already exists, skipping ", hash[:])
			return nil
		}

		s.storeRetryCh <- &storeUtxo{
			idx:      0,
			utxoHash: hash,
			txHash:   hash,
			lockTime: nLockTime,
		}
		return fmt.Errorf("error in aerospike store PutBins (time taken: %s) : %w", time.Since(start).String(), err)
	}

	return nil
}

func (s *Store) Spend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	if s.dbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.dbTimeout)
		defer cancel()
	}

	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in aerospike Spend: %v\n", recoverErr)
		}
	}()

	//expiration := uint32(time.Now().Add(24 * time.Hour).Unix())

	policy := util.GetAerospikeWritePolicy(0, s.expiration)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	if s.filterEnabled {
		policy.FilterExpression = aerospike.ExpAnd(
			// check whether txid has been set = spent
			aerospike.ExpNot(aerospike.ExpBinExists("txid")),

			aerospike.ExpOr(
				// anything below the block height is spendable, including 0
				aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(int64(s.blockHeight.Load()))),

				aerospike.ExpAnd(
					aerospike.ExpGreaterEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(500000000)),
					// TODO Note that since the adoption of BIP 113, the time-based nLockTime is compared to the 11-block median
					// time past (the median timestamp of the 11 blocks preceding the block in which the transaction is mined),
					// and not the block time itself.
					aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(time.Now().Unix())),
				),
			),
		)
	} else {
		s.logger.Warnf("[UTXO] filter expressions disabled")
	}

	spentSpends := make([]*utxostore.Spend, 0, len(spends))

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("timeout spending %d of %d aerospike utxos", i, len(spends))
			}
			return fmt.Errorf("context cancelled spending %d of %d aerospike utxos", i, len(spends))
		default:
			// if err = s.spendUtxo(policy, spend); err != nil {
			// 	// TODO remove this hack
			// 	// TEMP TEMP TEMP - we need to figure out why utxos are not stored properly
			// 	// there are no double spends, so we can just ignore this error for now to be able to test performance
			// 	s.logger.Warnf("[BACKUP_UTXO_STORE] failed to spend utxo %s on tx %s:%d: %v", spend.Hash.String(), spend.TxID.String(), spend.Vout, err)
			// 	if err = s.storeUtxo(util.GetAerospikeWritePolicy(0, 0), spend.Hash, 0); err != nil {
			// 		s.logger.Errorf("[BACKUP_UTXO_STORE] failed to store utxo as backup in spendUtxo %s on tx %s:%d: %v", spend.Hash.String(), spend.TxID.String(), spend.Vout, err)
			// 	} else {
			if err = s.spendUtxo(policy, spend); err != nil {
				// 	_ = s.UnSpend(context.Background(), spentSpends)
				// 	return err
				// }
				// return nil
				// }

				// revert the spent utxos
				_ = s.UnSpend(context.Background(), spentSpends)
				return err
			} else {
				spentSpends = append(spentSpends, spend)
			}
		}
	}

	prometheusUtxoSpend.Inc()

	return nil
}

func (s *Store) spendUtxo(policy *aerospike.WritePolicy, spend *utxostore.Spend) error {
	key, err := aerospike.NewKey(s.namespace, "utxo", spend.Hash[:])
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Spend", err.Error()).Inc()
		return fmt.Errorf("error failed creating key in aerospike Spend: %w", err)
	}

	bin := aerospike.NewBin("txid", spend.SpendingTxID.CloneBytes())
	start := time.Now()
	err = s.client.PutBins(policy, key, bin)
	if err != nil {
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			s.logger.Debugf("utxo %s was not found: %s", spend.Hash.String(), err.Error())
			return utxostore.ErrNotFound
		}

		// check whether we had the same value set as before
		prometheusUtxoGet.Inc()
		readPolicy := util.GetAerospikeReadPolicy()
		startGet := time.Now()
		value, getErr := s.client.Get(readPolicy, key, "txid", "locktime")
		if getErr != nil {
			return fmt.Errorf("could not see if the value was the same as before (time taken: %s) : %w", time.Since(startGet).String(), getErr)
		}
		valueBytes, ok := value.Bins["txid"].([]byte)
		if ok && len(valueBytes) == 32 {
			spendingTxHash, err := chainhash.NewHash(valueBytes)
			if [32]byte(valueBytes) == *spend.SpendingTxID {
				prometheusUtxoReSpend.Inc()
				s.logger.Warnf("utxo %s has already been marked as spent (will skip) by %s", spend.Hash.String(), spendingTxHash)
				return nil
			} else {
				prometheusUtxoSpendSpent.Inc()
				if err != nil {
					return fmt.Errorf("chainhash error: %w", err)
				}

				s.logger.Debugf("utxo %s was spent by %s", spend.Hash.String(), spendingTxHash)

				return utxostore.NewErrSpent(spendingTxHash)
			}
		}
		prometheusUtxoErrors.WithLabelValues("Spend", err.Error()).Inc()

		if errors.Is(err, aerospike.ErrFilteredOut) {
			if len(valueBytes) == 32 {
				spendingTxHash, err := chainhash.NewHash(valueBytes)
				if err != nil {
					return fmt.Errorf("chainhash error: %w", err)
				}

				s.logger.Errorf("utxo %s is already spent by %s", spend.Hash.String(), spendingTxHash.String())
				spendingTxID, err := chainhash.NewHash(valueBytes)
				if err != nil {
					return fmt.Errorf("chainhash error: %w", err)
				}
				return utxostore.NewErrSpent(spendingTxID)
			}

			// we've determined that this utxo was not filtered out due to being spent, so it must be due to locktime
			s.logger.Errorf("utxo %s is not spendable in block %d: %s", spend.Hash.String(), s.blockHeight.Load(), err.Error())
			lockTime, ok := value.Bins["locktime"].(uint32)
			if !ok {
				lockTime = 0
			}

			return utxostore.NewErrLockTime(lockTime, s.blockHeight.Load())
		}

		return fmt.Errorf("error in aerospike spend PutBins (time taken: %s): %w", time.Since(start).String(), err)
	}

	return nil
}

func (s *Store) UnSpend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	if s.dbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.dbTimeout)
		defer cancel()
	}

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("timeout un-spending %d of %d aerospike utxos", i, len(spends))
			}
			return fmt.Errorf("context cancelled un-spending %d of %d aerospike utxos", i, len(spends))
		default:

			if err = s.unSpend(ctx, spend); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Store) unSpend(_ context.Context, spend *utxostore.Spend) error {
	key, err := aerospike.NewKey(s.namespace, "utxo", spend.Hash[:])
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Reset", err.Error()).Inc()
		s.logger.Errorf("ERROR in aerospike Reset: %v\n", err)
		return err
	}

	start := time.Now()
	value, getErr := s.client.Get(util.GetAerospikeReadPolicy(), key, "locktime")
	if getErr != nil {
		prometheusUtxoErrors.WithLabelValues("Get", getErr.Error()).Inc()
		s.logger.Errorf("ERROR in aerospike get key (time taken %s) : %v\n", time.Since(start).String(), getErr)
		return getErr
	}
	nLockTime, ok := value.Bins["locktime"].(int)
	if !ok {
		nLockTime = 0
	}

	policy := util.GetAerospikeWritePolicy(2, math.MaxUint32)

	startDelete := time.Now()
	_, err = s.client.Delete(policy, key)
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Reset", err.Error()).Inc()
		s.logger.Errorf("ERROR in aerospike Reset delete key (time taken: %s) : %v\n", time.Since(startDelete).String(), err)
		return err
	}

	prometheusUtxoReset.Inc()

	return s.storeUtxo(nil, *spend.Hash, uint32(nLockTime))
}

func (s *Store) Delete(_ context.Context, tx *bt.Tx) error {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

	key, err := aerospike.NewKey(s.namespace, "utxo", tx.TxIDChainHash()[:])
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Delete", err.Error()).Inc()
		s.logger.Errorf("ERROR in aerospike Delete: %v\n", err)
		return err
	}

	start := time.Now()
	_, err = s.client.Delete(policy, key)
	if err != nil {
		// if the key is not found, we don't need to delete, it's not there anyway
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return utxostore.ErrNotFound
		}

		prometheusUtxoErrors.WithLabelValues("Delete", err.Error()).Inc()
		return fmt.Errorf("error in aerospike delete key (time taken: %s): %v", time.Since(start).String(), err)
	}

	prometheusUtxoDelete.Inc()

	return nil
}

func (s *Store) DeleteSpends(_ bool) {
	// noop
}
