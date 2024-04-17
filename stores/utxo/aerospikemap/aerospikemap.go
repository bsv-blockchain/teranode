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

	"golang.org/x/sync/errgroup"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/aerospike/aerospike-client-go/v7/types"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusUtxoMapGet prometheus.Counter
	//prometheusUtxoMapStore prometheus.Counter
	//prometheusUtxoMapReStore    prometheus.Counter
	//prometheusUtxoMapStoreSpent prometheus.Counter
	prometheusUtxoMapSpend      prometheus.Counter
	prometheusUtxoMapReSpend    prometheus.Counter
	prometheusUtxoMapSpendSpent prometheus.Counter
	prometheusUtxoMapReset      prometheus.Counter
	prometheusUtxoMapDelete     prometheus.Counter
	prometheusUtxoMapErrors     *prometheus.CounterVec
)

func init() {
	prometheusUtxoMapGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_get",
			Help: "Number of utxo get calls done to aerospike",
		},
	)
	//prometheusUtxoMapStore = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "aerospike_map_utxo_store",
	//		Help: "Number of utxo store calls done to aerospike",
	//	},
	//)
	//prometheusUtxoMapStoreSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "aerospike_map_utxo_store_spent",
	//		Help: "Number of utxo store calls that were already spent to aerospike",
	//	},
	//)
	//prometheusUtxoMapReStore = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "aerospike_map_utxo_restore",
	//		Help: "Number of utxo restore calls done to aerospike",
	//	},
	//)
	prometheusUtxoMapSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_spend",
			Help: "Number of utxo spend calls done to aerospike",
		},
	)
	prometheusUtxoMapReSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_respend",
			Help: "Number of utxo respend calls done to aerospike",
		},
	)
	prometheusUtxoMapSpendSpent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_spend_spent",
			Help: "Number of utxo spend calls that were already spent done to aerospike",
		},
	)
	prometheusUtxoMapReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_reset",
			Help: "Number of utxo reset calls done to aerospike",
		},
	)
	prometheusUtxoMapDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_delete",
			Help: "Number of utxo delete calls done to aerospike",
		},
	)
	prometheusUtxoMapErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_errors",
			Help: "Number of utxo errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)

	if gocore.Config().GetBool("aerospike_debug", true) {
		asl.Logger.SetLevel(asl.DEBUG)
	}

}

type batchSpend struct {
	spend *utxostore.Spend
	done  chan error
}

type batchLastSpend struct {
	key  *aerospike.Key
	hash chainhash.Hash
	time int
}

type Store struct {
	u                *url.URL
	client           *uaerospike.Client
	namespace        string
	setName          string
	logger           ulogger.Logger
	blockHeight      atomic.Uint32
	expiration       uint32
	batchId          atomic.Uint64
	spendBatcher     *batcher.Batcher2[batchSpend]
	lastSpendCh      chan *batchLastSpend
	lastSpendBatcher *batcher.Batcher2[batchLastSpend]
}

var (
	binNames = []string{
		"spendable",
		"fee",
		"size",
		"locktime",
		"utxos",
		"parentTxHashes",
		"blockIDs",
	}
)

func New(logger ulogger.Logger, u *url.URL) (*Store, error) {
	namespace := u.Path[1:]

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger = logger.New("aero", ulogger.WithLevel(logLevelStr))

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

	logger.Infof("[Aerospike] map utxo store initialised with namespace: %s, set: %s", namespace, setName)
	s := &Store{
		u:           u,
		client:      client,
		namespace:   namespace,
		setName:     setName,
		logger:      logger,
		blockHeight: atomic.Uint32{},
		expiration:  expiration,
	}

	spendBatcherEnabled := gocore.Config().GetBool("utxostore_spendBatcherEnabled", true)
	if spendBatcherEnabled {
		batchSize, _ := gocore.Config().GetInt("utxostore_spendBatcherSize", 256)
		batchDuration, _ := gocore.Config().GetInt("utxostore_spendBatcherDurationMillis", 10)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.spendBatcher = batcher.New[batchSpend](batchSize, duration, s.sendSpendBatch, true)
	}

	lastSpendBatcherEnabled := gocore.Config().GetBool("utxostore_lastSpendBatcherEnabled", true)
	if lastSpendBatcherEnabled {
		batchSize, _ := gocore.Config().GetInt("utxostore_lastSpendBatcherSize", 256)
		batchDuration, _ := gocore.Config().GetInt("utxostore_lastSpendBatcherDurationMillis", 5)
		duration := time.Duration(batchDuration) * time.Millisecond
		s.lastSpendBatcher = batcher.New[batchLastSpend](batchSize, duration, s.sendLastSpendBatch, true)
		s.lastSpendCh = make(chan *batchLastSpend, 1_000_000)

		// start the worker
		go func() {
			for lastSpend := range s.lastSpendCh {
				s.lastSpendBatcher.Put(lastSpend)
			}
		}()
	}

	return s, nil
}

func (s *Store) sendSpendBatch(batch []*batchSpend) {
	batchId := s.batchId.Add(1)
	s.logger.Debugf("[SPEND_BATCH] sending batch %d of %d spends", batchId, len(batch))
	defer func() {
		s.logger.Debugf("[SPEND_BATCH] sending batch %d of %d spends DONE", batchId, len(batch))
	}()

	batchPolicy := util.GetAerospikeBatchPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	var key *aerospike.Key
	var err error
	for idx, bItem := range batch {
		key, err = aerospike.NewKey(s.namespace, s.setName, bItem.spend.TxID.CloneBytes())
		if err != nil {
			// we just return the error on the channel, we cannot process this utxo any further
			bItem.done <- fmt.Errorf("[SPEND_BATCH][%s] failed to init new aerospike key for spend: %w", bItem.spend.Hash.String(), err)
			continue
		}

		batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32)
		batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
		batchWritePolicy.FilterExpression = aerospike.ExpAnd(
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
			aerospike.ExpEq(aerospike.ExpMapGetByKey(
				aerospike.MapReturnType.VALUE,
				aerospike.ExpTypeNIL,
				aerospike.ExpStringVal(bItem.spend.Hash.String()),
				aerospike.ExpMapBin("utxos"),
			), aerospike.ExpNilValue()),
		)
		batchRecords[idx] = aerospike.NewBatchWrite(batchWritePolicy, key, []*aerospike.Operation{
			aerospike.MapPutOp(
				aerospike.DefaultMapPolicy(),
				"utxos",
				bItem.spend.Hash.String(),
				bItem.spend.SpendingTxID.CloneBytes(),
			),
			aerospike.GetBinOp("utxos"),
		}...)
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[SPEND_BATCH][%d] failed to batch spend aerospike map utxos in batchId %d: %v", batchId, len(batch), err)
		for idx, bItem := range batch {
			bItem.done <- fmt.Errorf("[SPEND_BATCH][%s] failed to batch spend aerospike map utxo in batchId %d: %d - %w", bItem.spend.Hash.String(), batchId, idx, err)
		}
	}

	// batchOperate may have no errors, but some of the records may have failed
	for idx, batchRecord := range batchRecords {
		spend := batch[idx].spend
		err = batchRecord.BatchRec().Err
		if err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(err, &aErr) && aErr != nil {
				if aErr.ResultCode == types.KEY_EXISTS_ERROR {
					s.logger.Warnf("[SPEND_BATCH][%s] spend already exists in batch %d, skipping", spend.Hash.String(), batchId)
					batch[idx].done <- nil
					continue
				} else if aErr.ResultCode == types.FILTERED_OUT {
					// get the record
					record, getErr := s.client.Get(nil, batchRecord.BatchRec().Key)
					if getErr != nil {
						err = errors.Join(err, getErr)
					} else {
						if record != nil && record.Bins != nil {

							locktime, ok := record.Bins["locktime"].(int)
							if ok {
								status := utxostore.CalculateUtxoStatus(nil, uint32(locktime), s.blockHeight.Load())
								if status == utxostore.Status_LOCKED {
									s.logger.Errorf("utxo %s is not spendable in block %d: %s", spend.Hash.String(), s.blockHeight.Load(), err.Error())
									batch[idx].done <- utxostore.NewErrLockTime(uint32(locktime), s.blockHeight.Load())
								}
							}

							utxoMap, ok := record.Bins["utxos"].(map[interface{}]interface{})
							if ok {
								valueBytes, ok := utxoMap[spend.Hash.String()].([]byte)
								if ok {
									if len(valueBytes) == 32 {
										spendingTxHash := chainhash.Hash(valueBytes)
										if spendingTxHash.Equal(*spend.SpendingTxID) {
											s.logger.Warnf("[SPEND_BATCH][%s] spend already exists in batch %d for tx %s, skipping", spend.Hash.String(), batchId, spendingTxHash.String())
											batch[idx].done <- nil
										} else {
											// spent by another transaction
											batch[idx].done <- utxostore.NewErrSpent(spend.TxID, spend.Vout, spend.Hash, &spendingTxHash)
										}
										continue
									}
								}
							}
						}
					}
				}
			}

			batch[idx].done <- fmt.Errorf("[SPEND_BATCH][%s] error in aerospike spend batch record, locktime %d: %d - %w", spend.Hash.String(), s.blockHeight.Load(), batchId, err)
		} else {
			response := batchRecord.BatchRec().Record
			// check whether all utxos are spent
			if response != nil && response.Bins != nil && response.Bins["utxos"] != nil {
				utxosValue, ok := response.Bins["utxos"].(aerospike.OpResults)
				if ok {
					if len(utxosValue) == 2 {
						// utxos are in index 1 of the response
						utxos, ok := utxosValue[1].(map[interface{}]interface{})
						if ok {
							spentUtxos := 0
							for _, v := range utxos {
								if v != nil {
									spentUtxos++
								}
							}
							if spentUtxos == len(utxos) {
								// mark document as spent and add expiration for TTL
								if s.lastSpendBatcher != nil {
									s.lastSpendBatcher.Put(&batchLastSpend{
										key:  key,
										hash: *spend.Hash,
										time: int(time.Now().Unix()),
									})
								} else {
									ttlPolicy := util.GetAerospikeWritePolicy(0, s.expiration)
									ttlPolicy.RecordExistsAction = aerospike.UPDATE_ONLY
									_, err = s.client.Operate(ttlPolicy, batchRecord.BatchRec().Key, aerospike.PutOp(
										aerospike.NewBin(
											"lastSpend",
											aerospike.NewIntegerValue(int(time.Now().Unix())),
										),
									))
									if err != nil {
										batch[idx].done <- fmt.Errorf("[SPEND_BATCH][%s] could not set lastSpend: %w", spend.Hash.String(), err)
									}
								}
							}
						}
					}
				}
			} else {
				s.logger.Warnf("[SPEND_BATCH][%s] utxos not found in aerospike in batch %d", spend.Hash.String(), batchId)
			}

			//s.logger.Warnf("[SPEND_BATCH][%s] successfully spent utxo in aerospike in batch %d", spend.Hash.String(), batchId)
			batch[idx].done <- nil
		}
	}
}

// sendLastSpendBatch sends a batch of last spend times to aerospike
// this is fire and forget, since it is not really a fatal thing if some of the records fail, it just means they won't get deleted
// all failures will be added on a retry channel for further processing
func (s *Store) sendLastSpendBatch(batch []*batchLastSpend) {
	batchId := s.batchId.Add(1)
	s.logger.Debugf("[LAST_SPEND_BATCH] sending last spend time %d of %d items", batchId, len(batch))
	defer func() {
		s.logger.Debugf("[LAST_SPEND_BATCH] sending last spend time %d of %d item DONE", batchId, len(batch))
	}()

	batchPolicy := util.GetAerospikeBatchPolicy()

	batchWritePolicy := util.GetAerospikeBatchWritePolicy(0, s.expiration) // set expiration

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	var err error
	for idx, bItem := range batch {
		batchRecords[idx] = aerospike.NewBatchWrite(batchWritePolicy, bItem.key, aerospike.PutOp(
			aerospike.NewBin(
				"lastSpend",
				aerospike.NewIntegerValue(bItem.time),
			),
		))
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[LAST_SPEND_BATCH][%d] failed to batch last spend time aerospike map utxos in batchId %d: %v", batchId, len(batch), err)
		// re-add all the items to the channel
		for _, bItem := range batch {
			s.lastSpendCh <- bItem
		}
		return
	}

	// batchOperate may have no errors, but some of the records may have failed
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			// key not found, no need to update
			s.logger.Warnf("[LAST_SPEND_BATCH][%s] key not found in aerospike in last spend batch %d: %v", batch[idx].hash.String(), batchId, err)
			continue
		} else if err != nil {
			s.logger.Errorf("[LAST_SPEND_BATCH][%s] error in aerospike last spend batch record, retrying: %d - %v", batch[idx].hash.String(), idx, err)
			// re-add the item to the channel
			s.lastSpendCh <- batch[idx]
		}
	}
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
	key, err := aerospike.NewKey("test", "set", "key")
	if err != nil {
		return -1, details, err
	}

	bin := aerospike.NewBin("bin", "value")
	err = s.client.PutBins(writePolicy, key, bin)
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
	prometheusUtxoMapGet.Inc()

	key, aErr := aerospike.NewKey(s.namespace, s.setName, spend.TxID[:])
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
			return &utxostore.Response{
				Status: int(utxostore.Status_NOT_FOUND),
			}, nil
		}
		s.logger.Errorf("Failed to get aerospike key: %v\n", aErr)
		return nil, aErr
	}

	var err error
	var spendingTxId *chainhash.Hash
	lockTime := uint32(0)
	if value != nil {
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		if ok {
			spendingTxIdBytes, ok := utxoMap[spend.Hash.String()].([]byte)
			if ok && spendingTxIdBytes != nil {
				spendingTxId, err = chainhash.NewHash(spendingTxIdBytes)
				if err != nil {
					return nil, fmt.Errorf("chainhash error: %w", err)
				}
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
func (s *Store) Store(_ context.Context, _ *bt.Tx, _ ...uint32) error {
	// no-op - should have already been stored by the tx meta store
	return nil
}

func (s *Store) StoreFromHashes(_ context.Context, _ chainhash.Hash, _ []chainhash.Hash, _ uint32) error {
	// not supported in aerospikemap implementation
	return nil
}

func (s *Store) Spend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoMapErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in aerospike Spend: %v\n", recoverErr)
		}
	}()

	spentSpends := make([]*utxostore.Spend, 0, len(spends))

	if s.spendBatcher != nil {
		g := errgroup.Group{}
		for _, spend := range spends {
			if spend == nil {
				continue
			}

			spend := spend
			g.Go(func() error {
				done := make(chan error)
				s.spendBatcher.Put(&batchSpend{
					spend: spend,
					done:  done,
				})

				// this waits for the batch to be sent and the response to be received from the batch operation
				batchErr := <-done
				if batchErr != nil {
					return batchErr
				}

				spentSpends = append(spentSpends, spend)

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			// revert the successfully spent utxos
			_ = s.UnSpend(ctx, spentSpends)
			return fmt.Errorf("error in aerospike spend record: %w", err)
		}

		prometheusUtxoMapSpend.Add(float64(len(spends)))

		return nil
	}

	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("timeout spending %d of %d utxos", i, len(spends))
			}
			return fmt.Errorf("context cancelled spending %d of %d utxos", i, len(spends))

		default:
			if spend == nil {
				continue
			}

			err = s.spendUtxo(policy, spend)
			if err != nil {
				if errors.Is(err, utxostore.NewErrSpent(spend.TxID, spend.Vout, spend.Hash, spend.SpendingTxID)) {
					return err
				}

				// another error encountered, reverse all spends and return error
				if resetErr := s.UnSpend(context.Background(), spends); resetErr != nil {
					s.logger.Errorf("ERROR in aerospike reset: %v\n", resetErr)
				}

				return err
			}
		}
	}

	return nil
}

func (s *Store) spendUtxo(policy *aerospike.WritePolicy, spend *utxostore.Spend) error {
	key, err := aerospike.NewKey(s.namespace, s.setName, spend.TxID[:])
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Spend", err.Error()).Inc()
		return fmt.Errorf("error failed creating key in aerospike Spend: %w", err)
	}

	policy.FilterExpression = aerospike.ExpAnd(
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

		// spent check - value of utxo hash in map should be nil
		aerospike.ExpEq(aerospike.ExpMapGetByKey(
			aerospike.MapReturnType.VALUE,
			aerospike.ExpTypeNIL,
			aerospike.ExpStringVal(spend.Hash.String()),
			aerospike.ExpMapBin("utxos"),
		), aerospike.ExpNilValue()),
	)

	response, err := s.client.Operate(policy, key, []*aerospike.Operation{
		aerospike.MapPutOp(
			aerospike.DefaultMapPolicy(),
			"utxos",
			spend.Hash.String(),
			spend.SpendingTxID.CloneBytes(),
		),
		aerospike.GetBinOp("utxos"),
	}...)
	if err != nil {
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			s.logger.Debugf("utxo %s was not found: %s", spend.TxID.String(), err.Error())
			return utxostore.ErrNotFound
		}

		if errors.Is(err, aerospike.ErrFilteredOut) {
			prometheusUtxoMapGet.Inc()
			errPolicy := util.GetAerospikeReadPolicy()
			errPolicy.ReplicaPolicy = aerospike.MASTER // we only want to read from the master for tx metadata, due to blockIDs being updated

			value, getErr := s.client.Get(errPolicy, key, "utxos", "locktime")
			if getErr != nil {
				return fmt.Errorf("could not see if the value was the same as before: %w", getErr)
			}

			locktime, ok := value.Bins["locktime"].(int)
			if ok {
				status := utxostore.CalculateUtxoStatus(nil, uint32(locktime), s.blockHeight.Load())
				if status == utxostore.Status_LOCKED {
					s.logger.Errorf("utxo %s is not spendable in block %d: %s", spend.Hash.String(), s.blockHeight.Load(), err.Error())
					return utxostore.NewErrLockTime(uint32(locktime), s.blockHeight.Load())
				}
			}

			// check whether we had the same value set as before
			utxosValue, ok := value.Bins["utxos"].(map[interface{}]interface{})
			if ok {
				// get utxo from map
				valueBytes, ok := utxosValue[spend.Hash.String()].([]byte)
				if ok {
					valueHash := chainhash.Hash(valueBytes)
					if spend.TxID.Equal(valueHash) {
						prometheusUtxoMapReSpend.Inc()
						return nil
					} else {
						prometheusUtxoMapSpendSpent.Inc()
						spendingTxHash, err := chainhash.NewHash(valueBytes)
						if err != nil {
							return utxostore.ErrChainHash
						}

						s.logger.Debugf("utxo %s was spent by %s", spend.TxID.String(), spendingTxHash)
						return utxostore.NewErrSpent(spend.TxID, spend.Vout, spend.Hash, spendingTxHash)
					}
				}
			}
		}

		prometheusUtxoMapErrors.WithLabelValues("Spend", err.Error()).Inc()
		return errors.Join(utxostore.ErrStore, errors.New("error in aerospike spend PutBins"), err)
	}

	prometheusUtxoMapSpend.Inc()

	// check whether all utxos are spent
	utxosValue, ok := response.Bins["utxos"].([]interface{})
	if ok {
		if len(utxosValue) == 2 {
			// utxos are in index 1 of the response
			utxos, ok := utxosValue[1].(map[interface{}]interface{})
			if ok {
				spentUtxos := 0
				for _, v := range utxos {
					if v != nil {
						spentUtxos++
					}
				}
				if spentUtxos == len(utxos) {
					// mark document as spent and add expiration for TTL
					if s.lastSpendBatcher != nil {
						s.lastSpendBatcher.Put(&batchLastSpend{
							key:  key,
							hash: *spend.Hash,
							time: int(time.Now().Unix()),
						})
					} else {
						ttlPolicy := util.GetAerospikeWritePolicy(0, s.expiration)
						ttlPolicy.RecordExistsAction = aerospike.UPDATE_ONLY
						_, err = s.client.Operate(ttlPolicy, key, aerospike.PutOp(
							aerospike.NewBin(
								"lastSpend",
								aerospike.NewIntegerValue(int(time.Now().Unix())),
							),
						))
						if err != nil {
							return fmt.Errorf("could not set lastSpend: %w", err)
						}
					}
				}
			}
		}
	}

	return nil
}

func (s *Store) UnSpend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("timeout un-spending %d of %d utxos", i, len(spends))
			}
			return fmt.Errorf("context cancelled un-spending %d of %d utxos", i, len(spends))
		default:
			if err = s.unSpend(ctx, spend); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Store) unSpend(_ context.Context, spend *utxostore.Spend) error {
	policy := util.GetAerospikeWritePolicy(3, math.MaxUint32)

	key, err := aerospike.NewKey(s.namespace, s.setName, spend.TxID[:])
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", err.Error()).Inc()
		return err
	}

	_, err = s.client.Operate(policy, key, aerospike.MapPutOp(
		aerospike.DefaultMapPolicy(),
		"utxos",
		spend.Hash.String(),
		nil,
	))

	prometheusUtxoMapReset.Inc()

	return err
}

func (s *Store) Delete(_ context.Context, tx *bt.Tx) error {
	policy := util.GetAerospikeWritePolicy(0, 0)

	key, err := aerospike.NewKey(s.namespace, s.setName, tx.TxIDChainHash()[:])
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Delete", err.Error()).Inc()
		s.logger.Errorf("ERROR panic in aerospike Delete: %v\n", err)
		return err
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		// if the key is not found, we don't need to delete, it's not there anyway
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return nil
		}

		prometheusUtxoMapErrors.WithLabelValues("Delete", err.Error()).Inc()
		return errors.Join(errors.New("error in aerospike delete key"), err)
	}

	prometheusUtxoMapDelete.Inc()

	return nil
}

func (s *Store) DeleteSpends(_ bool) {
	// noop
}

//func getBinsToStore(ctx context.Context, tx *bt.Tx, lockTime uint32) ([]*aerospike.Bin, error) {
//	fee, utxoHashes, err := utxostore.GetFeesAndUtxoHashes(ctx, tx)
//	if err != nil {
//		prometheusUtxoMapErrors.WithLabelValues("Store", err.Error()).Inc()
//		return nil, fmt.Errorf("failed to get fees and utxo hashes: %v", err)
//	}
//
//	utxos := make(map[interface{}]interface{})
//	for i, utxoHash := range utxoHashes {
//		select {
//		case <-ctx.Done():
//			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
//				return nil, fmt.Errorf("timeout getBinsToStore#1 %d of %d utxos", i, len(utxoHashes))
//			}
//			return nil, fmt.Errorf("context cancelled getBinsToStore#1 %d of %d utxos", i, len(utxoHashes))
//		default:
//			utxos[utxoHash.String()] = aerospike.NewNullValue()
//		}
//	}
//
//	parentTxHashes := make([][]byte, 0, len(tx.Inputs))
//	for i, input := range tx.Inputs {
//		select {
//		case <-ctx.Done():
//			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
//				return nil, fmt.Errorf("timeout getBinsToStore#2 %d of %d utxos", i, len(tx.Inputs))
//			}
//			return nil, fmt.Errorf("context cancelled getBinsToStore#2 %d of %d utxos", i, len(tx.Inputs))
//		default:
//			parentTxHashes = append(parentTxHashes, input.PreviousTxIDChainHash().CloneBytes())
//		}
//	}
//
//	blockIDs := make([]uint32, 0)
//
//	bins := []*aerospike.Bin{
//		aerospike.NewBin("tx", tx.ExtendedBytes()),
//		aerospike.NewBin("fee", aerospike.NewIntegerValue(int(fee))),
//		aerospike.NewBin("size", aerospike.NewIntegerValue(tx.Size())),
//		aerospike.NewBin("locktime", aerospike.NewIntegerValue(int(lockTime))),
//		aerospike.NewBin("utxos", aerospike.NewMapValue(utxos)),
//		aerospike.NewBin("parentTxHashes", parentTxHashes),
//		aerospike.NewBin("blockIDs", blockIDs),
//	}
//
//	return bins, nil
//}
