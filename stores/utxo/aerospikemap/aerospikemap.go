// //go:build aerospike

package aerospikemap

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
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

type Store struct {
	u           *url.URL
	client      *uaerospike.Client
	namespace   string
	setName     string
	logger      ulogger.Logger
	blockHeight uint32
	expiration  uint32
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
	return &Store{
		u:           u,
		client:      client,
		namespace:   namespace,
		setName:     setName,
		logger:      logger,
		blockHeight: 0,
		expiration:  expiration,
	}, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.logger.Debugf("setting block height to %d", blockHeight)
	s.blockHeight = blockHeight
	return nil
}

func (s *Store) GetBlockHeight() (uint32, error) {
	return s.blockHeight, nil
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
		Status:       int(utxostore.CalculateUtxoStatus(spendingTxId, lockTime, s.blockHeight)),
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
				// error encountered, reverse all spends and return error
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
			aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(int64(s.blockHeight))),

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
			value, getErr := s.client.Get(util.GetAerospikeReadPolicy(), key, "utxos", "locktime")
			if getErr != nil {
				return fmt.Errorf("could not see if the value was the same as before: %w", getErr)
			}

			locktime, ok := value.Bins["locktime"].(int)
			if ok {
				status := utxostore.CalculateUtxoStatus(nil, uint32(locktime), s.blockHeight)
				if status == utxostore.Status_LOCKED {
					s.logger.Errorf("utxo %s is not spendable in block %d: %s", spend.Hash.String(), s.blockHeight, err.Error())
					return utxostore.NewErrLockTime(uint32(locktime), s.blockHeight)
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
