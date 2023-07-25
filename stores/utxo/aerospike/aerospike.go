// //go:build aerospike

package aerospike

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/aerospike/aerospike-client-go/v6"
	"github.com/aerospike/aerospike-client-go/v6/types"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusUtxoGet        prometheus.Counter
	prometheusUtxoStore      prometheus.Counter
	prometheusUtxoReStore    prometheus.Counter
	prometheusUtxoStoreSpent prometheus.Counter
	prometheusUtxoSpend      prometheus.Counter
	prometheusUtxoReSpend    prometheus.Counter
	prometheusUtxoSpendSpent prometheus.Counter
	prometheusUtxoReset      prometheus.Counter
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
	prometheusUtxoStoreSpent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_store_spent",
			Help: "Number of utxo store calls that were already spent to aerospike",
		},
	)
	prometheusUtxoReStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_restore",
			Help: "Number of utxo restore calls done to aerospike",
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

type Store struct {
	client      *aerospike.Client
	namespace   string
	blockHeight uint32
}

func New(url *url.URL) (*Store, error) {
	//asl.Logger.SetLevel(asl.DEBUG)

	namespace := url.Path[1:]

	client, err := util.GetAerospikeClient(url)
	if err != nil {
		return nil, err
	}

	return &Store{
		client:      client,
		namespace:   namespace,
		blockHeight: 0,
	}, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.blockHeight = blockHeight
	return nil
}

func (s *Store) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	prometheusUtxoGet.Inc()
	return nil, nil
}

func (s *Store) Store(_ context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.CREATE_ONLY
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Store", err.Error()).Inc()
		fmt.Printf("Failed to store new aerospike key: %v\n", err)
		return nil, err
	}

	bins := []*aerospike.Bin{
		aerospike.NewBin("locktime", nLockTime),
	}

	err = s.client.PutBins(policy, key, bins...)
	if err != nil {
		// check whether we already set this utxo
		prometheusUtxoGet.Inc()
		value, getErr := s.client.Get(nil, key, "txid")
		if getErr == nil && value != nil {
			txid, ok := value.Bins["txid"].([]byte)
			if ok && len(txid) != 0 {
				prometheusUtxoStoreSpent.Inc()
				spendingTxHash, err := chainhash.NewHash(txid)
				if err != nil {
					return nil, err
				}
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: spendingTxHash,
				}, nil
			}

			prometheusUtxoReStore.Inc()
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		}

		prometheusUtxoErrors.WithLabelValues("Store", getErr.Error()).Inc()
		if getErr.Error() == types.ResultCodeToString(types.KEY_NOT_FOUND_ERROR) {
			fmt.Printf("Failed to find aerospike key in utxostore: %v\n", err)
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_NOT_FOUND),
			}, nil // todo fix should raise error
		}
		fmt.Printf("Error occurred in aerospike store: %v\n", getErr)
		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK), // should be created, we need this for the block assembly
	}, nil
}

func (s *Store) BatchStore(_ context.Context, hashes []*chainhash.Hash) (*utxostore.BatchResponse, error) {
	batchWritePolicy := aerospike.NewBatchWritePolicy()
	batchWritePolicy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
	batchWritePolicy.RecordExistsAction = aerospike.CREATE_ONLY
	batchWritePolicy.CommitLevel = aerospike.COMMIT_ALL // strong consistency
	batchWritePolicy.Generation = 0

	batchPolicy := aerospike.NewBatchPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(hashes))
	for _, hash := range hashes {
		key, _ := aerospike.NewKey(s.namespace, "utxo", hash[:])

		bin := aerospike.NewBin("locktime", 0)
		record := aerospike.NewBatchWrite(batchWritePolicy, key,
			aerospike.PutOp(bin),
		)

		// Add to batch
		batchRecords = append(batchRecords, record)
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Store", err.Error()).Inc()
		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &utxostore.BatchResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s *Store) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (utxoResponse *utxostore.UTXOResponse, err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			fmt.Printf("ERROR panic in aerospike Spend: %v\n", recoverErr)
		}
	}()

	// set the default we return when we recover from a panic
	utxoResponse = &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}

	//expiration := uint32(time.Now().Add(24 * time.Hour).Unix())
	policy := util.GetAerospikeWritePolicy(1, 0)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Spend", err.Error()).Inc()
		fmt.Printf("ERROR panic in aerospike Spend: %v\n", err)
		return nil, err
	}

	policy.FilterExpression = aerospike.ExpOr(
		// anything below the block height is spendable, including 0
		aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(int64(s.blockHeight))),
		aerospike.ExpAnd(
			aerospike.ExpGreaterEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(500000000)),
			// TODO Note that since the adoption of BIP 113, the time-based nLockTime is compared to the 11-block median
			// time past (the median timestamp of the 11 blocks preceding the block in which the transaction is mined),
			// and not the block time itself.
			aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(time.Now().Unix())),
		),
	)

	bin := aerospike.NewBin("txid", txID.CloneBytes())
	err = s.client.PutBins(policy, key, bin)
	if err != nil {
		if errors.Is(err, aerospike.ErrFilteredOut) {
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_LOCK_TIME),
			}, nil
		}

		// check whether we had the same value set as before
		prometheusUtxoGet.Inc()
		value, getErr := s.client.Get(nil, key, "txid")
		if getErr != nil {
			return nil, getErr
		}
		valueBytes, ok := value.Bins["txid"].([]byte)
		if ok && len(valueBytes) == 32 {
			if [32]byte(valueBytes) == *txID {
				prometheusUtxoReSpend.Inc()
				return &utxostore.UTXOResponse{
					Status: int(utxostore_api.Status_OK),
				}, nil
			} else {
				prometheusUtxoSpendSpent.Inc()
				spendingTxHash, err := chainhash.NewHash(valueBytes)
				if err != nil {
					return nil, err
				}
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: spendingTxHash,
				}, nil
			}
		}
		prometheusUtxoErrors.WithLabelValues("Spend", err.Error()).Inc()
		fmt.Printf("ERROR in aerospike Spend PutBins: %v\n", err)
		return nil, err
	}

	prometheusUtxoSpend.Inc()

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s *Store) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	policy := util.GetAerospikeWritePolicy(2, 0)
	policy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("MoveUpBlock", err.Error()).Inc()
		fmt.Printf("ERROR panic in aerospike MoveUpBlock: %v\n", err)
		return nil, err
	}

	value, getErr := s.client.Get(util.GetAerospikeReadPolicy(), key, "locktime")
	if getErr != nil {
		prometheusUtxoErrors.WithLabelValues("Get", err.Error()).Inc()
		fmt.Printf("ERROR panic in aerospike get key: %v\n", err)
		return nil, err
	}
	nLockTime, ok := value.Bins["locktime"].(int)
	if !ok {
		nLockTime = 0
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		prometheusUtxoErrors.WithLabelValues("Reset", err.Error()).Inc()
		fmt.Printf("ERROR panic in aerospike Reset delete key: %v\n", err)
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return s.Store(ctx, hash, uint32(nLockTime))
}

func (s *Store) DeleteSpends(_ bool) {
	// noop
}
