package aerospike

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	aero "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
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
}

type Store struct {
	client    *aero.Client
	namespace string
}

func New(host string, port int, namespace string) (*Store, error) {
	client, err := aero.NewClient(host, port)
	if err != nil {
		return nil, err
	}

	return &Store{
		client:    client,
		namespace: namespace,
	}, nil
}

func (s Store) Get(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	prometheusUtxoGet.Inc()
	return nil, nil
}

func (s Store) Store(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	policy := aero.NewWritePolicy(0, 0)
	policy.RecordExistsAction = aero.CREATE_ONLY
	policy.CommitLevel = aero.COMMIT_ALL // strong consistency

	key, err := aero.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		return nil, err
	}

	bins := aero.BinMap{
		"txid": []byte{},
	}
	err = s.client.Put(policy, key, bins)
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
				return &store.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: spendingTxHash,
				}, nil
			}

			prometheusUtxoReStore.Inc()
			return &store.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		}

		if getErr.Error() == types.ResultCodeToString(types.KEY_NOT_FOUND_ERROR) {
			return &store.UTXOResponse{
				Status: int(utxostore_api.Status_NOT_FOUND),
			}, nil
		}

		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_OK), // should be created, we need this for the block assembly
	}, nil
}

func (s Store) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (utxoResponse *store.UTXOResponse, err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			fmt.Printf("ERROR panic in aerospike Spend: %v", recoverErr)
		}
	}()

	// set the default we return when we recover from a panic
	utxoResponse = &store.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}

	policy := aero.NewWritePolicy(1, 0)
	policy.RecordExistsAction = aero.UPDATE_ONLY
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.CommitLevel = aero.COMMIT_ALL // strong consistency

	key, err := aero.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		return nil, err
	}
	bins := aero.BinMap{
		"txid": txID.CloneBytes(),
	}

	err = s.client.Put(policy, key, bins)
	if err != nil {
		// check whether we had the same value set as before
		prometheusUtxoGet.Inc()
		value, getErr := s.client.Get(nil, key, "txid")
		if getErr != nil {
			return nil, getErr
		}
		valueBytes, ok := value.Bins["txid"].([]byte)
		if ok {
			if [32]byte(valueBytes) == [32]byte(txID[:]) {
				prometheusUtxoReSpend.Inc()
				return &store.UTXOResponse{
					Status: int(utxostore_api.Status_OK),
				}, nil
			} else {
				prometheusUtxoSpendSpent.Inc()
				spendingTxHash, err := chainhash.NewHash(valueBytes)
				if err != nil {
					return nil, err
				}
				return &store.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: spendingTxHash,
				}, nil
			}
		}
		return nil, err
	}

	prometheusUtxoSpend.Inc()

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s Store) Reset(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	policy := aero.NewWritePolicy(2, 0)
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.CommitLevel = aero.COMMIT_ALL // strong consistency

	key, err := aero.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		return nil, err
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return s.Store(ctx, hash)
}
