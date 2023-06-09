//go:build aerospike

package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/aerospike/aerospike-client-go/v6"
	asl "github.com/aerospike/aerospike-client-go/v6/logger"
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
	client    *aerospike.Client
	namespace string
}

func New(url *url.URL) (*Store, error) {
	asl.Logger.SetLevel(asl.DEBUG)

	if len(url.Path) < 1 {
		return nil, fmt.Errorf("aerospike namespace not found")
	}
	namespace := url.Path[1:]

	policy := aerospike.NewClientPolicy()
	policy.LimitConnectionsToQueueSize = false
	policy.ConnectionQueueSize = 1024

	if url.User != nil {
		policy.AuthMode = 2

		policy.User = url.User.Username()
		var ok bool
		policy.Password, ok = url.User.Password()
		if !ok {
			policy.User = ""
			policy.Password = ""
		}
	}

	// url can be either aerospike://host:port/namespace or aerospike://host:port,host:port/namespace
	hosts := []*aerospike.Host{}
	urlHosts := strings.Split(url.Host, ",")
	for _, host := range urlHosts {
		hostParts := strings.Split(host, ":")
		if len(hostParts) == 2 {
			port, err := strconv.ParseInt(hostParts[1], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid port %v", hostParts[1])
			}

			hosts = append(hosts, &aerospike.Host{
				Name: hostParts[0],
				Port: int(port),
			})
		} else if len(hostParts) == 1 {
			hosts = append(hosts, &aerospike.Host{
				Name: hostParts[0],
				Port: 3000,
			})
		} else {
			return nil, fmt.Errorf("invalid host %v", host)
		}
	}

	fmt.Printf("url %v policy %v\n", url, policy)

	client, err := aerospike.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Connected %v", client.IsConnected())

	return &Store{
		client:    client,
		namespace: namespace,
	}, nil
}

func (s *Store) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	prometheusUtxoGet.Inc()
	return nil, nil
}

func (s *Store) Store(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	policy := aerospike.NewWritePolicy(0, 0)
	policy.RecordExistsAction = aerospike.CREATE_ONLY
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		return nil, err
	}

	bins := aerospike.BinMap{
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

		if getErr.Error() == types.ResultCodeToString(types.KEY_NOT_FOUND_ERROR) {
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_NOT_FOUND),
			}, nil
		}

		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK), // should be created, we need this for the block assembly
	}, nil
}

func (s *Store) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (utxoResponse *utxostore.UTXOResponse, err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			fmt.Printf("ERROR panic in aerospike Spend: %v", recoverErr)
		}
	}()

	// set the default we return when we recover from a panic
	utxoResponse = &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}

	policy := aerospike.NewWritePolicy(1, 0)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		return nil, err
	}
	bins := aerospike.BinMap{
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
		return nil, err
	}

	prometheusUtxoSpend.Inc()

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s *Store) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	policy := aerospike.NewWritePolicy(2, 0)
	policy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
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

func (s *Store) DeleteSpends(_ bool) {
	// noop
}
