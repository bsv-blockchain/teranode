//go:build aerospike

package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/txstatus"
	"github.com/aerospike/aerospike-client-go/v6"
	asl "github.com/aerospike/aerospike-client-go/v6/logger"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusTxStatusGet      prometheus.Counter
	prometheusTxStatusSet      prometheus.Counter
	prometheusTxStatusSetMined prometheus.Counter
	prometheusTxStatusDelete   prometheus.Counter
)

func init() {
	prometheusTxStatusGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txstatus_get",
			Help: "Number of txstatus get calls done to aerospike",
		},
	)
	prometheusTxStatusSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txstatus_set",
			Help: "Number of txstatus set calls done to aerospike",
		},
	)
	prometheusTxStatusSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txstatus_set_mined",
			Help: "Number of txstatus set_mined calls done to aerospike",
		},
	)
	prometheusTxStatusDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txstatus_delete",
			Help: "Number of txstatus delete calls done to aerospike",
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

func (s *Store) Get(_ context.Context, hash *chainhash.Hash) (*txstatus.Status, error) {
	prometheusTxStatusGet.Inc()

	key, aeroErr := aerospike.NewKey(s.namespace, "txstatus", hash[:])
	if aeroErr != nil {
		return nil, aeroErr
	}

	var value *aerospike.Record
	value, aeroErr = s.client.Get(aerospike.NewPolicy(), key)
	if aeroErr != nil {
		return nil, aeroErr
	}

	if value == nil {
		return nil, nil
	}

	var err error

	var parentTxHashes []*chainhash.Hash
	if value.Bins["parentTxHashes"] != nil {
		parentTxHashesInterface, ok := value.Bins["parentTxHashes"].([]interface{})
		if ok {
			parentTxHashes = make([]*chainhash.Hash, len(parentTxHashesInterface))
			for i, v := range parentTxHashesInterface {
				parentTxHashes[i], err = chainhash.NewHash(v.([]byte))
				if err != nil {
					return nil, err
				}
			}
		}
	}

	var utxoHashes []*chainhash.Hash
	if value.Bins["utxoHashes"] != nil {
		utxoHashesInterface, ok := value.Bins["utxoHashes"].([]interface{})
		if ok {
			utxoHashes = make([]*chainhash.Hash, len(utxoHashesInterface))
			for i, v := range utxoHashesInterface {
				utxoHashes[i], err = chainhash.NewHash(v.([]byte))
				if err != nil {
					return nil, err
				}
			}
		}
	}

	var blockHashes []*chainhash.Hash
	if value.Bins["blockHashes"] != nil {
		blockHashesInterface := value.Bins["blockHashes"].([]interface{})
		blockHashes = make([]*chainhash.Hash, len(blockHashesInterface))
		for i, v := range blockHashesInterface {
			blockHashes[i], err = chainhash.NewHash(v.([]byte))
			if err != nil {
				return nil, err
			}
		}
	}

	// transform the aerospike interface{} into the correct types
	status := &txstatus.Status{
		Fee:            uint64(value.Bins["fee"].(int)),
		ParentTxHashes: parentTxHashes,
		UtxoHashes:     utxoHashes,
		FirstSeen:      time.Unix(int64(value.Bins["firstSeen"].(int)), 0),
		BlockHashes:    blockHashes,
	}

	return status, nil
}

func (s *Store) Create(_ context.Context, hash *chainhash.Hash, fee uint64, parentTxHashes []*chainhash.Hash, utxoHashes []*chainhash.Hash) error {
	policy := aerospike.NewWritePolicy(0, 0)
	policy.RecordExistsAction = aerospike.CREATE_ONLY
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "txstatus", hash[:])
	if err != nil {
		return err
	}

	parentTxHashesInterface := make([]interface{}, len(parentTxHashes))
	for i, v := range parentTxHashes {
		parentTxHashesInterface[i] = v[:]
	}

	utxoHashesInterface := make([]interface{}, len(utxoHashes))
	for i, v := range utxoHashes {
		utxoHashesInterface[i] = v[:]
	}

	bins := aerospike.BinMap{
		"fee":            int(fee),
		"parentTxHashes": parentTxHashesInterface,
		"utxoHashes":     utxoHashesInterface,
		"firstSeen":      time.Now().Unix(),
	}
	err = s.client.Put(policy, key, bins)
	if err != nil {
		return err
	}

	prometheusTxStatusSet.Inc()

	return nil
}

func (s *Store) SetMined(_ context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	policy := aerospike.NewWritePolicy(0, 0)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "txstatus", hash[:])
	if err != nil {
		return err
	}

	readPolicy := aerospike.NewPolicy()
	record, err := s.client.Get(readPolicy, key, "blockHashes")
	if err != nil {
		return err
	}

	blockHashes, ok := record.Bins["blockHashes"].([]interface{})
	if !ok {
		blockHashes = make([]interface{}, 0)
	}
	blockHashes = append(blockHashes, *blockHash)

	bin := aerospike.NewBin("blockHashes", blockHashes)

	err = s.client.PutBins(policy, key, bin)
	if err != nil {
		return err
	}

	prometheusTxStatusSetMined.Inc()

	return nil
}

func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	//TODO implement me
	prometheusTxStatusDelete.Inc()
	panic("implement me")
}
