// //go:build aerospike

package aerospike

import (
	"context"
	"net/url"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/txmeta"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/aerospike/aerospike-client-go/v6"
	asl "github.com/aerospike/aerospike-client-go/v6/logger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusTxMetaGet      prometheus.Counter
	prometheusTxMetaSet      prometheus.Counter
	prometheusTxMetaSetMined prometheus.Counter
	prometheusTxMetaDelete   prometheus.Counter
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
	client    *aerospike.Client
	namespace string
}

func New(url *url.URL) (*Store, error) {
	asl.Logger.SetLevel(asl.DEBUG)

	namespace := url.Path[1:]

	client, err := util.GetAerospikeClient(url)
	if err != nil {
		return nil, err
	}

	return &Store{
		client:    client,
		namespace: namespace,
	}, nil
}

func (s *Store) Get(_ context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	prometheusTxMetaGet.Inc()

	key, aeroErr := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if aeroErr != nil {
		return nil, aeroErr
	}

	var value *aerospike.Record

	readPolicy := util.GetAerospikeReadPolicy()
	value, aeroErr = s.client.Get(readPolicy, key)
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

	var nLockTime uint32
	if value.Bins["lockTime"] != nil {
		nLockTime = uint32(value.Bins["lockTime"].(int))
	}

	// transform the aerospike interface{} into the correct types
	status := &txmeta.Data{
		Fee:            uint64(value.Bins["fee"].(int)),
		ParentTxHashes: parentTxHashes,
		UtxoHashes:     utxoHashes,
		FirstSeen:      time.Unix(int64(value.Bins["firstSeen"].(int)), 0),
		BlockHashes:    blockHashes,
		LockTime:       nLockTime,
	}

	return status, nil
}

func (s *Store) Create(_ context.Context, hash *chainhash.Hash, fee uint64, parentTxHashes []*chainhash.Hash,
	utxoHashes []*chainhash.Hash, nLockTime uint32) error {

	policy := util.GetAerospikeWritePolicy(0, 0)
	policy.RecordExistsAction = aerospike.CREATE_ONLY
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
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

	bins := []*aerospike.Bin{
		aerospike.NewBin("fee", int(fee)),
		aerospike.NewBin("parentTxHashes", parentTxHashesInterface),
		aerospike.NewBin("utxoHashes", utxoHashesInterface),
		aerospike.NewBin("firstSeen", time.Now().Unix()),
		aerospike.NewBin("lockTime", int(nLockTime)),
	}
	err = s.client.PutBins(policy, key, bins...)
	if err != nil {
		return err
	}

	prometheusTxMetaSet.Inc()

	return nil
}

func (s *Store) SetMined(_ context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	policy := util.GetAerospikeWritePolicy(0, 0)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency
	//policy.Expiration = uint32(time.Now().Add(24 * time.Hour).Unix())

	key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if err != nil {
		return err
	}

	readPolicy := util.GetAerospikeReadPolicy()
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

	prometheusTxMetaSetMined.Inc()

	return nil
}

func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	policy := util.GetAerospikeWritePolicy(0, 0)
	policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if err != nil {
		return err
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		return err
	}

	prometheusTxMetaDelete.Inc()

	return nil
}
