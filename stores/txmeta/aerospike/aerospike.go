// //go:build aerospike

package aerospike

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/aerospike/aerospike-client-go/v6"
	asl "github.com/aerospike/aerospike-client-go/v6/logger"
	"github.com/aerospike/aerospike-client-go/v6/types"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
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
	timeout   time.Duration
	namespace string
}

func New(u *url.URL) (*Store, error) {
	asl.Logger.SetLevel(asl.DEBUG)

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

	return &Store{
		client:    client,
		namespace: namespace,
		timeout:   timeout,
	}, nil
}

func (s *Store) Get(_ context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	prometheusTxMetaGet.Inc()

	key, aeroErr := aerospike.NewKey(s.namespace, "txmeta", hash[:])
	if aeroErr != nil {
		return nil, aeroErr
	}

	var value *aerospike.Record

	options := make([]util.AerospikeReadPolicyOptions, 0)

	if s.timeout > 0 {
		options = append(options, util.WithTotalTimeout(s.timeout))
	}

	readPolicy := util.GetAerospikeReadPolicy(options...)
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
		Tx:             value.Bins["tx"].(*bt.Tx),
		Fee:            uint64(value.Bins["fee"].(int)),
		SizeInBytes:    uint64(value.Bins["sizeInBytes"].(int)),
		ParentTxHashes: parentTxHashes,
		UtxoHashes:     utxoHashes,
		FirstSeen:      uint32(value.Bins["firstSeen"].(int)),
		BlockHashes:    blockHashes,
		LockTime:       nLockTime,
	}

	return status, nil
}

func (s *Store) Create(_ context.Context, tx *bt.Tx, hash *chainhash.Hash, fee uint64, sizeInBytes uint64, parentTxHashes []*chainhash.Hash,
	utxoHashes []*chainhash.Hash, nLockTime uint32) error {

	options := make([]util.AerospikeWritePolicyOptions, 0)

	if s.timeout > 0 {
		options = append(options, util.WithTotalTimeoutWrite(s.timeout))
	}

	policy := util.GetAerospikeWritePolicy(0, 0, options...)
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
		aerospike.NewBin("tx", tx),
		aerospike.NewBin("fee", int(fee)),
		aerospike.NewBin("sizeInBytes", int(sizeInBytes)),
		aerospike.NewBin("parentTxHashes", parentTxHashesInterface),
		aerospike.NewBin("utxoHashes", utxoHashesInterface),
		aerospike.NewBin("firstSeen", time.Now().Unix()),
		aerospike.NewBin("lockTime", int(nLockTime)),
	}
	err = s.client.PutBins(policy, key, bins...)
	if err != nil {
		aeroErr := &aerospike.AerospikeError{}
		if ok := errors.As(err, &aeroErr); ok {
			if aeroErr.ResultCode == types.KEY_EXISTS_ERROR {
				return txmeta.ErrAlreadyExists
			}
		}

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
