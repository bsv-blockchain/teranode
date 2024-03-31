// //go:build aerospike

package aerospikemap

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
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
	setName = "utxo"

	prometheusTxMetaGet      prometheus.Counter
	prometheusTxMetaSet      prometheus.Counter
	prometheusTxMetaSetMined prometheus.Counter
	prometheusTxMetaDelete   prometheus.Counter
)

func init() {
	prometheusTxMetaGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_get",
			Help: "Number of txmeta get calls done to aerospike",
		},
	)
	prometheusTxMetaSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set",
			Help: "Number of txmeta set calls done to aerospike",
		},
	)
	prometheusTxMetaSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined",
			Help: "Number of txmeta set_mined calls done to aerospike",
		},
	)
	prometheusTxMetaDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_delete",
			Help: "Number of txmeta delete calls done to aerospike",
		},
	)

	if gocore.Config().GetBool("aerospike_debug", true) {
		asl.Logger.SetLevel(asl.DEBUG)
	}

}

type Store struct {
	client    *uaerospike.Client
	namespace string
}

func New(logger ulogger.Logger, u *url.URL) (*Store, error) {
	logger = logger.New("aero_map_store")

	namespace := u.Path[1:]

	client, err := util.GetAerospikeClient(logger, u)
	if err != nil {
		return nil, err
	}

	return &Store{
		client:    client,
		namespace: namespace,
	}, nil
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"fee", "size", "parentTxHashes", "blockIDs"})
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.get(ctx, hash, []string{"tx", "fee", "size", "parentTxHashes", "blockIDs"})
}

func (s *Store) get(_ context.Context, hash *chainhash.Hash, bins []string) (*txmeta.Data, error) {
	prometheusTxMetaGet.Inc()

	// in the map implementation, we are using the utxo store for the data
	key, aeroErr := aerospike.NewKey(s.namespace, setName, hash[:])
	if aeroErr != nil {
		return nil, aeroErr
	}

	var value *aerospike.Record

	readPolicy := util.GetAerospikeReadPolicy()
	value, aeroErr = s.client.Get(readPolicy, key, bins...)
	if aeroErr != nil {
		return nil, aeroErr
	}

	if value == nil {
		return nil, nil
	}

	var parentTxHashes []chainhash.Hash
	if value.Bins["parentTxHashes"] != nil {
		parentTxHashesInterface, ok := value.Bins["parentTxHashes"].([]interface{})
		if ok {
			parentTxHashes = make([]chainhash.Hash, len(parentTxHashesInterface))
			for i, v := range parentTxHashesInterface {
				if len(v.([]byte)) != 32 {
					return nil, fmt.Errorf("invalid parentTxHashes length")
				}
				parentTxHashes[i] = chainhash.Hash(v.([]byte))
			}
		}
	}

	var blockIDs []uint32
	if value.Bins["blockIDs"] != nil {
		// convert from []interface{} to []uint32
		for _, v := range value.Bins["blockIDs"].([]interface{}) {
			blockIDs = append(blockIDs, uint32(v.(int)))
		}
	}

	var tx *bt.Tx
	var err error
	if value.Bins["tx"] != nil {
		tx, err = bt.NewTxFromBytes(value.Bins["tx"].([]byte))
		if err != nil {
			return nil, fmt.Errorf("invalid tx: %w", err)
		}
	}

	// transform the aerospike interface{} into the correct types
	status := &txmeta.Data{
		Tx:             tx,
		Fee:            uint64(value.Bins["fee"].(int)),
		SizeInBytes:    uint64(value.Bins["size"].(int)),
		ParentTxHashes: parentTxHashes,
		BlockIDs:       blockIDs,
	}

	return status, nil
}

func (s *Store) MetaBatchDecorate(ctx context.Context, hashes []*txmeta.MissingTxHash, fields ...string) error {
	return errors.New("not implemented")
}

func (s *Store) Create(_ context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	prometheusTxMetaSet.Inc()

	// this is a no-op for the txmeta map implementation - it is created in the utxo store
	// we need to return it for performance reasons in the validator
	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	return txMeta, nil
}

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	for _, hash := range hashes {
		if err = s.SetMined(ctx, hash, blockID); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) SetMined(_ context.Context, hash *chainhash.Hash, blockID uint32) error {
	policy := util.GetAerospikeWritePolicy(0, 0)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	//policy.Expiration = uint32(time.Now().Add(24 * time.Hour).Unix())

	key, err := aerospike.NewKey(s.namespace, setName, hash[:])
	if err != nil {
		return err
	}

	readPolicy := util.GetAerospikeReadPolicy()
	record, err := s.client.Get(readPolicy, key, "blockIDs")
	if err != nil {
		return err
	}

	blockIDs, ok := record.Bins["blockIDs"].([]interface{})
	if !ok {
		blockIDs = make([]interface{}, 0)
	}
	blockIDs = append(blockIDs, blockID)

	bin := aerospike.NewBin("blockIDs", blockIDs)

	err = s.client.PutBins(policy, key, bin)
	if err != nil {
		return err
	}

	prometheusTxMetaSetMined.Inc()

	return nil
}

func (s *Store) Delete(_ context.Context, _ *chainhash.Hash) error {
	prometheusTxMetaDelete.Inc()

	// this is a no-op for the map implementation - it is deleted in the utxo store
	return nil
}
