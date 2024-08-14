// //go:build aerospike

package aerospike

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"

	"github.com/bitcoin-sv/ubsv/stores/blob"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/ordishs/gocore"
)

const defaultUxtoBatchSize = 20_000

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

type Store struct {
	url             *url.URL
	client          *uaerospike.Client
	namespace       string
	setName         string
	expiration      uint32
	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32
	logger          ulogger.Logger
	batchId         atomic.Uint64
	storeBatcher    *batcher.Batcher2[batchStoreItem]
	getBatcher      *batcher.Batcher2[batchGetItem]
	spendBatcher    *batcher.Batcher2[batchSpend]
	externalStore   blob.Store
	utxoBatchSize   int
}

func New(logger ulogger.Logger, aerospikeURL *url.URL) (*Store, error) {
	initPrometheusMetrics()
	if gocore.Config().GetBool("aerospike_debug", true) {
		asl.Logger.SetLevel(asl.DEBUG)
	}

	namespace := aerospikeURL.Path[1:]

	client, err := util.GetAerospikeClient(logger, aerospikeURL)
	if err != nil {
		return nil, err
	}

	placeholderKey, err = aerospike.NewKey(namespace, "placeholderKey", "placeHolderKey")
	if err != nil {
		log.Fatal("Failed to init placeholder key")
	}

	expiration := uint32(0)
	expirationValue := aerospikeURL.Query().Get("expiration")
	if expirationValue != "" {
		expiration64, err := strconv.ParseUint(expirationValue, 10, 64)
		if err != nil {
			return nil, errors.NewInvalidArgumentError("could not parse expiration %s", expirationValue, err)
		}
		expiration = uint32(expiration64)
	}

	setName := aerospikeURL.Query().Get("set")
	if setName == "" {
		setName = "txmeta"
	}

	externalStoreUrl, err := url.Parse(aerospikeURL.Query().Get("externalStore"))
	if err != nil {
		return nil, err
	}

	externalStore, err := blob.NewStore(logger, externalStoreUrl)
	if err != nil {
		return nil, err
	}

	s := &Store{
		url:           aerospikeURL,
		client:        client,
		namespace:     namespace,
		setName:       setName,
		expiration:    expiration,
		logger:        logger,
		externalStore: externalStore,
		utxoBatchSize: 20_000, // Do not change this value, it is used to calculate the offset for the output
	}

	storeBatchSize, _ := gocore.Config().GetInt("utxostore_storeBatcherSize", 256)
	storeBatchDurationStr, _ := gocore.Config().GetInt("utxostore_storeBatcherDurationMillis", 10)
	storeBatchDuration := time.Duration(storeBatchDurationStr) * time.Millisecond
	s.storeBatcher = batcher.New[batchStoreItem](storeBatchSize, storeBatchDuration, s.sendStoreBatch, true)

	getBatchSize, _ := gocore.Config().GetInt("utxostore_getBatcherSize", 1024)
	getBatchDurationStr, _ := gocore.Config().GetInt("utxostore_getBatcherDurationMillis", 10)
	getBatchDuration := time.Duration(getBatchDurationStr) * time.Millisecond
	s.getBatcher = batcher.New[batchGetItem](getBatchSize, getBatchDuration, s.sendGetBatch, true)

	// Make sure the udf lua scripts are installed in the cluster
	// update the version of the lua script when a new version is launched, do not re-use the old one
	if err = registerLuaIfNecessary(client, luaPackage, ubsvLUA); err != nil {
		return nil, errors.NewStorageError("Failed to register udfLUA", err)
	}

	spendBatchSize, _ := gocore.Config().GetInt("utxostore_spendBatcherSize", 256)
	spendBatchDurationStr, _ := gocore.Config().GetInt("utxostore_spendBatcherDurationMillis", 10)
	spendBatchDuration := time.Duration(spendBatchDurationStr) * time.Millisecond
	s.spendBatcher = batcher.New[batchSpend](spendBatchSize, spendBatchDuration, s.sendSpendBatchLua, true)

	logger.Infof("[Aerospike] map txmeta store initialised with namespace: %s, set: %s", namespace, setName)

	return s, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.logger.Debugf("setting block height to %d", blockHeight)
	s.blockHeight.Store(blockHeight)
	return nil
}

func (s *Store) GetBlockHeight() uint32 {
	return s.blockHeight.Load()
}

func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	s.logger.Debugf("setting median block time to %d", medianTime)
	s.medianBlockTime.Store(medianTime)
	return nil
}

func (s *Store) GetMedianBlockTime() uint32 {
	return s.medianBlockTime.Load()
}

func (s *Store) Health(ctx context.Context) (int, string, error) {
	/* As written by one of the Aerospike developers, Go contexts are not supported:

	The Aerospike Go Client is a high performance library that supports hundreds of thousands
	of transactions per second per instance. Context support would require us to spawn a new
	goroutine for every request, adding significant overhead to the scheduler and GC.

	I am convinced that most users would benchmark their code with the context support and
	decide against using it after noticing the incurred penalties.

	Therefore, we will extract the Deadline from the context and use it as a timeout for the
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

	details := fmt.Sprintf("url: %s, namespace: %s", s.url.String(), s.namespace)

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

func (s *Store) calculateOffsetForOutput(vout uint32) uint32 {
	if s.utxoBatchSize == 0 {
		s.utxoBatchSize = defaultUxtoBatchSize
	}

	return vout % uint32(s.utxoBatchSize)
}
