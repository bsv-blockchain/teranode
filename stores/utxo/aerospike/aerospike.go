// //go:build aerospike

package aerospike

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

const MaxTxSizeInStoreInBytes = 32 * 1024

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

type batcherIfc[T any] interface {
	Put(item *T, payloadSize ...int)
	Trigger()
}

type Store struct {
	ctx                        context.Context // store the global context for things that run in the background
	url                        *url.URL
	client                     *uaerospike.Client
	namespace                  string
	setName                    string
	expiration                 uint32
	blockHeight                atomic.Uint32
	medianBlockTime            atomic.Uint32
	logger                     ulogger.Logger
	batchID                    atomic.Uint64
	storeBatcher               batcherIfc[batchStoreItem]
	getBatcher                 batcherIfc[batchGetItem]
	spendBatcher               batcherIfc[batchSpend]
	outpointBatcher            batcherIfc[batchOutpoint]
	externalStore              blob.Store
	utxoBatchSize              int
	externalizeAllTransactions bool
	externalTxCache            *util.ExpiringConcurrentCache[chainhash.Hash, *bt.Tx]
}

func New(ctx context.Context, logger ulogger.Logger, aerospikeURL *url.URL) (*Store, error) {
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

		if expiration == 0 {
			logger.Infof("expiration is set to 0 meaning the default aerospike namespace TTL setting will be used")
		}

		// nolint: gosec
		expiration = uint32(expiration64)
	}

	setName := aerospikeURL.Query().Get("set")
	if setName == "" {
		setName = "txmeta"
	}

	externalStoreURL, err := url.Parse(aerospikeURL.Query().Get("externalStore"))
	if err != nil {
		return nil, err
	}

	externalStore, err := blob.NewStore(logger, externalStoreURL)
	if err != nil {
		return nil, err
	}

	// It's very dangerous to change this number after a node has been running for a while
	// Do not change this value after starting, it is used to calculate the offset for the output
	utxoBatchSize, _ := gocore.Config().GetInt("utxostore_utxoBatchSize", 128)
	if utxoBatchSize < 1 || utxoBatchSize > math.MaxUint32 {
		return nil, errors.NewInvalidArgumentError("utxoBatchSize must be between 1 and %d", math.MaxUint32)
	}

	// the external tx cache is used to cache externally stored transactions for a short time after being read from
	// the store. Transactions with lots of outputs, being spent at the same time, benefit greatly from this cache,
	// since external cache takes care of concurrent reads to the same transaction.
	var externalTxCache *util.ExpiringConcurrentCache[chainhash.Hash, *bt.Tx]
	if gocore.Config().GetBool("utxostore_useExternalTxCache", true) {
		externalTxCache = util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](10 * time.Second)
	}

	s := &Store{
		ctx:                        ctx,
		url:                        aerospikeURL,
		client:                     client,
		namespace:                  namespace,
		setName:                    setName,
		expiration:                 expiration,
		logger:                     logger,
		externalStore:              externalStore,
		utxoBatchSize:              utxoBatchSize,
		externalizeAllTransactions: gocore.Config().GetBool("utxostore_externalizeAllTransactions", false),
		externalTxCache:            externalTxCache,
	}

	storeBatchSize, _ := gocore.Config().GetInt("utxostore_storeBatcherSize", 256)
	storeBatchDurationStr, _ := gocore.Config().GetInt("utxostore_storeBatcherDurationMillis", 10)
	storeBatchDuration := time.Duration(storeBatchDurationStr) * time.Millisecond

	if storeBatchSize > 1 {
		s.storeBatcher = batcher.New[batchStoreItem](storeBatchSize, storeBatchDuration, s.sendStoreBatch, true)
	} else {
		s.logger.Warnf("Store batch size is set to %d, store batching is disabled", storeBatchSize)
	}

	getBatchSize, _ := gocore.Config().GetInt("utxostore_getBatcherSize", 1024)
	getBatchDurationStr, _ := gocore.Config().GetInt("utxostore_getBatcherDurationMillis", 10)
	getBatchDuration := time.Duration(getBatchDurationStr) * time.Millisecond
	s.getBatcher = batcher.New[batchGetItem](getBatchSize, getBatchDuration, s.sendGetBatch, true)

	// Make sure the udf lua scripts are installed in the cluster
	// update the version of the lua script when a new version is launched, do not re-use the old one
	if err = registerLuaIfNecessary(logger, client, luaPackage, ubsvLUA); err != nil {
		return nil, errors.NewStorageError("Failed to register udfLUA", err)
	}

	spendBatchSize, _ := gocore.Config().GetInt("utxostore_spendBatcherSize", 256)
	spendBatchDurationStr, _ := gocore.Config().GetInt("utxostore_spendBatcherDurationMillis", 10)
	spendBatchDuration := time.Duration(spendBatchDurationStr) * time.Millisecond
	s.spendBatcher = batcher.New[batchSpend](spendBatchSize, spendBatchDuration, s.sendSpendBatchLua, true)

	outpointBatchSize, _ := gocore.Config().GetInt("utxostore_outpointBatcherSize", 256)
	outpointBatchDurationStr, _ := gocore.Config().GetInt("utxostore_outpointBatcherDurationMillis", 10)
	outpointBatchDuration := time.Duration(outpointBatchDurationStr) * time.Millisecond
	s.outpointBatcher = batcher.New[batchOutpoint](outpointBatchSize, outpointBatchDuration, s.sendOutpointBatch, true)

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

func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
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
	key, err := aerospike.NewKey(s.namespace, s.setName, "key")
	if err != nil {
		return http.StatusServiceUnavailable, details, err
	}

	bin := aerospike.NewBin("bin", "value")

	err = s.client.PutBins(writePolicy, key, bin)
	if err != nil {
		return http.StatusServiceUnavailable, details, err
	}

	policy := aerospike.NewPolicy()
	if timeout > 0 {
		policy.TotalTimeout = timeout
	}

	_, err = s.client.Get(policy, key)
	if err != nil {
		return http.StatusServiceUnavailable, details, err
	}

	return http.StatusOK, details, nil
}

func (s *Store) calculateOffsetForOutput(vout uint32) uint32 {
	// nolint: gosec
	return vout % uint32(s.utxoBatchSize)
}
