// //go:build aerospike

// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through TTL
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - nrUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	batcher "github.com/bitcoin-sv/teranode/util/batcher_temp"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

const MaxTxSizeInStoreInBytes = 32 * 1024

var (
	binNames = []utxo.FieldName{
		utxo.FieldUnspendable,
		utxo.FieldFee,
		utxo.FieldSizeInBytes,
		utxo.FieldLockTime,
		utxo.FieldUtxos,
		utxo.FieldParentTxHashes,
		utxo.FieldBlockIDs,
		utxo.FieldUtxoSpendableIn,
		utxo.FieldFrozen,
		utxo.FieldConflicting,
	}
)

type batcherIfc[T any] interface {
	Put(item *T, payloadSize ...int)
	Trigger()
}

// Store implements the UTXO store interface using Aerospike.
// It is thread-safe for concurrent access.
type Store struct {
	ctx              context.Context // store the global context for things that run in the background
	url              *url.URL
	client           *uaerospike.Client
	namespace        string
	setName          string
	expiration       time.Duration
	blockHeight      atomic.Uint32
	medianBlockTime  atomic.Uint32
	logger           ulogger.Logger
	settings         *settings.Settings
	batchID          atomic.Uint64
	storeBatcher     batcherIfc[BatchStoreItem]
	getBatcher       batcherIfc[batchGetItem]
	spendBatcher     batcherIfc[batchSpend]
	outpointBatcher  batcherIfc[batchOutpoint]
	incrementBatcher batcherIfc[batchIncrement]
	externalStore    blob.Store
	utxoBatchSize    int
	externalTxCache  *util.ExpiringConcurrentCache[chainhash.Hash, *bt.Tx]
}

// New creates a new Aerospike-based UTXO store.
// The URL format is: aerospike://host:port/namespace?set=setname&expiration=seconds
//
// URL parameters:
//   - set: Aerospike set name (default: txmeta)
//   - expiration: TTL for spent UTXOs in seconds
//   - externalStore: URL for blob storage of large transactions
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, aerospikeURL *url.URL) (*Store, error) {
	InitPrometheusMetrics()

	if tSettings.Aerospike.Debug {
		asl.Logger.SetLevel(asl.DEBUG)
	}

	namespace := aerospikeURL.Path[1:]

	client, err := util.GetAerospikeClient(logger, aerospikeURL, tSettings)
	if err != nil {
		return nil, err
	}

	placeholderKey, err = aerospike.NewKey(namespace, "placeholderKey", "placeHolderKey")
	if err != nil {
		log.Fatal("Failed to init placeholder key")
	}

	expiration := time.Duration(0)

	expirationValue := aerospikeURL.Query().Get("expiration")
	if expirationValue != "" {
		expiration, err = time.ParseDuration(expirationValue)
		if err != nil {
			return nil, errors.NewInvalidArgumentError("could not parse expiration %s", expirationValue, err)
		}

		if expiration > 0 && expiration < time.Second {
			return nil, errors.NewInvalidArgumentError("expiration must be at least 1 second")
		}

		if expiration == 0 {
			logger.Infof("expiration is set to 0 meaning the default Aerospike TTL setting will be used")
		} else {
			logger.Infof("expiration is set to %s (%.0f seconds)", expirationValue, expiration.Seconds())
		}
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
	utxoBatchSize := tSettings.UtxoStore.UtxoBatchSize
	if utxoBatchSize < 1 || utxoBatchSize > math.MaxUint32 {
		return nil, errors.NewInvalidArgumentError("utxoBatchSize must be between 1 and %d", math.MaxUint32)
	}

	// the external tx cache is used to cache externally stored transactions for a short time after being read from
	// the store. Transactions with lots of outputs, being spent at the same time, benefit greatly from this cache,
	// since external cache takes care of concurrent reads to the same transaction.
	var externalTxCache *util.ExpiringConcurrentCache[chainhash.Hash, *bt.Tx]
	if tSettings.UtxoStore.UseExternalTxCache {
		externalTxCache = util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](10 * time.Second)
	}

	s := &Store{
		ctx:             ctx,
		url:             aerospikeURL,
		client:          client,
		namespace:       namespace,
		setName:         setName,
		expiration:      expiration,
		logger:          logger,
		settings:        tSettings,
		externalStore:   externalStore,
		utxoBatchSize:   utxoBatchSize,
		externalTxCache: externalTxCache,
	}

	storeBatchSize := tSettings.UtxoStore.StoreBatcherSize
	storeBatchDuration := tSettings.Aerospike.StoreBatcherDuration

	if storeBatchSize > 1 {
		s.storeBatcher = batcher.New[BatchStoreItem](storeBatchSize, storeBatchDuration, s.sendStoreBatch, true)
	} else {
		s.logger.Warnf("Store batch size is set to %d, store batching is disabled", storeBatchSize)
	}

	getBatchSize := s.settings.UtxoStore.GetBatcherSize
	getBatchDurationStr := s.settings.UtxoStore.GetBatcherDurationMillis
	getBatchDuration := time.Duration(getBatchDurationStr) * time.Millisecond
	s.getBatcher = batcher.New[batchGetItem](getBatchSize, getBatchDuration, s.sendGetBatch, true)

	// Make sure the udf lua scripts are installed in the cluster
	// update the version of the lua script when a new version is launched, do not re-use the old one
	if err = registerLuaIfNecessary(logger, client, LuaPackage, teranodeLUA); err != nil {
		return nil, errors.NewStorageError("Failed to register udfLUA", err)
	}

	spendBatchSize := s.settings.UtxoStore.SpendBatcherSize
	spendBatchDurationStr := s.settings.UtxoStore.SpendBatcherDurationMillis
	spendBatchDuration := time.Duration(spendBatchDurationStr) * time.Millisecond
	s.spendBatcher = batcher.New[batchSpend](spendBatchSize, spendBatchDuration, s.sendSpendBatchLua, true)

	outpointBatchSize := s.settings.UtxoStore.OutpointBatcherSize
	outpointBatchDurationStr := s.settings.UtxoStore.OutpointBatcherDurationMillis
	outpointBatchDuration := time.Duration(outpointBatchDurationStr) * time.Millisecond
	s.outpointBatcher = batcher.New[batchOutpoint](outpointBatchSize, outpointBatchDuration, s.sendOutpointBatch, true)

	incrementBatchSize := tSettings.UtxoStore.IncrementBatcherSize
	incrementBatchDurationStr := tSettings.UtxoStore.IncrementBatcherDurationMillis
	incrementBatchDuration := time.Duration(incrementBatchDurationStr) * time.Millisecond
	s.incrementBatcher = batcher.New[batchIncrement](incrementBatchSize, incrementBatchDuration, s.sendIncrementBatch, true)

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
