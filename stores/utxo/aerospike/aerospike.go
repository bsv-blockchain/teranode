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
//   - Automatic cleanup of spent UTXOs through DAH
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
//   - totalUtxos: Total number of UTXOs
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	asl "github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/aerospike/cleanup"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	batcher "github.com/bitcoin-sv/teranode/util/batcher"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Ensure Store implements the utxo.Store interface
var _ utxo.Store = (*Store)(nil)

const MaxTxSizeInStoreInBytes = 32 * 1024

var (
	binNames = []fields.FieldName{
		fields.Unspendable,
		fields.Fee,
		fields.SizeInBytes,
		fields.LockTime,
		fields.Utxos,
		fields.TxInpoints,
		fields.BlockIDs,
		fields.UtxoSpendableIn,
		fields.Conflicting,
	}
)

type batcherIfc[T any] interface {
	Put(item *T, payloadSize ...int)
	Trigger()
}

// Store implements the UTXO store interface using Aerospike.
// It is thread-safe for concurrent access.
type Store struct {
	ctx                context.Context // store the global context for things that run in the background
	url                *url.URL
	client             *uaerospike.Client
	namespace          string
	setName            string
	blockHeight        atomic.Uint32
	medianBlockTime    atomic.Uint32
	logger             ulogger.Logger
	settings           *settings.Settings
	batchID            atomic.Uint64
	storeBatcher       batcherIfc[BatchStoreItem]
	getBatcher         batcherIfc[batchGetItem]
	spendBatcher       batcherIfc[batchSpend]
	outpointBatcher    batcherIfc[batchOutpoint]
	incrementBatcher   batcherIfc[batchIncrement]
	setDAHBatcher      batcherIfc[batchDAH]
	unspendableBatcher batcherIfc[batchUnspendable]
	externalStore      blob.Store
	utxoBatchSize      int
	externalTxCache    *util.ExpiringConcurrentCache[chainhash.Hash, *bt.Tx]
	indexMutex         sync.Mutex // Mutex for index creation operations
	indexOnce          sync.Once  // Ensures index creation/wait is only done once per process
}

// New creates a new Aerospike-based UTXO store.
// The URL format is: aerospike://host:port/namespace?set=setname&
// URL parameters:
//   - set: Aerospike set name (default: txmeta)
//   - or blob storage of large transactions
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
		ctx:       ctx,
		url:       aerospikeURL,
		client:    client,
		namespace: namespace,
		setName:   setName,
		logger:    logger,

		settings:        tSettings,
		externalStore:   externalStore,
		utxoBatchSize:   utxoBatchSize,
		externalTxCache: externalTxCache,
	}

	// Ensure index creation/wait is only done once per process
	if cleanup.IndexName != "" {
		s.indexOnce.Do(func() {
			if s.client != nil && s.client.Client != nil {
				exists, err := s.indexExists(cleanup.IndexName)
				if err != nil {
					s.logger.Errorf("Failed to check index existence: %v", err)
					return
				}

				if !exists {
					// Only one process should try to create the index
					err := s.CreateIndexIfNotExists(ctx, cleanup.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC)
					if err != nil {
						s.logger.Errorf("Failed to create index: %v", err)
					}
				}

				notMinedIndexName := "notMinedIndex"

				exists, err = s.indexExists(notMinedIndexName)
				if err != nil {
					s.logger.Errorf("Failed to check notMinedIndex existence: %v", err)
					return
				}

				if !exists {
					// Only one process should try to create the index
					err := s.CreateIndexIfNotExists(ctx, notMinedIndexName, fields.NotMined.String(), aerospike.NUMERIC)
					if err != nil {
						s.logger.Errorf("Failed to create notMinedIndex: %v", err)
					}
				}
			}
		})
	}

	storeBatchSize := tSettings.UtxoStore.StoreBatcherSize
	storeBatchDuration := tSettings.Aerospike.StoreBatcherDuration

	if storeBatchSize > 1 {
		s.storeBatcher = batcher.New(storeBatchSize, storeBatchDuration, s.sendStoreBatch, true)
	} else {
		s.logger.Warnf("Store batch size is set to %d, store batching is disabled", storeBatchSize)
	}

	getBatchSize := s.settings.UtxoStore.GetBatcherSize
	getBatchDurationStr := s.settings.UtxoStore.GetBatcherDurationMillis
	getBatchDuration := time.Duration(getBatchDurationStr) * time.Millisecond
	s.getBatcher = batcher.New(getBatchSize, getBatchDuration, s.sendGetBatch, true)

	// Make sure the udf lua scripts are installed in the cluster
	// update the version of the lua script when a new version is launched, do not re-use the old one
	if err = registerLuaIfNecessary(logger, client, LuaPackage, teranodeLUA); err != nil {
		return nil, errors.NewStorageError("Failed to register udfLUA", err)
	}

	spendBatchSize := s.settings.UtxoStore.SpendBatcherSize
	spendBatchDurationStr := s.settings.UtxoStore.SpendBatcherDurationMillis
	spendBatchDuration := time.Duration(spendBatchDurationStr) * time.Millisecond
	s.spendBatcher = batcher.New(spendBatchSize, spendBatchDuration, s.sendSpendBatchLua, true)

	outpointBatchSize := s.settings.UtxoStore.OutpointBatcherSize
	outpointBatchDurationStr := s.settings.UtxoStore.OutpointBatcherDurationMillis
	outpointBatchDuration := time.Duration(outpointBatchDurationStr) * time.Millisecond
	s.outpointBatcher = batcher.New(outpointBatchSize, outpointBatchDuration, s.sendOutpointBatch, true)

	incrementBatchSize := tSettings.UtxoStore.IncrementBatcherSize
	incrementBatchDurationStr := tSettings.UtxoStore.IncrementBatcherDurationMillis
	incrementBatchDuration := time.Duration(incrementBatchDurationStr) * time.Millisecond
	s.incrementBatcher = batcher.New(incrementBatchSize, incrementBatchDuration, s.sendIncrementBatch, true)

	setDAHBatchSize := tSettings.UtxoStore.SetDAHBatcherSize
	setDAHBatchDurationStr := tSettings.UtxoStore.SetDAHBatcherDurationMillis
	setDAHBatchDuration := time.Duration(setDAHBatchDurationStr) * time.Millisecond
	s.setDAHBatcher = batcher.New(setDAHBatchSize, setDAHBatchDuration, s.sendSetDAHBatch, true)

	unspendableBatcherSize := tSettings.UtxoStore.UnspendableBatcherSize
	unspendableBatchDurationStr := tSettings.UtxoStore.UnspendableBatcherDurationMillis
	unspendableBatchDuration := time.Duration(unspendableBatchDurationStr) * time.Millisecond
	s.unspendableBatcher = batcher.New(unspendableBatcherSize, unspendableBatchDuration, s.setUnspendableBatch, true)

	logger.Infof("[Aerospike] map txmeta store initialised with namespace: %s, set: %s", namespace, setName)

	return s, nil
}

func (s *Store) SetLogger(logger ulogger.Logger) {
	s.logger = logger
}

func (s *Store) GetClient() *uaerospike.Client {
	return s.client
}

func (s *Store) GetNamespace() string {
	return s.namespace
}

func (s *Store) GetSet() string {
	return s.setName
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

	details := "Aerospike store" // don't include sensitive info like url, password, etc

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
	if s.utxoBatchSize <= 0 {
		s.logger.Errorf("utxoBatchSize is zero or negative, cannot calculate offset (vout=%d)", vout)
		return 0
	}

	// Check if utxoBatchSize exceeds the maximum value of uint32
	if s.utxoBatchSize > math.MaxUint32 {
		s.logger.Errorf("utxoBatchSize (%d) exceeds uint32 max value", s.utxoBatchSize)
		return 0
	}

	return vout % uint32(s.utxoBatchSize)
}

// CreateIndexIfNotExists creates an index only if it doesn't already exist
// This method allows one caller to create the index in the background while
// other callers can immediately continue without waiting
func (s *Store) CreateIndexIfNotExists(ctx context.Context, indexName, binName string, indexType aerospike.IndexType) error {
	if s.client.Client == nil {
		return nil // For unit tests, we don't have a client
	}

	// First, check if the index already exists without a lock
	exists, err := s.indexExists(indexName)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	// Check if the index exists again but this time with a lock
	s.indexMutex.Lock()

	exists, err = s.indexExists(indexName)
	if err != nil {
		s.indexMutex.Unlock()
		return err
	}

	if exists {
		s.indexMutex.Unlock()
		return nil
	}

	// Create the index (synchronously)
	policy := aerospike.NewWritePolicy(0, 0)

	s.logger.Infof("Creating index %s:%s:%s", s.namespace, s.setName, indexName)

	if _, err := s.client.CreateIndex(policy, s.namespace, s.setName, indexName, binName, indexType); err != nil {
		s.logger.Errorf("Failed to create index %s:%s:%s: %v", s.namespace, s.setName, indexName, err)
		s.indexMutex.Unlock()

		return err
	}

	// Unlock the mutex and allow the index to continue being created in the background
	s.indexMutex.Unlock()

	return nil
}

// waitForIndexReady polls Aerospike until the index is ready or times out
func (s *Store) waitForIndexReady(ctx context.Context, indexName string) error {
	if s.client.Client == nil {
		return nil // For unit tests, we don't have a client
	}

	s.logger.Infof("Waiting for index %s:%s:%s to be built", s.namespace, s.setName, indexName)

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			// Query index status
			node, err := s.client.Client.Cluster().GetRandomNode()
			if err != nil {
				return err
			}

			policy := aerospike.NewInfoPolicy()

			infoMap, err := node.RequestInfo(policy, "sindex")
			if err != nil {
				return err
			}

			for _, v := range infoMap {
				if strings.Contains(v, fmt.Sprintf("ns=%s:indexname=%s:set=%s", s.namespace, indexName, s.setName)) && strings.Contains(v, "RW") {
					s.logger.Infof("Index %s:%s:%s built in %s", s.namespace, s.setName, indexName, time.Since(start))

					return nil // Index is ready
				}
			}

			time.Sleep(1 * time.Second)
		}
	}
}

// indexExists checks if an index with the given name exists in the namespace
func (s *Store) indexExists(indexName string) (bool, error) {
	// Get a random node from the cluster
	node, err := s.client.Client.Cluster().GetRandomNode()
	if err != nil {
		return false, err
	}

	// Create an info policy
	policy := aerospike.NewInfoPolicy()

	// Request index information from the node
	infoMap, err := node.RequestInfo(policy, "sindex")
	if err != nil {
		return false, err
	}

	// Parse the response to check for the index
	for _, v := range infoMap {
		if strings.Contains(v, fmt.Sprintf("ns=%s:indexname=%s:set=%s", s.namespace, indexName, s.setName)) {
			return true, nil
		}
	}

	return false, nil
}
