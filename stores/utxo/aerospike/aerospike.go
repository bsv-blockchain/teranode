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
	"github.com/bitcoin-sv/teranode/pkg/go-batcher"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/aerospike/cleanup"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

				unminedSinceIndexName := "unminedSinceIndex"

				exists, err = s.indexExists(unminedSinceIndexName)
				if err != nil {
					s.logger.Errorf("Failed to check unminedSinceIndex existence: %v", err)
					return
				}

				if !exists {
					// Only one process should try to create the index
					err := s.CreateIndexIfNotExists(ctx, unminedSinceIndexName, fields.UnminedSince.String(), aerospike.NUMERIC)
					if err != nil {
						s.logger.Errorf("Failed to create unminedSinceIndex: %v", err)
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

// SetLogger updates the logger instance used by the store.
// This method is safe to call concurrently.
func (s *Store) SetLogger(logger ulogger.Logger) {
	s.logger = logger
}

// GetClient returns the underlying Aerospike client instance.
// This method is safe to call concurrently and is primarily used for testing
// and advanced operations that require direct access to the Aerospike client.
func (s *Store) GetClient() *uaerospike.Client {
	return s.client
}

// GetNamespace returns the Aerospike namespace used by this store.
// This method is safe to call concurrently.
func (s *Store) GetNamespace() string {
	return s.namespace
}

// GetSet returns the Aerospike set name used by this store.
// This method is safe to call concurrently.
func (s *Store) GetSet() string {
	return s.setName
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.logger.Debugf("setting block height to %d", blockHeight)
	s.blockHeight.Store(blockHeight)
	s.externalStore.SetCurrentBlockHeight(blockHeight)

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

// calculateOffsetForOutput calculates the offset within a batch for a given output index.
// This is used to determine which batch record contains a specific UTXO when transactions
// have more outputs than can fit in a single Aerospike record.
//
// Parameters:
//   - vout: The output index to calculate the offset for
//
// Returns:
//   - uint32: The offset within the batch, or 0 if utxoBatchSize is invalid
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

// QueryOldUnminedTransactions returns transaction hashes for unmined transactions older than the cutoff height.
// This method is used by the store-agnostic cleanup implementation.
func (s *Store) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	s.logger.Debugf("[QueryOldUnminedTransactions] Querying unmined transactions older than block height %d", cutoffBlockHeight)

	// Create a query to find all unmined transactions using the unminedSinceIndex
	stmt := aerospike.NewStatement(s.namespace, s.setName)

	// Query for records where UnminedSince <= cutoffBlockHeight
	// This leverages the secondary index on the UnminedSince field for efficient querying
	err := stmt.SetFilter(aerospike.NewRangeFilter(fields.UnminedSince.String(), 1, int64(cutoffBlockHeight)))
	if err != nil {
		return nil, errors.NewProcessingError("failed to set filter for unmined transaction query", err)
	}

	// Use query to get old unmined transactions
	queryPolicy := aerospike.NewQueryPolicy()
	queryPolicy.MaxRetries = 3
	queryPolicy.SocketTimeout = 30 * time.Second
	queryPolicy.TotalTimeout = 120 * time.Second

	recordset, err := s.client.Query(queryPolicy, stmt)
	if err != nil {
		return nil, errors.NewProcessingError("failed to query old unmined transactions", err)
	}
	defer recordset.Close()

	txHashes := make([]chainhash.Hash, 0)

	// Process each unmined transaction
	for res := range recordset.Results() {
		if res.Err != nil {
			s.logger.Errorf("[QueryOldUnminedTransactions] Error reading record: %v", res.Err)
			continue
		}

		record := res.Record
		if record == nil {
			continue
		}

		// The query already filtered by UnminedSince <= cutoffBlockHeight
		// so all records here are candidates for cleanup

		// Get the transaction hash from the record
		txIDBytes, exists := record.Bins[fields.TxID.String()]
		if !exists {
			s.logger.Warnf("[QueryOldUnminedTransactions] Record missing TxID field")
			continue
		}

		txIDBytesSlice, ok := txIDBytes.([]byte)
		if !ok || len(txIDBytesSlice) != 32 {
			s.logger.Warnf("[QueryOldUnminedTransactions] Invalid TxID format")
			continue
		}

		txHash := chainhash.Hash{}
		copy(txHash[:], txIDBytesSlice)
		txHashes = append(txHashes, txHash)
	}

	s.logger.Debugf("[QueryOldUnminedTransactions] Found %d old unmined transactions", len(txHashes))

	return txHashes, nil
}

// PreserveTransactions marks transactions to be preserved from deletion until a specific block height.
// This clears any existing DeleteAtHeight and sets PreserveUntil to the specified height.
// Used to protect parent transactions when cleaning up unmined transactions.
func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	// Use batch operations for efficiency
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE

	batchRecords := make([]aerospike.BatchRecordIfc, len(txIDs))

	for i, txID := range txIDs {
		key, err := aerospike.NewKey(s.namespace, s.setName, txID[:])
		if err != nil {
			s.logger.Errorf("[PreserveTransactions] Failed to create key for tx %s: %v", txID.String(), err)
			continue
		}

		batchRecords[i] = aerospike.NewBatchWrite(batchWritePolicy, key,
			aerospike.PutOp(aerospike.NewBin(fields.PreserveUntil.String(), int(preserveUntilHeight))),
			aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), nil)))
	}

	// Execute batch operation
	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.NewStorageError("failed to preserve transactions", err)
	}

	// Check results
	preservedCount := 0

	for i, record := range batchRecords {
		if record.BatchRec().Err == nil {
			preservedCount++
		} else {
			s.logger.Warnf("[PreserveTransactions] Failed to preserve tx %s: %v",
				txIDs[i].String(), record.BatchRec().Err)
		}
	}

	s.logger.Debugf("[PreserveTransactions] Successfully preserved %d out of %d transactions", preservedCount, len(txIDs))

	return nil
}

// ProcessExpiredPreservations handles transactions whose preservation period has expired.
// For each transaction with PreserveUntil <= currentHeight, it sets an appropriate DeleteAtHeight
// and clears the PreserveUntil field.
func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	// Create a query to find records with expired PreserveUntil
	stmt := aerospike.NewStatement(s.namespace, s.setName)

	// Query for records where PreserveUntil <= currentHeight
	err := stmt.SetFilter(aerospike.NewRangeFilter(fields.PreserveUntil.String(), 1, int64(currentHeight)))
	if err != nil {
		return errors.NewStorageError("failed to set filter for expired preservations", err)
	}

	queryPolicy := aerospike.NewQueryPolicy()
	queryPolicy.MaxRetries = 3
	queryPolicy.SocketTimeout = 30 * time.Second
	queryPolicy.TotalTimeout = 120 * time.Second

	recordset, err := s.client.Query(queryPolicy, stmt)
	if err != nil {
		return errors.NewStorageError("failed to query expired preservations", err)
	}
	defer recordset.Close()

	// Process records in batches
	batchSize := 100
	batch := make([]aerospike.BatchRecordIfc, 0, batchSize)
	txIDs := make([]chainhash.Hash, 0, batchSize)

	processedCount := 0

	for res := range recordset.Results() {
		if res.Err != nil {
			s.logger.Errorf("[ProcessExpiredPreservations] Error reading record: %v", res.Err)
			continue
		}

		record := res.Record
		if record == nil {
			continue
		}

		// Get the transaction ID
		txIDBytes, exists := record.Bins[fields.TxID.String()]
		if !exists {
			continue
		}

		txIDBytesSlice, ok := txIDBytes.([]byte)
		if !ok || len(txIDBytesSlice) != 32 {
			continue
		}

		txHash := chainhash.Hash{}
		copy(txHash[:], txIDBytesSlice)

		key, err := aerospike.NewKey(s.namespace, s.setName, txHash[:])
		if err != nil {
			s.logger.Errorf("[ProcessExpiredPreservations] Failed to create key for tx %s: %v", txHash.String(), err)
			continue
		}

		// Calculate DeleteAtHeight based on retention policy
		deleteAtHeight := currentHeight + s.settings.UtxoStore.BlockHeightRetention

		batchWritePolicy := util.GetAerospikeBatchWritePolicy(s.settings)
		batchWritePolicy.RecordExistsAction = aerospike.UPDATE

		batch = append(batch, aerospike.NewBatchWrite(batchWritePolicy, key,
			aerospike.PutOp(aerospike.NewBin(fields.DeleteAtHeight.String(), int(deleteAtHeight))),
			aerospike.PutOp(aerospike.NewBin(fields.PreserveUntil.String(), nil))))

		txIDs = append(txIDs, txHash)

		// Process batch when full
		if len(batch) >= batchSize {
			if err := s.processBatchExpiredPreservations(ctx, batch, txIDs); err != nil {
				s.logger.Errorf("[ProcessExpiredPreservations] Failed to process batch: %v", err)
			} else {
				processedCount += len(batch)
			}

			batch = batch[:0]
			txIDs = txIDs[:0]
		}
	}

	// Process remaining records
	if len(batch) > 0 {
		if err := s.processBatchExpiredPreservations(ctx, batch, txIDs); err != nil {
			s.logger.Errorf("[ProcessExpiredPreservations] Failed to process final batch: %v", err)
		} else {
			processedCount += len(batch)
		}
	}

	s.logger.Infof("[ProcessExpiredPreservations] Processed %d expired preservations at height %d", processedCount, currentHeight)

	return nil
}

// processBatchExpiredPreservations is a helper function to process a batch of expired preservations
func (s *Store) processBatchExpiredPreservations(ctx context.Context, batch []aerospike.BatchRecordIfc, txIDs []chainhash.Hash) error {
	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	err := s.client.BatchOperate(batchPolicy, batch)
	if err != nil {
		return err
	}

	// Log any failures
	for i, record := range batch {
		if record.BatchRec().Err != nil {
			s.logger.Warnf("[ProcessExpiredPreservations] Failed to update tx %s: %v",
				txIDs[i].String(), record.BatchRec().Err)
		}
	}

	return nil
}
