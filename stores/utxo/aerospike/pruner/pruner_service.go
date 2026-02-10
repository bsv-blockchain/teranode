package pruner

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// Ensure Store implements the Pruner Service interface
var _ pruner.Service = (*Service)(nil)

var IndexName, _ = gocore.Config().Get("pruner_IndexName", "pruner_dah_index")

var (
	prometheusMetricsInitOnce     sync.Once
	prometheusUtxoCleanupBatch    prometheus.Histogram
	prometheusUtxoRecordErrors    prometheus.Counter
	prometheusUtxoBatchQueryError prometheus.Counter
)

// Options contains configuration options for the cleanup service
type Options struct {
	// Logger is the logger to use
	Logger ulogger.Logger

	// Ctx is the context to use to signal shutdown
	Ctx context.Context

	// IndexWaiter is used to wait for Aerospike indexes to be built
	IndexWaiter IndexWaiter

	// Client is the Aerospike client to use
	Client *uaerospike.Client

	// ExternalStore is the external blob store to use for external transactions
	ExternalStore blob.Store

	// Namespace is the Aerospike namespace to use
	Namespace string

	// Set is the Aerospike set to use
	Set string

	// GetPersistedHeight returns the last block height processed by block persister
	// Used to coordinate cleanup with block persister progress (can be nil)
	GetPersistedHeight func() uint32

	// Observers is a list of observers to notify when pruning completes
	Observers []pruner.Observer
}

// Service manages background jobs for cleaning up records based on block height
// Service implements the pruner.Service interface for Aerospike-backed UTXO stores.
// This service extracts configuration values as fields during initialization rather than
// storing the settings object, optimizing for performance in hot paths where settings
// are accessed millions of times (e.g., utxoBatchSize in per-record processing loops).
type Service struct {
	logger      ulogger.Logger
	client      *uaerospike.Client
	external    blob.Store
	namespace   string
	set         string
	ctx         context.Context
	indexWaiter IndexWaiter
	indexReady  atomic.Bool
	notifier    *PrunerEventNotifier

	// Configuration values extracted from settings for performance
	utxoBatchSize                  int
	blockHeightRetention           uint32
	defensiveEnabled               bool
	defensiveBatchReadSize         int
	chunkSize                      int
	chunkGroupLimit                int
	progressLogInterval            time.Duration
	partitionQueries               int     // Number of parallel partition queries (0 = auto-detect)
	connectionPoolWarningThreshold float64 // Threshold for connection pool auto-adjustment (0.0-1.0)
	utxoSetTTL                     bool    // Use TTL expiration instead of hard delete

	// Cached field names (avoid repeated String() allocations in hot paths)
	fieldTxID, fieldUtxos, fieldInputs, fieldDeletedChildren, fieldExternal        string
	fieldDeleteAtHeight, fieldTotalExtraRecs, fieldUnminedSince, fieldBlockHeights string

	// Internally reused variables
	queryPolicy      *aerospike.QueryPolicy
	writePolicy      *aerospike.WritePolicy
	batchWritePolicy *aerospike.BatchWritePolicy
	batchPolicy      *aerospike.BatchPolicy

	// getPersistedHeight returns the last block height processed by block persister
	// Used to coordinate cleanup with block persister progress (can be nil)
	getPersistedHeight func() uint32
}

// parentUpdateInfo holds accumulated parent update information for batching
type parentUpdateInfo struct {
	key         *aerospike.Key
	childHashes []*chainhash.Hash // Child transactions being deleted
}

// externalFileInfo holds information about external files to delete
type externalFileInfo struct {
	txHash   *chainhash.Hash
	fileType fileformat.FileType
}

// NewService creates a new cleanup service
func NewService(tSettings *settings.Settings, opts Options) (*Service, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
	}

	if opts.Client == nil {
		return nil, errors.NewProcessingError("client is required")
	}

	if opts.IndexWaiter == nil {
		return nil, errors.NewProcessingError("index waiter is required")
	}

	if opts.Namespace == "" {
		return nil, errors.NewProcessingError("namespace is required")
	}

	if opts.Set == "" {
		return nil, errors.NewProcessingError("set is required")
	}

	if opts.ExternalStore == nil {
		return nil, errors.NewProcessingError("external store is required")
	}

	// Initialize prometheus metrics if not already initialized
	prometheusMetricsInitOnce.Do(func() {
		prometheusUtxoCleanupBatch = promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "utxo_cleanup_batch_duration_seconds",
			Help:    "Time taken to process a batch of cleanup jobs",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		})
		prometheusUtxoRecordErrors = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_record_errors_total",
			Help: "Total number of Aerospike record-level errors during pruning",
		})
		prometheusUtxoBatchQueryError = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_batch_query_errors_total",
			Help: "Total number of Aerospike batch query errors during child verification",
		})
	})

	// Use the configured query policy from settings (configured via aerospike_queryPolicy URL)
	queryPolicy := util.GetAerospikeQueryPolicy(tSettings)
	queryPolicy.IncludeBinData = true // Need to include bin data for cleanup processing

	// Use the configured write policy from settings
	writePolicy := util.GetAerospikeWritePolicy(tSettings, 0)

	// Use the configured batch policies from settings
	batchWritePolicy := util.GetAerospikeBatchWritePolicy(tSettings)
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	// Use the configured batch policy from settings (configured via aerospike_batchPolicy URL)
	batchPolicy := util.GetAerospikeBatchPolicy(tSettings)

	notifier := NewPrunerEventNotifier()
	for _, observer := range opts.Observers {
		notifier.AddObserver(observer)
	}

	service := &Service{
		logger:                         opts.Logger,
		client:                         opts.Client,
		external:                       opts.ExternalStore,
		namespace:                      opts.Namespace,
		set:                            opts.Set,
		ctx:                            opts.Ctx,
		indexWaiter:                    opts.IndexWaiter,
		notifier:                       notifier,
		queryPolicy:                    queryPolicy,
		writePolicy:                    writePolicy,
		batchWritePolicy:               batchWritePolicy,
		batchPolicy:                    batchPolicy,
		getPersistedHeight:             opts.GetPersistedHeight,
		utxoBatchSize:                  tSettings.UtxoStore.UtxoBatchSize,
		blockHeightRetention:           tSettings.GetUtxoStoreBlockHeightRetention(),
		defensiveEnabled:               tSettings.Pruner.UTXODefensiveEnabled,
		defensiveBatchReadSize:         tSettings.Pruner.UTXODefensiveBatchReadSize,
		chunkSize:                      tSettings.Pruner.UTXOChunkSize,
		chunkGroupLimit:                tSettings.Pruner.UTXOChunkGroupLimit,
		progressLogInterval:            tSettings.Pruner.UTXOProgressLogInterval,
		partitionQueries:               tSettings.Pruner.UTXOPartitionQueries,
		connectionPoolWarningThreshold: tSettings.Pruner.ConnectionPoolWarningThreshold,
		utxoSetTTL:                     tSettings.Pruner.UTXOSetTTL,
		fieldTxID:                      fields.TxID.String(),
		fieldUtxos:                     fields.Utxos.String(),
		fieldInputs:                    fields.Inputs.String(),
		fieldDeletedChildren:           fields.DeletedChildren.String(),
		fieldExternal:                  fields.External.String(),
		fieldDeleteAtHeight:            fields.DeleteAtHeight.String(),
		fieldTotalExtraRecs:            fields.TotalExtraRecs.String(),
		fieldUnminedSince:              fields.UnminedSince.String(),
		fieldBlockHeights:              fields.BlockHeights.String(),
	}

	return service, nil
}

// Start starts the cleanup service and waits for the required index to be ready.
// This method returns immediately and starts a goroutine to wait for index readiness.
func (s *Service) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Validate connection pool settings and auto-adjust if necessary
	s.validateConnectionPoolSettings()

	go func() {
		if err := s.indexWaiter.WaitForIndexReady(ctx, IndexName); err != nil {
			s.logger.Errorf("Timeout or error waiting for index to be built: %v", err)
			return
		}
		s.indexReady.Store(true)
		s.logger.Infof("[AerospikeCleanupService] index ready")
	}()
}

// SetPersistedHeightGetter sets the function used to get block persister progress.
// This should be called after service creation to wire up coordination with block persister.
func (s *Service) SetPersistedHeightGetter(getter func() uint32) {
	s.getPersistedHeight = getter
}

// AddObserver adds an observer to be notified when pruning completes.
// This method is thread-safe and can be called after service creation.
func (s *Service) AddObserver(observer pruner.Observer) {
	s.notifier.AddObserver(observer)
}

// getConfigValue queries Aerospike for a specific configuration parameter
func (s *Service) getConfigValue(configParam string) (string, error) {
	// Get the first node in the cluster
	nodes := s.client.GetNodes()
	if len(nodes) == 0 {
		return "", errors.NewProcessingError("no Aerospike nodes available")
	}
	node := nodes[0]

	// Request the service context configuration
	info, err := node.RequestInfo(aerospike.NewInfoPolicy(), "get-config:context=service")
	if err != nil {
		return "", errors.NewProcessingError("failed to get Aerospike config info: %v", err)
	}

	// The response is a map; the value for our key is a semicolon-separated string
	configStr, ok := info["get-config:context=service"]
	if !ok {
		return "", errors.NewProcessingError("service config not found in response")
	}

	// Parse the config string to find the requested parameter
	configPairs := strings.Split(configStr, ";")
	for _, pair := range configPairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 && kv[0] == configParam {
			return kv[1], nil
		}
	}

	return "", errors.NewProcessingError("config parameter %s not found", configParam)
}

// calculatePartitionWorkers determines the optimal number of partition workers
// based on CPU cores and Aerospike's query-threads-limit configuration
func (s *Service) calculatePartitionWorkers() int {
	maxPartitions := aerospike.NewPartitionFilterAll().Count // Total partitions in Aerospike (4096)

	// If explicitly configured, use that value
	if s.partitionQueries > 0 {
		return min(s.partitionQueries, maxPartitions) // Cap at total partitions
	}

	// Auto-detect based on CPU cores and Aerospike query-threads-limit
	queryThreadsLimitStr, err := s.getConfigValue("query-threads-limit")
	if err != nil {
		s.logger.Warnf("Failed to get query-threads-limit from Aerospike: %v, defaulting to runtime.NumCPU()", err)
		return runtime.NumCPU()
	}

	queryThreadsLimit, err := strconv.ParseInt(queryThreadsLimitStr, 10, 64)
	if err != nil {
		s.logger.Warnf("Failed to parse query-threads-limit: %v, defaulting to runtime.NumCPU()", err)
		return runtime.NumCPU()
	}

	// Check that queryThreadsLimit fits in int before conversion
	if queryThreadsLimit > int64(math.MaxInt) || queryThreadsLimit < int64(math.MinInt) {
		s.logger.Warnf("query-threads-limit value %d out of range, defaulting to runtime.NumCPU()", queryThreadsLimit)
		return runtime.NumCPU()
	}

	numPartitionQueries := runtime.NumCPU()

	// Ensure we don't exceed query-threads-limit, assuming each partition query uses up to 4 threads
	queryLimits := int(queryThreadsLimit) / 4
	if queryThreadsLimit > 0 && numPartitionQueries > queryLimits {
		numPartitionQueries = queryLimits
	}

	// Ensure at least 1 worker, cap at total partitions
	return max(1, min(numPartitionQueries, maxPartitions))
}

// getConnectionQueueSize returns the Aerospike connection pool size from the client.
// Returns 128 as fallback if client doesn't support the method.
func (s *Service) getConnectionQueueSize() int {
	if s.client != nil {
		return s.client.GetConnectionQueueSize()
	}
	return 128 // Default fallback
}

// validateConnectionPoolSettings validates that pruner concurrency settings won't exceed
// the Aerospike connection pool. If they would, automatically adjusts chunkGroupLimit
// to prevent connection pool exhaustion and logs a WARNING.
func (s *Service) validateConnectionPoolSettings() {
	// Get Aerospike ConnectionQueueSize from client
	connectionQueueSize := s.getConnectionQueueSize()

	// Calculate max concurrent connections pruner will use
	numWorkers := s.calculatePartitionWorkers()
	maxPrunerConnections := (numWorkers * s.chunkGroupLimit) + numWorkers

	// Calculate recommended max using configured threshold
	recommendedMax := int(float64(connectionQueueSize) * s.connectionPoolWarningThreshold)

	if maxPrunerConnections > recommendedMax {
		// Auto-adjust chunkGroupLimit to prevent connection pool exhaustion
		adjusted := recommendedMax / (numWorkers + 1)
		if adjusted < 1 {
			adjusted = 1 // Ensure at least 1
		}

		s.logger.Warnf(
			"Pruner concurrency would exhaust Aerospike connection pool. "+
				"Max pruner connections: %d, ConnectionQueueSize: %d, Recommended max: %d. "+
				"Auto-adjusting pruner_utxoChunkGroupLimit from %d to %d to prevent exhaustion.",
			maxPrunerConnections, connectionQueueSize, recommendedMax,
			s.chunkGroupLimit, adjusted,
		)
		s.chunkGroupLimit = adjusted
	} else {
		s.logger.Infof(
			"Pruner connection pool validation passed. Max pruner connections: %d, "+
				"ConnectionQueueSize: %d (%.1f%% utilization)",
			maxPrunerConnections, connectionQueueSize,
			float64(maxPrunerConnections)/float64(connectionQueueSize)*100,
		)
	}
}

// partitionWorker processes a range of Aerospike partitions and returns counts
// This is a hybrid approach combining:
// - Partition queries from unmined_iterator.go (parallel Aerospike queries)
// - Chunk processing from current pruner (parallel chunk operations)
func (s *Service) partitionWorker(
	ctx context.Context,
	blockHeight uint32,
	safeCleanupHeight uint32,
	partitionStart int,
	partitionCount int,
) (processed int64, skipped int64, err error) {

	// Each worker creates its own policy for complete independence (no shared state)
	policy := *s.queryPolicy
	policy.RecordQueueSize = s.chunkSize // Optimal: buffer = 1x chunk size for good pipelining

	// Create statement with delete_at_height filter
	stmt := aerospike.NewStatement(s.namespace, s.set)

	// Set the filter to find records with delete_at_height <= safeCleanupHeight
	if err := stmt.SetFilter(aerospike.NewRangeFilter(s.fieldDeleteAtHeight, 1, int64(safeCleanupHeight))); err != nil {
		return 0, 0, err
	}

	// Fetch bins based on defensive mode
	// Note: DeleteAtHeight is only used in query filter (server-side), not in processing logic
	binNames := []string{s.fieldTxID, s.fieldExternal, s.fieldTotalExtraRecs, s.fieldInputs}
	if s.defensiveEnabled {
		binNames = append(binNames, s.fieldUtxos, s.fieldDeletedChildren)
	}
	stmt.BinNames = binNames

	// Create partition filter for this worker's range
	partitionFilter := aerospike.NewPartitionFilterByRange(partitionStart, partitionCount)

	// Query this partition range
	recordset, err := s.client.QueryPartitions(&policy, stmt, partitionFilter)
	if err != nil {
		s.logger.Errorf("[partitionWorker] Aerospike partition query failed (partitions %d-%d): %v",
			partitionStart, partitionStart+partitionCount-1, err)
		return 0, 0, err
	}
	defer recordset.Close()

	// Process recordset with parallel chunk processing
	result := recordset.Results()
	chunk := make([]*aerospike.Result, 0, s.chunkSize)

	// Local counters per worker (no atomic operations during processing)
	var totalProcessed, totalSkipped int64
	var mu sync.Mutex // Protect local counters from chunk goroutines

	// Each worker has its own errgroup for parallel chunk processing
	chunkGroup := &errgroup.Group{}
	util.SafeSetLimit(chunkGroup, s.chunkGroupLimit) // 10 concurrent chunks per worker

	submitChunk := func(chunkToProcess []*aerospike.Result) {
		// No copy needed - chunks are read-only (optimization: saves 160K allocations per pruning operation)
		chunkGroup.Go(func() error {
			processed, skipped, err := s.processRecordChunk(ctx, blockHeight, chunkToProcess)
			if err != nil {
				return err
			}
			// Batch update: accumulate locally per worker (mutex protected)
			mu.Lock()
			totalProcessed += int64(processed)
			totalSkipped += int64(skipped)
			mu.Unlock()
			return nil
		})
	}

	// Accumulate and process chunks
	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		default:
		}

		rec, ok := <-result
		if !ok || rec == nil {
			if len(chunk) > 0 {
				submitChunk(chunk)
			}
			break
		}

		chunk = append(chunk, rec)
		if len(chunk) >= s.chunkSize {
			submitChunk(chunk)
			chunk = chunk[:0]
		}
	}

	if err := chunkGroup.Wait(); err != nil {
		return 0, 0, err
	}

	return totalProcessed, totalSkipped, nil
}

// workerResult holds the result from a partition worker
type workerResult struct {
	processed int64
	skipped   int64
	err       error
}

// PruneWithPartitions implements parallel partition-based pruning
// This method splits the Aerospike keyspace (4096 partitions) across multiple workers
// for maximum throughput, achieving 100x performance improvement over sequential queries
func (s *Service) PruneWithPartitions(ctx context.Context, blockHeight uint32, blockHashStr string, numPartitionQueries int) (int64, error) {
	startTime := time.Now()

	// BLOCK PERSISTER COORDINATION: Calculate safe cleanup height
	// (keep existing logic from sequential implementation)
	safeCleanupHeight := blockHeight

	if s.getPersistedHeight != nil {
		persistedHeight := s.getPersistedHeight()

		// Only apply limitation if block persister has actually processed blocks (height > 0)
		if persistedHeight > 0 {
			retention := s.blockHeightRetention

			// Calculate max safe height: persisted_height + retention
			maxSafeHeight := persistedHeight + retention
			if maxSafeHeight < safeCleanupHeight {
				s.logger.Infof("Limiting cleanup from height %d to %d (persisted: %d, retention: %d)",
					blockHeight, maxSafeHeight, persistedHeight, retention)
				safeCleanupHeight = maxSafeHeight
			}
		}
	}

	// Calculate partition distribution
	// Get total partitions from Aerospike client library (always 4096 in Aerospike architecture)
	totalPartitions := aerospike.NewPartitionFilterAll().Count
	partitionsPerQuery := totalPartitions / numPartitionQueries
	remainingPartitions := totalPartitions % numPartitionQueries

	s.logger.Infof("[pruner][%s:%d] phase 2: starting parallel pruning with %d partition workers (safe cleanup height: %d, total partitions: %d)",
		blockHashStr, blockHeight, numPartitionQueries, safeCleanupHeight, totalPartitions)

	// Launch partition workers
	results := make(chan workerResult, numPartitionQueries)
	var wg sync.WaitGroup

	partitionStart := 0
	for i := 0; i < numPartitionQueries; i++ {
		partitionCount := partitionsPerQuery
		if i < remainingPartitions {
			partitionCount++ // Distribute remainder
		}

		wg.Add(1)
		go func(start, count int) {
			defer wg.Done() // Call Done() AFTER sending to channel
			processed, skipped, err := s.partitionWorker(ctx, blockHeight, safeCleanupHeight,
				start, count)
			results <- workerResult{processed, skipped, err}
		}(partitionStart, partitionCount)

		partitionStart += partitionCount
	}

	// Close results channel when all workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Aggregate results from all workers
	var totalProcessed, totalSkipped int64

	for result := range results {
		if result.err != nil {
			s.logger.Errorf("Partition worker error: %v", result.err)
			return 0, result.err // First error stops everything
		}
		totalProcessed += result.processed
		totalSkipped += result.skipped
	}

	// Log completion with effective throughput
	elapsed := time.Since(startTime)
	tps := float64(totalProcessed) / elapsed.Seconds()

	// Format TPS for readability (e.g., "24.3M records/sec" for large numbers)
	var tpsStr string
	if tps >= 1_000_000 {
		tpsStr = fmt.Sprintf("%.1fM records/sec", tps/1_000_000)
	} else if tps >= 1_000 {
		tpsStr = fmt.Sprintf("%.1fK records/sec", tps/1_000)
	} else {
		tpsStr = fmt.Sprintf("%.2f records/sec", tps)
	}

	p := message.NewPrinter(language.English)
	formattedTotal := p.Sprintf("%d", totalProcessed)
	formattedSkipped := p.Sprintf("%d", totalSkipped)

	var modeStr string
	if s.defensiveEnabled {
		modeStr = ", defensive logic"
	}
	if s.utxoSetTTL {
		modeStr += ", TTL mode"
	}

	s.logger.Infof("[pruner][%s:%d] phase 2: completed parallel pruning in %v: pruned %s records, skipped %s records (%s%s)",
		blockHashStr, blockHeight, elapsed, formattedTotal, formattedSkipped, tpsStr, modeStr)

	prometheusUtxoCleanupBatch.Observe(float64(elapsed.Microseconds()) / 1_000_000)

	s.notifier.NotifyPruneComplete(blockHeight, totalProcessed)

	return totalProcessed, nil
}

// Prune removes transactions marked for deletion at or before the specified height.
// Returns the number of records processed and any error encountered.
// This method is synchronous and blocks until pruning completes or context is cancelled.
func (s *Service) Prune(ctx context.Context, blockHeight uint32, blockHashStr string) (int64, error) {
	if blockHeight == 0 {
		return 0, errors.NewProcessingError("block height cannot be zero")
	}

	// Wait for index to be ready
	if !s.indexReady.Load() {
		return 0, errors.NewProcessingError("index not ready yet")
	}

	// Calculate optimal number of partition workers
	numWorkers := s.calculatePartitionWorkers()

	// Log pruner trigger - note that blockHeight is from BlockPersisted notification
	// which may lag behind current block validation height during catchup
	if s.getPersistedHeight != nil && s.getPersistedHeight() > 0 {
		s.logger.Debugf("[pruner][%s:%d] phase 2: DAH pruner triggered with %d partition workers (block persister last reported: %d)",
			blockHashStr, blockHeight, numWorkers, s.getPersistedHeight())
	} else {
		s.logger.Debugf("[pruner][%s:%d] phase 2: DAH pruner triggered with %d partition workers",
			blockHashStr, blockHeight, numWorkers)
	}

	if s.utxoSetTTL {
		s.logger.Infof("Pruner operating in TTL mode (records expire via nsup)")
	}

	// Always use partition-based approach (even if numWorkers=1)
	return s.PruneWithPartitions(ctx, blockHeight, blockHashStr, numWorkers)
}

// processRecordChunk processes a chunk of parent records with batched child verification
// Returns: (processedCount, skippedCount, error)
func (s *Service) processRecordChunk(ctx context.Context, blockHeight uint32, chunk []*aerospike.Result) (int, int, error) {
	if len(chunk) == 0 {
		return 0, 0, nil
	}

	// Defensive child verification is conditional on the UTXODefensiveEnabled setting
	// When disabled, parents are deleted without verifying children are stable
	var safetyMap map[string]bool
	var parentToChildren map[string][]string

	// Track record errors for batch-level reporting (avoid log flooding)
	var recordErrorCount int
	var firstRecordError error

	if !s.defensiveEnabled {
		// Defensive mode disabled - allow all deletions without child verification
		safetyMap = make(map[string]bool)
		parentToChildren = make(map[string][]string)
	} else {
		// Step 1: Extract ALL unique spending children from chunk
		// For each parent record, we extract all spending child TX hashes from spent UTXOs
		// We must verify EVERY child is stable before deleting the parent
		uniqueSpendingChildren := make(map[string][]byte, 100000) // hex hash -> bytes (typical: ~50-100 children per chunk)
		parentToChildren = make(map[string][]string, len(chunk))  // parent record key -> child hashes
		deletedChildren := make(map[string]bool, 20)              // child hash -> already deleted (typical: 0-20)

		for _, rec := range chunk {
			if rec.Err != nil || rec.Record == nil || rec.Record.Bins == nil {
				// Skip errored/empty records - errors will be tracked in main processing loop
				continue
			}

			// Extract deletedChildren map from parent record
			// If a child is in this map, it means it was already pruned and shouldn't block parent deletion
			if deletedChildrenRaw, hasDeleted := rec.Record.Bins[s.fieldDeletedChildren]; hasDeleted {
				if deletedMap, ok := deletedChildrenRaw.(map[interface{}]interface{}); ok {
					for childHashIface := range deletedMap {
						if childHashStr, ok := childHashIface.(string); ok {
							deletedChildren[childHashStr] = true
							// s.logger.Debugf("Worker %d: Found deleted child in parent record: %s", workerID, childHashStr[:8])
						}
					}
				} else {
					s.logger.Debugf("deletedChildren bin wrong type: %T", deletedChildrenRaw)
				}
			}

			// Extract all spending children from this parent's UTXOs
			utxosRaw, hasUtxos := rec.Record.Bins[s.fieldUtxos]
			if !hasUtxos {
				continue
			}

			utxosList, ok := utxosRaw.([]interface{})
			if !ok {
				continue
			}

			parentKey := rec.Record.Key.String()
			childrenForThisParent := make([]string, 0, 16) // Pre-allocate for typical ~10 spent UTXOs per tx

			// Scan all UTXOs for spending data
			for _, utxoRaw := range utxosList {
				utxoBytes, ok := utxoRaw.([]byte)
				if !ok || len(utxoBytes) < 68 { // 32 (utxo hash) + 36 (spending data)
					continue
				}

				// spending_data starts at byte 32, first 32 bytes of spending_data is child TX hash
				childTxHashBytes := utxoBytes[32:64]

				// Check if this is actual spending data (not all zeros)
				hasSpendingData := false
				for _, b := range childTxHashBytes {
					if b != 0 {
						hasSpendingData = true
						break
					}
				}

				if hasSpendingData {
					hexHash := chainhash.Hash(childTxHashBytes).String()
					uniqueSpendingChildren[hexHash] = childTxHashBytes
					childrenForThisParent = append(childrenForThisParent, hexHash)
					// s.logger.Debugf("Worker %d: Extracted spending child from UTXO: %s", workerID, hexHash[:8])
				}
			}

			if len(childrenForThisParent) > 0 {
				parentToChildren[parentKey] = childrenForThisParent
			}
		}

		// Step 2: Batch verify all unique children (single BatchGet call for entire chunk)
		if len(uniqueSpendingChildren) > 0 {
			safetyMap = s.batchVerifyChildrenSafety(uniqueSpendingChildren, blockHeight, deletedChildren)
		} else {
			safetyMap = make(map[string]bool)
		}
	}

	// Step 3: Accumulate operations for entire chunk, then flush once (efficient batching)
	allParentUpdates := make(map[string]*parentUpdateInfo, 1000) // Accumulate all parent updates for chunk
	allDeletions := make([]*aerospike.Key, 0, 1000)              // Accumulate all deletions for chunk
	allExternalFiles := make([]*externalFileInfo, 0, 10)         // Accumulate external files (<1%)
	processedCount := 0
	skippedCount := 0

	for _, rec := range chunk {
		if rec.Err != nil {
			if firstRecordError == nil {
				firstRecordError = rec.Err
			}
			recordErrorCount++
			prometheusUtxoRecordErrors.Inc()
			continue
		}
		if rec.Record == nil || rec.Record.Bins == nil {
			continue
		}

		txIDBytes, ok := rec.Record.Bins[s.fieldTxID].([]byte)
		if !ok || len(txIDBytes) != 32 {
			continue
		}

		txHash, err := chainhash.NewHash(txIDBytes)
		if err != nil {
			continue
		}

		// Check if children are safe (defensive mode only)
		parentKey := rec.Record.Key.String()
		childrenHashes, hasChildren := parentToChildren[parentKey]

		if hasChildren && len(childrenHashes) > 0 {
			allSafe := true
			var unsafeChild string
			for _, childHash := range childrenHashes {
				if !safetyMap[childHash] {
					allSafe = false
					unsafeChild = childHash
					break
				}
			}

			if !allSafe {
				// Skip this record - at least one child not stable
				s.logger.Infof("Defensive skip - parent %s cannot be deleted due to unstable child %s (%d children total)",
					txHash.String(), unsafeChild, len(childrenHashes))
				skippedCount++
				continue
			}
		}

		// Safe to delete - get inputs for parent updates
		inputs, err := s.getTxInputsFromBins(ctx, blockHeight, rec.Record.Bins, txHash)
		if err != nil {
			return 0, 0, err
		}

		// Accumulate parent updates
		for _, input := range inputs {
			keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.utxoBatchSize)
			parentKeyStr := string(keySource)

			if existing, ok := allParentUpdates[parentKeyStr]; ok {
				existing.childHashes = append(existing.childHashes, txHash)
			} else {
				parentKey, err := aerospike.NewKey(s.namespace, s.set, keySource)
				if err != nil {
					return 0, 0, err
				}
				allParentUpdates[parentKeyStr] = &parentUpdateInfo{
					key:         parentKey,
					childHashes: []*chainhash.Hash{txHash},
				}
			}
		}

		// Accumulate external files
		external, isExternal := rec.Record.Bins[s.fieldExternal].(bool)
		if isExternal && external {
			fileType := fileformat.FileTypeOutputs
			if len(inputs) > 0 {
				fileType = fileformat.FileTypeTx
			}
			allExternalFiles = append(allExternalFiles, &externalFileInfo{
				txHash:   txHash,
				fileType: fileType,
			})
		}

		// Accumulate deletions (master + child records)
		allDeletions = append(allDeletions, rec.Record.Key)

		if totalExtraRecs, hasExtraRecs := rec.Record.Bins[s.fieldTotalExtraRecs].(int); hasExtraRecs && totalExtraRecs > 0 {
			for i := 1; i <= totalExtraRecs; i++ {
				childKeySource := uaerospike.CalculateKeySourceInternal(txHash, uint32(i))
				childKey, err := aerospike.NewKey(s.namespace, s.set, childKeySource)
				if err == nil {
					allDeletions = append(allDeletions, childKey)
				}
			}
		}

		processedCount++
	}

	// Flush all accumulated operations in one batch per chunk
	if err := s.flushCleanupBatches(ctx, allParentUpdates, allDeletions, allExternalFiles); err != nil {
		return 0, 0, err
	}

	// Report record-level errors once per chunk (avoid log flooding)
	if recordErrorCount > 0 {
		s.logger.Errorf("Aerospike record errors in chunk: %d records failed (sample error: %v)", recordErrorCount, firstRecordError)
	}

	return processedCount, skippedCount, nil
}

// batchVerifyChildrenSafety checks multiple child transactions at once to determine if their parents
// can be safely deleted. This is much more efficient than checking each child individually.
//
// Safety guarantee: A parent can only be deleted if ALL spending children have been mined and stable
// for at least 288 blocks. This prevents orphaning children by ensuring we never delete a parent while
// ANY of its spending children might still be reorganized out of the chain.
//
// The spending children are extracted from the parent's UTXO spending_data (embedded in each spent UTXO).
// This ensures we verify EVERY child that spent any output, not just one representative child.
//
// Parameters:
//   - spendingChildrenHashes: Map of child TX hashes to verify (32 bytes each) - ALL unique children
//   - currentBlockHeight: Current block height for safety window calculation
//
// Returns:
//   - map[string]bool: Map of childHash (hex string) -> isSafe (true = this child is stable)
func (s *Service) batchVerifyChildrenSafety(lastSpenderHashes map[string][]byte, currentBlockHeight uint32, deletedChildren map[string]bool) map[string]bool {
	if len(lastSpenderHashes) == 0 {
		return make(map[string]bool)
	}

	safetyMap := make(map[string]bool, len(lastSpenderHashes))

	// Mark already-deleted children as safe immediately
	// If a child is in deletedChildren, it means it was already pruned successfully
	// and shouldn't block the parent from being pruned
	for hexHash := range deletedChildren {
		if _, exists := lastSpenderHashes[hexHash]; exists {
			safetyMap[hexHash] = true
		}
	}

	// Process children in batches to avoid overwhelming Aerospike
	batchSize := s.defensiveBatchReadSize
	if batchSize <= 0 {
		batchSize = 1024 // Default batch size if not configured
	}

	// Convert map to slice for batching, skipping already-deleted children
	// Children in deletedChildren are already marked as safe, no need to query Aerospike
	hashEntries := make([]childHashEntry, 0, len(lastSpenderHashes))
	for hexHash, hashBytes := range lastSpenderHashes {
		// Skip children that are already marked as safe (deleted)
		if safetyMap[hexHash] {
			continue
		}
		hashEntries = append(hashEntries, childHashEntry{hexHash: hexHash, hashBytes: hashBytes})
	}

	// Process in batches
	for i := 0; i < len(hashEntries); i += batchSize {
		end := i + batchSize
		if end > len(hashEntries) {
			end = len(hashEntries)
		}
		batch := hashEntries[i:end]

		s.processBatchOfChildren(batch, safetyMap, currentBlockHeight)
	}

	return safetyMap
}

// childHashEntry holds a child transaction hash for batch processing
type childHashEntry struct {
	hexHash   string
	hashBytes []byte
}

// processBatchOfChildren verifies a batch of child transactions
func (s *Service) processBatchOfChildren(batch []childHashEntry, safetyMap map[string]bool, currentBlockHeight uint32) {
	// Create batch read operations
	batchPolicy := aerospike.NewBatchPolicy()
	batchPolicy.MaxRetries = 3
	batchPolicy.TotalTimeout = 120 * time.Second

	readPolicy := aerospike.NewBatchReadPolicy()
	readPolicy.ReadModeSC = aerospike.ReadModeSCSession

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	hashToKey := make(map[string]string, len(batch)) // hex hash -> key for mapping

	for _, entry := range batch {
		hexHash := entry.hexHash
		hashBytes := entry.hashBytes
		if len(hashBytes) != 32 {
			s.logger.Warnf("[batchVerifyChildrenSafety] Invalid hash length for %s", hexHash)
			safetyMap[hexHash] = false
			continue
		}

		childHash, err := chainhash.NewHash(hashBytes)
		if err != nil {
			s.logger.Warnf("[batchVerifyChildrenSafety] Failed to create hash: %v", err)
			safetyMap[hexHash] = false
			continue
		}

		key, err := aerospike.NewKey(s.namespace, s.set, childHash[:])
		if err != nil {
			s.logger.Warnf("[batchVerifyChildrenSafety] Failed to create key for child %s: %v", childHash.String(), err)
			safetyMap[hexHash] = false
			continue
		}

		batchRecords = append(batchRecords, aerospike.NewBatchRead(
			readPolicy,
			key,
			[]string{s.fieldUnminedSince, s.fieldBlockHeights},
		))
		hashToKey[hexHash] = key.String()
	}

	if len(batchRecords) == 0 {
		return
	}

	// Execute batch operation
	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[processBatchOfChildren] Batch operation failed (affected %d child records): %v", len(batchRecords), err)
		prometheusUtxoBatchQueryError.Inc()
		// Mark all in this batch as unsafe on batch error
		for hexHash := range hashToKey {
			safetyMap[hexHash] = false
		}
		return
	}

	// Process results - use configured retention setting as safety window
	safetyWindow := s.blockHeightRetention

	// Build reverse map for O(1) lookup instead of O(n²) nested loop
	// This avoids scanning all batch records for each child hash
	keyToRecord := make(map[string]*aerospike.BatchRecord, len(batchRecords))
	for _, batchRec := range batchRecords {
		keyToRecord[batchRec.BatchRec().Key.String()] = batchRec.BatchRec()
	}

	// Track individual record errors in batch (avoid log flooding)
	var batchRecordErrorCount int
	var firstBatchRecordError error

	for hexHash, keyStr := range hashToKey {
		// O(1) map lookup instead of O(n) scan
		record := keyToRecord[keyStr]
		if record == nil {
			safetyMap[hexHash] = false
			continue
		}

		if record.Err != nil {
			// Check if this is a "key not found" error - child was already deleted
			// This can happen due to race conditions when processing chunks in parallel:
			// - Chunk 1 deletes child C and updates parent A's deletedChildren
			// - Chunk 2 already loaded parent A (before the update) and now queries child C
			// - Child C is gone, so we get KEY_NOT_FOUND_ERROR
			// In this case, the child is ALREADY deleted, so it's safe to consider it stable
			if aerospikeErr, ok := record.Err.(*aerospike.AerospikeError); ok {
				if aerospikeErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
					// Idempotent race handling: Child already deleted by concurrent partition processing
					// This is safe - child is gone so parent's deletedChildren map doesn't need updating
					safetyMap[hexHash] = true
					continue
				}
			}
			// Any other error → be conservative, don't delete parent
			// Track for batch-level reporting (avoid log flooding)
			if firstBatchRecordError == nil {
				firstBatchRecordError = record.Err
			}
			batchRecordErrorCount++
			safetyMap[hexHash] = false
			continue
		}

		if record.Record == nil || record.Record.Bins == nil {
			safetyMap[hexHash] = false
			continue
		}

		bins := record.Record.Bins

		// Check unmined status
		unminedSince, hasUnminedSince := bins[s.fieldUnminedSince]
		if hasUnminedSince && unminedSince != nil {
			// Child is unmined, not safe
			safetyMap[hexHash] = false
			continue
		}

		// Check block heights
		blockHeightsRaw, hasBlockHeights := bins[s.fieldBlockHeights]
		if !hasBlockHeights {
			// No block heights, treat as not safe
			safetyMap[hexHash] = false
			continue
		}

		blockHeightsList, ok := blockHeightsRaw.([]interface{})
		if !ok || len(blockHeightsList) == 0 {
			safetyMap[hexHash] = false
			continue
		}

		// Find maximum block height
		var maxChildBlockHeight uint32
		for _, heightRaw := range blockHeightsList {
			height, ok := heightRaw.(int)
			if ok && uint32(height) > maxChildBlockHeight {
				maxChildBlockHeight = uint32(height)
			}
		}

		if maxChildBlockHeight == 0 {
			safetyMap[hexHash] = false
			continue
		}

		// Check if child has been stable long enough
		if currentBlockHeight < maxChildBlockHeight+safetyWindow {
			safetyMap[hexHash] = false
		} else {
			safetyMap[hexHash] = true
		}
	}

	// Report individual record errors from this batch (avoid log flooding)
	if batchRecordErrorCount > 0 {
		s.logger.Warnf("[batchVerifyChildrenSafety] %d child record errors in batch (sample error: %v)", batchRecordErrorCount, firstBatchRecordError)
	}
}

// extractInputReference extracts only the previous TX reference from input bytes
// without deserializing the full Input object. This is 5-10x faster than Input.ReadFrom()
// because it skips parsing ScriptSig (which can be 0-10KB) and Sequence fields.
//
// Input wire format:
//
//	Bytes 0-31:   Previous TX ID (32 bytes)
//	Bytes 32-35:  Previous output index (4 bytes, little-endian uint32)
//	Bytes 36+:    ScriptSig length + ScriptSig + Sequence (not needed for parent updates)
//
// Returns:
//   - prevTxID: Previous transaction ID (32 bytes)
//   - prevIndex: Previous output index
//   - error: If input bytes are malformed
func extractInputReference(inputBytes []byte) (prevTxID []byte, prevIndex uint32, err error) {
	if len(inputBytes) < 36 {
		return nil, 0, errors.NewProcessingError("input bytes too short: %d bytes (need 36)", len(inputBytes))
	}

	// Bytes 0-31: Previous TX ID
	prevTxID = inputBytes[0:32]

	// Bytes 32-35: Previous output index (little-endian)
	prevIndex = binary.LittleEndian.Uint32(inputBytes[32:36])

	return prevTxID, prevIndex, nil
}

func (s *Service) getTxInputsFromBins(ctx context.Context, blockHeight uint32, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	var inputs []*bt.Input

	external, ok := bins[s.fieldExternal].(bool)
	if ok && external {
		// transaction is external, we need to get the data from the external store
		txBytes, err := s.external.Get(ctx, txHash.CloneBytes(), fileformat.FileTypeTx)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				// Check if outputs exist (sometimes only outputs are stored)
				exists, err := s.external.Exists(ctx, txHash.CloneBytes(), fileformat.FileTypeOutputs)
				if err != nil {
					return nil, errors.NewProcessingError("error checking existence of outputs for external tx %s at height %d: %v", txHash.String(), blockHeight, err)
				}

				if exists {
					// Only outputs exist, no inputs needed for cleanup
					return nil, nil
				}

				// Idempotent: External file missing (cleaned by LocalDAH or previous run)
				// We can still proceed with record deletion - return empty inputs list
				s.logger.Debugf("external tx %s already deleted from blob store at height %d, proceeding to delete Aerospike record",
					txHash.String(), blockHeight)
				return []*bt.Input{}, nil
			}
			// Other errors should still be reported
			return nil, errors.NewProcessingError("error getting external tx %s at height %d: %v", txHash.String(), blockHeight, err)
		}

		tx, err := bt.NewTxFromBytes(txBytes)
		if err != nil {
			return nil, errors.NewProcessingError("invalid tx bytes for external tx %s at height %d: %v", txHash.String(), blockHeight, err)
		}

		inputs = tx.Inputs
	} else {
		// get the inputs from the record directly (internal transactions)
		inputsValue := bins[s.fieldInputs]
		if inputsValue == nil {
			// Inputs field might be nil for certain records (e.g., coinbase)
			return []*bt.Input{}, nil
		}

		inputInterfaces, ok := inputsValue.([]interface{})
		if !ok {
			// Log more helpful error with actual type
			return nil, errors.NewProcessingError("inputs field has unexpected type %T (expected []interface{}) at height %d",
				inputsValue, blockHeight)
		}

		inputs = make([]*bt.Input, len(inputInterfaces))

		// OPTIMIZATION: Use fast extraction instead of full Input deserialization
		// This skips parsing ScriptSig (can be 0-10KB) and Sequence fields
		// 5-10x faster than Input.ReadFrom() which parses the entire input
		for i, inputInterface := range inputInterfaces {
			inputBytes := inputInterface.([]byte)

			// Fast path: extract only PreviousTxID and Index (36 bytes)
			prevTxID, prevIndex, err := extractInputReference(inputBytes)
			if err != nil {
				return nil, errors.NewProcessingError("failed to extract input reference at height %d: %v", blockHeight, err)
			}

			// Create minimal Input object with only the fields needed for parent updates
			prevHash, err := chainhash.NewHash(prevTxID)
			if err != nil {
				return nil, errors.NewProcessingError("invalid previous tx id at height %d: %v", blockHeight, err)
			}

			inputs[i] = &bt.Input{
				PreviousTxOutIndex: prevIndex,
				// UnlockingScript and SequenceNumber not needed for parent updates - left as zero values
			}
			if err := inputs[i].PreviousTxIDAdd(prevHash); err != nil {
				return nil, errors.NewProcessingError("failed to add previous tx id at height %d: %v", blockHeight, err)
			}
		}
	}

	return inputs, nil
}

// flushCleanupBatches flushes accumulated parent updates, external file deletions, and Aerospike deletions
func (s *Service) flushCleanupBatches(ctx context.Context, parentUpdates map[string]*parentUpdateInfo, deletions []*aerospike.Key, externalFiles []*externalFileInfo) error {
	// Execute parent updates first
	if len(parentUpdates) > 0 {
		if err := s.executeBatchParentUpdates(ctx, parentUpdates); err != nil {
			return err
		}
	}

	// Delete external files before Aerospike records (fail-safe: if file deletion fails, we keep the record)
	if len(externalFiles) > 0 {
		if err := s.executeBatchExternalFileDeletions(ctx, externalFiles); err != nil {
			return err
		}
	}

	// Delete Aerospike records last
	if len(deletions) > 0 {
		if err := s.executeBatchDeletions(ctx, deletions); err != nil {
			return err
		}
	}

	return nil
}

// extractTxHash extracts the transaction hash from record bins
func (s *Service) extractTxHash(bins aerospike.BinMap) (*chainhash.Hash, error) {
	txIDBytes, ok := bins[s.fieldTxID].([]byte)
	if !ok || len(txIDBytes) != 32 {
		return nil, errors.NewProcessingError("invalid or missing txid")
	}

	txHash, err := chainhash.NewHash(txIDBytes)
	if err != nil {
		return nil, errors.NewProcessingError("invalid txid bytes: %v", err)
	}

	return txHash, nil
}

// extractInputs extracts the transaction inputs from record bins
func (s *Service) extractInputs(ctx context.Context, blockHeight uint32, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	return s.getTxInputsFromBins(ctx, blockHeight, bins, txHash)
}

// executeBatchParentUpdates performs Phase 2a: updates parent records to mark that their
// child transactions have been deleted (adds to deletedChildren map).
//
// IDEMPOTENCY: This operation is safely re-runnable:
// - Missing parents (KEY_NOT_FOUND) are skipped - they were already deleted
// - Duplicate updates are no-ops - deletedChildren map updates are idempotent
// - Partial batch failures can be retried without side effects
//
// This must complete before Phase 2b (child deletion) to maintain referential integrity.
func (s *Service) executeBatchParentUpdates(ctx context.Context, updates map[string]*parentUpdateInfo) error {
	if len(updates) == 0 {
		return nil
	}

	// Convert map to batch operations
	// Track deleted children by adding child tx hashes to the DeletedChildren map
	mapPolicy := aerospike.DefaultMapPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(updates))

	for _, info := range updates {
		// For each child transaction being deleted, add it to the DeletedChildren map
		ops := make([]*aerospike.Operation, len(info.childHashes))
		for i, childHash := range info.childHashes {
			ops[i] = aerospike.MapPutOp(mapPolicy, s.fieldDeletedChildren,
				aerospike.NewStringValue(childHash.String()), aerospike.BoolValue(true))
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(s.batchWritePolicy, info.key, ops...))
	}

	// Check context before expensive operation
	select {
	case <-ctx.Done():
		s.logger.Infof("Context cancelled, skipping parent update batch")
		return ctx.Err()
	default:
	}

	// Execute batch
	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Batch parent update failed: %v", err)
		return errors.NewStorageError("batch parent update failed", err)
	}

	// Check for errors
	successCount := 0
	notFoundCount := 0
	errorCount := 0

	for _, rec := range batchRecords {
		if rec.BatchRec().Err != nil {
			if rec.BatchRec().Err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
				// Idempotent: Parent may have been deleted by concurrent pruning or LocalDAH cleanup
				// This is a success condition - parent is already gone so we don't need to update it
				notFoundCount++
				continue
			}
			// Log other errors
			s.logger.Errorf("Parent update error for key %v: %v", rec.BatchRec().Key, rec.BatchRec().Err)
			errorCount++
		} else {
			successCount++
		}
	}

	// Return error if any individual record operations failed
	if errorCount > 0 {
		return errors.NewStorageError("%d parent update operations failed", errorCount)
	}

	return nil
}

// executeBatchDeletions performs Phase 2b: removes child transaction records from Aerospike
// after their parents have been updated (Phase 2a).
//
// IDEMPOTENCY: This operation is safely re-runnable:
// - Already-deleted records (KEY_NOT_FOUND) are counted as success
// - Multiple delete attempts on same record are harmless
// - Partial batch failures can be retried without side effects
//
// Parents must be updated first (Phase 2a) before calling this function.
func (s *Service) executeBatchDeletions(ctx context.Context, keys []*aerospike.Key) error {
	if len(keys) == 0 {
		return nil
	}

	batchRecords := make([]aerospike.BatchRecordIfc, len(keys))

	if s.utxoSetTTL {
		ttlWritePolicy := aerospike.NewBatchWritePolicy()
		ttlWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
		ttlWritePolicy.Expiration = 1

		for i, key := range keys {
			batchRecords[i] = aerospike.NewBatchWrite(ttlWritePolicy, key, aerospike.TouchOp())
		}
	} else {
		batchDeletePolicy := aerospike.NewBatchDeletePolicy()
		for i, key := range keys {
			batchRecords[i] = aerospike.NewBatchDelete(batchDeletePolicy, key)
		}
	}

	// Check context before expensive operation
	select {
	case <-ctx.Done():
		s.logger.Infof("Context cancelled, skipping deletion batch")
		return ctx.Err()
	default:
	}

	// Execute batch
	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Batch deletion failed for %d records: %v", len(keys), err)
		return errors.NewStorageError("batch deletion failed", err)
	}

	// Check for errors and count successes
	successCount := 0
	alreadyDeletedCount := 0
	errorCount := 0

	for _, rec := range batchRecords {
		if rec.BatchRec().Err != nil {
			if rec.BatchRec().Err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
				// Idempotent: Record already deleted by concurrent pruning or previous run
				// This operation is safely re-runnable - treat as success
				alreadyDeletedCount++
			} else {
				s.logger.Errorf("Deletion error for key %v: %v", rec.BatchRec().Key, rec.BatchRec().Err)
				errorCount++
			}
		} else {
			successCount++
		}
	}

	// Return error if any individual record operations failed
	if errorCount > 0 {
		return errors.NewStorageError("%d deletion operations failed", errorCount)
	}

	return nil
}

// executeBatchExternalFileDeletions performs Phase 3: removes external blob files
// for transactions that have been deleted from Aerospike (Phase 2b).
//
// IDEMPOTENCY: This operation is safely re-runnable:
// - Already-deleted files (ErrNotFound) are counted as success
// - Files deleted by LocalDAH cleanup are handled gracefully
// - Partial batch failures can be retried without side effects
//
// This runs after Phase 2b (child deletion) but can run concurrently with other blocks.
func (s *Service) executeBatchExternalFileDeletions(ctx context.Context, files []*externalFileInfo) error {
	if len(files) == 0 {
		return nil
	}

	successCount := 0
	alreadyDeletedCount := 0
	errorCount := 0

	for _, fileInfo := range files {
		// Check context before each deletion
		select {
		case <-ctx.Done():
			s.logger.Infof("Context cancelled, stopping external file deletions")
			return ctx.Err()
		default:
		}

		// Delete the external file
		err := s.external.Del(ctx, fileInfo.txHash.CloneBytes(), fileInfo.fileType)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				// Idempotent: File already deleted by LocalDAH cleanup, concurrent pruning, or previous run
				// This operation is safely re-runnable - treat as success
				alreadyDeletedCount++
				s.logger.Debugf("External file for tx %s (type %d) already deleted", fileInfo.txHash.String(), fileInfo.fileType)
			} else {
				s.logger.Errorf("Failed to delete external file for tx %s (type %d): %v", fileInfo.txHash.String(), fileInfo.fileType, err)
				errorCount++
			}
		} else {
			successCount++
		}
	}

	s.logger.Debugf("External file deletion batch - success: %d, already deleted: %d, errors: %d", successCount, alreadyDeletedCount, errorCount)

	// Return error if any deletions failed
	if errorCount > 0 {
		return errors.NewStorageError("%d external file deletions failed", errorCount)
	}

	return nil
}

// ProcessSingleRecord processes a single transaction for cleanup (for testing/manual cleanup)
// This is a simplified wrapper around the batch operations for single-record processing
func (s *Service) ProcessSingleRecord(txHash *chainhash.Hash, inputs []*bt.Input) error {
	if len(inputs) == 0 {
		return nil // No parents to update
	}

	// Build parent updates map
	parentUpdates := make(map[string]*parentUpdateInfo, len(inputs)) // One parent per input (worst case)
	for _, input := range inputs {
		keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.utxoBatchSize)
		parentKeyStr := string(keySource)

		if existing, ok := parentUpdates[parentKeyStr]; ok {
			existing.childHashes = append(existing.childHashes, txHash)
		} else {
			parentKey, err := aerospike.NewKey(s.namespace, s.set, keySource)
			if err != nil {
				return errors.NewProcessingError("failed to create parent key", err)
			}
			parentUpdates[parentKeyStr] = &parentUpdateInfo{
				key:         parentKey,
				childHashes: []*chainhash.Hash{txHash},
			}
		}
	}

	// Execute parent updates synchronously
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return s.executeBatchParentUpdates(ctx, parentUpdates)
}
