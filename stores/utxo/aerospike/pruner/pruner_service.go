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

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/txparse"
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

// TimeoutError indicates that a query operation timed out or encountered a network error.
// This error type is used to distinguish retriable timeout errors from other errors.
type TimeoutError struct {
	cause error
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("query timeout or network error: %v", e.cause)
}

func (e *TimeoutError) Unwrap() error {
	return e.cause
}

var (
	prometheusMetricsInitOnce                 sync.Once
	prometheusUtxoCleanupBatch                prometheus.Histogram
	prometheusUtxoRecordErrors                prometheus.Counter
	prometheusUtxoBatchQueryError             prometheus.Counter
	prometheusUtxoRecordsDeleted              prometheus.Counter
	prometheusUtxoRecordsDeletedSkipped       prometheus.Counter
	prometheusUtxoParentsUpdated              prometheus.Counter
	prometheusUtxoParentsUpdatedSkipped       prometheus.Counter
	prometheusUtxoExternalFilesDeleted        prometheus.Counter
	prometheusUtxoExternalFilesDeletedSkipped prometheus.Counter
	prometheusUtxoRetryAttempts               prometheus.Counter
	prometheusUtxoTimeoutEvents               prometheus.Counter
	prometheusUtxoParentsSkippedPruned        prometheus.Counter
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

	// LuaPackage is the name of the registered Lua UDF module
	LuaPackage string

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
	settings    *settings.Settings
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
	partitionWorkerFn              func(ctx context.Context, blockHeight uint32, partitionStart int, partitionCount int, prunedSet *PrunedTxSet) (int64, int64, error)

	// Lua UDF module name
	luaPackage string

	// Cached field names (avoid repeated String() allocations in hot paths)
	fieldTxID, fieldUtxos, fieldInputs, fieldDeletedChildren, fieldExternal        string
	fieldDeleteAtHeight, fieldTotalExtraRecs, fieldUnminedSince, fieldBlockHeights string

	// Internally reused variables
	queryPolicy *aerospike.QueryPolicy
	writePolicy *aerospike.WritePolicy
	batchPolicy *aerospike.BatchPolicy
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
func NewService(settings *settings.Settings, opts Options) (*Service, error) {
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
		prometheusUtxoRecordsDeleted = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_records_deleted_total",
			Help: "Total number of UTXO records deleted during pruning (updated incrementally)",
		})
		prometheusUtxoRecordsDeletedSkipped = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_records_deleted_skipped_total",
			Help: "Total number of UTXO records skipped during pruning (updated incrementally)",
		})
		prometheusUtxoParentsUpdated = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_parents_updated_total",
			Help: "Total number of parent records updated during pruning (updated incrementally)",
		})
		prometheusUtxoParentsUpdatedSkipped = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_parents_updated_skipped_total",
			Help: "Total number of parent records skipped during pruning (updated incrementally)",
		})
		prometheusUtxoExternalFilesDeleted = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_external_files_deleted_total",
			Help: "Total number of external files deleted during pruning (updated incrementally)",
		})
		prometheusUtxoExternalFilesDeletedSkipped = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_external_files_deleted_skipped_total",
			Help: "Total number of external files skipped during pruning (updated incrementally)",
		})
		prometheusUtxoRetryAttempts = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_retry_attempts_total",
			Help: "Total number of retry attempts across all pruning operations (indicates catchup with timeouts)",
		})
		prometheusUtxoTimeoutEvents = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_timeout_events_total",
			Help: "Total number of timeout events requiring retry during pruning operations",
		})
		prometheusUtxoParentsSkippedPruned = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_parents_skipped_pruned_total",
			Help: "Number of parent updates skipped because parent was already pruned in this session",
		})
	})

	// Use the configured query policy from settings (configured via aerospike_queryPolicy URL)
	queryPolicy := util.GetAerospikeQueryPolicy(settings)
	queryPolicy.IncludeBinData = true // Need to include bin data for cleanup processing

	// Use the configured write policy from settings
	writePolicy := util.GetAerospikeWritePolicy(settings, 0)

	// Use the configured batch policy from settings (configured via aerospike_batchPolicy URL)
	batchPolicy := util.GetAerospikeBatchPolicy(settings)

	notifier := NewPrunerEventNotifier()
	for _, observer := range opts.Observers {
		notifier.AddObserver(observer)
	}

	service := &Service{
		logger:                         opts.Logger,
		client:                         opts.Client,
		settings:                       settings,
		external:                       opts.ExternalStore,
		namespace:                      opts.Namespace,
		set:                            opts.Set,
		ctx:                            opts.Ctx,
		indexWaiter:                    opts.IndexWaiter,
		notifier:                       notifier,
		luaPackage:                     opts.LuaPackage,
		queryPolicy:                    queryPolicy,
		writePolicy:                    writePolicy,
		batchPolicy:                    batchPolicy,
		utxoBatchSize:                  settings.UtxoStore.UtxoBatchSize,
		blockHeightRetention:           settings.GetUtxoStoreBlockHeightRetention(),
		defensiveEnabled:               settings.Pruner.UTXODefensiveEnabled,
		defensiveBatchReadSize:         settings.Pruner.UTXODefensiveBatchReadSize,
		chunkSize:                      settings.Pruner.UTXOChunkSize,
		chunkGroupLimit:                settings.Pruner.UTXOChunkGroupLimit,
		progressLogInterval:            settings.Pruner.UTXOProgressLogInterval,
		partitionQueries:               settings.Pruner.UTXOPartitionQueries,
		connectionPoolWarningThreshold: settings.Pruner.ConnectionPoolWarningThreshold,
		utxoSetTTL:                     settings.Pruner.UTXOSetTTL,
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

	service.partitionWorkerFn = service.partitionWorker

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
	partitionStart int,
	partitionCount int,
	prunedSet *PrunedTxSet,
) (processed int64, skipped int64, err error) {

	// Each worker creates its own policy for complete independence (no shared state)
	policy := *s.queryPolicy
	policy.RecordQueueSize = s.chunkSize // Optimal: buffer = 1x chunk size for good pipelining

	// Create statement with delete_at_height filter
	stmt := aerospike.NewStatement(s.namespace, s.set)

	// Set the filter to find records with delete_at_height <= blockHeight
	if err := stmt.SetFilter(aerospike.NewRangeFilter(s.fieldDeleteAtHeight, 1, int64(blockHeight))); err != nil {
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

	// Two-stage pipeline: reader registers TXIDs in shared set before processor handles them
	// This eliminates cross-worker race conditions for parent-update skipping
	// Derive read-ahead buffer size from chunkSize with a conservative cap so memory scales predictably.
	readAheadBase := s.chunkSize
	if readAheadBase <= 0 {
		readAheadBase = 1000 // fall back to default chunk size if unset
	}
	// Allow modest read-ahead, but cap to avoid excessive buffered records.
	readAheadBuffer := min(readAheadBase*2, 10000)
	if readAheadBuffer < 1 {
		readAheadBuffer = 1
	}
	pipeline := make(chan *aerospike.Result, readAheadBuffer)

	// Stage 1: Reader — streams from Aerospike, registers TXIDs, forwards to pipeline
	var readerErr error
	var readerDone sync.WaitGroup
	readerDone.Add(1)
	go func() {
		defer readerDone.Done()
		defer close(pipeline)
		for {
			// Read from Aerospike with cancellation support to avoid blocking on stalled recordsets
			var rec *aerospike.Result
			select {
			case <-ctx.Done():
				return
			case r, ok := <-recordset.Results():
				if !ok || r == nil {
					return
				}
				rec = r
			}

			// Register TXID in shared set before forwarding (if record is valid)
			if rec.Err == nil && rec.Record != nil && rec.Record.Bins != nil {
				if txIDBytes, ok := rec.Record.Bins[s.fieldTxID].([]byte); ok && len(txIDBytes) == 32 {
					var h chainhash.Hash
					copy(h[:], txIDBytes)
					if prunedSet != nil {
						prunedSet.Add(h)
					}
				}
			}

			// Check for timeout/network errors
			if rec.Err != nil {
				var asErr aerospike.Error
				if errors.As(rec.Err, &asErr) {
					if asErr.Matches(types.TIMEOUT, types.NETWORK_ERROR, types.NO_RESPONSE, types.SERVER_NOT_AVAILABLE) {
						s.logger.Infof("Partition range [%d-%d] hit timeout/network error, stopping reader: %v",
							partitionStart, partitionStart+partitionCount-1, rec.Err)
						readerErr = &TimeoutError{cause: rec.Err}
						return
					}
				}
			}

			// Send to pipeline with cancellation support to avoid deadlock on shutdown
			select {
			case pipeline <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Stage 2: Processor — reads from pipeline, chunks, and processes
	chunk := make([]*aerospike.Result, 0, s.chunkSize)
	var totalProcessed, totalSkipped int64
	var mu sync.Mutex

	chunkGroup := &errgroup.Group{}
	util.SafeSetLimit(chunkGroup, s.chunkGroupLimit)

	submitChunk := func(chunkToProcess []*aerospike.Result) {
		chunkGroup.Go(func() error {
			processed, skipped, err := s.processRecordChunk(ctx, blockHeight, chunkToProcess, prunedSet)
			if err != nil {
				return err
			}
			mu.Lock()
			totalProcessed += int64(processed)
			totalSkipped += int64(skipped)
			mu.Unlock()

			if processed > 0 {
				prometheusUtxoRecordsDeleted.Add(float64(processed))
			}
			if skipped > 0 {
				prometheusUtxoRecordsDeletedSkipped.Add(float64(skipped))
			}
			return nil
		})
	}

	for rec := range pipeline {
		select {
		case <-ctx.Done():
			// Close recordset to unblock the reader from recordset.Results()
			recordset.Close()
			readerDone.Wait()
			return 0, 0, ctx.Err()
		default:
		}

		chunk = append(chunk, rec)
		if len(chunk) >= s.chunkSize {
			submitChunk(chunk)
			chunk = make([]*aerospike.Result, 0, s.chunkSize)
		}
	}

	if len(chunk) > 0 {
		submitChunk(chunk)
	}

	if err := chunkGroup.Wait(); err != nil {
		readerDone.Wait()
		return 0, 0, err
	}

	// Check if reader hit a timeout
	readerDone.Wait()
	if readerErr != nil {
		return totalProcessed, totalSkipped, readerErr
	}

	return totalProcessed, totalSkipped, nil
}

// partitionRange represents a contiguous range of Aerospike partitions assigned to a worker
type partitionRange struct {
	start int
	count int
}

// workerResult holds the result from a partition worker
type workerResult struct {
	processed      int64
	skipped        int64
	err            error
	partitionStart int
	partitionCount int
}

// PruneWithPartitions implements parallel partition-based pruning with retry logic for timeout handling.
// This method splits the Aerospike keyspace (4096 partitions) across multiple workers
// for maximum throughput, achieving 100x performance improvement over sequential queries.
//
// Timeout Handling: When a worker encounters a timeout or network error, only the failed partition
// ranges are retried — successfully completed ranges are skipped. This prevents a feedback loop
// where re-processing already-deleted records generates KEY_NOT_FOUND errors and additional load.
// All operations are idempotent, so partial retries are safe.
func (s *Service) PruneWithPartitions(ctx context.Context, blockHeight uint32, blockHashStr string, numPartitionQueries int) (int64, error) {
	startTime := time.Now()
	maxRetries := 10 // Reasonable limit to prevent infinite loop
	var lastErr error

	// Build initial partition ranges from the distribution logic
	totalPartitions := aerospike.NewPartitionFilterAll().Count
	partitionsPerQuery := totalPartitions / numPartitionQueries
	remainingPartitions := totalPartitions % numPartitionQueries

	pendingRanges := make([]partitionRange, 0, numPartitionQueries)
	partitionStart := 0
	for i := 0; i < numPartitionQueries; i++ {
		partitionCount := partitionsPerQuery
		if i < remainingPartitions {
			partitionCount++ // Distribute remainder
		}
		pendingRanges = append(pendingRanges, partitionRange{start: partitionStart, count: partitionCount})
		partitionStart += partitionCount
	}

	// Shared set tracking TXIDs scanned for pruning — used to skip wasteful parent updates.
	// Disabled when defensive mode is on: records may be skipped (child not stable) after the
	// reader registers them, which would incorrectly suppress parent updates for records still
	// in Aerospike.
	//
	// Reused across retry attempts intentionally. The set's contract is "this TXID was scanned
	// as a deletion candidate in this session" — so reuse is correct because:
	//   1. Add is idempotent (duplicate keys are no-ops), so re-scanning a partition after
	//      a timeout simply re-registers entries that were already there.
	//   2. CheckAndRemove is destructive (one consumer per parent), which is fine: if a parent
	//      was already consumed by a child in a prior attempt, the parent has either been
	//      deleted (so the next child's skip is correct) or its deletion is still pending in
	//      a retry partition (so the next child issues a real update and incurs a wasted
	//      Aerospike op — a perf nit, not a correctness bug).
	//   3. The skip only suppresses the deletedChildren bin update on the parent. That bin is
	//      only consulted by the defensive-mode safety check, which is OFF whenever prunedSet
	//      is non-nil, so a missed update has no behavioural consequence.
	// Allocating a fresh set per attempt would break the cross-partition dedup that drove this
	// optimisation: most parent/child pairs span partitions.
	//
	// Memory is bounded by prunedTxSetMaxEntries. In tight-chain workloads CheckAndRemove
	// reclaims entries quickly so peak Len() stays small, but production sessions can scan
	// hundreds of millions of records (~500M observed on dev-scale-1). Without a cap, a
	// workload where most parents live in prior blocks would keep every TXID added,
	// growing memory linearly with session size. The cap silently degrades the optimisation
	// back to baseline once hit — no correctness impact, just no further skips.
	var prunedSet *PrunedTxSet
	if !s.defensiveEnabled {
		prunedSet = NewPrunedTxSet(256, prunedTxSetMaxEntries)
	}

	// Cumulative counters persist across retry attempts
	var cumulativeProcessed, cumulativeSkipped int64

	// Retry loop: on timeout, only retry the partition ranges that failed
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			prometheusUtxoRetryAttempts.Inc()
		}

		s.logger.Infof("[pruner][%s:%d] phase 2: pruning attempt %d with %d partition workers (total partitions: %d)",
			blockHashStr, blockHeight, attempt, len(pendingRanges), totalPartitions)

		// Launch partition workers for pending ranges only
		results := make(chan workerResult, len(pendingRanges))
		var wg sync.WaitGroup

		for _, pr := range pendingRanges {
			wg.Add(1)
			go func(r partitionRange) {
				defer wg.Done()
				processed, skipped, err := s.partitionWorkerFn(ctx, blockHeight, r.start, r.count, prunedSet)
				results <- workerResult{
					processed:      processed,
					skipped:        skipped,
					err:            err,
					partitionStart: r.start,
					partitionCount: r.count,
				}
			}(pr)
		}

		go func() {
			wg.Wait()
			close(results)
		}()

		// Classify each worker result as success or timeout
		var attemptProcessed, attemptSkipped int64
		var failedRanges []partitionRange
		var hasTimeout bool
		var nonTimeoutErr error

		for result := range results {
			attemptProcessed += result.processed
			attemptSkipped += result.skipped

			if result.err != nil {
				var timeoutErr *TimeoutError
				if errors.As(result.err, &timeoutErr) {
					hasTimeout = true
					failedRanges = append(failedRanges, partitionRange{
						start: result.partitionStart,
						count: result.partitionCount,
					})
				} else {
					nonTimeoutErr = result.err
				}
			}
		}

		// Accumulate partial progress from this attempt (including from timed-out workers)
		cumulativeProcessed += attemptProcessed
		cumulativeSkipped += attemptSkipped

		// Non-timeout errors fail immediately (even if some workers also timed out)
		if nonTimeoutErr != nil {
			s.logger.Errorf("[pruner][%s:%d] phase 2: partition worker error (non-timeout): %v", blockHashStr, blockHeight, nonTimeoutErr)
			return 0, nonTimeoutErr
		}

		if hasTimeout {
			prometheusUtxoTimeoutEvents.Inc()

			p := message.NewPrinter(language.English)
			s.logger.Infof("[pruner][%s:%d] phase 2: timeout on attempt %d, %d/%d workers timed out, processed %s records so far. Retrying failed partition ranges...",
				blockHashStr, blockHeight, attempt, len(failedRanges), len(pendingRanges), p.Sprintf("%d", cumulativeProcessed))

			lastErr = errors.NewProcessingError("timeout in %d partition ranges", len(failedRanges))
			pendingRanges = failedRanges // Only retry the failed ranges
			continue
		}

		// Success — all pending partitions processed without timeout
		elapsed := time.Since(startTime)
		tps := float64(cumulativeProcessed) / elapsed.Seconds()

		var tpsStr string
		if tps >= 1_000_000 {
			tpsStr = fmt.Sprintf("%.1fM records/sec", tps/1_000_000)
		} else if tps >= 1_000 {
			tpsStr = fmt.Sprintf("%.1fK records/sec", tps/1_000)
		} else {
			tpsStr = fmt.Sprintf("%.2f records/sec", tps)
		}

		p := message.NewPrinter(language.English)
		formattedTotal := p.Sprintf("%d", cumulativeProcessed)
		formattedSkipped := p.Sprintf("%d", cumulativeSkipped)

		var modeStr string
		if s.defensiveEnabled {
			modeStr = ", defensive logic"
		}
		if s.utxoSetTTL {
			modeStr += ", TTL mode"
		}

		var attemptsStr string
		if attempt > 1 {
			attemptsStr = fmt.Sprintf(" (after %d attempts)", attempt)
		}

		s.logger.Infof("[pruner][%s:%d] phase 2: completed parallel pruning in %v: pruned %s records, skipped %s records (%s%s)%s",
			blockHashStr, blockHeight, elapsed, formattedTotal, formattedSkipped, tpsStr, modeStr, attemptsStr)

		prometheusUtxoCleanupBatch.Observe(float64(elapsed.Microseconds()) / 1_000_000)

		s.notifier.NotifyPruneComplete(blockHeight, cumulativeProcessed)

		return cumulativeProcessed, nil
	}

	// Max retries exceeded
	s.logger.Warnf("[pruner][%s:%d] phase 2: max retries (%d) exceeded for pruning, processed %d records before giving up, last error: %v",
		blockHashStr, blockHeight, maxRetries, cumulativeProcessed, lastErr)
	return 0, errors.NewProcessingError("max retries (%d) exceeded: %v", maxRetries, lastErr)
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

	// Log pruner trigger
	s.logger.Debugf("[pruner][%s:%d] phase 2: DAH pruner triggered with %d partition workers",
		blockHashStr, blockHeight, numWorkers)

	if s.utxoSetTTL {
		s.logger.Infof("Pruner operating in TTL mode (records expire via nsup)")
	}

	// Always use partition-based approach (even if numWorkers=1)
	return s.PruneWithPartitions(ctx, blockHeight, blockHashStr, numWorkers)
}

// processRecordChunk processes a chunk of parent records with batched child verification
// Returns: (processedCount, skippedCount, error)
func (s *Service) processRecordChunk(ctx context.Context, blockHeight uint32, chunk []*aerospike.Result, prunedSet *PrunedTxSet) (int, int, error) {
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
		uniqueSpendingChildren := make(map[string][]byte, 1000)  // hex hash -> bytes (typical: ~50-100 children per chunk)
		parentToChildren = make(map[string][]string, len(chunk)) // parent record key -> child hashes
		deletedChildren := make(map[string]bool, 20)             // child hash -> already deleted (typical: 0-20)

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
	allParentUpdates := make(map[string]*parentUpdateInfo, 1000)
	allDeletions := make([]*aerospike.Key, 0, 1000)      // Accumulate all deletions for chunk
	allExternalFiles := make([]*externalFileInfo, 0, 10) // Accumulate external files (<1%)
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

		// Accumulate parent updates, skipping parents already pruned in this session
		for _, input := range inputs {
			// Check if parent TX was already pruned — if so, skip the update
			parentTxID := input.PreviousTxIDChainHash()
			if prunedSet != nil && prunedSet.CheckAndRemove(*parentTxID) {
				prometheusUtxoParentsSkippedPruned.Inc()
				continue
			}

			keySource := uaerospike.CalculateKeySource(parentTxID, input.PreviousTxOutIndex, s.utxoBatchSize)
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
		// OPTIMIZATION: Use streaming parser that only extracts input references (prevTxID + prevIndex),
		// skipping all scripts and outputs. This avoids downloading and deserializing the entire
		// transaction which can be megabytes, achieving ~90% bandwidth reduction for external txs.
		reader, err := s.external.GetIoReader(ctx, txHash.CloneBytes(), fileformat.FileTypeTx)
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
		defer reader.Close()

		inputs, err = txparse.ParseInputReferencesFromExtendedTx(reader)
		if err != nil {
			return nil, errors.NewProcessingError("failed to parse input references for external tx %s at height %d: %v", txHash.String(), blockHeight, err)
		}
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

	// Delete Aerospike records before external files (fail-safe: if file deletion fails,
	// orphaned files are harmless; if record deletion fails, both remain consistent)
	if !s.settings.Pruner.SkipDeletions {
		if len(deletions) > 0 {
			if err := s.executeBatchDeletions(ctx, deletions); err != nil {
				return err
			}
		}
	}

	if len(externalFiles) > 0 {
		if err := s.executeBatchExternalFileDeletions(ctx, externalFiles); err != nil {
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
// Uses a Lua UDF (addDeletedChildren) instead of BatchWrite+MapPutItemsOp so that
// missing parent records are handled server-side without generating KEY_NOT_FOUND errors
// in Aerospike client metrics.
//
// IDEMPOTENCY: This operation is safely re-runnable:
// - Missing parents are silently skipped by the Lua UDF (TX_NOT_FOUND)
// - Duplicate updates are no-ops - deletedChildren map updates are idempotent
// - Partial batch failures can be retried without side effects
//
// This must complete before Phase 2b (child deletion) to maintain referential integrity.
func (s *Service) executeBatchParentUpdates(ctx context.Context, updates map[string]*parentUpdateInfo) error {
	if len(updates) == 0 {
		return nil
	}

	if s.luaPackage != "" {
		return s.executeBatchParentUpdatesUDF(ctx, updates)
	}

	return s.executeBatchParentUpdatesBatchWrite(ctx, updates)
}

// executeBatchParentUpdatesUDF uses a Lua UDF (addDeletedChildren) so that missing parent
// records are handled server-side without generating KEY_NOT_FOUND errors in Aerospike client metrics.
func (s *Service) executeBatchParentUpdatesUDF(ctx context.Context, updates map[string]*parentUpdateInfo) error {
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(updates))

	for _, info := range updates {
		childHashList := make([]interface{}, 0, len(info.childHashes))
		for _, childHash := range info.childHashes {
			childHashList = append(childHashList, childHash.String())
		}

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(
			batchUDFPolicy,
			info.key,
			s.luaPackage,
			"addDeletedChildren",
			aerospike.NewValue(childHashList),
		))
	}

	select {
	case <-ctx.Done():
		s.logger.Infof("Context cancelled, skipping parent update batch")
		return ctx.Err()
	default:
	}

	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Batch parent update failed: %v", err)
		return errors.NewStorageError("batch parent update failed", err)
	}

	successCount := 0
	notFoundCount := 0
	errorCount := 0

	for _, rec := range batchRecords {
		batchRec := rec.BatchRec()
		if batchRec.Err != nil {
			s.logger.Errorf("Parent update error for key %v: %v", batchRec.Key, batchRec.Err)
			errorCount++
			continue
		}

		if batchRec.Record != nil && batchRec.Record.Bins != nil {
			if resp, ok := batchRec.Record.Bins["SUCCESS"]; ok {
				if respMap, ok := resp.(map[interface{}]interface{}); ok {
					if status, ok := respMap["status"].(string); ok {
						switch status {
						case "OK":
							successCount++
						case "ERROR":
							if errCode, ok := respMap["errorCode"].(string); ok && errCode == "TX_NOT_FOUND" {
								notFoundCount++
							} else {
								s.logger.Errorf("Parent update Lua error for key %v: %v", batchRec.Key, respMap)
								errorCount++
							}
						}
						continue
					}
				}
			}
		}

		successCount++
	}

	if errorCount > 0 {
		return errors.NewStorageError("%d parent update operations failed", errorCount)
	}

	if successCount > 0 {
		prometheusUtxoParentsUpdated.Add(float64(successCount))
	}

	if notFoundCount > 0 {
		prometheusUtxoParentsUpdatedSkipped.Add(float64(notFoundCount))
	}

	return nil
}

// executeBatchParentUpdatesBatchWrite is the fallback when Lua UDF is not configured.
// Uses BatchWrite+MapPutItemsOp with UPDATE_ONLY policy. Missing records generate
// KEY_NOT_FOUND in Aerospike client metrics but are handled gracefully.
func (s *Service) executeBatchParentUpdatesBatchWrite(ctx context.Context, updates map[string]*parentUpdateInfo) error {
	batchWritePolicy := aerospike.NewBatchWritePolicy()
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	mapPolicy := aerospike.DefaultMapPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(updates))

	for _, info := range updates {
		items := make(map[interface{}]interface{}, len(info.childHashes))
		for _, childHash := range info.childHashes {
			items[childHash.String()] = true
		}

		op := aerospike.MapPutItemsOp(mapPolicy, s.fieldDeletedChildren, items)
		batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, info.key, op))
	}

	select {
	case <-ctx.Done():
		s.logger.Infof("Context cancelled, skipping parent update batch")
		return ctx.Err()
	default:
	}

	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Batch parent update failed: %v", err)
		return errors.NewStorageError("batch parent update failed", err)
	}

	successCount := 0
	notFoundCount := 0
	errorCount := 0

	for _, rec := range batchRecords {
		if rec.BatchRec().Err != nil {
			if rec.BatchRec().Err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
				notFoundCount++
				continue
			}
			s.logger.Errorf("Parent update error for key %v: %v", rec.BatchRec().Key, rec.BatchRec().Err)
			errorCount++
		} else {
			successCount++
		}
	}

	if errorCount > 0 {
		return errors.NewStorageError("%d parent update operations failed", errorCount)
	}

	if successCount > 0 {
		prometheusUtxoParentsUpdated.Add(float64(successCount))
	}

	if notFoundCount > 0 {
		prometheusUtxoParentsUpdatedSkipped.Add(float64(notFoundCount))
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
				s.logger.Debugf("External file for tx %s (type %s) already deleted", fileInfo.txHash.String(), fileInfo.fileType)
			} else {
				s.logger.Errorf("Failed to delete external file for tx %s (type %s): %v", fileInfo.txHash.String(), fileInfo.fileType, err)
				errorCount++
			}
		} else {
			successCount++
		}
	}

	s.logger.Debugf("External file deletion batch - success: %d, already deleted: %d, errors: %d", successCount, alreadyDeletedCount, errorCount)

	// Update metric with successful deletions
	if successCount > 0 {
		prometheusUtxoExternalFilesDeleted.Add(float64(successCount))
	}

	if alreadyDeletedCount > 0 {
		prometheusUtxoExternalFilesDeletedSkipped.Add(float64(alreadyDeletedCount))
	}

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
