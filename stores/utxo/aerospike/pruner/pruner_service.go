package pruner

import (
	"bytes"
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
	"github.com/bsv-blockchain/go-subtree"
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

// prometheusUtxoRecordsSkippedUnmined counts records the pruner kept because they are currently unmined.
// It is registered at package init, outside the metrics Once, so it is never nil in tests that
// initialise the other collectors themselves. Distinct from the defensive-mode skip count.
var prometheusUtxoRecordsSkippedUnmined = promauto.NewCounter(prometheus.CounterOpts{
	Name: "utxo_pruner_records_skipped_unmined_total",
	Help: "Total number of records kept by the pruner because they are currently unmined and not conflicting",
})

// prometheusUtxoDAHClearedUnmined counts records whose deleteAtHeight the pruner cleared because they
// are unmined. Package-level for the same reason as prometheusUtxoRecordsSkippedUnmined.
var prometheusUtxoDAHClearedUnmined = promauto.NewCounter(prometheus.CounterOpts{
	Name: "utxo_pruner_dah_cleared_unmined_total",
	Help: "Total number of records whose deleteAtHeight was cleared because they are unmined",
})

var (
	prometheusMetricsInitOnce                 sync.Once
	prometheusUtxoCleanupBatch                prometheus.Histogram
	prometheusUtxoRecordErrors                prometheus.Counter
	prometheusUtxoInputResolutionErrors       prometheus.Counter
	prometheusUtxoParentUpdateFailures        prometheus.Counter
	prometheusUtxoBatchQueryError             prometheus.Counter
	prometheusUtxoRecordsDeleted              prometheus.Counter
	prometheusUtxoRecordsDeletedSkipped       prometheus.Counter
	prometheusUtxoParentsUpdated              prometheus.Counter
	prometheusUtxoParentsUpdatedSkipped       prometheus.Counter
	prometheusUtxoChildrenHeldUnverified      prometheus.Counter
	prometheusUtxoMarkersSuperseded           prometheus.Counter
	prometheusUtxoMarkerWritesRefused         prometheus.Counter
	prometheusUtxoMarkerWithdrawFailures      prometheus.Counter
	prometheusUtxoExternalFilesDeleted        prometheus.Counter
	prometheusUtxoExternalFilesDeletedSkipped prometheus.Counter
	prometheusUtxoRetryAttempts               prometheus.Counter
	prometheusUtxoTimeoutEvents               prometheus.Counter
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

	// Commit level for the pruner's own record removals only. Parent updates and
	// every write outside the pruner stay on COMMIT_ALL — see prune_policies.go.
	removalCommitLevel aerospike.CommitLevel

	partitionWorkerFn func(ctx context.Context, blockHeight uint32, partitionStart int, partitionCount int) (int64, int64, error)

	// Cached field names (avoid repeated String() allocations in hot paths)
	fieldTxID, fieldUtxos, fieldInputs, fieldDeletedChildren, fieldExternal        string
	fieldDeleteAtHeight, fieldTotalExtraRecs, fieldUnminedSince, fieldBlockHeights string
	fieldConflicting                                                               string

	// batchOperateFn overrides the batch calls of the unmined guard and of the
	// marker and delete path; tests only.
	batchOperateFn func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error

	// readMastersFn overrides readMasters; tests only.
	readMastersFn func(keys []*aerospike.Key) ([]masterRead, error)

	// Internally reused variables
	queryPolicy *aerospike.QueryPolicy
	writePolicy *aerospike.WritePolicy
	batchPolicy *aerospike.BatchPolicy
}

// parentUpdateInfo holds accumulated parent update information for batching
type parentUpdateInfo struct {
	key         *aerospike.Key
	childHashes []*chainhash.Hash // Child transactions being deleted
	// seen dedups childHashes. One consolidation transaction can spend many
	// outputs of the same parent, and every one of those inputs asks for the
	// same (parent, child) marker. Without this, a 10k-input spend of a single
	// parent built a 10k-element list of the identical 64-char hex string in one
	// batch record.
	seen map[chainhash.Hash]struct{}
	// offsets holds, per child, the positions in this record's utxos bin that
	// the child's inputs name. A child may be marked only when at least one of
	// those outputs still records it as the spender: see keepSpendHolders.
	offsets map[chainhash.Hash][]uint32
	// markerFailed records that this record's marker write did not land, or
	// that its outcome could not be read. Set by writeVerifiedMarkers.
	markerFailed bool
	// released holds the children that are conflicting losers. ProcessConflicting
	// released every input they spent, the uncontested ones too, so an unspent
	// output they name does not hold them back: see keepSpendHolders.
	released map[chainhash.Hash]struct{}
}

// add appends childHash unless this parent record already has it queued, and
// records the utxos-bin offset the child claims to have spent.
func (p *parentUpdateInfo) add(childHash *chainhash.Hash, offset uint32) {
	if p.offsets == nil {
		p.offsets = make(map[chainhash.Hash][]uint32, 1)
	}

	p.offsets[*childHash] = append(p.offsets[*childHash], offset)

	if _, ok := p.seen[*childHash]; ok {
		return
	}

	p.seen[*childHash] = struct{}{}
	p.childHashes = append(p.childHashes, childHash)
}

// markReleased records that childHash is a conflicting loser.
func (p *parentUpdateInfo) markReleased(childHash *chainhash.Hash) {
	if p.released == nil {
		p.released = make(map[chainhash.Hash]struct{}, 1)
	}

	p.released[*childHash] = struct{}{}
}

// merge folds another record's queued children for the same parent into p.
func (p *parentUpdateInfo) merge(other *parentUpdateInfo) {
	for _, childHash := range other.childHashes {
		for _, offset := range other.offsets[*childHash] {
			p.add(childHash, offset)
		}
	}

	for childHash := range other.released {
		p.markReleased(&childHash)
	}
}

// pendingDeletion is one scanned record and every Aerospike key that removing it
// requires (master plus any pagination records). Deletion is gated on the
// record's parent markers having landed, so the keys have to stay grouped by
// child transaction rather than flattened into one list.
type pendingDeletion struct {
	txHash *chainhash.Hash
	keys   []*aerospike.Key
	// page is set when the scanned record is a pagination record found on its
	// own, so keys holds no master record. A refused page delete must not read
	// as the master surviving: see buildCarriedDeletes.
	page bool
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
		prometheusUtxoInputResolutionErrors = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_input_resolution_errors_total",
			Help: "Total number of records retained during pruning because their input references could not be resolved",
		})
		prometheusUtxoParentUpdateFailures = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_parent_update_failures_total",
			Help: "Total number of parent replay-marker writes that failed; the children they protect are held back from deletion and reconsidered next cycle",
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
		prometheusUtxoChildrenHeldUnverified = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_children_held_unverified_total",
			Help: "Total number of (child, parent record) pairs held back from deletion because the parent output the child claims is unspent, missing or malformed, so no marker can be justified",
		})
		prometheusUtxoMarkersSuperseded = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_markers_superseded_total",
			Help: "Total number of (child, parent record) pairs left unmarked because the parent output names a different spender (a conflicting loser)",
		})
		prometheusUtxoMarkerWritesRefused = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_marker_writes_refused_total",
			Help: "Total number of parent records whose verified marker write was refused because an output does not name a queued child as its spender; each is then read to sort its children",
		})
		prometheusUtxoMarkerWithdrawFailures = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_pruner_marker_withdraw_failures_total",
			Help: "Total number of parent records from which the replay markers of a held-back child could not be withdrawn; retried next cycle",
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
		removalCommitLevel:             removalCommitLevel(settings.Pruner.RelaxRemovalCommitLevel),
		fieldTxID:                      fields.TxID.String(),
		fieldUtxos:                     fields.Utxos.String(),
		fieldInputs:                    fields.Inputs.String(),
		fieldDeletedChildren:           fields.DeletedChildren.String(),
		fieldExternal:                  fields.External.String(),
		fieldDeleteAtHeight:            fields.DeleteAtHeight.String(),
		fieldTotalExtraRecs:            fields.TotalExtraRecs.String(),
		fieldUnminedSince:              fields.UnminedSince.String(),
		fieldBlockHeights:              fields.BlockHeights.String(),
		fieldConflicting:               fields.Conflicting.String(),
	}

	// Every parent of a deleted record gets an addDeletedChildren write. Do
	// not skip parents through a probabilistic "already pruned" set: a false
	// positive drops the replay marker on a live parent (see #1701). Missing
	// parents already return TX_NOT_FOUND / KEY_NOT_FOUND and are counted as
	// skipped.
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
	// UnminedSince and Conflicting are needed to protect currently-unmined records (see dropUnminedRecords).
	binNames := []string{s.fieldTxID, s.fieldExternal, s.fieldTotalExtraRecs, s.fieldInputs, s.fieldUnminedSince, s.fieldConflicting}
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
	util.SafeSetLimit(s.logger, chunkGroup, s.chunkGroupLimit)

	// Each chunk's deletes ride the next chunk's batch; whatever is left is
	// sent when the worker stops, on every path. A worker stopping on an error
	// or a cancellation still drains: the carried children are cleared and
	// their markers have landed, so leaving them present behind those markers
	// would refuse their own re-spend.
	carry := &deleteCarry{}

	defer func() {
		// A chunk still in flight adds to the carry when it finishes, so the
		// drain waits for every chunk first. Wait returns the same error again
		// when the body already waited.
		_ = chunkGroup.Wait()

		// Detached on every path, not only after an error: a cancellation
		// landing between any check here and the drain's send would discard
		// a carry whose markers have landed.
		deleted, heldBack, drainErr := s.drainCarryDetached(ctx, carry)

		processed += int64(deleted)
		skipped += int64(heldBack)

		if deleted > 0 {
			prometheusUtxoRecordsDeleted.Add(float64(deleted))
		}

		if heldBack > 0 {
			prometheusUtxoRecordsDeletedSkipped.Add(float64(heldBack))
		}

		if drainErr != nil && err == nil {
			err = drainErr
		}
	}()

	submitChunk := func(chunkToProcess []*aerospike.Result) {
		chunkGroup.Go(func() error {
			processed, skipped, err := s.processRecordChunkCarry(ctx, blockHeight, chunkToProcess, carry)
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
				processed, skipped, err := s.partitionWorkerFn(ctx, blockHeight, r.start, r.count)
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

// verifyChunkChildren runs the defensive check for a whole chunk in two steps:
// pull every spending child named by the scanned records' own utxos bins, then
// verify them all in one BatchGet.
//
// Returns the per-child stability verdict and, per scanned record key, the
// children that record's outputs name. Extracted from processRecordChunkCarry so the
// chunk loop reads as the decision it makes rather than the bin parsing it needs.
func (s *Service) verifyChunkChildren(chunk []*aerospike.Result, blockHeight uint32) (map[string]bool, map[string][]string) {
	uniqueSpendingChildren := make(map[string][]byte, 1000)   // hex hash -> bytes (typical: ~50-100 children per chunk)
	parentToChildren := make(map[string][]string, len(chunk)) // parent record key -> child hashes
	deletedChildren := make(map[string]bool, 20)              // child hash -> already deleted (typical: 0-20)

	for _, rec := range chunk {
		if rec.Err != nil || rec.Record == nil || rec.Record.Bins == nil {
			// Skip errored/empty records - errors will be tracked in main processing loop
			continue
		}

		// A child in the deletedChildren map was already pruned and must not
		// block its parent's deletion.
		s.collectDeletedChildren(rec.Record.Bins, deletedChildren)

		children := extractSpendingChildren(rec.Record.Bins[s.fieldUtxos], uniqueSpendingChildren)
		if len(children) > 0 {
			parentToChildren[rec.Record.Key.String()] = children
		}
	}

	if len(uniqueSpendingChildren) == 0 {
		return make(map[string]bool), parentToChildren
	}

	return s.batchVerifyChildrenSafety(uniqueSpendingChildren, blockHeight, deletedChildren), parentToChildren
}

// collectDeletedChildren folds one record's deletedChildren bin into the
// chunk-wide set of children already known to be pruned.
func (s *Service) collectDeletedChildren(bins aerospike.BinMap, into map[string]bool) {
	raw, ok := bins[s.fieldDeletedChildren]
	if !ok {
		return
	}

	deletedMap, ok := raw.(map[interface{}]interface{})
	if !ok {
		s.logger.Debugf("deletedChildren bin wrong type: %T", raw) // urlsafe: logs only the Go type of a bin value, never a URL
		return
	}

	for childHashIface := range deletedMap {
		if childHashStr, ok := childHashIface.(string); ok {
			into[childHashStr] = true
		}
	}
}

// extractSpendingChildren reads the child transactions named by a record's spent
// utxos, adding each to the chunk-wide unique set and returning this record's
// own list. Each utxo entry is the 32-byte utxo hash followed by 36 bytes of
// spending data, whose first 32 bytes are the spending transaction's id; an
// all-zero id means unspent.
func extractSpendingChildren(utxosRaw interface{}, unique map[string][]byte) []string {
	utxosList, ok := utxosRaw.([]interface{})
	if !ok {
		return nil
	}

	children := make([]string, 0, 16) // Pre-allocate for typical ~10 spent UTXOs per tx

	for _, utxoRaw := range utxosList {
		utxoBytes, ok := utxoRaw.([]byte)
		if !ok || len(utxoBytes) < 68 { // 32 (utxo hash) + 36 (spending data)
			continue
		}

		childTxHashBytes := utxoBytes[32:64]
		if isZeroBytes(childTxHashBytes) {
			continue
		}

		hexHash := chainhash.Hash(childTxHashBytes).String()
		unique[hexHash] = childTxHashBytes
		children = append(children, hexHash)
	}

	return children
}

// isZeroBytes reports whether every byte is zero, which is how an unspent utxo
// records its spending data.
func isZeroBytes(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}

	return true
}

// processRecordChunkCarry processes a chunk of parent records with batched
// child verification. Its marker writes go in one batch with the deletes the
// carry holds from the previous chunk, and its own cleared deletes are left in
// the carry (flushChunk). Returns: (processedCount, skippedCount, error), where
// processed counts the records this chunk's batch confirmed deleted, which are
// the previous chunk's.
func (s *Service) processRecordChunkCarry(ctx context.Context, blockHeight uint32, chunk []*aerospike.Result, carry *deleteCarry) (int, int, error) {
	if len(chunk) == 0 {
		return 0, 0, nil
	}

	// Do not delete a record that is currently unmined, unless it is conflicting. Counted separately
	// from the defensive skip count.
	chunk, _, clears := s.dropUnminedRecords(chunk)
	s.clearUnminedDAH(ctx, clears)

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

	// Track records retained because their input references could not be resolved
	var inputErrorCount int
	var firstInputError error

	if !s.defensiveEnabled {
		// Defensive mode disabled - allow all deletions without child verification
		safetyMap = make(map[string]bool)
		parentToChildren = make(map[string][]string)
	} else {
		safetyMap, parentToChildren = s.verifyChunkChildren(chunk, blockHeight)
	}

	// Step 3: Accumulate operations for entire chunk, then flush once (efficient batching)
	allParentUpdates := make(map[string]*parentUpdateInfo, 1000)
	allDeletions := make([]*pendingDeletion, 0, 1000)    // Accumulate all deletions for chunk
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
		// parentToChildren is only filled in defensive mode. Key.String formats the
		// key with fmt.Sprintf, ~6% of the pruner's CPU when done for every record,
		// so skip it when there is nothing to look up.
		var childrenHashes []string

		var hasChildren bool

		if len(parentToChildren) > 0 {
			childrenHashes, hasChildren = parentToChildren[rec.Record.Key.String()]
		}

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

		// Safe to delete - get inputs for parent updates.
		//
		// A record whose input references cannot be resolved is retained, not
		// fatal. Retention is the safe outcome: the record itself carries the
		// replay protection its parents would otherwise inherit. Every later
		// cycle reconsiders it, but a transaction whose external blob is gone
		// for good will fail here every time, so for those records retention is
		// permanent and utxo_pruner_input_resolution_errors_total is the signal
		// to act on. Failing the chunk instead would unwind through
		// partitionWorker into PruneWithPartitions, where a non-TimeoutError is
		// never retried, so a single unresolvable record would block pruning
		// node-wide forever.
		inputs, err := s.getTxInputsFromBins(ctx, blockHeight, rec.Record.Bins, txHash)
		if err != nil {
			inputErrorCount++
			if firstInputError == nil {
				firstInputError = err
			}
			prometheusUtxoInputResolutionErrors.Inc()

			continue
		}

		// A probabilistic filter cannot prove a parent record is absent, so every
		// input gets its marker. Markers are staged per record and only merged
		// into the chunk-wide map once every input resolved, so a failure here
		// skips this record alone.
		recordUpdates := make(map[string]*parentUpdateInfo, len(inputs))

		var updateErr error

		for _, input := range inputs {
			if updateErr = s.addParentUpdatesForInput(recordUpdates, input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, txHash); updateErr != nil {
				break
			}
		}

		if updateErr != nil {
			inputErrorCount++
			if firstInputError == nil {
				firstInputError = updateErr
			}
			prometheusUtxoInputResolutionErrors.Inc()

			continue
		}

		if conflicting, _ := rec.Record.Bins[s.fieldConflicting].(bool); conflicting {
			for _, info := range recordUpdates {
				info.markReleased(txHash)
			}
		}

		for source, info := range recordUpdates {
			if existing, ok := allParentUpdates[source]; ok {
				existing.merge(info)
			} else {
				allParentUpdates[source] = info
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

		// Accumulate deletions (master + pagination records), grouped by the
		// transaction they belong to so flushChunk can hold back exactly
		// the records whose parent markers did not land.
		deletion := &pendingDeletion{txHash: txHash, keys: []*aerospike.Key{rec.Record.Key}, page: s.isPaginationRecord(rec.Record, txHash)}

		if totalExtraRecs, hasExtraRecs := rec.Record.Bins[s.fieldTotalExtraRecs].(int); hasExtraRecs && totalExtraRecs > 0 {
			for i := 1; i <= totalExtraRecs; i++ {
				childKeySource := uaerospike.CalculateKeySourceInternal(txHash, uint32(i))
				childKey, err := aerospike.NewKey(s.namespace, s.set, childKeySource)
				if err == nil {
					deletion.keys = append(deletion.keys, childKey)
				}
			}
		}

		allDeletions = append(allDeletions, deletion)

		processedCount++
	}

	// Persist parent markers before removing any child records. Records whose
	// markers did not land are held back rather than deleted, and reported here
	// so they are not counted as pruned. The records counted as processed are
	// the ones this chunk's batch confirmed deleted.
	deleted, heldBack, err := s.flushChunk(ctx, carry, allParentUpdates, allDeletions, allExternalFiles)
	if err != nil {
		return deleted, skippedCount + heldBack, err
	}

	processedCount = deleted

	if heldBack > 0 {
		skippedCount += heldBack

		s.logger.Warnf("Pruner held back %d records in chunk whose parent replay markers failed to write; they are reconsidered next cycle", heldBack)
	}

	// Report record-level errors once per chunk (avoid log flooding)
	if recordErrorCount > 0 {
		s.logger.Errorf("Aerospike record errors in chunk: %d records failed (sample error: %v)", recordErrorCount, firstRecordError)
	}

	if inputErrorCount > 0 {
		// Warn, not Error: the cycle succeeded and these records are safe where
		// they are. If the count does not fall across cycles the blobs are gone
		// for good and the records will never prune; alert on
		// utxo_pruner_input_resolution_errors_total rather than on this line.
		s.logger.Warnf("Pruner retained %d records in chunk whose input references could not be resolved; they keep their own replay protection and are reconsidered next cycle (sample error: %v)", inputErrorCount, firstInputError)
	}

	return processedCount, skippedCount, nil
}

// masterRead is the result of reading one master record's unminedSince and conflicting bins.
type masterRead struct {
	record *aerospike.Record
	err    error
}

// readMasters reads the unminedSince and conflicting bins of the given master keys in one batch.
// A returned error means the whole batch failed.
func (s *Service) readMasters(keys []*aerospike.Key) ([]masterRead, error) {
	if s.readMastersFn != nil {
		return s.readMastersFn(keys)
	}

	readPolicy := aerospike.NewBatchReadPolicy()
	readPolicy.ReadModeSC = aerospike.ReadModeSCSession

	batchRecords := make([]aerospike.BatchRecordIfc, len(keys))
	for i, key := range keys {
		batchRecords[i] = aerospike.NewBatchRead(readPolicy, key, []string{s.fieldUnminedSince, s.fieldConflicting})
	}

	if err := s.batchOperate(batchRecords); err != nil {
		return nil, err
	}

	reads := make([]masterRead, len(keys))
	for i := range batchRecords {
		br := batchRecords[i].BatchRec()
		reads[i] = masterRead{record: br.Record, err: br.Err}
	}

	return reads, nil
}

// dropUnminedRecords removes from the chunk every record that is currently unmined (unminedSince set)
// and not conflicting, and returns the remaining records plus the number dropped.
//
// This is defence in depth, not a guarantee. A transaction marked unmined by
// MarkTransactionsOnLongestChain(false) keeps its deleteAtHeight, so the DAH query can return it.
// Deleting it would lose a transaction that may be mined again. Conflicting transactions get a DAH
// while unmined and must stay prunable. The state is read from the query stream and the deletes are
// unconditional, so a transaction that becomes unmined between the query and the delete can still be
// deleted. TTL-based deletion (utxoSetTTL) is likewise not covered once a record has been touched.
//
// A record protected on positive evidence (not on an uncertain read) is returned in clears so that
// its deleteAtHeight, and that of its pagination records, can be cleared with clearUnminedDAH. That
// matches the store invariant (Lua setDeleteAtHeight clears the DAH of an unmined record) and takes
// the record out of the DAH index. Records kept because a read failed keep their DAH and are retried
// next cycle.
//
// Masters are identified by the totalExtraRecs bin. A master written without that bin is also handled:
// it is the record stored under the bare txid, found by comparing key digests.
//
// Pagination records of large transactions carry their own deleteAtHeight but have no unminedSince bin,
// so they are judged by their master record, which is read in one batch. Otherwise the pagination
// records would be deleted from under a protected master. The page is kept whenever the answer is
// uncertain (batch failure, per-record error other than key-not-found, empty result, key build error);
// the next prune cycle retries it. A page whose master does not exist (orphan) is deleted.
func (s *Service) dropUnminedRecords(chunk []*aerospike.Result) ([]*aerospike.Result, int, []*dahClear) {
	isProtected := func(bins aerospike.BinMap) bool {
		if v, ok := bins[s.fieldUnminedSince]; !ok || v == nil {
			return false
		}

		conflicting, _ := bins[s.fieldConflicting].(bool)

		return !conflicting
	}

	type pagination struct {
		idx       int
		masterKey *aerospike.Key
	}

	var pages []pagination

	protected := make([]bool, len(chunk))
	dropped := 0

	protect := func(i int) {
		protected[i] = true
		dropped++
	}

	clearsByMaster := make(map[string]*dahClear)

	addClear := func(masterKey *aerospike.Key, pageKeys ...*aerospike.Key) {
		c, ok := clearsByMaster[string(masterKey.Digest())]
		if !ok {
			c = &dahClear{masterKey: masterKey}
			clearsByMaster[string(masterKey.Digest())] = c
		}

		for _, pk := range pageKeys {
			d := string(pk.Digest())
			if _, dup := c.seen[d]; dup {
				continue
			}

			if c.seen == nil {
				c.seen = make(map[string]struct{}, len(pageKeys))
			}

			c.seen[d] = struct{}{}
			c.pageKeys = append(c.pageKeys, pk)
		}
	}

	for i, rec := range chunk {
		if rec.Err != nil || rec.Record == nil || rec.Record.Bins == nil {
			continue
		}

		bins := rec.Record.Bins

		if extraRecs, isMaster := bins[s.fieldTotalExtraRecs]; isMaster {
			if isProtected(bins) {
				protect(i)

				var pageKeys []*aerospike.Key

				if n, _ := extraRecs.(int); n > 0 {
					if h, herr := chainhash.NewHash(bytesBin(bins[s.fieldTxID])); herr == nil {
						for num := 1; num <= n; num++ {
							if k, kerr := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(h, uint32(num))); kerr == nil { //nolint:gosec
								pageKeys = append(pageKeys, k)
							}
						}
					}
				}

				addClear(rec.Record.Key, pageKeys...)
			}

			continue
		}

		// No totalExtraRecs bin: either a master from a writer that does not set it, or a pagination
		// record. Only the master record is stored under the bare txid.
		txIDBytes, ok := bins[s.fieldTxID].([]byte)
		if !ok || len(txIDBytes) != chainhash.HashSize {
			continue
		}

		txHash, err := chainhash.NewHash(txIDBytes)
		if err != nil {
			continue
		}

		masterKey, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(txHash, 0))
		if err != nil {
			// cannot tell whether this is the master or a page, so keep it
			protect(i)

			continue
		}

		if isProtected(bins) {
			protect(i)

			if bytes.Equal(masterKey.Digest(), rec.Record.Key.Digest()) {
				addClear(rec.Record.Key) // legacy master
			}

			continue
		}

		if bytes.Equal(masterKey.Digest(), rec.Record.Key.Digest()) {
			continue // this record is the master and is not protected
		}

		pages = append(pages, pagination{idx: i, masterKey: masterKey})
	}

	if len(pages) > 0 {
		keys := make([]*aerospike.Key, len(pages))
		for i, p := range pages {
			keys[i] = p.masterKey
		}

		reads, batchErr := s.readMasters(keys)
		if batchErr != nil {
			s.logger.Errorf("[pruner] failed to read master records of %d pagination records, keeping them: %v", len(pages), batchErr)
		}

		for i, p := range pages {
			keep := batchErr != nil || len(reads) != len(pages)
			if !keep {
				r := reads[i]

				switch {
				case r.err != nil:
					var asErr aerospike.Error
					// only a missing master (orphan page) may be deleted
					keep = !(errors.As(r.err, &asErr) && asErr.Matches(types.KEY_NOT_FOUND_ERROR))
				case r.record == nil || r.record.Bins == nil:
					keep = true // neither an error nor a record: ambiguous
				default:
					keep = isProtected(r.record.Bins)
					if keep {
						// positively unmined: the master and this page lose their DAH
						addClear(p.masterKey, chunk[p.idx].Record.Key)
					}
				}
			}

			if keep {
				protect(p.idx)
			}
		}
	}

	if dropped == 0 {
		return chunk, 0, nil
	}

	prometheusUtxoRecordsSkippedUnmined.Add(float64(dropped))

	kept := make([]*aerospike.Result, 0, len(chunk)-dropped)
	for i, rec := range chunk {
		if !protected[i] {
			kept = append(kept, rec)
		}
	}

	clears := make([]*dahClear, 0, len(clearsByMaster))
	for _, c := range clearsByMaster {
		clears = append(clears, c)
	}

	return kept, dropped, clears
}

// dahClear names a protected master whose deleteAtHeight, and that of the listed pagination
// records, is to be cleared.
type dahClear struct {
	masterKey *aerospike.Key
	pageKeys  []*aerospike.Key
	seen      map[string]struct{} // digests of pageKeys, to dedupe in O(1)
}

func bytesBin(v interface{}) []byte {
	b, _ := v.([]byte)
	return b
}

// batchOperate runs a batch; batchOperateFn overrides it in tests.
func (s *Service) batchOperate(records []aerospike.BatchRecordIfc) error {
	if s.batchOperateFn != nil {
		return s.batchOperateFn(s.batchPolicy, records)
	}

	return s.client.BatchOperate(s.batchPolicy, records)
}

// hadDAH reports whether a clear write found a deleteAtHeight to clear. The write reads the bin
// before clearing it, so the result is the old value, either bare or as the first operation result.
func (s *Service) hadDAH(br *aerospike.BatchRecord) bool {
	if br.Record == nil || br.Record.Bins == nil {
		return false
	}

	switch v := br.Record.Bins[s.fieldDeleteAtHeight].(type) {
	case int:
		return true
	case aerospike.OpResults:
		return len(v) > 0 && v[0] != nil
	case []interface{}:
		return len(v) > 0 && v[0] != nil
	}

	return false
}

// maxPageClearBatch bounds one pagination-record write in clearUnminedDAH. It matches the store's
// maxChildDAHPagesPerBatch (stores/utxo/aerospike), which bounds the same kind of write.
const maxPageClearBatch = 4096

// clearUnminedDAH clears deleteAtHeight on protected unmined masters and then on their pagination
// records, one batch each, and returns how many records actually had a DAH cleared. The master write
// only applies while the record is still unmined, not conflicting and not preserved, so a concurrent
// re-mine is not undone; when it is filtered out the pages are left alone too. The pages are written
// only after their master succeeded.
//
// A master that has no DAH itself (it was reached through a page, or an earlier cycle cleared it)
// is not counted, but its pages are still cleared: otherwise a page that kept its DAH would be returned
// by the DAH query every cycle.
//
// If the master is re-mined between the master clear and the page clear, a page can end up without a
// DAH while the master has one. That does not leak: the pages are deleted by computed key together
// with their master when the master is pruned.
//
// A failure is only logged: nothing is deleted on its account and the records are returned by the DAH
// query again next cycle. Nothing is written when deletions are skipped (dry run) or ctx is done.
func (s *Service) clearUnminedDAH(ctx context.Context, clears []*dahClear) int {
	if len(clears) == 0 || s.settings.Pruner.SkipDeletions || ctx.Err() != nil {
		return 0
	}

	// read the old DAH first so only records that had one are counted
	clearOps := []*aerospike.Operation{
		aerospike.GetBinOp(s.fieldDeleteAtHeight),
		aerospike.PutOp(aerospike.NewBin(s.fieldDeleteAtHeight, nil)),
	}

	masterPolicy := aerospike.NewBatchWritePolicy()
	masterPolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	masterPolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(s.fieldUnminedSince),
		aerospike.ExpNot(aerospike.ExpBinExists(fields.PreserveUntil.String())),
		aerospike.ExpOr(
			aerospike.ExpNot(aerospike.ExpBinExists(s.fieldConflicting)),
			aerospike.ExpNot(aerospike.ExpBoolBin(s.fieldConflicting)),
		),
	)

	masterBatch := make([]aerospike.BatchRecordIfc, len(clears))
	for i, c := range clears {
		masterBatch[i] = aerospike.NewBatchWrite(masterPolicy, c.masterKey, clearOps...)
	}

	if err := s.batchOperate(masterBatch); err != nil {
		s.logger.Errorf("[pruner] failed to clear deleteAtHeight of %d unmined records, retrying next cycle: %v", len(clears), err)
		return 0
	}

	pagePolicy := aerospike.NewBatchWritePolicy()
	pagePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	var pageBatch []aerospike.BatchRecordIfc

	masters, pages, failed := 0, 0, 0

	for i, c := range clears {
		br := masterBatch[i].BatchRec()
		if br.Err != nil {
			var asErr aerospike.Error
			if errors.As(br.Err, &asErr) && asErr.Matches(types.FILTERED_OUT, types.KEY_NOT_FOUND_ERROR) {
				continue // re-mined, conflicting, preserved or gone: leave it alone
			}

			failed++

			continue
		}

		if s.hadDAH(br) {
			masters++
		}

		for _, pk := range c.pageKeys {
			pageBatch = append(pageBatch, aerospike.NewBatchWrite(pagePolicy, pk, clearOps...))
		}
	}

	var (
		missing      int
		missingPages []string
	)

	// Sent in chunks, as the store side does for child DAH writes: a chunk holding a few large
	// unmined external txs would otherwise be one write too big to finish before its timeout.
	for start := 0; start < len(pageBatch) && ctx.Err() == nil; start += maxPageClearBatch {
		chunk := pageBatch[start:min(start+maxPageClearBatch, len(pageBatch))]

		if err := s.batchOperate(chunk); err != nil {
			s.logger.Errorf("[pruner] failed to clear deleteAtHeight of %d pagination records, retrying next cycle: %v", len(chunk), err)
			continue
		}

		for _, r := range chunk {
			br := r.BatchRec()

			switch {
			case br.Err != nil:
				// a page that is already gone has nothing left to clear and nothing to retry,
				// but under a still-protected master it is lost data: leave a trace
				var asErr aerospike.Error
				if !errors.As(br.Err, &asErr) || !asErr.Matches(types.KEY_NOT_FOUND_ERROR) {
					failed++
				} else {
					if len(missingPages) < 10 {
						missingPages = append(missingPages, fmt.Sprintf("%x", br.Key.Digest()))
					}

					missing++
				}
			case s.hadDAH(br):
				pages++
			}
		}
	}

	if failed > 0 {
		s.logger.Errorf("[pruner] failed to clear deleteAtHeight of %d unmined records, retrying next cycle", failed)
	}

	if missing > 0 {
		s.logger.Warnf("[pruner] %d pagination records of unmined transactions were already deleted (first key digests: %v)", missing, missingPages)
	}

	if cleared := masters + pages; cleared > 0 {
		prometheusUtxoDAHClearedUnmined.Add(float64(cleared))
		s.logger.Infof("[pruner] cleared deleteAtHeight of %d unmined records (%d masters, %d pagination records)", cleared, masters, pages)
	}

	return masters + pages
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
					// An outputs-only blob is written for zero-input transactions
					// alone (create.go guards both write sites on
					// len(tx.Inputs) == 0), so this record has no parents that
					// could need a replay marker. Deleting it unmarked is safe,
					// which is why this branch and the one below take opposite
					// decisions on a similar-looking missing-inputs situation.
					return nil, nil
				}

				// Both blobs are gone, so the inputs are unrecoverable and no
				// parent can be marked. The caller retains the record instead:
				// its own presence is the replay protection the markers would
				// have provided.
				return nil, errors.NewProcessingError("missing external tx %s at height %d: cannot safely prune without input references", txHash.String(), blockHeight)
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
			// Comma-ok, not a bare assertion: this runs inside an errgroup
			// goroutine (chunkGroup.Go -> processRecordChunkCarry), which does not
			// recover, so a bin holding a non-[]byte element would kill the
			// pruner process instead of retaining the one record.
			inputBytes, ok := inputInterface.([]byte)
			if !ok {
				return nil, errors.NewProcessingError("input %d has unexpected type %T (expected []byte) at height %d", i, inputInterface, blockHeight)
			}

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

// addParentUpdate accumulates replay protection for a surviving parent record.
// It is the single producer of parentUpdateInfo values: processRecordChunkCarry and
// ProcessSingleRecord both go through it, so the two cannot disagree about which
// records get a marker.
func (s *Service) addParentUpdate(updates map[string]*parentUpdateInfo, source []byte, childHash *chainhash.Hash, offset uint32) error {
	if existing, ok := updates[string(source)]; ok {
		existing.add(childHash, offset)

		return nil
	}

	key, err := aerospike.NewKey(s.namespace, s.set, source)
	if err != nil {
		return err
	}

	info := &parentUpdateInfo{key: key, seen: make(map[chainhash.Hash]struct{}, 1)}
	info.add(childHash, offset)
	updates[string(source)] = info

	return nil
}

// addParentUpdatesForInput queues the one marker write a spent outpoint needs:
// on the output PAGE that holds this vout, which is the record the spend path
// reads (uaerospike.CalculateKeySource, the same key Lua's spend targets).
//
// No copy goes on the master record. When the page differs from the master
// (vout >= utxoBatchSize) nothing reads a master entry in either defensive mode:
// the Lua spend reads the page it targets, and the defensive scan matches
// deletedChildren against the scanned record's own utxos bin, which on the master
// holds outputs 0..utxoBatchSize-1 only (splitIntoBatches), so a child of a
// higher output is never named there. What the copy did do was accumulate ~70
// bytes per pruned child on a record that never shrinks; a 16k-output parent
// crossed write-block-size at roughly 15k children and every later marker write
// to it failed with RECORD_TOO_BIG. Page-only marking bounds each record to one
// entry per output it actually holds.
//
// Queueing is not marking. The offset within the page is kept with the child so
// keepSpendHolders can drop the marker when that output does not record this
// child as its spender, which is what a conflicting loser looks like.
func (s *Service) addParentUpdatesForInput(updates map[string]*parentUpdateInfo, parentTxID *chainhash.Hash, vout uint32, childHash *chainhash.Hash) error {
	if s.utxoBatchSize <= 0 {
		return errors.NewProcessingError("utxo batch size %d cannot address output %d of %s", s.utxoBatchSize, vout, parentTxID.String())
	}

	return s.addParentUpdate(updates, uaerospike.CalculateKeySource(parentTxID, vout, s.utxoBatchSize), childHash, vout%uint32(s.utxoBatchSize))
}

// spendHolderVerdict is what one parent record says about one queued child.
type spendHolderVerdict int

const (
	// holderNamesChild: an output the child's input names records the child as
	// its spender. The child is marked on this record.
	holderNamesChild spendHolderVerdict = iota
	// holderNamesOther: every output the child's input names is spent by a
	// well-formed element naming a different transaction. That is what a
	// conflicting loser looks like, and it needs no marker here.
	holderNamesOther
	// holderUnverified: at least one named output is unspent, missing,
	// malformed or frozen, and none names the child. Nothing can be said about
	// the child's claim to it, so the child is held back rather than deleted
	// unmarked.
	holderUnverified
)

// markingPlan is the outcome of keepSpendHolders for one batch of parent records.
type markingPlan struct {
	// blocked holds every child that must not be deleted this cycle.
	blocked map[chainhash.Hash]struct{}
	// superseded counts, per child, the parent records dropped because their
	// output names a different spender.
	superseded map[chainhash.Hash]int
	// existing is the number of queued parent records that were present.
	existing int
}

func (p *markingPlan) block(childHash chainhash.Hash) {
	if p.blocked == nil {
		p.blocked = make(map[chainhash.Hash]struct{})
	}

	p.blocked[childHash] = struct{}{}
}

// keepSpendHolders reads every queued parent record and keeps, per record, only
// the children that record's outputs actually name as their spender. The
// returned plan lists the children that must be held back rather than deleted
// unmarked.
//
// The spend path consults a marker for the transaction ASKING to spend and
// ahead of every other answer, and nothing in the spend path ever clears a
// marker. A marker for a child that never held the output is therefore not
// inert: a conflicting loser names the winner's outpoint in its inputs, is a
// pruning candidate like any other tombstoned record, and once the winner's
// spend is released (a reorg, ReverseProcessConflicting) the loser's genuinely
// fresh spend would be refused forever. The SQL pruner applies the same rule in
// its marker INSERT, which joins to the parent output and requires its spending
// data to name the child.
//
// Only a well-formed element naming a different spender drops a child without
// a marker. Everything else that fails to name the child holds it back: an
// unspent output (which is what a rolled-back spend leaves, the bare 32-byte
// utxo hash), an offset past the page, an element that is not bytes, and a
// record whose utxos bin is absent or not a list. A child deleted in any of
// those states would carry no marker, and a replay of it would be recreated and
// spend cleanly. Held-back children are counted, so a record that stays held
// back is visible rather than silently retained.
//
// A conflicting loser is the exception: ProcessConflicting released every
// input it spent, the uncontested ones too, so it is never held back for an
// output that does not name it, and a replay of it meets the winner's spend and
// is stored as conflicting again. The SQL pruner exempts conflicting rows the
// same way.
//
// A parent record that is gone needs no marker and is dropped. Any other read
// failure, including a record the batch never answered, holds back that record's
// children. A transport failure fails the call, because nothing is known about
// any record.
func (s *Service) keepSpendHolders(ctx context.Context, updates map[string]*parentUpdateInfo) (*markingPlan, error) {
	plan := &markingPlan{}

	if len(updates) == 0 {
		return plan, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	sources := make([]string, 0, len(updates))
	records := make([]aerospike.BatchRecordIfc, 0, len(updates))
	readPolicy := aerospike.NewBatchReadPolicy()

	for source, info := range updates {
		sources = append(sources, source)
		records = append(records, aerospike.NewBatchRead(readPolicy, info.key, []string{s.fieldUtxos}))
	}

	if err := s.batchOperate(records); err != nil && isBatchTransportError(err) {
		return nil, errors.NewStorageError("batch read of parent records before marking failed", err)
	}

	notFound := 0
	unverified := 0
	supersededCount := 0

	for i, rec := range records {
		source := sources[i]
		info := updates[source]
		batchRec := rec.BatchRec()

		if batchRec.Err != nil && batchRec.Err.Matches(types.KEY_NOT_FOUND_ERROR) {
			notFound++

			delete(updates, source)

			continue
		}

		// A record that failed to read is not known to be present, so it is
		// not counted as existing: its children are held back instead.
		if batchRec.Err != nil || batchRec.ResultCode != types.OK || batchRec.Record == nil {
			for _, childHash := range info.childHashes {
				plan.block(*childHash)
			}

			delete(updates, source)

			continue
		}

		plan.existing++

		utxos, isList := batchRec.Record.Bins[s.fieldUtxos].([]interface{})
		holders := make([]*chainhash.Hash, 0, len(info.childHashes))

		for _, childHash := range info.childHashes {
			verdict := holderUnverified
			if isList {
				verdict = spendHolder(utxos, info.offsets[*childHash], childHash)
			}

			_, released := info.released[*childHash]

			switch {
			case verdict == holderNamesChild:
				holders = append(holders, childHash)
			case verdict == holderNamesOther:
				supersededCount++

				if plan.superseded == nil {
					plan.superseded = make(map[chainhash.Hash]int)
				}

				plan.superseded[*childHash]++
			case released:
				// A conflicting loser: the output was released, not rolled back
				// from under a confirmed spend, and a replay meets the winner.
			default:
				unverified++

				plan.block(*childHash)
			}
		}

		if len(holders) == 0 {
			delete(updates, source)

			continue
		}

		info.childHashes = holders
	}

	if notFound > 0 && prometheusUtxoParentsUpdatedSkipped != nil {
		prometheusUtxoParentsUpdatedSkipped.Add(float64(notFound))
	}

	if unverified > 0 && prometheusUtxoChildrenHeldUnverified != nil {
		prometheusUtxoChildrenHeldUnverified.Add(float64(unverified))
	}

	if supersededCount > 0 && prometheusUtxoMarkersSuperseded != nil {
		prometheusUtxoMarkersSuperseded.Add(float64(supersededCount))
	}

	return plan, nil
}

// spendHolder reports what the given utxos-bin offsets say about childHash.
// Each spent element is the 32-byte utxo hash followed by 36 bytes of spending
// data whose first 32 bytes are the spender's txid (see extractSpendingChildren).
func spendHolder(utxos []interface{}, offsets []uint32, childHash *chainhash.Hash) spendHolderVerdict {
	if len(offsets) == 0 {
		return holderUnverified
	}

	namesOther := true

	for _, offset := range offsets {
		if int(offset) >= len(utxos) {
			namesOther = false

			continue
		}

		utxoBytes, ok := utxos[offset].([]byte)
		if !ok || len(utxoBytes) < 68 {
			namesOther = false

			continue
		}

		if bytes.Equal(utxoBytes[32:64], childHash[:]) {
			return holderNamesChild
		}

		// A frozen output carries the freeze sentinel (all 0xFF) where the
		// spending data goes. That names no transaction, so it is not the
		// well-formed "spent by someone else" a conflicting loser leaves, and
		// reading it as one deleted the child with no replay protection. It is
		// unverified: the child is held back until the output says something.
		if bytes.Equal(utxoBytes[32:68], subtree.FrozenBytes[:]) {
			namesOther = false
		}
	}

	if namesOther {
		return holderNamesOther
	}

	return holderUnverified
}

// deleteCarry holds the deletes a chunk has cleared, waiting for the batch that
// carries the next chunk's marker writes.
//
// Take child C in chunk 1, spending parents P and Q. Chunk 1's batch writes C's
// markers on P and Q. Only once both answer is C cleared, and its delete rides
// chunk 2's batch, next to chunk 2's own marker writes. So a delete never
// shares a batch with the marker writes it depends on, which is the ordering
// that keeps a pruned child from being deleted unprotected, and each chunk
// still costs one batch, as it did when markers and deletes shared one. The
// deletes left when a worker runs out of chunks go in one more batch
// (drainCarry).
//
// A master's pages wait the same way: they ride the batch after the one that
// deleted the master, so a refused master keeps its pages.
//
// One carry belongs to one partition worker. Its mutex serialises that
// worker's flushes, which run one at a time anyway at the default chunk group
// limit of 1.
type deleteCarry struct {
	mu sync.Mutex
	// deletions are cleared children, deleted by the next batch.
	deletions []*pendingDeletion
	// files are the external blobs of those children, deleted once their
	// record delete is known to have landed.
	files []*externalFileInfo
	// updates are the marker sets those children were cleared against, kept to
	// withdraw their markers if the delete is refused.
	updates []map[string]*parentUpdateInfo
	// pages are the pagination records of masters a batch deleted.
	pages []*aerospike.Key
}

func (c *deleteCarry) empty() bool {
	return len(c.deletions) == 0 && len(c.pages) == 0
}

// drainCarry sends the carry's remaining deletes, and then the pages of the
// masters those deletes removed, until nothing is left. It returns the
// children confirmed deleted and those held back.
func (s *Service) drainCarry(ctx context.Context, carry *deleteCarry) (deleted, heldBack int, err error) {
	for !carry.empty() {
		d, h, flushErr := s.flushChunk(ctx, carry, nil, nil, nil)
		deleted += d
		heldBack += h

		if flushErr != nil {
			return deleted, heldBack, flushErr
		}
	}

	return deleted, heldBack, nil
}

// drainCarryDetached drains under a context that ignores ctx's cancellation,
// bounded by drainTimeout. A partition worker drains this way whenever it
// stops: the carried children are cleared, their markers have landed, and a
// child left present behind its own markers would have its re-spend refused
// as a pruned replay.
func (s *Service) drainCarryDetached(ctx context.Context, carry *deleteCarry) (deleted, heldBack int, err error) {
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
	defer cancel()

	return s.drainCarry(drainCtx, carry)
}

// drainTimeout bounds the drain of a partition worker.
const drainTimeout = 30 * time.Second

// flushChunk sends one batch: the marker writes for this chunk's queued
// parents, and the deletes the carry holds from the previous batch's answers.
// It then puts this chunk's cleared children in the carry for the next batch.
// It returns the children whose delete this batch confirmed, and the children
// held back: this chunk's that a marker could not protect, and the carry's
// whose delete was refused.
//
// A marker write on a record this same batch deletes is not sent. A parent
// that is also a pruned child, cleared by the previous batch, is deleted here,
// and the rows of one batch run in no fixed order; the delete's own answer
// says whether the parent is gone, which is all the marker would have
// protected against. A queued child whose parent's delete did not definitely
// land is held back. In TTL mode the write is sent anyway: the "delete" there
// is a touch that leaves the parent readable for up to a second, and any
// write in that second gives it a full lifetime again.
//
// Aerospike batches are not atomic across records, so a failed marker write
// must leave the affected children in place for a later cycle, regardless of
// defensive mode. Per-record marker failures are isolated: they hold back the
// children of that one parent and are returned in heldBack. Only a whole-batch
// failure, the BatchOperate call itself or a cancelled context, is returned as
// an error, because that says nothing about any individual record. One
// permanently unwritable parent returned as a chunk error would re-fail every
// block forever: PruneWithPartitions never retries a non-TimeoutError, and
// services/pruner/worker.go logs the failure and advances lastProcessedHeight
// anyway, so NotifyPruneComplete would never fire again.
func (s *Service) flushChunk(ctx context.Context, carry *deleteCarry, parentUpdates map[string]*parentUpdateInfo, deletions []*pendingDeletion, externalFiles []*externalFileInfo) (deleted, heldBack int, err error) {
	// SkipDeletions is checked before any marker is written, not only before the
	// deletions. The spend path consults a marker for the transaction asking to
	// spend, so a marker for a child that is then deliberately NOT deleted turns
	// that live, mined child's own idempotent re-spend into a pruned-replay
	// rejection, and the block paths would delete it as a ghost.
	//
	// It gates the external blob deletions as well as the record deletions. The
	// pruner is the only production deleter of those blobs (the external store is
	// built with WithDisableDAH(true) and the blobs carry no DAH), so deleting the
	// blob while the record is deliberately retained leaves a record whose
	// transaction can never be read back.
	if s.settings.Pruner.SkipDeletions {
		return len(deletions), 0, nil
	}

	carry.mu.Lock()
	defer carry.mu.Unlock()

	prev := &deleteCarry{deletions: carry.deletions, files: carry.files, updates: carry.updates, pages: carry.pages}
	carry.deletions, carry.files, carry.updates, carry.pages = nil, nil, nil, nil

	deleteRecords, masters, deleteSlots := s.buildCarriedDeletes(prev)

	// In TTL mode a carried "delete" is a one-second expiry, and the parent can
	// be read, and written back to a full lifetime, until it lapses, so its
	// marker write is always sent.
	writeUpdates, tied := parentUpdates, map[string]*parentUpdateInfo(nil)
	if !s.utxoSetTTL {
		writeUpdates, tied = splitTiedUpdates(parentUpdates, deleteSlots)
	}

	plan, sent, markErr := s.markParentsWith(ctx, writeUpdates, parentUpdates, deleteRecords)
	if markErr != nil && !sent {
		// Nothing went out, so the carried deletes are still owed.
		carry.deletions, carry.files, carry.updates, carry.pages = prev.deletions, prev.files, prev.updates, prev.pages

		return 0, 0, markErr
	}

	results := classifyDeletionResults(deleteRecords, masters)

	deleted, heldBack, deleteErr := s.settleCarriedDeletes(ctx, carry, prev, deleteRecords, masters, results)

	if markErr != nil {
		return deleted, heldBack, markErr
	}

	blocked := plan.blocked

	// A child queued on a parent this batch deleted is protected only if the
	// delete landed. Otherwise the parent may still be there, unmarked.
	if len(tied) > 0 {
		tiedBlocked := make(map[chainhash.Hash]struct{})

		for _, info := range tied {
			if slot, ok := deleteSlots[string(info.key.Digest())]; ok && deleteDone(deleteRecords[slot].BatchRec()) {
				continue
			}

			for _, childHash := range info.childHashes {
				tiedBlocked[*childHash] = struct{}{}
			}
		}

		if len(tiedBlocked) > 0 {
			s.withdrawMarkers(parentUpdates, tiedBlocked)
			blocked = unionChildren(blocked, tiedBlocked)
		}
	}

	cleared := make([]*pendingDeletion, 0, len(deletions))

	for _, deletion := range deletions {
		if _, held := blocked[*deletion.txHash]; held {
			heldBack++

			continue
		}

		cleared = append(cleared, deletion)
	}

	cleared, pagesUnread := s.dropPagesOfPresentMasters(cleared)
	heldBack += pagesUnread

	clearedSet := make(map[chainhash.Hash]struct{}, len(cleared))
	for _, deletion := range cleared {
		clearedSet[*deletion.txHash] = struct{}{}
	}

	// A blob goes only with its record: never for a held-back child, and only
	// once the batch that deletes the record has answered.
	for _, file := range externalFiles {
		if _, ok := clearedSet[*file.txHash]; ok {
			carry.files = append(carry.files, file)
		}
	}

	if len(cleared) > 0 {
		carry.deletions = append(carry.deletions, cleared...)
		carry.updates = append(carry.updates, parentUpdates)
	}

	return deleted, heldBack, deleteErr
}

// buildCarriedDeletes builds the delete records for a carry: each child's
// master, or every key of a pagination record scanned on its own, and the
// pages of masters an earlier batch deleted. masters maps the batch index of
// each child's master record to the child, as classifyDeletionResults expects,
// and slots maps each deleted key's digest to its batch index.
func (s *Service) buildCarriedDeletes(prev *deleteCarry) ([]aerospike.BatchRecordIfc, map[int]*chainhash.Hash, map[string]int) {
	if prev.empty() {
		return nil, nil, nil
	}

	keys := make([]*aerospike.Key, 0, len(prev.deletions)+len(prev.pages))
	masters := make(map[int]*chainhash.Hash, len(prev.deletions))

	for _, deletion := range prev.deletions {
		if len(deletion.keys) == 0 {
			continue
		}

		if !deletion.page {
			masters[len(keys)] = deletion.txHash
			keys = append(keys, deletion.keys[0])

			continue
		}

		keys = append(keys, deletion.keys...)
	}

	keys = append(keys, prev.pages...)

	slots := make(map[string]int, len(keys))
	for i, key := range keys {
		slots[string(key.Digest())] = i
	}

	return buildDeletionBatchRecords(keys, s.utxoSetTTL, s.removalCommitLevel), masters, slots
}

// splitTiedUpdates separates the queued marker writes on records the same
// batch deletes from the rest. Both share their *parentUpdateInfo values with
// updates.
func splitTiedUpdates(updates map[string]*parentUpdateInfo, deleteSlots map[string]int) (write, tied map[string]*parentUpdateInfo) {
	if len(deleteSlots) == 0 {
		return updates, nil
	}

	write = make(map[string]*parentUpdateInfo, len(updates))

	for source, info := range updates {
		if _, deleting := deleteSlots[string(info.key.Digest())]; deleting {
			if tied == nil {
				tied = make(map[string]*parentUpdateInfo)
			}

			tied[source] = info

			continue
		}

		write[source] = info
	}

	return write, tied
}

// settleCarriedDeletes acts on the answers to a carry's deletes. A child whose
// master delete was refused is still present behind its markers, so they are
// withdrawn; a child whose delete went unanswered keeps them, because
// withdrawing after a delete that did land would leave the unmarked deletion
// this package exists to prevent, and the next cycle re-finds it if it is
// still there. A confirmed master's pages go in the carry for the next batch,
// and its blob is deleted now. Every record whose delete did not definitely
// land is counted in the returned error.
func (s *Service) settleCarriedDeletes(ctx context.Context, carry *deleteCarry, prev *deleteCarry, records []aerospike.BatchRecordIfc, masters map[int]*chainhash.Hash, results deletionResults) (deleted, heldBack int, err error) {
	if len(records) == 0 {
		return 0, 0, nil
	}

	if len(results.survivors) > 0 {
		// Each chunk's set is keyed by parent record, and two chunks can queue
		// the same parent, so the sets are merged under a per-chunk prefix.
		merged := make(map[string]*parentUpdateInfo)

		for i, updates := range prev.updates {
			for source, info := range updates {
				merged[strconv.Itoa(i)+":"+source] = info
			}
		}

		s.withdrawMarkers(merged, results.survivors)
	}

	heldBack = len(results.survivors)

	for _, deletion := range prev.deletions {
		if _, survived := results.survivors[*deletion.txHash]; survived {
			continue
		}

		if _, unknown := results.unanswered[*deletion.txHash]; unknown {
			continue
		}

		if deletion.page {
			if !pageKeysDone(records, deletion, masters) {
				continue
			}
		} else if len(deletion.keys) > 1 {
			carry.pages = append(carry.pages, deletion.keys[1:]...)
		}

		deleted++
	}

	remaining := make([]*externalFileInfo, 0, len(prev.files))

	for _, file := range prev.files {
		if _, survived := results.survivors[*file.txHash]; survived {
			continue
		}

		if _, unknown := results.unanswered[*file.txHash]; unknown {
			continue
		}

		remaining = append(remaining, file)
	}

	for _, rec := range results.failedRecords {
		s.logger.Errorf("Deletion error for key %v: result code %v, err %v", rec.Key, rec.ResultCode, rec.Err)
	}

	// remaining holds only the blobs of children whose master record is
	// definitely gone, so they are deleted even when another record in the
	// batch failed. No later scan finds a deleted record again, and the
	// external store keeps no delete-at-height for these blobs, so a blob
	// skipped here would be kept for good.
	var blobErr error
	if len(remaining) > 0 {
		blobErr = s.executeBatchExternalFileDeletions(ctx, remaining)
	}

	if len(results.failedRecords) > 0 {
		// a failed batch fails the cycle; the next cycle re-finds the records
		// that are still present
		if blobErr != nil {
			return deleted, heldBack, errors.NewStorageError("%d deletion operations failed", len(results.failedRecords), blobErr)
		}

		return deleted, heldBack, errors.NewStorageError("%d deletion operations failed", len(results.failedRecords))
	}

	if blobErr != nil {
		return deleted, heldBack, blobErr
	}

	return deleted, heldBack, nil
}

// pageKeysDone reports whether every key of a pagination record scanned on its
// own was deleted. Its keys are not masters, so classifyDeletionResults does
// not track it; the answers are found by key.
func pageKeysDone(records []aerospike.BatchRecordIfc, deletion *pendingDeletion, masters map[int]*chainhash.Hash) bool {
	want := make(map[string]struct{}, len(deletion.keys))
	for _, key := range deletion.keys {
		want[string(key.Digest())] = struct{}{}
	}

	for i, rec := range records {
		if _, isMaster := masters[i]; isMaster {
			continue
		}

		batchRec := rec.BatchRec()
		if _, ok := want[string(batchRec.Key.Digest())]; ok && !deleteDone(batchRec) {
			return false
		}
	}

	return true
}

// dropPagesOfPresentMasters removes from deletions every pagination record
// scanned on its own whose master record is not known to be gone. It returns how
// many of those it kept back because the master could not be read. A page whose
// master is present is counted nowhere, neither deleted nor skipped: the
// master's own deletion or hold-back counts once for the whole transaction,
// whichever chunk the master is in, so the pruner's deleted and skipped counts
// leave out the pages found on their own this way.
//
// The hold-back in flushChunk is keyed on the transaction, but it is
// filled per chunk from the master's inputs, and only the master carries them.
// A page scanned in a different chunk from its master finds nothing blocked, so
// without this a page was deleted while its master was deliberately kept for a
// failed marker write or a refused delete. A replay then rewrote the page with
// unspent outputs while the master answered ErrTxExists, and the historical
// spends on that page were lost.
//
// A present master needs none of its pages deleted here: the master's own
// deletion lists every page key, whichever chunk it is scanned in, and the
// batch after the master's delete removes them once the master is gone. So a
// page goes on its own only once its master is definitely gone, which is the
// leftover of an earlier cycle whose page deletes failed. A master read that
// fails keeps the page, because the master is not known to be gone.
func (s *Service) dropPagesOfPresentMasters(deletions []*pendingDeletion) ([]*pendingDeletion, int) {
	pages := make([]int, 0)
	records := make([]aerospike.BatchRecordIfc, 0)
	readPolicy := aerospike.NewBatchReadPolicy()

	keep := make([]bool, len(deletions))
	unread := 0

	for i, deletion := range deletions {
		keep[i] = true

		if !deletion.page {
			continue
		}

		masterKey, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(deletion.txHash, 0))
		if err != nil {
			keep[i] = false
			unread++

			continue
		}

		pages = append(pages, i)
		records = append(records, aerospike.NewBatchReadHeader(readPolicy, masterKey))
	}

	if len(records) > 0 {
		if err := s.batchOperate(records); err != nil && isBatchTransportError(err) {
			s.logger.Warnf("Pruner kept %d pagination records this cycle: reading their master records failed: %v", len(records), err)
		}

		for j, rec := range records {
			batchRec := rec.BatchRec()
			if batchRec.Err != nil && batchRec.Err.Matches(types.KEY_NOT_FOUND_ERROR) {
				continue
			}

			keep[pages[j]] = false

			if batchRec.Err != nil || batchRec.ResultCode != types.OK {
				unread++
			}
		}
	}

	kept := make([]*pendingDeletion, 0, len(deletions))

	for i, deletion := range deletions {
		if keep[i] {
			kept = append(kept, deletion)
		}
	}

	return kept, unread
}

// markParents writes the replay markers for the queued parent records, keeping
// only the children each record really names as a spender, and returns every
// child that must not be deleted this cycle: those a parent could not vouch
// for, and those whose marker did not land on every parent.
//
// Take a child C that spends output 3 of parent P. The marker write on P
// carries a filter: it writes deletedChildren[C] only if utxos[3] records C as
// its spender. Checking and writing happen in one record operation on the
// server, so no read of P comes first, and the server never writes a marker for
// a child the output does not name. A record with several children writes all
// of them or none: one child whose output does not name it refuses the whole
// record, which then answers FILTERED_OUT.
//
// Only refused records are read, with keepSpendHolders restricted to them, to
// find which child refused and why; their holders are then written again under
// the same filter. A record with a conflicting loser among its children takes
// that read before its write instead, because a loser whose output names
// someone else is deleted unmarked rather than held back, and the filter has no
// way to say so. Both are rare: a healthy cycle sends one batch here.
//
// Markers are all-or-nothing per child. A marker on a child that then stays in
// place is not inert: the spend path answers it ahead of every other check, so
// a later re-validation of the block holding that live child would be refused
// as a pruned replay, and the block paths would not compensate because the
// record was not created by that attempt. So a child held back by a refused
// record, or whose write failed on one parent, has its markers withdrawn from
// the others (withdrawMarkers).
func (s *Service) markParents(ctx context.Context, parentUpdates map[string]*parentUpdateInfo) (*markingPlan, error) {
	plan, _, err := s.markParentsWith(ctx, parentUpdates, parentUpdates, nil)

	return plan, err
}

// markParentsWith is markParents with two additions. extra records are sent in
// the same batch as the first marker write and left for the caller to read;
// sent reports whether that batch went out. all is the full set of records
// queued for the chunk, of which parentUpdates is the part to write: markers
// are withdrawn from every record in all, because a child can also be queued
// on a record whose write the caller left out.
func (s *Service) markParentsWith(ctx context.Context, parentUpdates, all map[string]*parentUpdateInfo, extra []aerospike.BatchRecordIfc) (plan *markingPlan, sent bool, err error) {
	plan = &markingPlan{}

	if len(parentUpdates) == 0 && len(extra) == 0 {
		return plan, false, nil
	}

	if withLosers := recordsWithReleased(parentUpdates); len(withLosers) > 0 {
		if err := s.verifyBeforeWrite(ctx, parentUpdates, withLosers, plan); err != nil {
			return nil, false, err
		}

		stripChildren(parentUpdates, plan.blocked)
	}

	refused, failed, sent, err := s.writeVerifiedMarkers(ctx, parentUpdates, plan, extra)
	if err != nil {
		// A context error is returned only by the check made before the batch
		// is sent, so no marker was written and there is nothing to withdraw.
		// Withdrawing anyway on every mid-cycle shutdown would send removes to
		// parents that never had the bin and count each one as a failure.
		if !sent {
			return nil, false, err
		}

		// Nothing is known about which records took the write, so withdraw
		// every child's marker. Best effort: a withdrawal that fails leaves a
		// marker on a child that is still present, and the next cycle, which
		// reconsiders that child, withdraws it again.
		s.withdrawMarkers(all, allChildren(all))

		return nil, true, err
	}

	withdraw := failed

	if len(refused) > 0 {
		if err := s.verifyBeforeWrite(ctx, parentUpdates, refused, plan); err != nil {
			s.withdrawMarkers(all, allChildren(all))

			return nil, true, err
		}

		stripChildren(refused, plan.blocked)

		// The read saw the holders an instant after the server refused the
		// write, so a record refused again has changed under the pruner; its
		// children are held back for the next cycle.
		again, failedAgain, _, err := s.writeVerifiedMarkers(ctx, refused, plan, nil)
		if err != nil {
			s.withdrawMarkers(all, allChildren(all))

			return nil, true, err
		}

		for _, info := range again {
			for _, childHash := range info.childHashes {
				plan.block(*childHash)
			}
		}

		withdraw = unionChildren(withdraw, failedAgain, plan.blocked)
	}

	if len(withdraw) > 0 {
		s.withdrawMarkers(all, withdraw)

		for childHash := range withdraw {
			plan.block(childHash)
		}
	}

	return plan, true, nil
}

// recordsWithReleased returns the queued records that list a conflicting loser.
func recordsWithReleased(updates map[string]*parentUpdateInfo) map[string]*parentUpdateInfo {
	var out map[string]*parentUpdateInfo

	for source, info := range updates {
		if len(info.released) == 0 {
			continue
		}

		if out == nil {
			out = make(map[string]*parentUpdateInfo)
		}

		out[source] = info
	}

	return out
}

// verifyBeforeWrite reads the given subset of updates with keepSpendHolders,
// folds its verdicts into plan, and drops from updates every record the read
// found gone or left with no holder. keepSpendHolders trims each record's
// children to its holders in place, so updates sees that too.
func (s *Service) verifyBeforeWrite(ctx context.Context, updates, subset map[string]*parentUpdateInfo, plan *markingPlan) error {
	sources := make([]string, 0, len(subset))
	for source := range subset {
		sources = append(sources, source)
	}

	read, err := s.keepSpendHolders(ctx, subset)
	if err != nil {
		return err
	}

	for _, source := range sources {
		if _, kept := subset[source]; !kept {
			delete(updates, source)
		}
	}

	plan.existing += read.existing

	for childHash := range read.blocked {
		plan.block(childHash)
	}

	for childHash, n := range read.superseded {
		if plan.superseded == nil {
			plan.superseded = make(map[chainhash.Hash]int)
		}

		plan.superseded[childHash] += n
	}

	return nil
}

// writeVerifiedMarkers sends one filtered marker write per queued record
// (buildVerifiedMarkerRecords) and classifies the answers. It returns the
// records the filter refused, keyed as in updates, and the children whose
// write failed or could not be read. A record the server wrote counts as
// present in plan.existing; a record that is gone needs no marker.
func (s *Service) writeVerifiedMarkers(ctx context.Context, updates map[string]*parentUpdateInfo, plan *markingPlan, extra []aerospike.BatchRecordIfc) (refusedRecords map[string]*parentUpdateInfo, failedChildren map[chainhash.Hash]struct{}, sent bool, err error) {
	if len(updates) == 0 && len(extra) == 0 {
		return nil, nil, false, nil
	}

	select {
	case <-ctx.Done():
		s.logger.Infof("Context cancelled, skipping parent update batch")
		return nil, nil, false, ctx.Err()
	default:
	}

	markerRecords, sources := s.buildVerifiedMarkerRecords(updates)

	batchRecords := markerRecords
	if len(extra) > 0 {
		batchRecords = make([]aerospike.BatchRecordIfc, 0, len(markerRecords)+len(extra))
		batchRecords = append(batchRecords, markerRecords...)
		batchRecords = append(batchRecords, extra...)
	}

	if err := s.batchOperate(batchRecords); err != nil {
		if isBatchTransportError(err) {
			s.logger.Errorf("Batch parent update failed at the transport: %v", err)

			return nil, nil, true, errors.NewStorageError("batch parent update failed", err)
		}

		// A server-side rejection reached the server, so the per-record results
		// are readable; see flushChunk for why one unwritable parent must not
		// fail the chunk.
		s.logger.Warnf("Batch parent update returned a server-side rejection, classifying per record: %v", err)
	}

	var (
		refused  map[string]*parentUpdateInfo
		tally    parentUpdateTally
		filtered int
	)

	for i, rec := range markerRecords {
		info := updates[sources[i]]
		batchRec := rec.BatchRec()

		// A refusal that is in doubt may follow an attempt the client retried,
		// which may have written the markers before the record changed. It is
		// a failed write, so its children's markers are withdrawn from it too.
		if batchRec.Err != nil && batchRec.Err.Matches(types.FILTERED_OUT) && !batchRec.InDoubt && !batchRec.Err.IsInDoubt() {
			if refused == nil {
				refused = make(map[string]*parentUpdateInfo)
			}

			refused[sources[i]] = info
			filtered++

			continue
		}

		outcome, outcomeErr := classifyParentUpdateResult(batchRec)

		switch outcome {
		case parentUpdateOK:
			tally.success++
			plan.existing++

		case parentUpdateNotFound:
			tally.notFound++

		default:
			tally.failed++

			if tally.firstErr == nil {
				tally.firstErr = outcomeErr
			}

			info.markerFailed = true

			if tally.blocked == nil {
				tally.blocked = make(map[chainhash.Hash]struct{})
			}

			for _, childHash := range info.childHashes {
				tally.blocked[*childHash] = struct{}{}
			}
		}
	}

	if filtered > 0 && prometheusUtxoMarkerWritesRefused != nil {
		prometheusUtxoMarkerWritesRefused.Add(float64(filtered))
	}

	if len(markerRecords) == 0 {
		return nil, nil, true, nil
	}

	return refused, s.reportParentUpdateTally(tally, len(markerRecords)), true, nil
}

// buildVerifiedMarkerRecords builds one UPDATE_ONLY MapPutItems write per
// queued record, adding every queued child to deletedChildren, under a filter
// that holds only when each child is named as the spender by at least one of
// the outputs it claims on that record (markerFilter). It returns the records
// with the update keys they were built from, index-aligned.
func (s *Service) buildVerifiedMarkerRecords(updates map[string]*parentUpdateInfo) ([]aerospike.BatchRecordIfc, []string) {
	mapPolicy := aerospike.DefaultMapPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(updates))
	sources := make([]string, 0, len(updates))

	for source, info := range updates {
		items := make(map[interface{}]interface{}, len(info.childHashes))
		for _, childHash := range info.childHashes {
			items[childHash.String()] = true
		}

		policy := aerospike.NewBatchWritePolicy()
		policy.RecordExistsAction = aerospike.UPDATE_ONLY
		policy.FilterExpression = s.markerFilter(info)
		// A marker never changes a record's lifetime. In TTL mode the parent
		// may be expiring in this same batch, and a write that took the
		// namespace default would give it a full lifetime back after the
		// pruner had already counted it gone and removed its pages and blob.
		policy.Expiration = aerospike.TTLDontUpdate

		op := aerospike.MapPutItemsOp(mapPolicy, s.fieldDeletedChildren, items)
		batchRecords = append(batchRecords, aerospike.NewBatchWrite(policy, info.key, op))
		sources = append(sources, source)
	}

	return batchRecords, sources
}

// markerFilter is true when, for every queued child of the record, at least one
// of the utxos-bin offsets that child claims holds an element whose spending
// data names it: bytes 32 to 64 of the element, the spender's txid, the same
// bytes spendHolder compares.
//
// An element that cannot be read that way, an unspent 32-byte hash or an
// offset past the list, makes the comparison unknown rather than false, and an
// unknown filter refuses the write, as a false one does. So every shape that is
// not a spend by the child refuses, and keepSpendHolders then sorts the refused
// record's children into holders, superseded and unverified.
func (s *Service) markerFilter(info *parentUpdateInfo) *aerospike.Expression {
	perChild := make([]*aerospike.Expression, 0, len(info.childHashes))

	for _, childHash := range info.childHashes {
		offsets := info.offsets[*childHash]
		names := make([]*aerospike.Expression, 0, len(offsets))

		for _, offset := range offsets {
			element := aerospike.ExpListGetByIndex(aerospike.ListReturnTypeValue, aerospike.ExpTypeBLOB,
				aerospike.ExpIntVal(int64(offset)), aerospike.ExpListBin(s.fieldUtxos))
			spender := aerospike.ExpBitGet(aerospike.ExpIntVal(256), aerospike.ExpIntVal(256), element)
			names = append(names, aerospike.ExpEq(spender, aerospike.ExpBlobVal(childHash.CloneBytes())))
		}

		switch len(names) {
		case 0:
			// No claimed offset: nothing can vouch for the child.
			perChild = append(perChild, aerospike.ExpBoolVal(false))
		case 1:
			perChild = append(perChild, names[0])
		default:
			perChild = append(perChild, aerospike.ExpOr(names...))
		}
	}

	switch len(perChild) {
	case 0:
		return aerospike.ExpBoolVal(false)
	case 1:
		return perChild[0]
	default:
		return aerospike.ExpAnd(perChild...)
	}
}

// allChildren returns every child queued on any record.
func allChildren(updates map[string]*parentUpdateInfo) map[chainhash.Hash]struct{} {
	all := make(map[chainhash.Hash]struct{})

	for _, info := range updates {
		for _, childHash := range info.childHashes {
			all[*childHash] = struct{}{}
		}
	}

	return all
}

// unionChildren returns the union of the given child sets, or nil when all are
// empty.
func unionChildren(sets ...map[chainhash.Hash]struct{}) map[chainhash.Hash]struct{} {
	var out map[chainhash.Hash]struct{}

	for _, set := range sets {
		for childHash := range set {
			if out == nil {
				out = make(map[chainhash.Hash]struct{})
			}

			out[childHash] = struct{}{}
		}
	}

	return out
}

// stripChildren removes every blocked child from the queued parent updates, and
// drops a parent entry that is left with no child.
func stripChildren(updates map[string]*parentUpdateInfo, blocked map[chainhash.Hash]struct{}) {
	if len(blocked) == 0 {
		return
	}

	for source, info := range updates {
		kept := info.childHashes[:0]

		for _, childHash := range info.childHashes {
			if _, held := blocked[*childHash]; !held {
				kept = append(kept, childHash)
			}
		}

		if len(kept) == 0 {
			delete(updates, source)

			continue
		}

		info.childHashes = kept
	}
}

// withdrawMarkers removes the markers of the given children from every queued
// parent that lists them. It is the compensation for a marker set that landed
// on some parents of a child and not on others: that child is held back, so it
// stays present, and a marker naming a present transaction would refuse its
// own re-spend.
//
// It uses a plain map remove rather than the Lua module, so it works the same
// on every marking path, and UPDATE_ONLY so it never creates a record. It does
// not take the cycle's context: it runs precisely when that context may have
// failed the write, and a cancelled compensation leaves the poison in place. A
// parent that is gone has lost its markers with it. Any other failure is
// counted and logged, never returned: the child is already held back, and the
// next cycle reconsiders it and withdraws again.
func (s *Service) withdrawMarkers(updates map[string]*parentUpdateInfo, children map[chainhash.Hash]struct{}) {
	if len(children) == 0 {
		return
	}

	policy := aerospike.NewBatchWritePolicy()
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	// Like the marker write, a withdrawal never changes a record's lifetime,
	// so it cannot revive a parent that is expiring in TTL mode.
	policy.Expiration = aerospike.TTLDontUpdate

	records := make([]aerospike.BatchRecordIfc, 0, len(updates))
	ownWriteFailed := make([]bool, 0, len(updates))

	for _, info := range updates {
		keys := make([]interface{}, 0, len(info.childHashes))

		for _, childHash := range info.childHashes {
			if _, withdraw := children[*childHash]; withdraw {
				keys = append(keys, childHash.String())
			}
		}

		if len(keys) == 0 {
			continue
		}

		op := aerospike.MapRemoveByKeyListOp(s.fieldDeletedChildren, keys, aerospike.MapReturnType.NONE)
		records = append(records, aerospike.NewBatchWrite(policy, info.key, op))
		ownWriteFailed = append(ownWriteFailed, info.markerFailed)
	}

	if len(records) == 0 {
		return
	}

	if err := s.batchOperate(records); err != nil && isBatchTransportError(err) {
		if prometheusUtxoMarkerWithdrawFailures != nil {
			prometheusUtxoMarkerWithdrawFailures.Add(float64(len(records)))
		}

		s.logger.Errorf("Withdrawing replay markers of %d held-back children from %d parents failed at the transport; the next cycle withdraws again: %v",
			len(children), len(records), err)

		return
	}

	failures := 0

	var firstErr error

	for i, rec := range records {
		batchRec := rec.BatchRec()
		if batchRec.Err == nil && batchRec.ResultCode == types.OK {
			continue
		}

		if batchRec.Err != nil && batchRec.Err.Matches(types.KEY_NOT_FOUND_ERROR) {
			continue
		}

		// A parent whose own marker write failed usually cannot take this write
		// either (a bin that is not a map, a record too big), and its marker did
		// not land there, so there is nothing to withdraw.
		if ownWriteFailed[i] {
			continue
		}

		failures++

		if firstErr == nil {
			firstErr = batchRec.Err
		}
	}

	if failures > 0 {
		if prometheusUtxoMarkerWithdrawFailures != nil {
			prometheusUtxoMarkerWithdrawFailures.Add(float64(failures))
		}

		s.logger.Errorf("Withdrawing replay markers of held-back children failed on %d of %d parents; the next cycle withdraws again (first: %v)",
			failures, len(records), firstErr)
	}
}

// reportParentUpdateTally records the batch's counters and returns the children
// that must not be deleted. A per-record failure is logged and isolated, never
// returned as an error: see flushChunk for why one poison record must not fail
// the cycle.
func (s *Service) reportParentUpdateTally(tally parentUpdateTally, total int) map[chainhash.Hash]struct{} {
	if tally.failed > 0 {
		prometheusUtxoParentUpdateFailures.Add(float64(tally.failed))

		s.logger.Errorf("Batch parent update: %d of %d parent updates failed, holding back %d child records for a later cycle (first: %v)",
			tally.failed, total, len(tally.blocked), tally.firstErr)
	}

	if tally.success > 0 {
		prometheusUtxoParentsUpdated.Add(float64(tally.success))
	}

	if tally.notFound > 0 {
		prometheusUtxoParentsUpdatedSkipped.Add(float64(tally.notFound))
	}

	return tally.blocked
}

// isBatchTransportError reports whether a BatchOperate error means the call did
// not reach the server, so nothing can be said about any individual record and
// the whole chunk has to be retried. Anything else - a UDF failure, a
// RECORD_TOO_BIG, a rejected bin type - reached the server and left readable
// per-record results, which the caller isolates instead of failing the cycle.
//
// The result-code set is the same one partitionWorker already treats as
// retryable when a scan record carries it.
func isBatchTransportError(err error) bool {
	var asErr aerospike.Error
	if !errors.As(err, &asErr) {
		// Not an aerospike error at all: nothing to classify per record, so be
		// conservative and treat it as fatal to the chunk.
		return true
	}

	return asErr.Matches(types.TIMEOUT, types.NETWORK_ERROR, types.NO_RESPONSE, types.SERVER_NOT_AVAILABLE)
}

// parentUpdateTally is the aggregate outcome of a parent-update batch region.
type parentUpdateTally struct {
	success  int
	notFound int
	failed   int
	// firstErr is the first failure encountered, typed as `error` rather than
	// `aerospike.Error` so a synthesised response error can be carried without
	// faking an aerospike.Error.
	firstErr error
	// blocked holds every child transaction whose marker did not land on at
	// least one of its parents. Those children must not be deleted this cycle.
	blocked map[chainhash.Hash]struct{}
}

// isKeyNotFound reports whether a per-record batch error is KEY_NOT_FOUND, the
// routine result for a parent or child the pruner already deleted. The client
// sets these as a bare *AerospikeError with nothing wrapped, which is answered
// from the ResultCode field; Matches, kept for anything else, boxes its variadic
// argument and so allocated once per not-found result on the cleanup hot path.
func isKeyNotFound(err aerospike.Error) bool {
	if bare, ok := err.(*aerospike.AerospikeError); ok { //nolint:errorlint // exact-type fast path; anything wrapped takes Matches below
		if bare == nil {
			return false
		}

		if bare.Unwrap() == nil {
			return bare.ResultCode == types.KEY_NOT_FOUND_ERROR
		}
	}

	return err.Matches(types.KEY_NOT_FOUND_ERROR)
}

// parentUpdateOutcome is how a single addDeletedChildren batch record is tallied.
type parentUpdateOutcome int

const (
	// parentUpdateOK — the parent's deletedChildren map was updated.
	parentUpdateOK parentUpdateOutcome = iota
	// parentUpdateNotFound — the parent record is gone. Never an error for this
	// op: the parent is normally one the pruner itself already deleted.
	parentUpdateNotFound
	// parentUpdateFailed — the update failed, or its outcome could not be read.
	parentUpdateFailed
)

// classifyParentUpdateResult maps one marker-write batch record onto the
// outcome the cleanup counters track.
//
// It fails CLOSED. Any answer the pruner cannot read is reported as a failure
// rather than counted as a silent success, because the caller deletes the child
// records on the strength of this classification: a marker write that silently
// did not happen is exactly the referential-integrity break the markers exist to
// prevent. A missing parent comes back as KEY_NOT_FOUND from the UPDATE_ONLY
// policy and is benign: it is normally one the pruner itself already deleted.
func classifyParentUpdateResult(batchRec *aerospike.BatchRecord) (parentUpdateOutcome, error) {
	if batchRec.Err != nil {
		if isKeyNotFound(batchRec.Err) {
			return parentUpdateNotFound, nil
		}

		return parentUpdateFailed, batchRec.Err
	}

	// A nil Err is not a success on its own. The client stamps Err only from its
	// response-parse path; a record the batch never got an answer for (a
	// node-level MAX_RETRIES_EXCEEDED, MAX_ERROR_RATE, an empty node list) keeps
	// the NO_RESPONSE it was prepared with and a nil Err. Counting that as OK
	// deleted the child with no marker, which is the replay the markers exist to
	// stop.
	if batchRec.ResultCode != types.OK {
		return parentUpdateFailed, errors.NewProcessingError("parent update got no answer for its record (result code %v)", batchRec.ResultCode)
	}

	return parentUpdateOK, nil
}

// isPaginationRecord reports whether a scanned record is one of txHash's
// pagination records rather than its master. A master carries the
// totalExtraRecs bin, or, from a writer that does not set it, is stored under
// the bare txid; a pagination record is neither.
func (s *Service) isPaginationRecord(record *aerospike.Record, txHash *chainhash.Hash) bool {
	if _, isMaster := record.Bins[s.fieldTotalExtraRecs]; isMaster {
		return false
	}

	masterKey, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(txHash, 0))
	if err != nil {
		return false
	}

	return !bytes.Equal(masterKey.Digest(), record.Key.Digest())
}

// deletionResults is what one deletion batch's per-record answers say.
type deletionResults struct {
	// survivors: children whose master delete definitely did not happen.
	survivors map[chainhash.Hash]struct{}
	// unanswered: children whose master delete may or may not have happened.
	unanswered map[chainhash.Hash]struct{}
	// failedRecords: every record, master or pagination, whose delete did not
	// definitely succeed.
	failedRecords []*aerospike.BatchRecord
}

// classifyDeletionResults sorts a deletion batch's per-record answers. A record
// counts as deleted only on an OK answer or KEY_NOT_FOUND. The result code is
// read as well as Err, as classifyParentUpdateResult and keepSpendHolders do:
// a record the server never answered keeps the NO_RESPONSE it was prepared
// with and a nil Err, and reading that as success let the caller delete the
// blob of a child whose record, and its markers, were still in place, leaving
// it unspendable and unprunable while the cycle reported it pruned.
//
// masters maps the batch index of each child's master record to the child.
func classifyDeletionResults(batchRecords []aerospike.BatchRecordIfc, masters map[int]*chainhash.Hash) deletionResults {
	results := deletionResults{
		survivors:  make(map[chainhash.Hash]struct{}),
		unanswered: make(map[chainhash.Hash]struct{}),
	}

	for i, rec := range batchRecords {
		batchRec := rec.BatchRec()

		if deleteDone(batchRec) {
			continue
		}

		results.failedRecords = append(results.failedRecords, batchRec)

		txHash, isMaster := masters[i]
		if !isMaster {
			continue
		}

		if deleteRefused(batchRec) {
			results.survivors[*txHash] = struct{}{}
		} else {
			results.unanswered[*txHash] = struct{}{}
		}
	}

	return results
}

// deleteDone reports whether the record is definitely gone: the server
// answered OK, or answered that the record was not there.
func deleteDone(rec *aerospike.BatchRecord) bool {
	if rec.Err != nil {
		return rec.Err.Matches(types.KEY_NOT_FOUND_ERROR)
	}

	return rec.ResultCode == types.OK || rec.ResultCode == types.KEY_NOT_FOUND_ERROR
}

// deleteRefused reports whether the server definitely did not remove the
// record: it answered with an error other than KEY_NOT_FOUND, and the error is
// not in doubt. A record the server never answered (NO_RESPONSE) or answered
// in doubt may have been removed, so it is not reported.
func deleteRefused(rec *aerospike.BatchRecord) bool {
	if rec.Err == nil {
		return false
	}

	if rec.Err.Matches(types.KEY_NOT_FOUND_ERROR) {
		return false
	}

	if rec.InDoubt || rec.Err.IsInDoubt() {
		return false
	}

	return !rec.Err.Matches(types.NO_RESPONSE)
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

	// Build parent updates map through the same producer processRecordChunkCarry
	// uses, so the manual-cleanup entry point cannot write a different set of
	// markers from the pruner it stands in for.
	parentUpdates := make(map[string]*parentUpdateInfo, len(inputs)) // One parent per input (worst case)
	for _, input := range inputs {
		if err := s.addParentUpdatesForInput(parentUpdates, input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, txHash); err != nil {
			return errors.NewProcessingError("failed to create parent key", err)
		}
	}

	// Execute parent updates synchronously
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	plan, err := s.markParents(ctx, parentUpdates)
	if err != nil {
		return err
	}

	// ProcessSingleRecord is the manual entry point an operator reaches for
	// after an incident, so a per-record marker failure has to surface here
	// rather than being isolated the way the batch pruner isolates it.
	if _, held := plan.blocked[*txHash]; held {
		return errors.NewStorageError("parent replay markers for %s could not be written", txHash.String())
	}

	// "No marker was needed" and "no parent could be marked" must not look the
	// same to the operator. A parent that is gone needs no marker; a parent that
	// is present but whose output names a different spender is left unmarked,
	// and the operator has to know that nothing was written for this
	// transaction.
	if plan.existing > 0 && len(parentUpdates) == 0 {
		return errors.NewProcessingError("no replay marker was written for %s: %d of its parent records are present and %d name a different spender for the outputs it claims",
			txHash.String(), plan.existing, plan.superseded[*txHash])
	}

	return nil
}
