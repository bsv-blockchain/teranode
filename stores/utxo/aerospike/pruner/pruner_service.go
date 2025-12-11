package pruner

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ensure Store implements the Pruner Service interface
var _ pruner.Service = (*Service)(nil)

var IndexName, _ = gocore.Config().Get("pruner_IndexName", "pruner_dah_index")

// Constants for the pruner service
const (
	// DefaultWorkerCount is the default number of worker goroutines
	DefaultWorkerCount = 4

	// DefaultMaxJobsHistory is the default number of jobs to keep in history
	DefaultMaxJobsHistory = 1000
)

var (
	prometheusMetricsInitOnce  sync.Once
	prometheusUtxoCleanupBatch prometheus.Histogram
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

	// WorkerCount is the number of worker goroutines to use
	WorkerCount int

	// MaxJobsHistory is the maximum number of jobs to keep in history
	MaxJobsHistory int

	// GetPersistedHeight returns the last block height processed by block persister
	// Used to coordinate cleanup with block persister progress (can be nil)
	GetPersistedHeight func() uint32
}

// Service manages background jobs for cleaning up records based on block height
type Service struct {
	logger      ulogger.Logger
	settings    *settings.Settings
	client      *uaerospike.Client
	external    blob.Store
	namespace   string
	set         string
	jobManager  *pruner.JobManager
	ctx         context.Context
	indexWaiter IndexWaiter

	// internally reused variables
	queryPolicy      *aerospike.QueryPolicy
	writePolicy      *aerospike.WritePolicy
	batchWritePolicy *aerospike.BatchWritePolicy
	batchPolicy      *aerospike.BatchPolicy

	// getPersistedHeight returns the last block height processed by block persister
	// Used to coordinate cleanup with block persister progress (can be nil)
	getPersistedHeight func() uint32

	// maxConcurrentOperations limits concurrent operations during cleanup processing
	// Auto-detected from Aerospike client connection queue size
	maxConcurrentOperations int
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

	// Determine max concurrent operations:
	// - Use connection queue size as the upper bound (to prevent connection exhaustion)
	// - If setting is configured (non-zero), use the minimum of setting and connection queue size
	// - If setting is 0 or unset, use connection queue size
	connectionQueueSize := opts.Client.GetConnectionQueueSize()
	maxConcurrentOps := connectionQueueSize
	if tSettings.UtxoStore.PrunerMaxConcurrentOperations > 0 {
		if tSettings.UtxoStore.PrunerMaxConcurrentOperations < maxConcurrentOps {
			maxConcurrentOps = tSettings.UtxoStore.PrunerMaxConcurrentOperations
		}
	}

	service := &Service{
		logger:                  opts.Logger,
		settings:                tSettings,
		client:                  opts.Client,
		external:                opts.ExternalStore,
		namespace:               opts.Namespace,
		set:                     opts.Set,
		ctx:                     opts.Ctx,
		indexWaiter:             opts.IndexWaiter,
		queryPolicy:             queryPolicy,
		writePolicy:             writePolicy,
		batchWritePolicy:        batchWritePolicy,
		batchPolicy:             batchPolicy,
		getPersistedHeight:      opts.GetPersistedHeight,
		maxConcurrentOperations: maxConcurrentOps,
	}

	// Create the job processor function
	jobProcessor := func(job *pruner.Job, workerID int) {
		service.processCleanupJob(job, workerID)
	}

	// Create the job manager
	jobManager, err := pruner.NewJobManager(pruner.JobManagerOptions{
		Logger:         opts.Logger,
		WorkerCount:    opts.WorkerCount,
		MaxJobsHistory: opts.MaxJobsHistory,
		JobProcessor:   jobProcessor,
	})
	if err != nil {
		return nil, err
	}

	service.jobManager = jobManager

	return service, nil
}

// Start starts the cleanup service and creates the required index if this has not been done already.  This method
// will return immediately but will not start the workers until the initialization is complete.
//
// The service will start a goroutine to initialize the service and create the required index if it does not exist.
// Once the initialization is complete, the service will start the worker goroutines to process cleanup jobs.
//
// The service will also create a rotating queue of cleanup jobs, which will be processed as the block height
// becomes available.  The rotating queue will always keep the most recent jobs and will drop older
// jobs if the queue is full or there is a job with a higher height.
func (s *Service) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	go func() {
		// All processes wait for the index to be built
		if err := s.indexWaiter.WaitForIndexReady(ctx, IndexName); err != nil {
			s.logger.Errorf("Timeout or error waiting for index to be built: %v", err)
		}

		// Only start job manager after index is built
		s.jobManager.Start(ctx)

		s.logger.Infof("[AerospikeCleanupService] started cleanup service")
	}()
}

// Stop stops the cleanup service and waits for all workers to exit.
// This ensures all goroutines are properly terminated before returning.
func (s *Service) Stop(ctx context.Context) error {
	// Stop the job manager
	s.jobManager.Stop()

	s.logger.Infof("[AerospikeCleanupService] stopped cleanup service")

	return nil
}

// SetPersistedHeightGetter sets the function used to get block persister progress.
// This should be called after service creation to wire up coordination with block persister.
func (s *Service) SetPersistedHeightGetter(getter func() uint32) {
	s.getPersistedHeight = getter
}

// UpdateBlockHeight updates the block height and triggers a cleanup job
func (s *Service) UpdateBlockHeight(blockHeight uint32, done ...chan string) error {
	if blockHeight == 0 {
		return errors.NewProcessingError("block height cannot be zero")
	}

	s.logger.Debugf("[AerospikeCleanupService] Updating block height to %d", blockHeight)

	// Pass the done channel to the job manager
	var doneChan chan string

	if len(done) > 0 {
		doneChan = done[0]
	}

	return s.jobManager.UpdateBlockHeight(blockHeight, doneChan)
}

// processCleanupJob processes a cleanup job
func (s *Service) processCleanupJob(job *pruner.Job, workerID int) {
	// Update job status to running
	job.SetStatus(pruner.JobStatusRunning)
	job.Started = time.Now()

	// Get the job's context for cancellation support
	jobCtx := job.Context()

	s.logger.Infof("Worker %d starting cleanup job for block height %d", workerID, job.BlockHeight)

	// BLOCK PERSISTER COORDINATION: Calculate safe cleanup height
	//
	// PROBLEM: Block persister creates .subtree_data files after a delay (BlockPersisterPersistAge blocks).
	// If we delete transactions before block persister creates these files, catchup will fail with
	// "subtree length does not match tx data length" (actually missing transactions).
	//
	// SOLUTION: Limit cleanup to transactions that block persister has already processed:
	//   safe_height = min(requested_cleanup_height, persisted_height + retention)
	//
	// EXAMPLE with retention=288, persisted=100, requested=200:
	//   - Block persister has processed blocks up to height 100
	//   - Those blocks' transactions are in .subtree_data files (safe to delete after retention)
	//   - Safe deletion height = 100 + 288 = 388... but wait, we want to clean height 200
	//   - Since 200 < 388, we can safely proceed with cleaning up to 200
	//
	// EXAMPLE where cleanup would be limited (persisted=50, requested=200, retention=100):
	//   - Block persister only processed up to height 50
	//   - Safe deletion = 50 + 100 = 150
	//   - Requested cleanup of 200 is LIMITED to 150 to protect unpersisted blocks 51-200
	//
	// HEIGHT=0 SPECIAL CASE: If persistedHeight=0, block persister isn't running or hasn't
	// processed any blocks yet. Proceed with normal cleanup without coordination.
	safeCleanupHeight := job.BlockHeight

	if s.getPersistedHeight != nil {
		persistedHeight := s.getPersistedHeight()

		// Only apply limitation if block persister has actually processed blocks (height > 0)
		if persistedHeight > 0 {
			retention := s.settings.GetUtxoStoreBlockHeightRetention()

			// Calculate max safe height: persisted_height + retention
			// Block persister at height N means blocks 0 to N are persisted in .subtree_data files.
			// Those transactions can be safely deleted after retention blocks.
			maxSafeHeight := persistedHeight + retention
			if maxSafeHeight < safeCleanupHeight {
				s.logger.Infof("Worker %d: Limiting cleanup from height %d to %d (persisted: %d, retention: %d)",
					workerID, job.BlockHeight, maxSafeHeight, persistedHeight, retention)
				safeCleanupHeight = maxSafeHeight
			}
		}
	}

	// Create a query statement
	stmt := aerospike.NewStatement(s.namespace, s.set)
	stmt.BinNames = []string{fields.TxID.String(), fields.DeleteAtHeight.String(), fields.Inputs.String(), fields.External.String(), fields.TotalExtraRecs.String()}

	// Set the filter to find records with a delete_at_height less than or equal to the safe cleanup height
	// This will automatically use the index since the filter is on the indexed bin
	err := stmt.SetFilter(aerospike.NewRangeFilter(fields.DeleteAtHeight.String(), 1, int64(safeCleanupHeight)))
	if err != nil {
		job.SetStatus(pruner.JobStatusFailed)
		job.Error = err
		job.Ended = time.Now()

		s.logger.Errorf("Worker %d: failed to set filter for cleanup job %d: %v", workerID, job.BlockHeight, err)

		return
	}

	// iterate through the results, process each record individually using batchers
	recordset, err := s.client.Query(s.queryPolicy, stmt)
	if err != nil {
		s.logger.Errorf("Worker %d: failed to execute query for cleanup job %d: %v", workerID, job.BlockHeight, err)
		s.markJobAsFailed(job, err)
		return
	}

	defer recordset.Close()

	result := recordset.Results()
	recordCount := int64(0)
	lastProgressLog := time.Now()
	progressLogInterval := 30 * time.Second

	// Log initial start
	s.logger.Infof("Worker %d: starting cleanup scan for height %d (delete_at_height <= %d)",
		workerID, job.BlockHeight, safeCleanupHeight)

	// Batch accumulation slices
	batchSize := s.settings.UtxoStore.PrunerDeleteBatcherSize
	parentUpdates := make(map[string]*parentUpdateInfo) // keyed by parent txid
	deletions := make([]*aerospike.Key, 0, batchSize)
	externalFiles := make([]*externalFileInfo, 0, batchSize)

	// Process records and accumulate into batches
	for {
		// Check for cancellation before processing next record
		select {
		case <-jobCtx.Done():
			s.logger.Infof("Worker %d: cleanup job for height %d cancelled", workerID, job.BlockHeight)
			recordset.Close()
			// Flush any accumulated operations before exiting (use parent context since jobCtx is cancelled)
			if err := s.flushCleanupBatches(s.ctx, workerID, job.BlockHeight, parentUpdates, deletions, externalFiles); err != nil {
				s.logger.Errorf("Worker %d: error flushing batches during cancellation: %v", workerID, err)
			}
			s.markJobAsFailed(job, errors.NewProcessingError("Worker %d: cleanup job for height %d cancelled", workerID, job.BlockHeight))
			return
		default:
		}

		rec, ok := <-result
		if !ok || rec == nil {
			break // No more records
		}

		if rec.Err != nil {
			s.logger.Errorf("Worker %d: error reading record: %v", workerID, rec.Err)
			continue
		}

		// Extract record data
		bins := rec.Record.Bins
		txHash, err := s.extractTxHash(bins)
		if err != nil {
			s.logger.Errorf("Worker %d: %v", workerID, err)
			continue
		}

		inputs, err := s.extractInputs(job, workerID, bins, txHash)
		if err != nil {
			s.logger.Errorf("Worker %d: %v", workerID, err)
			continue
		}

		// Accumulate parent updates
		for _, input := range inputs {
			// Calculate the parent key for this input
			keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.settings.UtxoStore.UtxoBatchSize)
			parentKeyStr := string(keySource)

			if existing, ok := parentUpdates[parentKeyStr]; ok {
				// Add this transaction to the list of deleted children for this parent
				existing.childHashes = append(existing.childHashes, txHash)
			} else {
				parentKey, err := aerospike.NewKey(s.namespace, s.set, keySource)
				if err != nil {
					s.logger.Errorf("Worker %d: failed to create parent key: %v", workerID, err)
					continue
				}
				parentUpdates[parentKeyStr] = &parentUpdateInfo{
					key:         parentKey,
					childHashes: []*chainhash.Hash{txHash},
				}
			}
		}

		// Handle external transactions: add file for deletion
		external, isExternal := bins[fields.External.String()].(bool)
		if isExternal && external {
			// Determine file type: if we found inputs, it's a .tx file, otherwise it's .outputs
			fileType := fileformat.FileTypeOutputs
			if len(inputs) > 0 {
				fileType = fileformat.FileTypeTx
			}
			externalFiles = append(externalFiles, &externalFileInfo{
				txHash:   txHash,
				fileType: fileType,
			})
		}

		// Accumulate deletions: master record + any child records
		deletions = append(deletions, rec.Record.Key)

		// If this is a multi-record transaction, delete all child records
		totalExtraRecs, hasExtraRecs := bins[fields.TotalExtraRecs.String()].(int)
		if hasExtraRecs && totalExtraRecs > 0 {
			// Generate keys for all child records: txid_1, txid_2, ..., txid_N
			for i := 1; i <= totalExtraRecs; i++ {
				childKeySource := uaerospike.CalculateKeySourceInternal(txHash, uint32(i))
				childKey, err := aerospike.NewKey(s.namespace, s.set, childKeySource)
				if err != nil {
					s.logger.Errorf("Worker %d: failed to create child key for %s_%d: %v", workerID, txHash.String(), i, err)
					continue
				}
				deletions = append(deletions, childKey)
			}
			s.logger.Debugf("Worker %d: deleting external tx %s with %d child records", workerID, txHash.String(), totalExtraRecs)
		}

		recordCount++

		// Log progress
		if recordCount%10000 == 0 || time.Since(lastProgressLog) > progressLogInterval {
			s.logger.Infof("Worker %d: cleanup progress for height %d - processed %d records so far",
				workerID, job.BlockHeight, recordCount)
			lastProgressLog = time.Now()
		}

		// Execute batch when full
		if len(deletions) >= batchSize {
			if err := s.flushCleanupBatches(jobCtx, workerID, job.BlockHeight, parentUpdates, deletions, externalFiles); err != nil {
				s.logger.Errorf("Worker %d: error flushing batches: %v", workerID, err)
				// Continue processing despite flush error - will retry at end
			}
			parentUpdates = make(map[string]*parentUpdateInfo)
			deletions = make([]*aerospike.Key, 0, batchSize)
			externalFiles = make([]*externalFileInfo, 0, batchSize)
		}
	}

	// Flush any remaining operations
	if err := s.flushCleanupBatches(jobCtx, workerID, job.BlockHeight, parentUpdates, deletions, externalFiles); err != nil {
		s.logger.Errorf("Worker %d: error flushing final batches: %v", workerID, err)
		s.markJobAsFailed(job, errors.NewStorageError("failed to flush final batches", err))
		return
	}

	// Check if we were cancelled
	wasCancelled := false
	select {
	case <-jobCtx.Done():
		wasCancelled = true
		s.logger.Infof("Worker %d: cleanup job for height %d was cancelled", workerID, job.BlockHeight)
	default:
		// Not cancelled
	}

	s.logger.Infof("Worker %d: processed %d records for cleanup job %d", workerID, recordCount, job.BlockHeight)

	// Set appropriate status based on cancellation
	if wasCancelled {
		job.SetStatus(pruner.JobStatusCancelled)
		s.logger.Infof("Worker %d cancelled cleanup job for block height %d after %v, processed %d records before cancellation",
			workerID, job.BlockHeight, time.Since(job.Started), recordCount)
	} else {
		job.SetStatus(pruner.JobStatusCompleted)
		s.logger.Infof("Worker %d completed cleanup job for block height %d in %v, processed %d records",
			workerID, job.BlockHeight, time.Since(job.Started), recordCount)
	}
	job.Ended = time.Now()

	prometheusUtxoCleanupBatch.Observe(float64(time.Since(job.Started).Microseconds()) / 1_000_000)
}

func (s *Service) getTxInputsFromBins(job *pruner.Job, workerID int, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	var inputs []*bt.Input

	external, ok := bins[fields.External.String()].(bool)
	if ok && external {
		// transaction is external, we need to get the data from the external store
		txBytes, err := s.external.Get(s.ctx, txHash.CloneBytes(), fileformat.FileTypeTx)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				// Check if outputs exist (sometimes only outputs are stored)
				exists, err := s.external.Exists(s.ctx, txHash.CloneBytes(), fileformat.FileTypeOutputs)
				if err != nil {
					return nil, errors.NewProcessingError("Worker %d: error checking existence of outputs for external tx %s in cleanup job %d", workerID, txHash.String(), job.BlockHeight, err)
				}

				if exists {
					// Only outputs exist, no inputs needed for cleanup
					return nil, nil
				}

				// External blob already deleted (by LocalDAH or previous cleanup), just need to delete Aerospike record
				s.logger.Debugf("Worker %d: external tx %s already deleted from blob store for cleanup job %d, proceeding to delete Aerospike record",
					workerID, txHash.String(), job.BlockHeight)
				return []*bt.Input{}, nil
			}
			// Other errors should still be reported
			return nil, errors.NewProcessingError("Worker %d: error getting external tx %s for cleanup job %d", workerID, txHash.String(), job.BlockHeight, err)
		}

		tx, err := bt.NewTxFromBytes(txBytes)
		if err != nil {
			return nil, errors.NewProcessingError("Worker %d: invalid tx bytes for external tx %s in cleanup job %d", workerID, txHash.String(), job.BlockHeight, err)
		}

		inputs = tx.Inputs
	} else {
		// get the inputs from the record directly
		inputsValue := bins[fields.Inputs.String()]
		if inputsValue == nil {
			// Inputs field might be nil for certain records (e.g., coinbase)
			return []*bt.Input{}, nil
		}

		inputInterfaces, ok := inputsValue.([]interface{})
		if !ok {
			// Log more helpful error with actual type
			return nil, errors.NewProcessingError("Worker %d: inputs field has unexpected type %T (expected []interface{}) for record in cleanup job %d",
				workerID, inputsValue, job.BlockHeight)
		}

		inputs = make([]*bt.Input, len(inputInterfaces))

		for i, inputInterface := range inputInterfaces {
			input := inputInterface.([]byte)
			inputs[i] = &bt.Input{}

			if _, err := inputs[i].ReadFrom(bytes.NewReader(input)); err != nil {
				return nil, errors.NewProcessingError("Worker %d: invalid input for record in cleanup job %d", workerID, job.BlockHeight, err)
			}
		}
	}

	return inputs, nil
}

func (s *Service) markJobAsFailed(job *pruner.Job, err error) {
	job.SetStatus(pruner.JobStatusFailed)
	job.Error = err
	job.Ended = time.Now()
}

// flushCleanupBatches flushes accumulated parent updates, external file deletions, and Aerospike deletions
func (s *Service) flushCleanupBatches(ctx context.Context, workerID int, blockHeight uint32, parentUpdates map[string]*parentUpdateInfo, deletions []*aerospike.Key, externalFiles []*externalFileInfo) error {
	// Execute parent updates first
	if len(parentUpdates) > 0 {
		if err := s.executeBatchParentUpdates(ctx, workerID, blockHeight, parentUpdates); err != nil {
			return err
		}
	}

	// Delete external files before Aerospike records (fail-safe: if file deletion fails, we keep the record)
	if len(externalFiles) > 0 {
		if err := s.executeBatchExternalFileDeletions(ctx, workerID, blockHeight, externalFiles); err != nil {
			return err
		}
	}

	// Delete Aerospike records last
	if len(deletions) > 0 {
		if err := s.executeBatchDeletions(ctx, workerID, blockHeight, deletions); err != nil {
			return err
		}
	}

	return nil
}

// extractTxHash extracts the transaction hash from record bins
func (s *Service) extractTxHash(bins aerospike.BinMap) (*chainhash.Hash, error) {
	txIDBytes, ok := bins[fields.TxID.String()].([]byte)
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
func (s *Service) extractInputs(job *pruner.Job, workerID int, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	return s.getTxInputsFromBins(job, workerID, bins, txHash)
}

// executeBatchParentUpdates executes a batch of parent update operations
func (s *Service) executeBatchParentUpdates(ctx context.Context, workerID int, blockHeight uint32, updates map[string]*parentUpdateInfo) error {
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
			ops[i] = aerospike.MapPutOp(mapPolicy, fields.DeletedChildren.String(),
				aerospike.NewStringValue(childHash.String()), aerospike.BoolValue(true))
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(s.batchWritePolicy, info.key, ops...))
	}

	// Check context before expensive operation
	select {
	case <-ctx.Done():
		s.logger.Infof("Worker %d: context cancelled, skipping parent update batch", workerID)
		return ctx.Err()
	default:
	}

	// Execute batch
	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Worker %d: batch parent update failed: %v", workerID, err)
		return errors.NewStorageError("batch parent update failed", err)
	}

	// Check for errors
	successCount := 0
	notFoundCount := 0
	errorCount := 0

	for _, rec := range batchRecords {
		if rec.BatchRec().Err != nil {
			// Ignore KEY_NOT_FOUND - parent may have been deleted already
			if rec.BatchRec().Err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
				notFoundCount++
				continue
			}
			// Log other errors
			s.logger.Errorf("Worker %d: parent update error for key %v: %v", workerID, rec.BatchRec().Key, rec.BatchRec().Err)
			errorCount++
		} else {
			successCount++
		}
	}

	// Return error if any individual record operations failed
	if errorCount > 0 {
		return errors.NewStorageError("Worker %d: %d parent update operations failed", workerID, errorCount)
	}

	return nil
}

// executeBatchDeletions executes a batch of deletion operations
func (s *Service) executeBatchDeletions(ctx context.Context, workerID int, blockHeight uint32, keys []*aerospike.Key) error {
	if len(keys) == 0 {
		return nil
	}

	// Create batch delete records
	batchDeletePolicy := aerospike.NewBatchDeletePolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, len(keys))
	for i, key := range keys {
		batchRecords[i] = aerospike.NewBatchDelete(batchDeletePolicy, key)
	}

	// Check context before expensive operation
	select {
	case <-ctx.Done():
		s.logger.Infof("Worker %d: context cancelled, skipping deletion batch", workerID)
		return ctx.Err()
	default:
	}

	// Execute batch
	if err := s.client.BatchOperate(s.batchPolicy, batchRecords); err != nil {
		s.logger.Errorf("Worker %d: batch deletion failed for %d records: %v", workerID, len(keys), err)
		return errors.NewStorageError("batch deletion failed", err)
	}

	// Check for errors and count successes
	successCount := 0
	alreadyDeletedCount := 0
	errorCount := 0

	for _, rec := range batchRecords {
		if rec.BatchRec().Err != nil {
			if rec.BatchRec().Err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
				// Already deleted
				alreadyDeletedCount++
			} else {
				s.logger.Errorf("Worker %d: deletion error for key %v: %v", workerID, rec.BatchRec().Key, rec.BatchRec().Err)
				errorCount++
			}
		} else {
			successCount++
		}
	}

	// Return error if any individual record operations failed
	if errorCount > 0 {
		return errors.NewStorageError("Worker %d: %d deletion operations failed", workerID, errorCount)
	}

	return nil
}

// executeBatchExternalFileDeletions deletes external blob files for transactions being pruned
func (s *Service) executeBatchExternalFileDeletions(ctx context.Context, workerID int, blockHeight uint32, files []*externalFileInfo) error {
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
			s.logger.Infof("Worker %d: context cancelled, stopping external file deletions", workerID)
			return ctx.Err()
		default:
		}

		// Delete the external file
		err := s.external.Del(ctx, fileInfo.txHash.CloneBytes(), fileInfo.fileType)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				// Already deleted (by LocalDAH cleanup or previous pruning)
				alreadyDeletedCount++
				s.logger.Debugf("Worker %d: external file for tx %s (type %d) already deleted", workerID, fileInfo.txHash.String(), fileInfo.fileType)
			} else {
				s.logger.Errorf("Worker %d: failed to delete external file for tx %s (type %d): %v", workerID, fileInfo.txHash.String(), fileInfo.fileType, err)
				errorCount++
			}
		} else {
			successCount++
		}
	}

	s.logger.Debugf("Worker %d: external file deletion batch - success: %d, already deleted: %d, errors: %d", workerID, successCount, alreadyDeletedCount, errorCount)

	// Return error if any deletions failed
	if errorCount > 0 {
		return errors.NewStorageError("Worker %d: %d external file deletions failed", workerID, errorCount)
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
	parentUpdates := make(map[string]*parentUpdateInfo)
	for _, input := range inputs {
		keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.settings.UtxoStore.UtxoBatchSize)
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
	return s.executeBatchParentUpdates(s.ctx, 0, 0, parentUpdates)
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *Service) GetJobs() []*pruner.Job {
	return s.jobManager.GetJobs()
}
