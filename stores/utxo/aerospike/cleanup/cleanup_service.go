package cleanup

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/cleanup"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
)

// Ensure Store implements the Cleanup Service interface
var _ cleanup.Service = (*Service)(nil)

var IndexName, _ = gocore.Config().Get("cleanup_IndexName", "cleanup_dah_index")

// batcherIfc defines the interface for batching operations
type batcherIfc[T any] interface {
	Put(item *T, payloadSize ...int)
	Trigger()
}

// Constants for the cleanup service
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
	jobManager  *cleanup.JobManager
	ctx         context.Context
	indexWaiter IndexWaiter

	// internally reused variables
	queryPolicy      *aerospike.QueryPolicy
	writePolicy      *aerospike.WritePolicy
	batchWritePolicy *aerospike.BatchWritePolicy

	// separate batchers for background batch processing
	parentUpdateBatcher batcherIfc[batchParentUpdate]
	deleteBatcher       batcherIfc[batchDelete]

	// getPersistedHeight returns the last block height processed by block persister
	// Used to coordinate cleanup with block persister progress (can be nil)
	getPersistedHeight func() uint32

	// maxConcurrentOperations limits concurrent operations during cleanup processing
	// Auto-detected from Aerospike client connection queue size
	maxConcurrentOperations int
}

// batchParentUpdate represents a parent record update operation in a batch
type batchParentUpdate struct {
	txHash *chainhash.Hash
	inputs []*bt.Input
	errCh  chan error
}

// batchDelete represents a record deletion operation in a batch
type batchDelete struct {
	key    *aerospike.Key
	txHash *chainhash.Hash
	errCh  chan error
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

	queryPolicy := aerospike.NewQueryPolicy()
	queryPolicy.IncludeBinData = true
	queryPolicy.MaxRetries = 3
	queryPolicy.SocketTimeout = 30 * time.Second
	queryPolicy.TotalTimeout = 120 * time.Second

	writePolicy := aerospike.NewWritePolicy(0, 0)
	writePolicy.MaxRetries = 3
	writePolicy.SocketTimeout = 30 * time.Second
	writePolicy.TotalTimeout = 120 * time.Second

	batchWritePolicy := aerospike.NewBatchWritePolicy()
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	// Determine max concurrent operations:
	// - Use connection queue size as the upper bound (to prevent connection exhaustion)
	// - If setting is configured (non-zero), use the minimum of setting and connection queue size
	// - If setting is 0 or unset, use connection queue size
	connectionQueueSize := opts.Client.GetConnectionQueueSize()
	maxConcurrentOps := connectionQueueSize
	if tSettings.UtxoStore.CleanupMaxConcurrentOperations > 0 {
		if tSettings.UtxoStore.CleanupMaxConcurrentOperations < maxConcurrentOps {
			maxConcurrentOps = tSettings.UtxoStore.CleanupMaxConcurrentOperations
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
		getPersistedHeight:      opts.GetPersistedHeight,
		maxConcurrentOperations: maxConcurrentOps,
	}

	// Create the job processor function
	jobProcessor := func(job *cleanup.Job, workerID int) {
		service.processCleanupJob(job, workerID)
	}

	// Create the job manager
	jobManager, err := cleanup.NewJobManager(cleanup.JobManagerOptions{
		Logger:         opts.Logger,
		WorkerCount:    opts.WorkerCount,
		MaxJobsHistory: opts.MaxJobsHistory,
		JobProcessor:   jobProcessor,
	})
	if err != nil {
		return nil, err
	}

	service.jobManager = jobManager

	// Initialize cleanup batchers using dedicated cleanup settings
	parentUpdateBatchSize := tSettings.UtxoStore.CleanupParentUpdateBatcherSize
	parentUpdateBatchDuration := time.Duration(tSettings.UtxoStore.CleanupParentUpdateBatcherDurationMillis) * time.Millisecond
	service.parentUpdateBatcher = batcher.New[batchParentUpdate](parentUpdateBatchSize, parentUpdateBatchDuration, service.sendParentUpdateBatch, true)

	deleteBatchSize := tSettings.UtxoStore.CleanupDeleteBatcherSize
	deleteBatchDuration := time.Duration(tSettings.UtxoStore.CleanupDeleteBatcherDurationMillis) * time.Millisecond
	service.deleteBatcher = batcher.New[batchDelete](deleteBatchSize, deleteBatchDuration, service.sendDeleteBatch, true)

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
func (s *Service) processCleanupJob(job *cleanup.Job, workerID int) {
	// Update job status to running
	job.SetStatus(cleanup.JobStatusRunning)
	job.Started = time.Now()

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
	stmt.BinNames = []string{fields.TxID.String(), fields.DeleteAtHeight.String(), fields.Inputs.String(), fields.External.String()}

	// Set the filter to find records with a delete_at_height less than or equal to the safe cleanup height
	// This will automatically use the index since the filter is on the indexed bin
	err := stmt.SetFilter(aerospike.NewRangeFilter(fields.DeleteAtHeight.String(), 1, int64(safeCleanupHeight)))
	if err != nil {
		job.SetStatus(cleanup.JobStatusFailed)
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
	recordCount := atomic.Int64{}

	g := &errgroup.Group{}
	util.SafeSetLimit(g, s.maxConcurrentOperations)

	for {
		rec, ok := <-result
		if !ok || rec == nil {
			break // No more records
		}

		currentRec := rec // capture for goroutine
		g.Go(func() error {
			if err := s.processRecordCleanup(job, workerID, currentRec); err != nil {
				return err
			}

			recordCount.Add(1)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		s.logger.Errorf(err.Error())
		s.markJobAsFailed(job, err)
		return
	}

	s.logger.Infof("Worker %d: processed %d records for cleanup job %d", workerID, recordCount.Load(), job.BlockHeight)

	job.SetStatus(cleanup.JobStatusCompleted)
	job.Ended = time.Now()

	prometheusUtxoCleanupBatch.Observe(float64(time.Since(job.Started).Microseconds()) / 1_000_000)

	if job.DoneCh != nil {
		job.DoneCh <- cleanup.JobStatusCompleted.String()
		close(job.DoneCh)
	}

	finalRecordCount := recordCount.Load()
	s.logger.Infof("Worker %d completed cleanup job for block height %d in %v, processed %d records", workerID, job.BlockHeight, job.Ended.Sub(job.Started), finalRecordCount)
}

func (s *Service) getTxInputsFromBins(job *cleanup.Job, workerID int, bins aerospike.BinMap, txHash *chainhash.Hash) ([]*bt.Input, error) {
	var inputs []*bt.Input

	external, ok := bins[fields.External.String()].(bool)
	if ok && external {
		// transaction is external, we need to get the data from the external store
		txBytes, err := s.external.Get(s.ctx, txHash.CloneBytes(), fileformat.FileTypeTx)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) {
				exists, err := s.external.Exists(s.ctx, txHash.CloneBytes(), fileformat.FileTypeOutputs)
				if err != nil {
					return nil, errors.NewProcessingError("Worker %d: error checking existence of outputs for external tx %s in cleanup job %d: %v", workerID, txHash.String(), job.BlockHeight, err)
				}

				if exists {
					// nothing more to do here, continue processing other records
					return nil, nil
				}

				return nil, errors.NewProcessingError("Worker %d: external tx %s not found in external store for cleanup job %d", workerID, txHash.String(), job.BlockHeight)
			}
		}

		tx, err := bt.NewTxFromBytes(txBytes)
		if err != nil {
			return nil, errors.NewProcessingError("Worker %d: invalid tx bytes for external tx %s in cleanup job %d: %v", workerID, txHash.String(), job.BlockHeight, err)
		}

		inputs = tx.Inputs
	} else {
		// get the inputs from the record directly
		inputInterfaces, ok := bins[fields.Inputs.String()].([]interface{})
		if !ok {
			return nil, errors.NewProcessingError("Worker %d: missing inputs for record in cleanup job %d", workerID, job.BlockHeight)
		}

		inputs = make([]*bt.Input, len(inputInterfaces))

		for i, inputInterface := range inputInterfaces {
			input := inputInterface.([]byte)
			inputs[i] = &bt.Input{}

			if _, err := inputs[i].ReadFrom(bytes.NewReader(input)); err != nil {
				return nil, errors.NewProcessingError("Worker %d: invalid input for record in cleanup job %d: %v", workerID, job.BlockHeight, err)
			}
		}
	}

	return inputs, nil
}

func (s *Service) markJobAsFailed(job *cleanup.Job, err error) {
	job.SetStatus(cleanup.JobStatusFailed)
	job.Error = err
	job.Ended = time.Now()

	if job.DoneCh != nil {
		job.DoneCh <- cleanup.JobStatusFailed.String()
		close(job.DoneCh)
	}
}

// sendParentUpdateBatch processes a batch of parent update operations
func (s *Service) sendParentUpdateBatch(batch []*batchParentUpdate) {
	if len(batch) == 0 {
		return
	}

	// Track which batch items are associated with which parent keys
	type parentInfo struct {
		key         *aerospike.Key
		childHashes []*chainhash.Hash
		batchItems  []*batchParentUpdate // Track which items depend on this parent
	}

	parentMap := make(map[string]*parentInfo)                   // Use key string for deduplication
	itemToParents := make(map[*batchParentUpdate][]*parentInfo) // Track parents per item

	// First pass: collect all parent updates and track relationships
	for _, item := range batch {
		if len(item.inputs) == 0 {
			// No inputs, send success immediately
			item.errCh <- nil
			continue
		}

		var itemParents []*parentInfo
		for _, input := range item.inputs {
			keySource := uaerospike.CalculateKeySource(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, s.settings.UtxoStore.UtxoBatchSize)
			parentKey, err := aerospike.NewKey(s.namespace, s.set, keySource)
			if err != nil {
				// Send error to this batch item immediately
				item.errCh <- errors.NewProcessingError("error creating parent key for input in external tx %s: %v", item.txHash.String(), err)
				continue
			}

			keyStr := parentKey.String()
			parent, exists := parentMap[keyStr]
			if !exists {
				parent = &parentInfo{
					key:         parentKey,
					childHashes: make([]*chainhash.Hash, 0),
					batchItems:  make([]*batchParentUpdate, 0),
				}
				parentMap[keyStr] = parent
			}

			// Add this child tx hash if not already present
			found := false
			for _, existingHash := range parent.childHashes {
				if existingHash.IsEqual(item.txHash) {
					found = true
					break
				}
			}
			if !found {
				parent.childHashes = append(parent.childHashes, item.txHash)
			}

			// Add this item to the parent's dependent items if not already there
			found = false
			for _, existingItem := range parent.batchItems {
				if existingItem == item {
					found = true
					break
				}
			}
			if !found {
				parent.batchItems = append(parent.batchItems, item)
			}

			itemParents = append(itemParents, parent)
		}
		itemToParents[item] = itemParents
	}

	if len(parentMap) == 0 {
		return // All items already handled
	}

	// Create batch operations
	mapPolicy := aerospike.DefaultMapPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(parentMap))
	parentsList := make([]*parentInfo, 0, len(parentMap))

	for _, parent := range parentMap {
		// Create multiple MapPutOp operations for this parent
		ops := make([]*aerospike.Operation, 0, len(parent.childHashes))
		for _, childTxHash := range parent.childHashes {
			ops = append(ops, aerospike.MapPutOp(mapPolicy, fields.DeletedChildren.String(), aerospike.NewStringValue(childTxHash.String()), aerospike.BoolValue(true)))
		}

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(
			s.batchWritePolicy,
			parent.key,
			ops...,
		))
		parentsList = append(parentsList, parent)
	}

	// Execute the batch operation
	err := s.client.BatchOperate(s.client.DefaultBatchPolicy, batchRecords)
	if err != nil {
		// Send error to all batch items that have parent updates
		for _, item := range batch {
			if len(itemToParents[item]) > 0 {
				item.errCh <- errors.NewProcessingError("error batch updating parent records: %v", err)
			}
		}
		return
	}

	// Check individual batch results and send responses to affected items
	for i, batchRec := range batchRecords {
		parent := parentsList[i]
		var itemErr error

		if batchRec.BatchRec().Err != nil {
			if !errors.Is(batchRec.BatchRec().Err, aerospike.ErrKeyNotFound) {
				// Real error occurred
				itemErr = errors.NewProcessingError("error updating parent record: %v", batchRec.BatchRec().Err)
			}
			// For ErrKeyNotFound, itemErr remains nil (success)
		}

		// Send response to all items that depend on this parent
		for _, item := range parent.batchItems {
			item.errCh <- itemErr
		}
	}
}

// sendDeleteBatch processes a batch of deletion operations
func (s *Service) sendDeleteBatch(batch []*batchDelete) {
	if len(batch) == 0 {
		return
	}

	// Create batch delete records
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))
	batchDeletePolicy := aerospike.NewBatchDeletePolicy()

	for _, item := range batch {
		batchRecords = append(batchRecords, aerospike.NewBatchDelete(batchDeletePolicy, item.key))
	}

	// Execute the batch delete operation
	err := s.client.BatchOperate(s.client.DefaultBatchPolicy, batchRecords)
	if err != nil {
		// Send error to all batch items
		for _, item := range batch {
			item.errCh <- errors.NewProcessingError("error batch deleting records: %v", err)
		}

		return
	}

	// Check individual batch results and send individual responses
	for i, batchRec := range batchRecords {
		if batchRec.BatchRec().Err != nil {
			if errors.Is(batchRec.BatchRec().Err, aerospike.ErrKeyNotFound) {
				// Record not found, treat as success (already deleted)
				batch[i].errCh <- nil
			} else {
				// Real error occurred
				batch[i].errCh <- errors.NewProcessingError("error deleting record for tx %s: %v", batch[i].txHash.String(), batchRec.BatchRec().Err)
			}
		} else {
			// Success
			batch[i].errCh <- nil
		}
	}
}

func (s *Service) ProcessSingleRecord(txid *chainhash.Hash, inputs []*bt.Input) error {
	errCh := make(chan error)

	s.parentUpdateBatcher.Put(&batchParentUpdate{
		txHash: txid,
		inputs: inputs,
		errCh:  errCh,
	})

	return <-errCh
}

// processRecordCleanup processes a single record for cleanup using batchers
func (s *Service) processRecordCleanup(job *cleanup.Job, workerID int, rec *aerospike.Result) error {
	if rec.Err != nil {
		return errors.NewProcessingError("Worker %d: error reading record for cleanup job %d: %v", workerID, job.BlockHeight, rec.Err)
	}

	// get all the unique parent records of the record being deleted
	bins := rec.Record.Bins
	if bins == nil {
		return errors.NewProcessingError("Worker %d: missing bins for record in cleanup job %d", workerID, job.BlockHeight)
	}

	txIDBytes, ok := bins[fields.TxID.String()].([]byte)
	if !ok || len(txIDBytes) != 32 {
		return errors.NewProcessingError("Worker %d: invalid or missing txid for record in cleanup job %d", workerID, job.BlockHeight)
	}

	txHash, err := chainhash.NewHash(txIDBytes)
	if err != nil {
		return errors.NewProcessingError("Worker %d: invalid txid bytes for record in cleanup job %d", workerID, job.BlockHeight)
	}

	inputs, err := s.getTxInputsFromBins(job, workerID, bins, txHash)
	if err != nil {
		return err
	}

	// inputs could be empty on seeded records, in which case we just delete the record
	// and do not need to update any parents
	if len(inputs) > 0 {
		// Update parents using batcher in goroutine
		parentErrCh := make(chan error)

		s.parentUpdateBatcher.Put(&batchParentUpdate{
			txHash: txHash,
			inputs: inputs,
			errCh:  parentErrCh,
		})

		// Wait for parent update to complete
		if err = <-parentErrCh; err != nil {
			return errors.NewProcessingError("Worker %d: error updating parents for tx %s in cleanup job %d: %v", workerID, txHash.String(), job.BlockHeight, err)
		}
	}

	// Delete the record using batcher in goroutine
	deleteErrCh := make(chan error)

	s.deleteBatcher.Put(&batchDelete{
		key:    rec.Record.Key,
		txHash: txHash,
		errCh:  deleteErrCh,
	})

	// Wait for deletion to complete
	if err = <-deleteErrCh; err != nil {
		return errors.NewProcessingError("Worker %d: error deleting record for tx %s in cleanup job %d: %v", workerID, txHash.String(), job.BlockHeight, err)
	}

	// Wait for all operations to complete
	return nil
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *Service) GetJobs() []*cleanup.Job {
	return s.jobManager.GetJobs()
}
