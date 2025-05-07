package cleanup

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/cleanup"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ensure Store implements the Cleanup Service interface
var _ cleanup.Service = (*Service)(nil)

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

	// Client is the Aerospike client to use
	Client *uaerospike.Client

	// Namespace is the Aerospike namespace to use
	Namespace string

	// Set is the Aerospike set to use
	Set string

	// WorkerCount is the number of worker goroutines to use
	WorkerCount int

	// MaxJobsHistory is the maximum number of jobs to keep in history
	MaxJobsHistory int
}

// Service manages background jobs for cleaning up records based on block height
type Service struct {
	mu                sync.RWMutex
	logger            ulogger.Logger
	client            *uaerospike.Client
	namespace         string
	set               string
	jobManager        *cleanup.JobManager
	initialized       bool
	ctx               context.Context
	indexMutex        sync.Mutex      // Mutex for index creation operations
	ongoingIndexOps   map[string]bool // Track ongoing index creation operations
	ongoingIndexMutex sync.RWMutex    // Mutex for the ongoing index operations map
}

// NewService creates a new cleanup service
func NewService(opts Options) (*Service, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
	}

	if opts.Client == nil {
		return nil, errors.NewProcessingError("client is required")
	}

	if opts.Namespace == "" {
		return nil, errors.NewProcessingError("namespace is required")
	}

	if opts.Set == "" {
		return nil, errors.NewProcessingError("set is required")
	}

	// Initialize prometheus metrics if not already initialized
	prometheusMetricsInitOnce.Do(func() {
		prometheusUtxoCleanupBatch = promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "utxo_cleanup_batch_duration_seconds",
			Help:    "Time taken to process a batch of cleanup jobs",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		})
	})

	service := &Service{
		logger:          opts.Logger,
		client:          opts.Client,
		namespace:       opts.Namespace,
		set:             opts.Set,
		ctx:             opts.Ctx,
		ongoingIndexOps: make(map[string]bool),
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
	// Check if the service is already initialized
	s.mu.RLock()
	initialized := s.initialized
	s.mu.RUnlock()

	if initialized {
		return
	}

	// Create a context that will be cancelled when the service is stopped
	if ctx == nil {
		ctx = context.Background()
	}

	// Start the job manager
	s.jobManager.Start(ctx)

	// Mark the service as initialized
	s.mu.Lock()
	s.initialized = true
	s.mu.Unlock()

	s.logger.Infof("[AerospikeCleanupService] started cleanup service")

	// Start a goroutine to initialize the service
	go func() {
		// Create the index if it doesn't exist
		// Skip index creation in tests with mock clients
		if s.client != nil && s.client.Client != nil {
			err := s.CreateIndexIfNotExists(ctx, "cleanup_dah_index", fields.DeleteAtHeight.String(), aerospike.NUMERIC)
			if err != nil {
				s.logger.Errorf("Failed to create index: %v", err)
				return
			}
		}

		// Mark the service as initialized
		s.mu.Lock()
		s.initialized = true
		s.mu.Unlock()
	}()
}

// Stop stops the cleanup service and waits for all workers to exit.
// This ensures all goroutines are properly terminated before returning.
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return nil
	}

	// Stop the job manager
	s.jobManager.Stop()

	// Mark the service as not initialized
	s.initialized = false

	s.logger.Infof("[AerospikeCleanupService] stopped cleanup service")

	return nil
}

// UpdateBlockHeight updates the block height and triggers a cleanup job
func (s *Service) UpdateBlockHeight(blockHeight uint32, done ...chan string) error {
	if blockHeight == 0 {
		return errors.NewProcessingError("block height cannot be zero")
	}

	s.logger.Infof("Updating block height to %d", blockHeight)

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

	// Create a query statement
	stmt := aerospike.NewStatement(s.namespace, s.set)

	// Set the filter to find records with a delete_at_height less than or equal to the current block height
	queryPolicy := aerospike.NewQueryPolicy()
	queryPolicy.MaxRetries = 3
	queryPolicy.SocketTimeout = 30 * time.Second
	queryPolicy.TotalTimeout = 120 * time.Second

	writePolicy := aerospike.NewWritePolicy(0, 0)
	writePolicy.MaxRetries = 3
	writePolicy.SocketTimeout = 30 * time.Second
	writePolicy.TotalTimeout = 120 * time.Second

	// This will automatically use the index since the filter is on the indexed bin
	err := stmt.SetFilter(aerospike.NewRangeFilter(fields.DeleteAtHeight.String(), 1, int64(job.BlockHeight)))
	if err != nil {
		job.SetStatus(cleanup.JobStatusFailed)
		job.Error = err
		job.Ended = time.Now()

		s.logger.Errorf("Worker %d: failed to set filter for cleanup job %d: %v", workerID, job.BlockHeight, err)

		return
	}

	// Execute the query with a delete operation
	task, err := s.client.QueryExecute(queryPolicy, writePolicy, stmt, aerospike.DeleteOp())
	if err != nil {
		job.SetStatus(cleanup.JobStatusFailed)
		job.Error = err
		job.Ended = time.Now()

		s.logger.Errorf("Worker %d: failed to execute cleanup job %d: %v", workerID, job.BlockHeight, err)

		if job.DoneCh != nil {
			job.DoneCh <- cleanup.JobStatusFailed.String()
			close(job.DoneCh)
		}

		return
	}

	// Wait for the task to complete
	for {
		// Check if the job has been cancelled
		select {
		case <-job.Context().Done():
			s.logger.Warnf("Worker %d: job for block height %d was cancelled during execution", workerID, job.BlockHeight)
			job.SetStatus(cleanup.JobStatusCancelled)
			job.Ended = time.Now()

			if job.DoneCh != nil {
				job.DoneCh <- cleanup.JobStatusCancelled.String()
				close(job.DoneCh)
			}

			return
		default: // Continue processing
		}

		done, err := task.IsDone()
		if err != nil {
			// Update job status to failed
			job.SetStatus(cleanup.JobStatusFailed)
			job.Error = err
			job.Ended = time.Now()

			s.logger.Errorf("Worker %d: failed to check status of cleanup job for block height %d: %v", workerID, job.BlockHeight, err)

			if job.DoneCh != nil {
				job.DoneCh <- cleanup.JobStatusFailed.String()
				close(job.DoneCh)
			}

			return
		}

		if done {
			// Task completed
			break
		}

		// Wait before checking again
		time.Sleep(100 * time.Millisecond)
	}

	job.SetStatus(cleanup.JobStatusCompleted)
	job.Ended = time.Now()

	prometheusUtxoCleanupBatch.Observe(float64(time.Since(job.Started).Microseconds()) / 1_000_000)

	if job.DoneCh != nil {
		job.DoneCh <- cleanup.JobStatusCompleted.String()
		close(job.DoneCh)
	}

	s.logger.Infof("Worker %d completed cleanup job for block height %d in %v", workerID, job.BlockHeight, job.Ended.Sub(job.Started))
}

// CreateIndexIfNotExists creates an index only if it doesn't already exist
// This method allows one caller to create the index in the background while
// other callers can immediately continue without waiting
func (s *Service) CreateIndexIfNotExists(ctx context.Context, indexName, binName string, indexType aerospike.IndexType) error {
	// First, check if the index already exists
	exists, err := s.indexExists(indexName)
	if err != nil {
		return err
	}

	// If the index already exists, we're done
	if exists {
		return nil
	}

	// Check if there's already an ongoing creation for this index
	s.ongoingIndexMutex.RLock()
	_, isOngoing := s.ongoingIndexOps[indexName]
	s.ongoingIndexMutex.RUnlock()

	if isOngoing {
		// Another thread is already creating this index, so we can return immediately
		s.logger.Infof("Index creation for %s is already in progress, skipping", indexName)
		return nil
	}

	// Try to acquire the lock to create the index
	s.indexMutex.Lock()

	// Check again if the index exists or if another thread started creating it
	// while we were waiting for the lock
	exists, err = s.indexExists(indexName)
	if err != nil {
		s.indexMutex.Unlock()
		return err
	}

	if exists {
		s.indexMutex.Unlock()
		return nil
	}

	s.ongoingIndexMutex.Lock()

	_, isOngoing = s.ongoingIndexOps[indexName]
	if isOngoing {
		s.ongoingIndexMutex.Unlock()
		s.indexMutex.Unlock()
		s.logger.Infof("Index creation for %s is already in progress, skipping", indexName)

		return nil
	}

	// Mark this index as being created
	s.ongoingIndexOps[indexName] = true
	s.ongoingIndexMutex.Unlock()

	// We can release the lock now since we've marked this index as being created
	s.indexMutex.Unlock()

	// Create the index in a goroutine
	go func() {
		defer func() {
			// Remove this index from the ongoing operations map when done
			s.ongoingIndexMutex.Lock()
			delete(s.ongoingIndexOps, indexName)
			s.ongoingIndexMutex.Unlock()
		}()

		s.logger.Infof("Starting background creation of index %s:%s", s.namespace, indexName)

		// Create the index since it doesn't exist
		policy := aerospike.NewWritePolicy(0, 0)

		task, err := s.client.CreateIndex(policy, s.namespace, s.set, indexName, binName, indexType)
		if err != nil {
			s.logger.Errorf("Failed to create index %s:%s: %v", s.namespace, indexName, err)
			return
		}

		// Wait for the task to complete
		for {
			// Check if the context is cancelled
			select {
			case <-ctx.Done():
				s.logger.Warnf("Create index %s:%s was cancelled during execution", s.namespace, indexName)
				return
			default: // Continue processing
			}

			done, err := task.IsDone()
			if err != nil {
				s.logger.Errorf("Error checking if index %s:%s creation is done: %v", s.namespace, indexName, err)
				return
			}

			if done {
				// Task completed
				s.logger.Infof("Create index %s:%s was completed successfully", s.namespace, indexName)
				return
			}

			// Wait before checking again
			time.Sleep(1 * time.Second)
		}
	}()

	s.logger.Infof("Index %s:%s creation started in background", s.namespace, indexName)

	return nil
}

// indexExists checks if an index with the given name exists in the namespace
func (s *Service) indexExists(indexName string) (bool, error) {
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
		if strings.Contains(v, fmt.Sprintf("ns=%s:indexname=%s:set=%s", s.namespace, indexName, s.set)) {
			return true, nil
		}
	}

	return false, nil
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *Service) GetJobs() []*cleanup.Job {
	return s.jobManager.GetJobs()
}
