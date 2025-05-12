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
	"github.com/ordishs/gocore"
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
	logger     ulogger.Logger
	client     *uaerospike.Client
	namespace  string
	set        string
	jobManager *cleanup.JobManager
	ctx        context.Context
	indexMutex sync.Mutex // Mutex for index creation operations

	indexOnce sync.Once // Ensures index creation/wait is only done once per process
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
		logger:    opts.Logger,
		client:    opts.Client,
		namespace: opts.Namespace,
		set:       opts.Set,
		ctx:       opts.Ctx,
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
	if ctx == nil {
		ctx = context.Background()
	}

	indexName, _ := gocore.Config().Get("cleanup_IndexName", "cleanup_dah_index")

	// Ensure index creation/wait is only done once per process
	s.indexOnce.Do(func() {
		if s.client != nil && s.client.Client != nil {
			exists, err := s.indexExists(indexName)
			if err != nil {
				s.logger.Errorf("Failed to check index existence: %v", err)
				return
			}

			if !exists {
				// Only one process should try to create the index
				err := s.CreateIndexIfNotExists(ctx, indexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC)
				if err != nil {
					s.logger.Errorf("Failed to create index: %v", err)
				}
			}
		}
	})

	// All processes wait for the index to be built
	if err := s.waitForIndexBuilt(ctx, indexName); err != nil {
		s.logger.Errorf("Timeout or error waiting for index to be built: %v", err)
	}

	// Only start job manager after index is built
	s.jobManager.Start(ctx)

	s.logger.Infof("[AerospikeCleanupService] started cleanup service")
}

// Stop stops the cleanup service and waits for all workers to exit.
// This ensures all goroutines are properly terminated before returning.
func (s *Service) Stop(ctx context.Context) error {
	// Stop the job manager
	s.jobManager.Stop()

	s.logger.Infof("[AerospikeCleanupService] stopped cleanup service")

	return nil
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

	s.logger.Infof("Creating index %s:%s:%s", s.namespace, s.set, indexName)

	if _, err := s.client.CreateIndex(policy, s.namespace, s.set, indexName, binName, indexType); err != nil {
		s.logger.Errorf("Failed to create index %s:%s:%s: %v", s.namespace, s.set, indexName, err)
		s.indexMutex.Unlock()

		return err
	}

	// Unlock the mutex and allow the index to continue being created in the background
	s.indexMutex.Unlock()

	return nil
}

// waitForIndexBuilt polls Aerospike until the index is ready or times out
func (s *Service) waitForIndexBuilt(ctx context.Context, indexName string) error {
	if s.client.Client == nil {
		return nil // For unit tests, we don't have a client
	}

	s.logger.Infof("Waiting for index %s:%s:%s to be built", s.namespace, s.set, indexName)

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
				if strings.Contains(v, fmt.Sprintf("ns=%s:indexname=%s:set=%s", s.namespace, indexName, s.set)) && strings.Contains(v, "RW") {
					s.logger.Infof("Index %s:%s:%s built in %s", s.namespace, s.set, indexName, time.Since(start))

					return nil // Index is ready
				}
			}

			time.Sleep(1 * time.Second)
		}
	}
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
