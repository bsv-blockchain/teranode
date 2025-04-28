package cleanup

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Constants for the cleanup service
const (
	// DefaultWorkerCount is the default number of worker goroutines
	DefaultWorkerCount = 4

	// DefaultMaxJobsHistory is the default number of jobs to keep in history
	DefaultMaxJobsHistory = 1000
)

var (
	prometheusUtxoCleanupBatch prometheus.Histogram
)

func init() {
	prometheusUtxoCleanupBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_cleanup_batch",
			Help:      "Duration of utxo cleanup batch",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}

// JobStatus represents the status of a cleanup job
type JobStatus int32

// Job statuses
const (
	JobStatusPending JobStatus = iota
	JobStatusRunning
	JobStatusCompleted
	JobStatusFailed
	JobStatusCancelled
)

// String returns the string representation of the job status
func (s JobStatus) String() string {
	switch s {
	case JobStatusPending:
		return "pending"
	case JobStatusRunning:
		return "running"
	case JobStatusCompleted:
		return "completed"
	case JobStatusFailed:
		return "failed"
	case JobStatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// Job represents a cleanup job
type Job struct {
	BlockHeight uint32
	status      atomic.Int32 // Using atomic for thread-safe access
	Error       error
	Created     time.Time
	Started     time.Time
	Ended       time.Time
	ctx         context.Context
	cancel      context.CancelFunc
}

// GetStatus returns the current status of the job
func (j *Job) GetStatus() JobStatus {
	return JobStatus(j.status.Load())
}

// setStatus sets the status of the job
func (j *Job) setStatus(status JobStatus) {
	j.status.Store(int32(status))
}

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

	// jobProcessor is an optional custom job processor, primarily for testing
	// If nil, the default processor will be used
	jobProcessor processor
}

// processor is a function type that processes a cleanup job
type processor func(s *Service, job *Job, workerID int)

// Service manages background jobs for cleaning up records based on block height
type Service struct {
	logger             ulogger.Logger
	client             *uaerospike.Client
	namespace          string
	set                string
	workerCount        int
	maxJobsHistory     int
	jobs               []*Job
	jobsMutex          sync.RWMutex
	currentBlockHeight atomic.Uint32
	ctx                context.Context
	cancelFunc         context.CancelFunc
	wg                 sync.WaitGroup
	jobProcessor       processor // Function to process jobs, can be overridden in tests
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

	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}

	if opts.MaxJobsHistory <= 0 {
		opts.MaxJobsHistory = DefaultMaxJobsHistory
	}

	if opts.Ctx == nil {
		opts.Ctx = context.Background()
	}

	ctx, cancelFunc := context.WithCancel(opts.Ctx)

	// Use the provided job processor or default to the standard implementation
	jobProcessor := opts.jobProcessor
	if jobProcessor == nil {
		jobProcessor = defaultJobProcessor
	}

	return &Service{
		logger:         opts.Logger,
		client:         opts.Client,
		namespace:      opts.Namespace,
		set:            opts.Set,
		workerCount:    workerCount,
		maxJobsHistory: opts.MaxJobsHistory,
		jobs:           make([]*Job, 0, opts.MaxJobsHistory),
		ctx:            ctx,
		cancelFunc:     cancelFunc,
		jobProcessor:   jobProcessor,
	}, nil
}

// Start starts the cleanup service
func (s *Service) Start() {
	// Start the worker pool
	for i := 0; i < s.workerCount; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
}

// Stop stops the cleanup service
func (s *Service) Stop() {
	s.cancelFunc()
	s.wg.Wait()
}

// UpdateBlockHeight updates the current block height
func (s *Service) UpdateBlockHeight(height uint32) error {
	if height == 0 {
		return errors.NewProcessingError("Cannot update block height to 0")
	}

	currentHeight := s.currentBlockHeight.Load()
	if height <= currentHeight {
		s.logger.Warnf("Block height %d is not higher than current height %d. Ignoring...", height, currentHeight)
		return nil
	}

	// Trigger a cleanup job for the new block height
	if err := s.triggerCleanup(height); err != nil {
		return err
	}

	s.currentBlockHeight.Store(height)

	return nil
}

// TriggerCleanup triggers a new cleanup job for the specified block height
func (s *Service) triggerCleanup(blockHeight uint32) error {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	if len(s.jobs) >= s.maxJobsHistory {
		if s.jobs[0].GetStatus() == JobStatusPending {
			// This should never happen because each job added marks all older jobs as cancelled
			return errors.NewProcessingError("maximum number of jobs reached (%d)", s.maxJobsHistory)
		}

		// Remove the oldest job
		s.jobs = s.jobs[1:]
	}

	ctx, cancelFunc := context.WithCancel(s.ctx)

	job := &Job{
		BlockHeight: blockHeight,
		Created:     time.Now(),
		ctx:         ctx,
		cancel:      cancelFunc,
	}
	job.setStatus(JobStatusPending)

	// Mark all older jobs that are pending as cancelled
	for i := len(s.jobs) - 1; i >= 0; i-- {
		if s.jobs[i].GetStatus() == JobStatusPending {
			s.jobs[i].setStatus(JobStatusCancelled)
		} else {
			break
		}
	}

	s.jobs = append(s.jobs, job)

	return nil
}

func (s *Service) GetJobs() []*Job {
	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()

	// Create a deep copy of the jobs to avoid data races
	clone := make([]*Job, len(s.jobs))

	for i, job := range s.jobs {
		// Create a new Job and manually copy each field
		// This avoids copying the atomic.Int32 field directly
		status := job.GetStatus() // Get the current status using atomic load

		newJob := &Job{
			BlockHeight: job.BlockHeight,
			Error:       job.Error,
			Created:     job.Created,
			Started:     job.Started,
			Ended:       job.Ended,
			ctx:         job.ctx,
			cancel:      job.cancel,
		}

		// Set the status using atomic store
		newJob.setStatus(status)

		clone[i] = newJob
	}

	return clone
}

// worker processes jobs from the queue
func (s *Service) worker(workerID int) {
	defer s.wg.Done()

	s.logger.Infof("Starting cleanup worker %d", workerID)

	for {
		// Check if the service is shutting down
		select {
		case <-s.ctx.Done():
			s.logger.Infof("Stopping cleanup worker %d", workerID)
			return
		default: // Continue processing
		}

		// Get the next job from the queue
		job := s.getNextJob()
		if job == nil {
			// No jobs available, wait for a signal or timeout
			select {
			case <-time.After(1 * time.Second):
				// Timeout, check again
				continue
			case <-s.ctx.Done():
				// Service is shutting down
				return
			}
		}

		// Process the job
		s.jobProcessor(s, job, workerID)
	}
}

// getNextJob gets the next job from the queue
func (s *Service) getNextJob() *Job {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	if len(s.jobs) == 0 {
		return nil
	}

	job := s.jobs[len(s.jobs)-1] // Get the last job

	if job.GetStatus() != JobStatusPending {
		return nil
	}

	// Mark the job as running
	job.setStatus(JobStatusRunning)

	return job
}

// defaultJobProcessor is the default implementation of JobProcessor
func defaultJobProcessor(s *Service, job *Job, workerID int) {

	s.logger.Infof("Worker %d starting cleanup job for block height %d", workerID, job.BlockHeight)

	// Check if the job has been cancelled
	select {
	case <-job.ctx.Done():
		s.logger.Infof("Worker %d: job %d was cancelled", workerID, job.BlockHeight)
		return
	default: // Continue processing
	}

	job.Started = time.Now()

	// Create a filter expression to find records with DeleteAtHeight <= current block height
	filterExp := aerospike.ExpLessEq(
		aerospike.ExpIntBin(fields.DeleteAtHeight.String()),
		aerospike.ExpIntVal(int64(job.BlockHeight)),
	)

	queryPolicy := aerospike.NewQueryPolicy()
	queryPolicy.FilterExpression = filterExp

	writePolicy := aerospike.NewWritePolicy(0, 0)
	writePolicy.FilterExpression = filterExp

	stmt := aerospike.NewStatement(s.namespace, s.set)

	// Execute the query with a delete operation
	task, err := s.client.QueryExecute(queryPolicy, writePolicy, stmt, aerospike.DeleteOp())
	if err != nil {
		job.setStatus(JobStatusFailed)
		job.Error = err
		job.Ended = time.Now()

		s.logger.Errorf("Worker %d: failed to execute cleanup job %d: %v", workerID, job.BlockHeight, err)

		return
	}

	// Wait for the task to complete
	for {
		// Check if the job has been cancelled
		select {
		case <-job.ctx.Done():
			s.logger.Warnf("Worker %d: job for block heigh %d was cancelled during execution", workerID, job.BlockHeight)
			return
		default: // Continue processing
		}

		status, err := task.IsDone()
		if err != nil {
			// Update job status to failed
			job.setStatus(JobStatusFailed)
			job.Error = err
			job.Ended = time.Now()

			s.logger.Errorf("Worker %d: failed to check status of cleanup job for block height %d: %v", workerID, job.BlockHeight, err)

			return
		}

		if status {
			// Task completed
			break
		}

		// Wait before checking again
		time.Sleep(100 * time.Millisecond)
	}

	job.setStatus(JobStatusCompleted)
	job.Ended = time.Now()

	prometheusUtxoCleanupBatch.Observe(float64(time.Since(job.Started).Microseconds()) / 1_000_000)

	s.logger.Infof("Worker %d completed cleanup job for block height %d in %v", workerID, job.BlockHeight, job.Ended.Sub(job.Started))
}
