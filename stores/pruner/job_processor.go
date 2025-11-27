package pruner

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/go-utils"
)

// JobManager manages background jobs for cleaning up records based on block height
type JobManager struct {
	mu                 sync.RWMutex
	logger             ulogger.Logger
	workerCount        int
	maxJobsHistory     int
	jobs               []*Job
	jobsMutex          sync.RWMutex
	currentBlockHeight atomic.Uint32
	ctx                context.Context
	cancelFunc         context.CancelFunc
	wg                 sync.WaitGroup
	jobProcessor       JobProcessorFunc
	workersStarted     bool
}

// JobManagerOptions contains configuration options for the job manager
type JobManagerOptions struct {
	// Logger is the logger to use
	Logger ulogger.Logger

	// WorkerCount is the number of worker goroutines to use
	WorkerCount int

	// MaxJobsHistory is the maximum number of jobs to keep in history
	MaxJobsHistory int

	// JobProcessor is the function that processes jobs
	JobProcessor JobProcessorFunc
}

// DefaultWorkerCount is the default number of worker goroutines
const DefaultWorkerCount = 4

// DefaultMaxJobsHistory is the default number of jobs to keep in history
const DefaultMaxJobsHistory = 1000

// NewJobManager creates a new job manager
func NewJobManager(opts JobManagerOptions) (*JobManager, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
	}

	if opts.JobProcessor == nil {
		return nil, errors.NewProcessingError("job processor is required")
	}

	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}

	maxJobsHistory := opts.MaxJobsHistory
	if maxJobsHistory <= 0 {
		maxJobsHistory = DefaultMaxJobsHistory
	}

	return &JobManager{
		logger:         opts.Logger,
		workerCount:    workerCount,
		maxJobsHistory: maxJobsHistory,
		jobProcessor:   opts.JobProcessor,
	}, nil
}

// Start starts the job manager
func (m *JobManager) Start(ctx context.Context) {
	// Check if workers are already started
	m.mu.RLock()
	workersStarted := m.workersStarted
	m.mu.RUnlock()

	// If workers are already running, don't start them again
	if workersStarted {
		return
	}

	// Create a new context if one is not provided
	if ctx == nil {
		ctx = context.Background()
	}

	// Store the context and create a cancel function
	m.mu.Lock()
	// Always create a cancellable context
	ctx, cancelFunc := context.WithCancel(ctx)
	m.ctx = ctx
	m.cancelFunc = cancelFunc
	m.workersStarted = true
	m.mu.Unlock()

	// Start the worker pool
	m.logger.Infof("[JobManager] Starting %d workers for pruner service", m.workerCount)

	for i := 0; i < m.workerCount; i++ {
		m.wg.Add(1)

		workerID := i

		go func() {
			defer m.wg.Done()
			m.worker(workerID)
		}()
	}
}

// Stop stops the job manager and waits for all workers to exit
func (m *JobManager) Stop() {
	m.mu.Lock()
	if m.cancelFunc != nil {
		m.cancelFunc()
	}
	m.mu.Unlock()

	// Signal any running jobs before waiting for workers
	m.jobsMutex.Lock()
	for _, job := range m.jobs {
		status := job.GetStatus()
		if status == JobStatusRunning || status == JobStatusPending {
			// Mark as cancelled and signal the channel
			job.SetStatus(JobStatusCancelled)
			job.Ended = time.Now()

			// Signal the doneCh so waiting goroutines aren't blocked
			m.sendAndClose(job.DoneCh, JobStatusCancelled.String())

			// Cancel the job's context if it has one
			job.Cancel()
		}
	}
	m.jobsMutex.Unlock()

	// Wait for all workers to exit
	m.wg.Wait()
}

// UpdateBlockHeight updates the current block height and triggers pruner if needed
func (m *JobManager) UpdateBlockHeight(height uint32, doneCh ...chan string) error {
	// Store the new block height
	m.currentBlockHeight.Store(height)

	// Trigger a pruner job for this height
	return m.TriggerPruner(height, doneCh...)
}

// TriggerPruner triggers a new pruner job for the specified block height
func (m *JobManager) TriggerPruner(blockHeight uint32, doneCh ...chan string) error {
	m.jobsMutex.Lock()
	defer m.jobsMutex.Unlock()

	// Check if we should add this job
	// Only add the job if it's for a higher block height than the last job in the slice
	if len(m.jobs) > 0 && blockHeight <= m.jobs[len(m.jobs)-1].BlockHeight {
		// We already have a job for this or a higher block height
		// Find the job with this block height and check its status
		for i := len(m.jobs) - 1; i >= 0; i-- {
			if m.jobs[i].BlockHeight == blockHeight {
				status := m.jobs[i].GetStatus()

				// If the job is already completed or failed, signal the new doneCh with the same status
				if status == JobStatusCompleted || status == JobStatusFailed {
					if len(doneCh) > 0 {
						m.sendAndClose(doneCh[0], status.String())
					}

					return nil
				}

				// If the job is pending or running, replace its doneCh with the new one
				// This ensures that the new test will get the signal when the job completes
				if status == JobStatusPending || status == JobStatusRunning {
					// If there's an existing doneCh, close it with a cancelled status
					m.sendAndClose(m.jobs[i].DoneCh, JobStatusCancelled.String())

					// Replace with the new doneCh
					if len(doneCh) > 0 {
						m.jobs[i].DoneCh = doneCh[0]
					}

					return nil
				}

				break
			}
		}

		// If we didn't find a job with this block height, signal the doneCh
		if len(doneCh) > 0 {
			m.sendAndClose(doneCh[0], JobStatusCancelled.String())
		}

		return nil
	}

	// Cancel all pending jobs regardless of block height
	// This ensures that any test waiting on a doneCh will receive a signal
	for i := len(m.jobs) - 1; i >= 0; i-- {
		if m.jobs[i].GetStatus() == JobStatusPending {
			m.jobs[i].SetStatus(JobStatusCancelled)
			m.jobs[i].Ended = time.Now()

			m.sendAndClose(m.jobs[i].DoneCh, JobStatusCancelled.String())

			m.jobs[i].Cancel()
		} else {
			break
		}
	}

	// Trim the history from the beginning, but stop if we encounter a running or pending job
	for len(m.jobs) >= m.maxJobsHistory {
		// Check if the first job is running or pending
		status := m.jobs[0].GetStatus()
		if status == JobStatusRunning || status == JobStatusPending {
			// Can't remove running or pending jobs
			break
		}

		// If it's a pending job (which shouldn't happen due to the check above, but just to be safe)
		if status == JobStatusPending {
			m.jobs[0].SetStatus(JobStatusCancelled)
			m.jobs[0].Ended = time.Now()

			m.sendAndClose(m.jobs[0].DoneCh, JobStatusCancelled.String())

			m.jobs[0].Cancel()
		}

		// Remove the first job
		m.jobs = m.jobs[1:]
	}

	// Create a new context for this job
	m.mu.RLock()
	ctx := m.ctx
	m.mu.RUnlock()

	// If the context is nil, create a background context
	if ctx == nil {
		ctx = context.Background()
	}

	// Create a new job
	job := NewJob(blockHeight, ctx, doneCh...)

	// Add the job to the history
	m.jobs = append(m.jobs, job)

	return nil
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (m *JobManager) GetJobs() []*Job {
	m.jobsMutex.RLock()
	defer m.jobsMutex.RUnlock()

	// Create a simple copy for testing purposes
	clone := make([]*Job, len(m.jobs))

	for i, job := range m.jobs {
		// Create a new Job with just the fields needed for tests
		newJob := &Job{
			BlockHeight: job.BlockHeight,
			DoneCh:      job.DoneCh,
		}

		// Set the status using atomic store
		newJob.SetStatus(job.GetStatus())

		clone[i] = newJob
	}

	return clone
}

// worker processes jobs from the queue
func (m *JobManager) worker(workerID int) {
	m.logger.Debugf("[JobManager] Worker %d started", workerID)

	for {
		// Check if the service is shutting down
		select {
		case <-m.ctx.Done():
			m.logger.Debugf("[JobManager] Worker %d shutting down", workerID)
			return
		default: // Continue processing
		}

		// Get the next job
		job := m.getNextJob()
		if job == nil {
			// No jobs available, wait a bit
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Process the job
		m.jobProcessor(job, workerID)

		m.sendAndClose(job.DoneCh, job.GetStatus().String())
	}
}

// sendAndClose attempts to send a value to a channel and then closes it.
// It handles the case where the channel might already be closed, logging any failures.
func (m *JobManager) sendAndClose(ch chan string, value string) {
	if ch == nil {
		return
	}

	utils.SafeSend(ch, value, time.Millisecond*10) // need timeout to avoid blocking
	utils.SafeClose(ch)
}

// getNextJob gets the next job from the queue
func (m *JobManager) getNextJob() *Job {
	m.jobsMutex.Lock()
	defer m.jobsMutex.Unlock()

	// Get the newest pending job (last in the slice)
	for i := len(m.jobs) - 1; i >= 0; i-- {
		if m.jobs[i].GetStatus() == JobStatusPending {
			// Mark the job as running
			m.jobs[i].SetStatus(JobStatusRunning)
			m.jobs[i].Started = time.Now()

			return m.jobs[i]
		}
	}

	return nil
}
