package example

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/stores/cleanup"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// ExampleCleanupService demonstrates how to use the JobManager
type ExampleCleanupService struct {
	jobManager *cleanup.JobManager
	logger     ulogger.Logger
}

// NewExampleCleanupService creates a new example cleanup service
func NewExampleCleanupService(logger ulogger.Logger) (*ExampleCleanupService, error) {
	// Create a job processor function that will handle the actual cleanup work
	jobProcessor := func(job *cleanup.Job, workerID int) {
		// Set job as running
		job.SetStatus(cleanup.JobStatusRunning)
		job.Started = time.Now()

		logger.Infof("Worker %d starting cleanup job for block height %d", workerID, job.BlockHeight)

		// Simulate some work
		time.Sleep(100 * time.Millisecond)

		// Check if the job has been cancelled
		select {
		case <-job.Context().Done():
			logger.Warnf("Worker %d: job for block height %d was cancelled during execution", workerID, job.BlockHeight)
			job.SetStatus(cleanup.JobStatusCancelled)
			job.Ended = time.Now()

			if job.DoneCh != nil {
				job.DoneCh <- cleanup.JobStatusCancelled.String()
				close(job.DoneCh)
			}

			return
		default: // Continue processing
		}

		// Mark job as completed
		job.SetStatus(cleanup.JobStatusCompleted)
		job.Ended = time.Now()

		logger.Infof("Worker %d completed cleanup job for block height %d in %v",
			workerID, job.BlockHeight, job.Ended.Sub(job.Started))

		if job.DoneCh != nil {
			job.DoneCh <- cleanup.JobStatusCompleted.String()
			close(job.DoneCh)
		}
	}

	// Create the job manager
	jobManager, err := cleanup.NewJobManager(cleanup.JobManagerOptions{
		Logger:         logger,
		WorkerCount:    2,
		MaxJobsHistory: 100,
		JobProcessor:   jobProcessor,
	})
	if err != nil {
		return nil, err
	}

	return &ExampleCleanupService{
		jobManager: jobManager,
		logger:     logger,
	}, nil
}

// Start starts the cleanup service
func (s *ExampleCleanupService) Start(ctx context.Context) {
	s.jobManager.Start(ctx)
}

// UpdateBlockHeight updates the current block height and triggers cleanup if needed
func (s *ExampleCleanupService) UpdateBlockHeight(height uint32, doneCh ...chan string) error {
	return s.jobManager.UpdateBlockHeight(height, doneCh...)
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *ExampleCleanupService) GetJobs() []*cleanup.Job {
	return s.jobManager.GetJobs()
}

// TriggerCleanup triggers a new cleanup job for the specified block height
func (s *ExampleCleanupService) TriggerCleanup(blockHeight uint32, doneCh ...chan string) error {
	return s.jobManager.TriggerCleanup(blockHeight, doneCh...)
}
