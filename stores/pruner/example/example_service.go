package example

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/stores/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// ExamplePrunerService demonstrates how to use the JobManager
type ExamplePrunerService struct {
	jobManager *pruner.JobManager
	logger     ulogger.Logger
}

// NewExamplePrunerService creates a new example pruner service
func NewExamplePrunerService(logger ulogger.Logger) (*ExamplePrunerService, error) {
	// Create a job processor function that will handle the actual pruner work
	jobProcessor := func(job *pruner.Job, workerID int) {
		// Set job as running
		job.SetStatus(pruner.JobStatusRunning)
		job.Started = time.Now()

		logger.Infof("Worker %d starting pruner job for block height %d", workerID, job.BlockHeight)

		// Simulate some work
		time.Sleep(100 * time.Millisecond)

		// Check if the job has been cancelled
		select {
		case <-job.Context().Done():
			logger.Warnf("Worker %d: job for block height %d was cancelled during execution", workerID, job.BlockHeight)
			job.SetStatus(pruner.JobStatusCancelled)
			job.Ended = time.Now()

			if job.DoneCh != nil {
				job.DoneCh <- pruner.JobStatusCancelled.String()
				close(job.DoneCh)
			}

			return
		default: // Continue processing
		}

		// Mark job as completed
		job.SetStatus(pruner.JobStatusCompleted)
		job.Ended = time.Now()

		logger.Infof("Worker %d completed pruner job for block height %d in %v",
			workerID, job.BlockHeight, job.Ended.Sub(job.Started))

		if job.DoneCh != nil {
			job.DoneCh <- pruner.JobStatusCompleted.String()
			close(job.DoneCh)
		}
	}

	// Create the job manager
	jobManager, err := pruner.NewJobManager(pruner.JobManagerOptions{
		Logger:         logger,
		WorkerCount:    2,
		MaxJobsHistory: 100,
		JobProcessor:   jobProcessor,
	})
	if err != nil {
		return nil, err
	}

	return &ExamplePrunerService{
		jobManager: jobManager,
		logger:     logger,
	}, nil
}

// Start starts the pruner service
func (s *ExamplePrunerService) Start(ctx context.Context) {
	s.jobManager.Start(ctx)
}

// UpdateBlockHeight updates the current block height and triggers pruner if needed
func (s *ExamplePrunerService) UpdateBlockHeight(height uint32, doneCh ...chan string) error {
	return s.jobManager.UpdateBlockHeight(height, doneCh...)
}

// GetJobs returns a copy of the current jobs list (primarily for testing)
func (s *ExamplePrunerService) GetJobs() []*pruner.Job {
	return s.jobManager.GetJobs()
}

// TriggerPruner triggers a new pruner job for the specified block height
func (s *ExamplePrunerService) TriggerPruner(blockHeight uint32, doneCh ...chan string) error {
	return s.jobManager.TriggerPruner(blockHeight, doneCh...)
}
