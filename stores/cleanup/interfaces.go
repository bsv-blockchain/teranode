package cleanup

import "context"

type Service interface {
	// Start starts the cleanup service.
	// This should not block.
	// The service should stop when the context is cancelled.
	Start(ctx context.Context)

	// UpdateBlockHeight updates the current block height and triggers cleanup if needed.
	// If doneCh is provided, it will be closed when the job completes.
	UpdateBlockHeight(height uint32, doneCh ...chan string) error
}

// CleanupServiceProvider defines an interface for stores that can provide a cleanup service.
type CleanupServiceProvider interface {
	// GetCleanupService returns a cleanup service for the store.
	// Returns nil if the store doesn't support cleanup.
	GetCleanupService() (Service, error)
}

// JobProcessorFunc is a function type that processes a cleanup job
type JobProcessorFunc func(job *Job, workerID int)

// JobManagerService extends the Service interface with job management capabilities
type JobManagerService interface {
	Service

	// GetJobs returns a copy of the current jobs list (primarily for testing)
	GetJobs() []*Job

	// TriggerCleanup triggers a new cleanup job for the specified block height
	// If doneCh is provided, it will be closed when the job completes
	TriggerCleanup(blockHeight uint32, doneCh ...chan string) error
}
