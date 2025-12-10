// Package pruner provides a job management framework for background cleanup operations
// based on blockchain height. It enables stores to automatically prune old data when
// blocks reach a certain age, coordinating with block persistence to ensure data safety.
package pruner

import "context"

// Service defines the interface for a pruner service that manages background cleanup jobs.
type Service interface {
	// Start starts the pruner service.
	// This should not block.
	// The service should stop when the context is cancelled.
	Start(ctx context.Context)

	// UpdateBlockHeight updates the current block height and triggers pruner if needed.
	// If doneCh is provided, it will be closed when the job completes.
	UpdateBlockHeight(height uint32, doneCh ...chan string) error

	// SetPersistedHeightGetter sets the function used to get block persister progress.
	// This allows pruner to coordinate with block persister to avoid premature deletion.
	SetPersistedHeightGetter(getter func() uint32)
}

// PrunerServiceProvider defines an interface for stores that can provide a pruner service.
type PrunerServiceProvider interface {
	// GetPrunerService returns a pruner service for the store.
	// Returns nil if the store doesn't support pruner functionality.
	GetPrunerService() (Service, error)
}

// JobProcessorFunc is a function type that processes a pruner job.
// Implementations should perform the cleanup work, update job status and timing,
// handle context cancellation, and signal completion through DoneCh if provided.
type JobProcessorFunc func(job *Job, workerID int)

// JobManagerService extends Service with job management capabilities for testing and monitoring.
type JobManagerService interface {
	Service

	// GetJobs returns a copy of the current jobs list (primarily for testing)
	GetJobs() []*Job

	// TriggerPruner triggers a new pruner job for the specified block height
	// If doneCh is provided, it will be closed when the job completes
	TriggerPruner(blockHeight uint32, doneCh ...chan string) error
}
