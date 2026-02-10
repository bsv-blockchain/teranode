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

	// Prune removes transactions marked for deletion at or before the specified height.
	// blockHashStr is the block hash for logging/tracing purposes.
	// Returns the number of records processed and any error encountered.
	// This method is synchronous and blocks until pruning completes or context is cancelled.
	Prune(ctx context.Context, height uint32, blockHashStr string) (recordsProcessed int64, err error)

	// SetPersistedHeightGetter sets the function used to get block persister progress.
	// This allows pruner to coordinate with block persister to avoid premature deletion.
	SetPersistedHeightGetter(getter func() uint32)

	// AddObserver adds an observer to be notified when pruning completes.
	// This method is thread-safe and can be called after service creation.
	AddObserver(observer Observer)
}

// PrunerServiceProvider defines an interface for stores that can provide a pruner service.
type PrunerServiceProvider interface {
	// GetPrunerService returns a pruner service for the store.
	// Returns nil if the store doesn't support pruner functionality.
	GetPrunerService() (Service, error)
}

// Observer defines an interface for components that want to be notified of pruner events.
type Observer interface {
	// OnPruneComplete is called when a pruning cycle completes.
	// height is the block height that was pruned up to.
	// recordsProcessed is the number of records that were pruned.
	OnPruneComplete(height uint32, recordsProcessed int64)
}
