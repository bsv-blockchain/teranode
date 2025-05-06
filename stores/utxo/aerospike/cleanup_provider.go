package aerospike

import (
	"sync"

	"github.com/bitcoin-sv/teranode/stores/cleanup"
	aerocleanup "github.com/bitcoin-sv/teranode/stores/utxo/aerospike/cleanup"
)

// Ensure Store implements the cleanup.CleanupProvider interface
var _ cleanup.CleanupServiceProvider = (*Store)(nil)

// singleton instance of the cleanup service
var (
	cleanupServiceInstance cleanup.Service
	cleanupServiceMutex    sync.Mutex
	cleanupServiceError    error
)

// GetCleanupService returns a cleanup service for the Aerospike store.
// This implements the cleanup.CleanupProvider interface.
func (s *Store) GetCleanupService() (cleanup.Service, error) {
	// Use a mutex to ensure thread safety when creating the singleton
	cleanupServiceMutex.Lock()
	defer cleanupServiceMutex.Unlock()

	// If the service has already been created, return it
	if cleanupServiceInstance != nil {
		return cleanupServiceInstance, cleanupServiceError
	}

	// Create options for the cleanup service
	opts := aerocleanup.Options{
		Logger:    s.logger,
		Client:    s.client,
		Namespace: s.namespace,
		Set:       s.setName,
		// Use default values for WorkerCount and MaxJobsHistory
	}

	// Create a new cleanup service
	cleanupService, err := aerocleanup.NewService(opts)
	if err != nil {
		cleanupServiceError = err
		return nil, err
	}

	// Store the singleton instance
	cleanupServiceInstance = cleanupService
	cleanupServiceError = nil

	return cleanupServiceInstance, nil
}
