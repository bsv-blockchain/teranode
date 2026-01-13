package aerospike

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/teranode/stores/cleanup"
	aerocleanup "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/cleanup"
)

// Ensure Store implements the cleanup.CleanupProvider interface
var _ cleanup.CleanupServiceProvider = (*Store)(nil)

// singleton instance of the cleanup service
var (
	cleanupServiceInstance cleanup.Service
	cleanupServiceMutex    sync.Mutex
	cleanupServiceError    error
)

// ResetCleanupServiceForTests resets the cleanup service singleton.
// This should only be called in tests to ensure clean state between test runs.
func ResetCleanupServiceForTests() {
	cleanupServiceMutex.Lock()
	defer cleanupServiceMutex.Unlock()

	// Stop the existing service if it exists
	if cleanupServiceInstance != nil {
		// Try to stop it gracefully if it's an aerospike cleanup service
		if aerospikeService, ok := cleanupServiceInstance.(*aerocleanup.Service); ok {
			if err := aerospikeService.Stop(context.Background()); err != nil {
				// Log but don't fail - tests need to continue
				_ = err
			}
		}
	}

	cleanupServiceInstance = nil
	cleanupServiceError = nil
}

// GetCleanupService returns a cleanup service for the Aerospike store.
// This implements the cleanup.CleanupProvider interface.
func (s *Store) GetCleanupService() (cleanup.Service, error) {
	// Check if DAH cleaner is disabled in settings
	if s.settings.UtxoStore.DisableDAHCleaner {
		return nil, nil
	}

	// Use a mutex to ensure thread safety when creating the singleton
	cleanupServiceMutex.Lock()
	defer cleanupServiceMutex.Unlock()

	// If the service has already been created, return it
	if cleanupServiceInstance != nil {
		return cleanupServiceInstance, cleanupServiceError
	}

	// Create options for the cleanup service
	opts := aerocleanup.Options{
		Logger:        s.logger,
		Ctx:           s.ctx,
		Client:        s.client,
		ExternalStore: s.externalStore,
		Namespace:     s.namespace,
		Set:           s.setName,
		IndexWaiter:   s,
	}

	// Create a new cleanup service
	cleanupService, err := aerocleanup.NewService(s.settings, opts)
	if err != nil {
		cleanupServiceError = err
		return nil, err
	}

	// Store the singleton instance
	cleanupServiceInstance = cleanupService
	cleanupServiceError = nil

	return cleanupServiceInstance, nil
}
