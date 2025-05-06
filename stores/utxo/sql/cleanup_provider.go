package sql

import (
	"sync"

	"github.com/bitcoin-sv/teranode/stores/cleanup"
	sqlcleanup "github.com/bitcoin-sv/teranode/stores/utxo/sql/cleanup"
)

// Ensure Store implements the cleanup.CleanupProvider interface
var _ cleanup.CleanupServiceProvider = (*Store)(nil)

// singleton instance of the cleanup service
var (
	cleanupServiceInstance cleanup.Service
	cleanupServiceMutex    sync.Mutex
)

// GetCleanupService returns a cleanup service for the SQL store.
// This implements the cleanup.CleanupProvider interface.
func (s *Store) GetCleanupService() (cleanup.Service, error) {
	// Use a mutex to ensure thread safety when creating the singleton
	cleanupServiceMutex.Lock()
	defer cleanupServiceMutex.Unlock()

	// If the service has already been created, return it
	if cleanupServiceInstance != nil {
		return cleanupServiceInstance, nil
	}

	maxJobHistory := 10

	// Create a new cleanup service
	cleanupService, err := sqlcleanup.NewService(sqlcleanup.Options{
		Logger:         s.logger,
		DB:             s.db,
		MaxJobsHistory: maxJobHistory,
	})
	if err != nil {
		return nil, err
	}

	// Store the singleton instance
	cleanupServiceInstance = cleanupService

	return cleanupServiceInstance, nil
}
