package aerospike

import (
	"sync"

	aeropruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

// Ensure Store implements the pruner.PrunerProvider interface
var _ pruner.PrunerServiceProvider = (*Store)(nil)

// singleton instance of the pruner service
var (
	prunerServiceInstance pruner.Service
	prunerServiceMutex    sync.Mutex
	prunerServiceError    error
)

// ResetPrunerServiceForTests resets the pruner service singleton.
// This should only be called in tests to ensure clean state between test runs.
func ResetPrunerServiceForTests() {
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	prunerServiceInstance = nil
	prunerServiceError = nil
}

// GetPrunerService returns a pruner service for the Aerospike store.
// This implements the pruner.PrunerProvider interface.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	// Check if DAH cleaner is disabled in settings
	if s.settings.UtxoStore.DisableDAHCleaner {
		return nil, nil
	}

	// Use a mutex to ensure thread safety when creating the singleton
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	// If the service has already been created, return it
	if prunerServiceInstance != nil {
		return prunerServiceInstance, prunerServiceError
	}

	// Create options for the pruner service
	opts := aeropruner.Options{
		Logger:        s.logger,
		Ctx:           s.ctx,
		Client:        s.client,
		ExternalStore: s.externalStore,
		Namespace:     s.namespace,
		Set:           s.setName,
		IndexWaiter:   s,
	}

	// Create a new pruner service
	prunerService, err := aeropruner.NewService(s.settings, opts)
	if err != nil {
		prunerServiceError = err
		return nil, err
	}

	// Store the singleton instance
	prunerServiceInstance = prunerService
	prunerServiceError = nil

	return prunerServiceInstance, nil
}
