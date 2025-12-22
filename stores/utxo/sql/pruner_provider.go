package sql

import (
	"sync"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	sqlpruner "github.com/bsv-blockchain/teranode/stores/utxo/sql/pruner"
)

// Ensure Store implements the pruner.PrunerProvider interface
var _ pruner.PrunerServiceProvider = (*Store)(nil)

// singleton instance of the pruner service
var (
	prunerServiceInstance pruner.Service
	prunerServiceMutex    sync.Mutex
)

// GetPrunerService returns a pruner service for the SQL store.
// This implements the pruner.PrunerProvider interface.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	// Use a mutex to ensure thread safety when creating the singleton
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	// If the service has already been created, return it
	if prunerServiceInstance != nil {
		return prunerServiceInstance, nil
	}

	// Create a new pruner service
	prunerService, err := sqlpruner.NewService(s.settings, sqlpruner.Options{
		Logger: s.logger,
		DB:     s.db,
	})
	if err != nil {
		return nil, err
	}

	// Store the singleton instance
	prunerServiceInstance = prunerService

	return prunerServiceInstance, nil
}
