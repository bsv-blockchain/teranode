package packedsql

import (
	"sync"

	packedpruner "github.com/bsv-blockchain/teranode/stores/utxo/packedsql/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

var _ pruner.PrunerServiceProvider = (*Store)(nil)

var (
	prunerServiceInstance pruner.Service
	prunerServiceMutex    sync.Mutex
)

func ResetPrunerServiceForTests() {
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	prunerServiceInstance = nil
}

func (s *Store) GetPrunerService() (pruner.Service, error) {
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	if prunerServiceInstance != nil {
		return prunerServiceInstance, nil
	}

	prunerService, err := packedpruner.NewService(s.settings, packedpruner.Options{
		Logger: s.logger,
		Pool:   s.pool,
	})
	if err != nil {
		return nil, err
	}

	prunerServiceInstance = prunerService

	return prunerServiceInstance, nil
}
