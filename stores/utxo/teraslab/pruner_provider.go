package teraslab

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

// Ensure Store implements the pruner.PrunerServiceProvider interface.
var _ pruner.PrunerServiceProvider = (*Store)(nil)

// GetPrunerService returns a pruner service for the TeraSlab store.
//
// Unlike the SQL/Aerospike backends — which run a client-side DAH cleaner that
// queries for delete-at-height-expired records and deletes them — TeraSlab's
// server owns the DAH lifecycle: spending the last UTXO of a mined,
// on-longest-chain tx queues it for deletion at DAH = spendHeight + retention,
// and the server reaps the queue when asked to process expirations up to a
// height. The pruner service is therefore a thin trigger over that server op.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	return &prunerService{store: s}, nil
}

// prunerService drives TeraSlab's server-side DAH/preservation reaping.
type prunerService struct {
	store     *Store
	mu        sync.Mutex
	observers []pruner.Observer
}

// Start is a no-op: TeraSlab reaps on demand when Prune is called, so there is
// no background loop to launch. The method exists to satisfy pruner.Service.
func (p *prunerService) Start(_ context.Context) {}

// Prune asks the TeraSlab server to delete every record whose delete-at-height
// (or preservation) has expired at or before height, and returns the number of
// records the server deleted. A store error aborts and propagates — pruning is
// a money-path operation and must not silently skip.
func (p *prunerService) Prune(ctx context.Context, height uint32, _ string) (int64, error) {
	result, err := p.store.client.ProcessExpiredPreservations(ctx, height, p.store.settings.GetUtxoStoreBlockHeightRetention())
	if err != nil {
		return 0, err
	}

	processed := int64(result.Deleted)

	p.mu.Lock()
	observers := make([]pruner.Observer, len(p.observers))
	copy(observers, p.observers)
	p.mu.Unlock()

	for _, o := range observers {
		o.OnPruneComplete(height, processed)
	}

	return processed, nil
}

// AddObserver registers an observer notified after each Prune completes.
func (p *prunerService) AddObserver(observer pruner.Observer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observers = append(p.observers, observer)
}
