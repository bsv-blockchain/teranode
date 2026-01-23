package aerospike

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
)

// Ensure Store implements the IndexWaiter interface
var _ pruner.IndexWaiter = (*Store)(nil)

// WaitForIndexBuilt implements the pruner.IndexWaiter interface
// It's a public wrapper around the private waitForIndexBuilt method
func (s *Store) WaitForIndexReady(ctx context.Context, indexName string) error {
	return s.waitForIndexReady(ctx, indexName)
}
