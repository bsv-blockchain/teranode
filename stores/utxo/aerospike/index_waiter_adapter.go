package aerospike

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike/cleanup"
)

// Ensure Store implements the IndexWaiter interface
var _ cleanup.IndexWaiter = (*Store)(nil)

// WaitForIndexBuilt implements the cleanup.IndexWaiter interface
// It's a public wrapper around the private waitForIndexBuilt method
func (s *Store) WaitForIndexReady(ctx context.Context, indexName string) error {
	return s.waitForIndexReady(ctx, indexName)
}
