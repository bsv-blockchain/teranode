package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// SupportsCreateFirst reports false: the utxoset store has no tentative creating state.
func (s *Store) SupportsCreateFirst() bool {
	return false
}

// FinalizeTransaction is a no-op: records are never created in a tentative state.
func (s *Store) FinalizeTransaction(ctx context.Context, tx *bt.Tx) error {
	return nil
}

// QueryStaleCreatingTxs returns nothing: records are never created in a tentative state.
func (s *Store) QueryStaleCreatingTxs(ctx context.Context, unminedSinceBefore uint32, limit int) ([]chainhash.Hash, error) {
	return nil, nil
}

// BeginCreateFirstRollback reports false and writes nothing. The store has no tentative
// state, so every record is final and there is nothing to roll back.
func (s *Store) BeginCreateFirstRollback(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	return false, nil
}
