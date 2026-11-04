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

// DeleteCompleteIfCreating deletes unconditionally, because the store has no tentative state.
func (s *Store) DeleteCompleteIfCreating(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	if err := s.DeleteComplete(ctx, hash); err != nil {
		return false, err
	}

	return true, nil
}
