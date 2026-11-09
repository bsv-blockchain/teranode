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
func (s *Store) FinalizeTransaction(ctx context.Context, tx *bt.Tx, attempt []byte) error {
	return nil
}

// QueryStaleCreatingTxs returns nothing: records are never created in a tentative state.
func (s *Store) QueryStaleCreatingTxs(ctx context.Context, unminedSinceBefore uint32, limit int) ([]chainhash.Hash, error) {
	return nil, nil
}

// EndCreateFirstRollback is DeleteComplete here. The store has no tentative state, so
// BeginCreateFirstRollback never marks a record and nothing calls this for one.
func (s *Store) EndCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) error {
	return s.DeleteComplete(ctx, hash)
}

// TakeRollbackHold reports false and writes nothing. The store has no tentative state, so no
// record is rolling back and no hold exists.
func (s *Store) TakeRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error) {
	return false, nil
}

// ReleaseRollbackHold does nothing. The store has no tentative state, so no hold is taken.
func (s *Store) ReleaseRollbackHold(ctx context.Context, hash *chainhash.Hash, attempt []byte) error {
	return nil
}

// ClearCreateFirstHold reports false and writes nothing. The store has no tentative state, so
// it never leaves a hold.
func (s *Store) ClearCreateFirstHold(ctx context.Context, hash *chainhash.Hash, adopt bool) (bool, error) {
	return false, nil
}

// BeginCreateFirstRollback reports false and writes nothing. The store has no tentative
// state, so every record is final and there is nothing to roll back.
func (s *Store) BeginCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) (bool, error) {
	return false, nil
}
