package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/conflictwal"
)

// The TeraSlab server exposes only UTXO-record-shaped RPCs, so it cannot hold
// the conflict-resolution WAL (#861). The store therefore opens a dedicated SQL
// connection (Postgres in production, SQLite for development — see New) and
// reuses the shared conflictwal package, the same logic the SQL UTXO backend
// uses. s.walDB / s.walEngine are set up in New().

// BeginConflictIntent durably records a conflict-resolution intent before the
// operation's first state mutation. See the utxo.Store interface contract.
func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	return conflictwal.Begin(ctx, s.walDB, s.walEngine, intent)
}

// CompleteConflictIntent removes the intent record once the operation's terminal
// step has committed.
func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	return conflictwal.Complete(ctx, s.walDB, intentID)
}

// PendingConflictIntents returns every begun-but-not-completed intent for
// startup replay.
func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	return conflictwal.Pending(ctx, s.walDB)
}
