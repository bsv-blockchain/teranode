package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/conflictwal"
)

// This file wires the SQL backend's utxo.Store conflict-resolution WAL methods
// (#861) to the shared, store-agnostic conflictwal package. The intents live in
// the conflict_intents table (DDL in createPostgresSchemaImpl / createSqliteSchema).

// BeginConflictIntent durably records an intent before the operation's first
// state mutation. Idempotent on the deterministic intent id.
func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	return conflictwal.Begin(ctx, s.db, s.engine, intent)
}

// CompleteConflictIntent removes the intent record once the terminal step
// committed. Removing an absent intent is idempotent (no error).
func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	return conflictwal.Complete(ctx, s.db, intentID)
}

// PendingConflictIntents returns every begun-but-not-completed intent.
func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	return conflictwal.Pending(ctx, s.db)
}
