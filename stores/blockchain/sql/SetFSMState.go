// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the SetFSMState method, which persists the current state of the
// blockchain's Finite State Machine (FSM) to the database. The FSM is used to track and
// manage the blockchain's operational state, including synchronization status, processing
// modes, and recovery states. This functionality is essential for maintaining blockchain
// consistency across node restarts and service interruptions. The implementation uses an
// SQL UPSERT operation to efficiently update or create the FSM state record, and also
// performs strategic cache invalidation to ensure consistency between the persisted state
// and in-memory caches, particularly important when external processes modify the blockchain.
package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
)

// SetFSMState persists the current state of the blockchain's Finite State Machine (FSM).
// This implements the blockchain.Store.SetFSMState interface method.
//
// The method stores the serialized representation of the blockchain's FSM state in the
// database. The FSM tracks the operational state of the blockchain, including synchronization
// status, processing modes, and recovery states. This state information is critical for
// maintaining blockchain consistency across node restarts and service interruptions in
// Teranode's high-throughput architecture.
//
// The implementation creates or updates the FSM state record in a single database operation.
// This approach ensures atomicity and minimizes database round-trips. After persisting
// the state, the method strategically invalidates in-memory caches to ensure consistency
// between the persisted state and cached data.
//
// Cache invalidation is particularly important when the FSM state changes, as it often
// indicates a significant operational mode change (e.g., from synchronizing to running,
// or from running to recovery). External processes like seeders or legacy services may
// also directly modify the blockchain database while the FSM is in certain states,
// necessitating cache invalidation when the state changes back to normal operation.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - fsmState: The serialized FSM state to persist, typically representing the current
//     operational mode of the blockchain
//
// Returns:
//   - error: Any error encountered during the operation, specifically:
//   - StorageError for database errors or cache invalidation failures
func (s *SQL) SetFSMState(ctx context.Context, fsmState string) error {
	// Serialize the FSM state to bytes
	fsmStateData := []byte(fsmState)

	// Use UPSERT to insert or update the FSM state
	const query = `
        INSERT INTO state (key, data, updated_at)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE SET
            data = EXCLUDED.data,
            updated_at = EXCLUDED.updated_at;
    `

	_, err := s.db.ExecContext(ctx, query, "fsm_state", fsmStateData)
	if err != nil {
		return errors.NewStorageError("failed to set FSM state", err)
	}

	// Reset the response cache to invalidate any cached block headers.
	// A seeder, legacy service or similar may have put blocks directly into the DB
	// the assumption is they need set FSM to IDLE or similar, do their work, and then set the FSM back to RUNNING
	s.ResetResponseCache()

	return nil
}
