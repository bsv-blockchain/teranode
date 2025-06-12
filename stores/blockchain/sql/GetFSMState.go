// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetFSMState method, which retrieves the current state of the
// blockchain's Finite State Machine (FSM). The FSM is used to track and manage the blockchain's
// operational state, including synchronization status, processing modes, and recovery states.
// This functionality is essential for maintaining blockchain consistency, coordinating state
// transitions, and managing recovery processes. The implementation performs a simple SQL query
// to retrieve the serialized FSM state from a dedicated state table, providing a persistent
// record of the blockchain's operational state across node restarts and service interruptions.
package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
)

// GetFSMState retrieves the current state of the blockchain's Finite State Machine (FSM).
// This implements the blockchain.Store.GetFSMState interface method.
//
// The method retrieves the serialized representation of the blockchain's FSM state from
// persistent storage. The FSM tracks the operational state of the blockchain, including
// synchronization status, processing modes, and recovery states. This state information
// is critical for maintaining blockchain consistency, coordinating state transitions,
// and managing recovery processes in Teranode's high-throughput architecture.
//
// The implementation performs a simple SQL query to retrieve the FSM state from a dedicated
// state table using the key 'fsm_state'. If no state is found (which might occur during
// initial setup or after a database reset), an empty string is returned, allowing the
// calling code to initialize the FSM with default values.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//
// Returns:
//   - string: The serialized FSM state as a string, or an empty string if no state is found
//   - error: Any error encountered during retrieval, specifically:
//     - StorageError for database errors or processing failures
func (s *SQL) GetFSMState(ctx context.Context) (string, error) {
	const query = `SELECT data FROM state WHERE key = $1;`

	var data []byte

	err := s.db.QueryRowContext(ctx, query, "fsm_state").Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			// Return default state or handle accordingly
			return "", nil
		}

		return "", errors.NewStorageError("failed to get FSM state", err)
	}

	// Deserialize the data back to string
	fsmState := string(data)

	return fsmState, nil
}
