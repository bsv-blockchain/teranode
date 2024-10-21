package sql

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
)

/*
func (s *SQL) SetFSMState(ctx context.Context, fsmState string) error {
	const query = `
        UPDATE state
        SET fsm_state = $1, updated_at = CURRENT_TIMESTAMP
        WHERE key = $2;
    `

	result, err := s.db.ExecContext(ctx, query, fsmState, "fsm_state")
	if err != nil {
		return errors.NewStorageError("failed to set FSM state: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.NewStorageError("failed to get affected rows: %w", err)
	}

	// If no rows were updated, insert a new row
	if rowsAffected == 0 {
		insertQuery := `
            INSERT INTO state (key, data, fsm_state, inserted_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP);
        `

		_, err = s.db.ExecContext(ctx, insertQuery, "fsm_state", []byte{}, fsmState)
		if err != nil {
			return errors.NewStorageError("failed to insert FSM state: %w", err)
		}
	}

	return nil
}
*/

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
		return errors.NewStorageError("failed to set FSM state: %w", err)
	}

	return nil
}
