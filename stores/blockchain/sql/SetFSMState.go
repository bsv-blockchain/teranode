package sql

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
)

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

	// Reset the blocks cache.
	// A seeder, legacy service or similar may have put blocks directly into the DB
	// the assumption is they need set FSM to IDLE or similar, do their work, and then set the FSM back to RUNNING
	if err = s.ResetBlocksCache(ctx); err != nil {
		return errors.NewStorageError("error clearing caches", err)
	}

	s.ResetResponseCache()

	return nil
}
