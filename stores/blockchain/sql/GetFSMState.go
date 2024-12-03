package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/ubsv/errors"
)

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
