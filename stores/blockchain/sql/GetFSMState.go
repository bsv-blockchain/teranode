package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/ubsv/errors"
)

func (s *SQL) GetFSMState(ctx context.Context) (string, error) {
	const query = `SELECT fsm_state FROM state WHERE key = $1;`

	var stateStr sql.NullString
	err := s.db.QueryRowContext(ctx, query, "fsm_state").Scan(&stateStr)
	if err != nil {
		//
		if errors.Is(err, sql.ErrNoRows) {
			// Return "" if db has no rows, meaning no state has been set
			return "", nil
		}

		return "", errors.NewStorageError("failed to get FSM state: %v", err)
	}

	if !stateStr.Valid {
		return "", errors.NewStorageError("fsm state is not valid")
	}

	return stateStr.String, nil
}
