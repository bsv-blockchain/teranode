package sql

import (
	"context"
	"database/sql"
	"fmt"
)

func (s *SQL) GetFSMState(ctx context.Context) (string, error) {
	const query = `SELECT fsm_state FROM state WHERE key = $1;`

	var stateStr sql.NullString
	err := s.db.QueryRowContext(ctx, query, "fsm_state").Scan(&stateStr)
	if err != nil {
		if err == sql.ErrNoRows {
			// Return default state or handle accordingly
			return "", nil
		}
		return "", fmt.Errorf("failed to get FSM state: %w", err)
	}

	if !stateStr.Valid {
		return "", fmt.Errorf("FSM state is NULL")
	}

	// Map the string back to FSMStateType
	//fsmStateValue, ok := blockchain_api.FSMStateType_value[stateStr.String]
	//if !ok {
	//	return blockchain.FSMStateType(0), fmt.Errorf("invalid FSM state value: %s", stateStr.String)
	//}

	return stateStr.String, nil
}
