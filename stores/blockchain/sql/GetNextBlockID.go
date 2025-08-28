package sql

import (
	"context"

	"github.com/bitcoin-sv/teranode/util"
)

// GetNextBlockID retrieves the next available block ID from the database.
// It ensures that block IDs are assigned sequentially and uniquely.
//
// Parameters:
//   - ctx: Context for managing request lifecycle and cancellation
//
// Returns:
//   - uint64: The next available block ID
//   - error: Error if retrieval fails
func (s *SQL) GetNextBlockID(ctx context.Context) (uint64, error) {
	if s.engine == util.Postgres {
		return s.getNextBlockIdFromPostgres(ctx)
	}

	return s.getNextBlockIdFromSQLite(ctx)
}

func (s *SQL) getNextBlockIdFromPostgres(ctx context.Context) (uint64, error) {
	// get the next id from the blocks tables in the database
	q := `
		SELECT nextval(pg_get_serial_sequence('blocks', 'id')) AS next_id;
	`
	var nextID uint64

	if err := s.db.QueryRowContext(ctx, q).Scan(&nextID); err != nil {
		return 0, err
	}

	return nextID, nil
}

func (s *SQL) getNextBlockIdFromSQLite(ctx context.Context) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	// Step 1: increment sequence
	_, err = tx.Exec(`
        UPDATE sqlite_sequence
        SET seq = seq + 1
        WHERE name = 'blocks'
    `)
	if err != nil {
		return 0, err
	}

	// Step 2: fetch the new value
	var id uint64
	err = tx.QueryRow(`
        SELECT seq
        FROM sqlite_sequence
        WHERE name = 'blocks'
    `).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}
