// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetState retrieves a value from the state key-value store.
// The state store is a general-purpose persistent storage for arbitrary data within the blockchain database.
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - key: The unique key identifier for the state item to retrieve
//
// Returns:
//   - []byte: The data associated with the key, if found
//   - error: Any error encountered during retrieval (including when key doesn't exist)
func (s *SQL) GetState(ctx context.Context, key string) ([]byte, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetState")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT data
		FROM state
		WHERE key = $1
	`

	var (
		data []byte
		err  error
	)

	if err = s.db.QueryRowContext(ctx, q, key).Scan(
		&data,
	); err != nil {
		return nil, err
	}

	return data, nil
}

// SetState stores or updates a value in the state key-value store.
// Uses an UPSERT (INSERT ... ON CONFLICT DO UPDATE) to atomically insert or update
// in a single query, avoiding the previous two-query pattern of SELECT then INSERT/UPDATE.
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - key: The unique key identifier for the state item to store or update
//   - data: The binary data to store for the given key
//
// Returns:
//   - error: Any error encountered during the storage operation
func (s *SQL) SetState(ctx context.Context, key string, data []byte) error {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:SetState")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		INSERT INTO state (key, data)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET
			data = EXCLUDED.data,
			updated_at = CURRENT_TIMESTAMP
	`

	if _, err := s.db.ExecContext(ctx, q, key, data); err != nil {
		return err
	}

	return nil
}
