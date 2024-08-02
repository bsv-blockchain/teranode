package sql

import (
	"context"

	"github.com/bitcoin-sv/ubsv/tracing"
)

func (s *SQL) GetState(ctx context.Context, key string) ([]byte, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetState")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT data
		FROM state
		WHERE key = $1
	`

	var data []byte
	var err error
	if err = s.db.QueryRowContext(ctx, q, key).Scan(
		&data,
	); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *SQL) SetState(ctx context.Context, key string, data []byte) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:SetState")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var q string
	currentState, _ := s.GetState(ctx, key)
	if currentState != nil {
		q = `
		UPDATE state
		SET data = $2, updated_at = CURRENT_TIMESTAMP
		WHERE key = $1
	`
	} else {
		q = `
		INSERT INTO state (key, data)
		VALUES ($1, $2)
	`
	}

	if _, err := s.db.ExecContext(ctx, q, key, data); err != nil {
		return err
	}

	return nil
}
