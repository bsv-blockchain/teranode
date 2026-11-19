package pruner

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type postgresPruneStatement struct {
	query string
	args  []any
	step  string
}

// prunePostgresBatch sends the existing ordered statements together. pgx runs
// them in one implicit transaction, retaining the temporary table's index and
// READ COMMITTED snapshots between statements. Closing the results checks the
// commit before the connection returns to database/sql's pool.
//
// QueryExecModeExec does not prepare later statements before creating their
// temporary table. Other modes and drivers use pruneCandidates' transaction
// path, preserving support for callers that supply their own database handle.
func (s *Service) prunePostgresBatch(ctx context.Context, statements [6]postgresPruneStatement) (deleted, heldBack int64, batched bool, err error) {
	connection, err := s.db.Conn(ctx)
	if err != nil {
		return 0, 0, true, &pruneStepError{step: "failed to begin pruning transaction", err: err}
	}
	defer connection.Close()

	err = connection.Raw(func(raw any) error {
		driver, ok := raw.(*stdlib.Conn)
		if !ok || driver.Conn().Config().DefaultQueryExecMode != pgx.QueryExecModeExec {
			return nil
		}
		batched = true

		batch := &pgx.Batch{}
		for _, statement := range statements {
			batch.Queue(statement.query, statement.args...)
		}
		results := driver.Conn().SendBatch(ctx, batch)
		defer results.Close()

		for i, statement := range statements {
			tag, execErr := results.Exec()
			if execErr != nil {
				return &pruneStepError{step: statement.step, err: execErr}
			}
			switch i {
			case 3:
				heldBack = tag.RowsAffected()
			case 5:
				deleted = tag.RowsAffected()
			}
		}
		if closeErr := results.Close(); closeErr != nil {
			return &pruneStepError{step: "failed to commit pruning transaction", err: closeErr}
		}
		return nil
	})
	if err != nil {
		return 0, 0, batched, err
	}
	return deleted, heldBack, batched, nil
}
