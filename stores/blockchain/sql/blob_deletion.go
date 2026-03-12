package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// ScheduledDeletion represents a blob deletion scheduled for a specific height.
type ScheduledDeletion struct {
	ID             int64
	BlobKey        []byte
	FileType       string
	StoreType      int32
	DeleteAtHeight uint32
	RetryCount     int
}

// ScheduleRequest represents a request to schedule a blob deletion.
type ScheduleRequest struct {
	BlobKey        []byte
	FileType       string
	StoreType      int32
	DeleteAtHeight uint32
}

// ListFilters represents filters for listing scheduled deletions.
type ListFilters struct {
	MinHeight     uint32
	MaxHeight     uint32
	StoreType     int32
	FilterByStore bool
	Limit         int
	Offset        int
}

func (s *SQL) ScheduleBlobDeletion(ctx context.Context, req *ScheduleRequest) (int64, error) {
	insertQuery := `
        INSERT INTO scheduled_blob_deletions
            (blob_key, file_type, store_type, delete_at_height)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (blob_key, file_type, store_type)
        DO UPDATE SET
            delete_at_height = EXCLUDED.delete_at_height,
            retry_count = 0,
            last_retry_at = NULL
    `

	result, err := s.db.ExecContext(ctx, insertQuery,
		req.BlobKey, req.FileType, req.StoreType, req.DeleteAtHeight)
	if err != nil {
		return 0, errors.NewStorageError("failed to schedule blob deletion", err)
	}

	id, err := result.LastInsertId()
	if err != nil || id == 0 {
		selectQuery := `
            SELECT id FROM scheduled_blob_deletions
            WHERE blob_key = $1 AND file_type = $2 AND store_type = $3
        `
		err = s.db.QueryRowContext(ctx, selectQuery, req.BlobKey, req.FileType, req.StoreType).Scan(&id)
		if err != nil {
			return 0, errors.NewStorageError("failed to retrieve blob deletion id", err)
		}
	}

	return id, nil
}

func (s *SQL) CancelBlobDeletion(ctx context.Context, blobKey []byte, fileType string, storeType int32) error {
	query := `
        DELETE FROM scheduled_blob_deletions
        WHERE blob_key = $1 AND file_type = $2 AND store_type = $3
        RETURNING id
    `

	var deletedID int64
	err := s.db.QueryRowContext(ctx, query, blobKey, fileType, storeType).Scan(&deletedID)
	if err != nil {
		if err == sql.ErrNoRows {
			return errors.NewNotFoundError("no pending deletion found")
		}
		return errors.NewStorageError("failed to cancel blob deletion", err)
	}

	return nil
}

func (s *SQL) GetPendingBlobDeletions(ctx context.Context, height uint32, limit int) ([]*ScheduledDeletion, error) {
	query := `
        SELECT id, blob_key, file_type, store_type, delete_at_height, retry_count
        FROM scheduled_blob_deletions
        WHERE delete_at_height <= $1
        ORDER BY delete_at_height ASC, id ASC
        LIMIT $2
    `

	if s.engine == "postgres" {
		query += "\n        FOR UPDATE SKIP LOCKED"
	}

	rows, err := s.db.QueryContext(ctx, query, height, limit)
	if err != nil {
		return nil, errors.NewStorageError("failed to get pending deletions", err)
	}
	defer rows.Close()

	var deletions []*ScheduledDeletion
	for rows.Next() {
		var d ScheduledDeletion
		err := rows.Scan(&d.ID, &d.BlobKey, &d.FileType, &d.StoreType,
			&d.DeleteAtHeight, &d.RetryCount)
		if err != nil {
			return nil, errors.NewStorageError("failed to scan deletion row", err)
		}
		deletions = append(deletions, &d)
	}

	return deletions, rows.Err()
}

func (s *SQL) RemoveBlobDeletion(ctx context.Context, id int64) error {
	query := `DELETE FROM scheduled_blob_deletions WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return errors.NewStorageError("failed to remove blob deletion", err)
	}
	return nil
}

func (s *SQL) IncrementBlobDeletionRetry(ctx context.Context, id int64, maxRetries int) (shouldRemove bool, newRetryCount int, err error) {
	query := `
        UPDATE scheduled_blob_deletions
        SET retry_count = retry_count + 1,
            last_retry_at = CURRENT_TIMESTAMP
        WHERE id = $1
        RETURNING retry_count
    `

	err = s.db.QueryRowContext(ctx, query, id).Scan(&newRetryCount)
	if err != nil {
		return false, 0, errors.NewStorageError("failed to increment retry count", err)
	}

	return newRetryCount >= maxRetries, newRetryCount, nil
}

func (s *SQL) ListScheduledBlobDeletions(ctx context.Context, filters *ListFilters) ([]*ScheduledDeletion, int, error) {
	where := []string{}
	args := []any{}
	argNum := 1

	if filters.MinHeight > 0 {
		where = append(where, fmt.Sprintf("delete_at_height >= $%d", argNum))
		args = append(args, filters.MinHeight)
		argNum++
	}

	if filters.MaxHeight > 0 {
		where = append(where, fmt.Sprintf("delete_at_height <= $%d", argNum))
		args = append(args, filters.MaxHeight)
		argNum++
	}

	if filters.FilterByStore {
		where = append(where, fmt.Sprintf("store_type = $%d", argNum))
		args = append(args, filters.StoreType)
		argNum++
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = "WHERE " + strings.Join(where, " AND ")
	}

	//nolint:gosec
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM scheduled_blob_deletions %s", whereClause)
	var total int
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, errors.NewStorageError("failed to count scheduled deletions", err)
	}

	//nolint:gosec
	query := fmt.Sprintf(`
        SELECT id, blob_key, file_type, store_type, delete_at_height, retry_count
        FROM scheduled_blob_deletions
        %s
        ORDER BY delete_at_height ASC, id ASC
        LIMIT $%d OFFSET $%d
    `, whereClause, argNum, argNum+1)

	args = append(args, filters.Limit, filters.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, errors.NewStorageError("failed to list scheduled deletions", err)
	}
	defer rows.Close()

	var deletions []*ScheduledDeletion
	for rows.Next() {
		var d ScheduledDeletion
		err := rows.Scan(&d.ID, &d.BlobKey, &d.FileType, &d.StoreType,
			&d.DeleteAtHeight, &d.RetryCount)
		if err != nil {
			return nil, 0, errors.NewStorageError("failed to scan deletion row", err)
		}
		deletions = append(deletions, &d)
	}

	return deletions, total, rows.Err()
}

// CompleteBlobDeletions handles batch completion of multiple deletions.
// Returns (removed_count, retry_incremented_count, error)
func (s *SQL) CompleteBlobDeletions(ctx context.Context, completedIDs []int64, failedIDs []int64, maxRetries int) (int, int, error) {
	if len(completedIDs) == 0 && len(failedIDs) == 0 {
		return 0, 0, nil
	}

	// Start a transaction for atomicity
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, errors.NewStorageError("failed to begin transaction", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	removedCount := 0
	retryIncrementedCount := 0

	// Remove completed deletions
	if len(completedIDs) > 0 {
		// Build placeholders for IN clause ($1, $2, $3, ...)
		placeholders := make([]string, len(completedIDs))
		args := make([]interface{}, len(completedIDs))
		for i, id := range completedIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}

		// Build query with placeholders (safe from SQL injection)
		placeholderStr := strings.Join(placeholders, ",")
		//#nosec G202 -- Safe: uses parameterized placeholders ($1, $2, etc), not user input
		query := "DELETE FROM scheduled_blob_deletions WHERE id IN (" + placeholderStr + ")"
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return 0, 0, errors.NewStorageError("failed to remove completed deletions", err)
		}

		rows, _ := result.RowsAffected()
		removedCount += int(rows)
	}

	// Handle failed deletions - increment retry count or remove if max retries exceeded
	if len(failedIDs) > 0 {
		for _, id := range failedIDs {
			// Increment retry count and check if we should remove
			updateQuery := `
				UPDATE scheduled_blob_deletions
				SET retry_count = retry_count + 1,
					last_retry_at = CURRENT_TIMESTAMP
				WHERE id = $1
				RETURNING retry_count
			`

			var newRetryCount int
			err := tx.QueryRowContext(ctx, updateQuery, id).Scan(&newRetryCount)
			if err != nil {
				// Row might have been deleted, skip
				continue
			}

			if newRetryCount >= maxRetries {
				// Max retries exceeded - remove it
				deleteQuery := `DELETE FROM scheduled_blob_deletions WHERE id = $1`
				_, err := tx.ExecContext(ctx, deleteQuery, id)
				if err == nil {
					removedCount++
				}
			} else {
				retryIncrementedCount++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, errors.NewStorageError("failed to commit transaction", err)
	}

	return removedCount, retryIncrementedCount, nil
}

// AcquireBlobDeletionBatch acquires a batch with locking using SELECT...FOR UPDATE SKIP LOCKED.
// The lockTimeoutSeconds parameter is currently unused but reserved for future use.
func (s *SQL) AcquireBlobDeletionBatch(ctx context.Context, height uint32, limit int, lockTimeoutSeconds int) ([]*ScheduledDeletion, error) {
	query := `
        SELECT id, blob_key, file_type, store_type, delete_at_height, retry_count
        FROM scheduled_blob_deletions
        WHERE delete_at_height <= $1
        ORDER BY delete_at_height ASC, id ASC
        LIMIT $2
    `

	// Add locking for PostgreSQL
	if s.engine == "postgres" {
		query += "\n        FOR UPDATE SKIP LOCKED"
	}

	rows, err := s.db.QueryContext(ctx, query, height, limit)
	if err != nil {
		return nil, errors.NewStorageError("failed to acquire deletion batch", err)
	}
	defer rows.Close()

	var deletions []*ScheduledDeletion
	for rows.Next() {
		var d ScheduledDeletion
		err := rows.Scan(&d.ID, &d.BlobKey, &d.FileType, &d.StoreType,
			&d.DeleteAtHeight, &d.RetryCount)
		if err != nil {
			return nil, errors.NewStorageError("failed to scan deletion row", err)
		}
		deletions = append(deletions, &d)
	}

	return deletions, rows.Err()
}
