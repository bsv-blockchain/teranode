package usql

import (
	"context"
	"database/sql"

	"github.com/ordishs/gocore"
)

var (
	stat = gocore.NewStat("SQL")
)

// DB is a wrapper around sql.DB that provides performance instrumentation,
// statistics tracking, and retry logic with exponential backoff for all SQL operations.
type DB struct {
	*sql.DB
	retryConfig RetryConfig
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &DB{
		DB:          db,
		retryConfig: DefaultRetryConfig(),
	}, nil
}

// SetRetryConfig sets the retry configuration for this database connection
func (db *DB) SetRetryConfig(config RetryConfig) {
	db.retryConfig = config
}

// GetRetryConfig returns the current retry configuration
func (db *DB) GetRetryConfig() RetryConfig {
	return db.retryConfig
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return retryQueryOperationNoContext(db.retryConfig, func() (*sql.Rows, error) {
		return db.DB.Query(query, args...)
	})
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return retryQueryOperation(ctx, db.retryConfig, func() (*sql.Rows, error) {
		return db.DB.QueryContext(ctx, query, args...)
	})
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	// Note: QueryRow doesn't return an error directly, it defers error checking to Scan()
	// So we cannot apply retry logic here without breaking the API
	// The error will be retried at the transaction/query level instead
	return db.DB.QueryRow(query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	// Note: QueryRow doesn't return an error directly, it defers error checking to Scan()
	// So we cannot apply retry logic here without breaking the API
	// The error will be retried at the transaction/query level instead
	return db.DB.QueryRowContext(ctx, query, args...)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return retryExecOperationNoContext(db.retryConfig, func() (sql.Result, error) {
		return db.DB.Exec(query, args...)
	})
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return retryExecOperation(ctx, db.retryConfig, func() (sql.Result, error) {
		return db.DB.ExecContext(ctx, query, args...)
	})
}
