package usql

import (
	"context"
	"database/sql"

	"github.com/ordishs/gocore"
)

var (
	stat = gocore.NewStat("SQL")
)

// DB is a wrapper around sql.DB that provides performance instrumentation
// and statistics tracking for all SQL operations.
type DB struct {
	*sql.DB
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &DB{db}, nil
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return db.DB.Query(query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return db.DB.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return db.DB.QueryRow(query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return db.DB.QueryRowContext(ctx, query, args...)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return db.DB.Exec(query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	return db.DB.ExecContext(ctx, query, args...)
}
