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
// statistics tracking, retry logic with exponential backoff, and circuit breaker
// protection for all SQL operations.
type DB struct {
	*sql.DB
	retryConfig    RetryConfig
	circuitBreaker *CircuitBreaker
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &DB{
		DB:             db,
		retryConfig:    DefaultRetryConfig(),
		circuitBreaker: nil, // Disabled by default
	}, nil
}

// SetRetryConfig sets the retry configuration for this database connection.
func (db *DB) SetRetryConfig(config RetryConfig) {
	db.retryConfig = config
}

// GetRetryConfig returns the current retry configuration.
func (db *DB) GetRetryConfig() RetryConfig {
	return db.retryConfig
}

// SetCircuitBreaker sets the circuit breaker for this database connection.
// Pass nil to disable the circuit breaker.
func (db *DB) SetCircuitBreaker(cb *CircuitBreaker) {
	db.circuitBreaker = cb
}

// GetCircuitBreaker returns the circuit breaker for this database connection.
// Returns nil if circuit breaker is not configured.
func (db *DB) GetCircuitBreaker() *CircuitBreaker {
	return db.circuitBreaker
}

// CircuitBreakerState returns the current state of the circuit breaker.
// Returns CircuitClosed if no circuit breaker is configured.
func (db *DB) CircuitBreakerState() CircuitState {
	if db.circuitBreaker == nil {
		return CircuitClosed
	}
	return db.circuitBreaker.State()
}

// CircuitBreakerStats returns statistics about the circuit breaker.
func (db *DB) CircuitBreakerStats() CircuitBreakerStats {
	if db.circuitBreaker == nil {
		return CircuitBreakerStats{State: CircuitClosed}
	}
	return db.circuitBreaker.Stats()
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	// Check circuit breaker before attempting the operation
	if db.circuitBreaker != nil && !db.circuitBreaker.Allow() {
		return nil, ErrCircuitOpen
	}

	rows, err := retryQueryOperationNoContext(db.retryConfig, func() (*sql.Rows, error) {
		return db.DB.Query(query, args...)
	})

	db.recordCircuitBreakerResult(err)
	return rows, err
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	// Check circuit breaker before attempting the operation
	if db.circuitBreaker != nil && !db.circuitBreaker.Allow() {
		return nil, ErrCircuitOpen
	}

	rows, err := retryQueryOperation(ctx, db.retryConfig, func() (*sql.Rows, error) {
		return db.DB.QueryContext(ctx, query, args...)
	})

	db.recordCircuitBreakerResult(err)
	return rows, err
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	// Note: QueryRow doesn't return an error directly, it defers error checking to Scan()
	// So we cannot apply circuit breaker or retry logic here without breaking the API.
	// The error will be handled at the transaction/query level instead.
	return db.DB.QueryRow(query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	// Note: QueryRow doesn't return an error directly, it defers error checking to Scan()
	// So we cannot apply circuit breaker or retry logic here without breaking the API.
	// The error will be handled at the transaction/query level instead.
	return db.DB.QueryRowContext(ctx, query, args...)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	// Check circuit breaker before attempting the operation
	if db.circuitBreaker != nil && !db.circuitBreaker.Allow() {
		return nil, ErrCircuitOpen
	}

	result, err := retryExecOperationNoContext(db.retryConfig, func() (sql.Result, error) {
		return db.DB.Exec(query, args...)
	})

	db.recordCircuitBreakerResult(err)
	return result, err
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := gocore.CurrentTime()
	defer func() {
		stat.NewStat(query).AddTime(start)
	}()

	// Check circuit breaker before attempting the operation
	if db.circuitBreaker != nil && !db.circuitBreaker.Allow() {
		return nil, ErrCircuitOpen
	}

	result, err := retryExecOperation(ctx, db.retryConfig, func() (sql.Result, error) {
		return db.DB.ExecContext(ctx, query, args...)
	})

	db.recordCircuitBreakerResult(err)
	return result, err
}

// recordCircuitBreakerResult records the result of an operation with the circuit breaker.
// Only infrastructure failures (retriable errors) are recorded as failures.
func (db *DB) recordCircuitBreakerResult(err error) {
	if db.circuitBreaker == nil {
		return
	}

	if err != nil && isRetriable(err) {
		db.circuitBreaker.RecordFailure()
	} else if err == nil {
		db.circuitBreaker.RecordSuccess()
	}
	// Non-retriable errors (business logic errors) are ignored by the circuit breaker
}
