package usql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/lib/pq"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// RetryConfig holds configuration for retry behavior
type RetryConfig struct {
	MaxAttempts int           // Maximum number of retry attempts
	BaseDelay   time.Duration // Base delay for exponential backoff
	Enabled     bool          // Whether retry logic is enabled or not
}

// DefaultRetryConfig returns the default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		Enabled:     true,
	}
}

// isRetriable determines if an error should be retried
// Returns true for connection-related errors, timeouts, and database lock errors
func isRetriable(err error) bool {
	if err == nil {
		return false
	}

	// PostgreSQL errors
	if pqErr, ok := err.(*pq.Error); ok {
		// Retriable PostgreSQL error codes:
		// 08000: connection_exception
		// 08003: connection_does_not_exist
		// 08006: connection_failure
		// 40001: serialization_failure (transaction conflicts)
		// 40P01: deadlock_detected
		// 55P03: lock_not_available
		// 57P03: cannot_connect_now
		code := string(pqErr.Code)
		return strings.HasPrefix(code, "08") || // Connection errors
			code == "40001" || // Serialization failure
			code == "40P01" || // Deadlock
			code == "55P03" || // Lock not available
			code == "57P03" // Cannot connect now
	}

	// SQLite errors
	if sqliteErr, ok := err.(*sqlite.Error); ok {
		code := sqliteErr.Code()
		return code == sqlite3.SQLITE_BUSY ||
			code == sqlite3.SQLITE_LOCKED ||
			code == sqlite3.SQLITE_IOERR ||
			code == sqlite3.SQLITE_CANTOPEN
	}

	// Check error message for common retriable patterns
	errStr := strings.ToLower(err.Error())
	retriablePatterns := []string{
		"connection refused",
		"connection reset",
		"connection closed",
		"broken pipe",
		"i/o timeout",
		"timeout",
		"deadline exceeded",
		"database is locked",
		"deadlock",
		"lock timeout",
		"too many connections",
		"could not connect",
		"unable to connect",
	}

	for _, pattern := range retriablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// calculateBackoff calculates the backoff duration with jitter
// Uses exponential backoff: baseDelay * 2^attempt
// Adds random jitter of ±25% to prevent thundering herd
func calculateBackoff(attempt int, baseDelay time.Duration) time.Duration {
	// Exponential backoff: 100ms, 200ms, 400ms, etc.
	delay := time.Duration(1<<uint(attempt)) * baseDelay

	// Add jitter (±25%)
	jitterRange := int64(delay / 4)
	if jitterRange > 0 {
		jitter, err := cryptoJitter(jitterRange * 2)
		if err == nil {
			delay += time.Duration(jitter - jitterRange)
		}
	}

	return delay
}

func cryptoJitter(max int64) (int64, error) {
	if max <= 0 {
		return 0, nil
	}

	value, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		return 0, err
	}

	return value.Int64(), nil
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// retryOperation executes an operation with retry logic
func retryOperation(ctx context.Context, config RetryConfig, operation func() error) error {
	ctx = normalizeContext(ctx)
	if !config.Enabled {
		return operation()
	}

	var lastErr error

	for attempt := 0; attempt <= config.MaxAttempts; attempt++ {
		// Record retry attempt metric
		if attempt > 0 {
			prometheusQueryRetries.WithLabelValues(fmt.Sprintf("%d", attempt)).Inc()
		}

		// Execute the operation
		err := operation()
		if err == nil {
			// Success - record metric if this was a retry
			if attempt > 0 {
				prometheusQueryRetrySuccess.Inc()
			}
			return nil
		}

		lastErr = err

		// Don't retry non-retriable errors
		if !isRetriable(err) {
			return err
		}

		// Don't retry if we've exhausted attempts
		if attempt >= config.MaxAttempts {
			prometheusQueryRetryExhausted.Inc()
			break
		}

		// Calculate backoff with jitter
		backoff := calculateBackoff(attempt, config.BaseDelay)

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// Continue to next attempt
		}
	}

	return lastErr
}

// retryQueryOperation is a specialized version for Query operations that return *sql.Rows
// Use this for context-aware queries (QueryContext)
func retryQueryOperation(ctx context.Context, config RetryConfig, operation func() (*sql.Rows, error)) (*sql.Rows, error) {
	ctx = normalizeContext(ctx)
	if !config.Enabled {
		return operation()
	}

	var lastErr error
	var rows *sql.Rows
	var err error

	for attempt := 0; attempt <= config.MaxAttempts; attempt++ {
		// Record retry attempt metric
		if attempt > 0 {
			prometheusQueryRetries.WithLabelValues(fmt.Sprintf("%d", attempt)).Inc()
		}

		// Execute the operation
		rows, err = operation()
		if err == nil {
			// Success - record metric if this was a retry
			if attempt > 0 {
				prometheusQueryRetrySuccess.Inc()
			}
			return rows, nil
		}

		lastErr = err
		if rows != nil {
			_ = rows.Close()
			rows = nil
		}

		// Don't retry non-retriable errors
		if !isRetriable(err) {
			return nil, err
		}

		// Don't retry if we've exhausted attempts
		if attempt >= config.MaxAttempts {
			prometheusQueryRetryExhausted.Inc()
			break
		}

		// Calculate backoff with jitter
		backoff := calculateBackoff(attempt, config.BaseDelay)

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
			// Continue to next attempt
		}
	}

	return rows, lastErr
}

// retryQueryOperationNoContext is for non-context Query operations
// Uses time.Sleep instead of context-aware select for backoff
func retryQueryOperationNoContext(config RetryConfig, operation func() (*sql.Rows, error)) (*sql.Rows, error) {
	if !config.Enabled {
		return operation()
	}

	var lastErr error
	var rows *sql.Rows
	var err error

	for attempt := 0; attempt <= config.MaxAttempts; attempt++ {
		// Record retry attempt metric
		if attempt > 0 {
			prometheusQueryRetries.WithLabelValues(fmt.Sprintf("%d", attempt)).Inc()
		}

		// Execute the operation
		rows, err = operation()
		if err == nil {
			// Success - record metric if this was a retry
			if attempt > 0 {
				prometheusQueryRetrySuccess.Inc()
			}
			return rows, nil
		}

		lastErr = err
		if rows != nil {
			_ = rows.Close()
			rows = nil
		}

		// Don't retry non-retriable errors
		if !isRetriable(err) {
			return nil, err
		}

		// Don't retry if we've exhausted attempts
		if attempt >= config.MaxAttempts {
			prometheusQueryRetryExhausted.Inc()
			break
		}

		// Calculate backoff with jitter and sleep
		backoff := calculateBackoff(attempt, config.BaseDelay)
		time.Sleep(backoff)
	}

	return rows, lastErr
}

// retryExecOperation is a specialized version for Exec operations that return sql.Result
// Use this for context-aware exec (ExecContext)
func retryExecOperation(ctx context.Context, config RetryConfig, operation func() (sql.Result, error)) (sql.Result, error) {
	ctx = normalizeContext(ctx)
	if !config.Enabled {
		return operation()
	}

	var lastErr error
	var result sql.Result
	var err error

	for attempt := 0; attempt <= config.MaxAttempts; attempt++ {
		// Record retry attempt metric
		if attempt > 0 {
			prometheusQueryRetries.WithLabelValues(fmt.Sprintf("%d", attempt)).Inc()
		}

		// Execute the operation
		result, err = operation()
		if err == nil {
			// Success - record metric if this was a retry
			if attempt > 0 {
				prometheusQueryRetrySuccess.Inc()
			}
			return result, nil
		}

		lastErr = err
		result = nil

		// Don't retry non-retriable errors
		if !isRetriable(err) {
			return nil, err
		}

		// Don't retry if we've exhausted attempts
		if attempt >= config.MaxAttempts {
			prometheusQueryRetryExhausted.Inc()
			break
		}

		// Calculate backoff with jitter
		backoff := calculateBackoff(attempt, config.BaseDelay)

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
			// Continue to next attempt
		}
	}

	return result, lastErr
}

// retryExecOperationNoContext is for non-context Exec operations
// Uses time.Sleep instead of context-aware select for backoff
func retryExecOperationNoContext(config RetryConfig, operation func() (sql.Result, error)) (sql.Result, error) {
	if !config.Enabled {
		return operation()
	}

	var lastErr error
	var result sql.Result
	var err error

	for attempt := 0; attempt <= config.MaxAttempts; attempt++ {
		// Record retry attempt metric
		if attempt > 0 {
			prometheusQueryRetries.WithLabelValues(fmt.Sprintf("%d", attempt)).Inc()
		}

		// Execute the operation
		result, err = operation()
		if err == nil {
			// Success - record metric if this was a retry
			if attempt > 0 {
				prometheusQueryRetrySuccess.Inc()
			}
			return result, nil
		}

		lastErr = err
		result = nil

		// Don't retry non-retriable errors
		if !isRetriable(err) {
			return nil, err
		}

		// Don't retry if we've exhausted attempts
		if attempt >= config.MaxAttempts {
			prometheusQueryRetryExhausted.Inc()
			break
		}

		// Calculate backoff with jitter and sleep
		backoff := calculateBackoff(attempt, config.BaseDelay)
		time.Sleep(backoff)
	}

	return result, lastErr
}
