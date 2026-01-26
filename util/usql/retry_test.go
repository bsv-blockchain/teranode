package usql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestDefaultRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()

	assert.Equal(t, 3, config.MaxAttempts, "Default max attempts should be 3")
	assert.Equal(t, 100*time.Millisecond, config.BaseDelay, "Default base delay should be 100ms")
	assert.True(t, config.Enabled, "Retry should be enabled by default")
}

func TestIsRetriable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "connection refused",
			err:      errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "connection refused"),
			expected: true,
		},
		{
			name:     "connection reset",
			err:      errors.New(errors.ERR_NETWORK_ERROR, "connection reset by peer"),
			expected: true,
		},
		{
			name:     "i/o timeout",
			err:      errors.New(errors.ERR_NETWORK_TIMEOUT, "i/o timeout"),
			expected: true,
		},
		{
			name:     "deadline exceeded",
			err:      errors.New(errors.ERR_CONTEXT_CANCELED, "context deadline exceeded"),
			expected: true,
		},
		{
			name:     "database locked",
			err:      errors.New(errors.ERR_STORAGE_ERROR, "database is locked"),
			expected: true,
		},
		{
			name:     "deadlock",
			err:      errors.New(errors.ERR_STORAGE_ERROR, "deadlock detected"),
			expected: true,
		},
		{
			name:     "too many connections",
			err:      errors.New(errors.ERR_STORAGE_ERROR, "too many connections"),
			expected: true,
		},
		{
			name:     "PostgreSQL connection error 08000",
			err:      &pq.Error{Code: "08000"},
			expected: true,
		},
		{
			name:     "PostgreSQL connection error 08003",
			err:      &pq.Error{Code: "08003"},
			expected: true,
		},
		{
			name:     "PostgreSQL connection error 08006",
			err:      &pq.Error{Code: "08006"},
			expected: true,
		},
		{
			name:     "PostgreSQL serialization failure",
			err:      &pq.Error{Code: "40001"},
			expected: true,
		},
		{
			name:     "PostgreSQL deadlock",
			err:      &pq.Error{Code: "40P01"},
			expected: true,
		},
		{
			name:     "PostgreSQL lock not available",
			err:      &pq.Error{Code: "55P03"},
			expected: true,
		},
		{
			name:     "PostgreSQL cannot connect now",
			err:      &pq.Error{Code: "57P03"},
			expected: true,
		},
		{
			name:     "PostgreSQL syntax error (non-retriable)",
			err:      &pq.Error{Code: "42601"},
			expected: false,
		},
		// Note: SQLite error tests removed due to unexported fields in sqlite.Error
		// These error types are covered through string-based error checking
		{
			name:     "generic application error (non-retriable)",
			err:      errors.New(errors.ERR_INVALID_ARGUMENT, "invalid input"),
			expected: false,
		},
		{
			name:     "SQL syntax error (non-retriable)",
			err:      errors.New(errors.ERR_ERROR, "syntax error at or near"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetriable(tt.err)
			assert.Equal(t, tt.expected, result, "Error: %v", tt.err)
		})
	}
}

func TestCalculateBackoff(t *testing.T) {
	baseDelay := 100 * time.Millisecond

	tests := []struct {
		name        string
		attempt     int
		expectedMin time.Duration
		expectedMax time.Duration
	}{
		{
			name:        "first retry (attempt 0)",
			attempt:     0,
			expectedMin: 75 * time.Millisecond,  // 100ms - 25% jitter
			expectedMax: 125 * time.Millisecond, // 100ms + 25% jitter
		},
		{
			name:        "second retry (attempt 1)",
			attempt:     1,
			expectedMin: 150 * time.Millisecond, // 200ms - 25% jitter
			expectedMax: 250 * time.Millisecond, // 200ms + 25% jitter
		},
		{
			name:        "third retry (attempt 2)",
			attempt:     2,
			expectedMin: 300 * time.Millisecond, // 400ms - 25% jitter
			expectedMax: 500 * time.Millisecond, // 400ms + 25% jitter
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple times to ensure jitter is working
			for i := 0; i < 10; i++ {
				backoff := calculateBackoff(tt.attempt, baseDelay)
				assert.GreaterOrEqual(t, backoff, tt.expectedMin, "Backoff should be at least min value")
				assert.LessOrEqual(t, backoff, tt.expectedMax, "Backoff should be at most max value")
			}
		})
	}
}

func TestRetryOperation_Success(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	callCount := 0
	operation := func() error {
		callCount++
		return nil
	}

	err := retryOperation(ctx, config, operation)

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount, "Operation should be called once on success")
}

func TestRetryOperation_SuccessAfterRetries(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	callCount := 0
	operation := func() error {
		callCount++
		if callCount < 3 {
			return errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "connection refused") // Retriable error
		}
		return nil
	}

	start := time.Now()
	err := retryOperation(ctx, config, operation)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.Equal(t, 3, callCount, "Operation should be called 3 times")
	// Should have at least 2 backoffs with jitter (allowing for -25% jitter on both)
	// Minimum: (10ms - 2.5ms) + (20ms - 5ms) = 22.5ms
	assert.GreaterOrEqual(t, duration, 20*time.Millisecond, "Should have backoff delays")
}

func TestRetryOperation_NonRetriableError(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	callCount := 0
	expectedErr := errors.New(errors.ERR_ERROR, "syntax error") // Non-retriable
	operation := func() error {
		callCount++
		return expectedErr
	}

	err := retryOperation(ctx, config, operation)

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 1, callCount, "Non-retriable error should not be retried")
}

func TestRetryOperation_ExhaustedRetries(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	callCount := 0
	expectedErr := errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "connection refused") // Retriable
	operation := func() error {
		callCount++
		return expectedErr
	}

	err := retryOperation(ctx, config, operation)

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 4, callCount, "Should try initial + 3 retries")
}

func TestRetryOperation_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		Enabled:     true,
	}

	callCount := 0
	operation := func() error {
		callCount++
		if callCount == 2 {
			cancel() // Cancel after first retry
		}
		return errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "connection refused") // Retriable
	}

	err := retryOperation(ctx, config, operation)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.LessOrEqual(t, callCount, 2, "Should stop on context cancellation")
}

func TestRetryOperation_Disabled(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     false, // Disabled
	}

	callCount := 0
	expectedErr := errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "connection refused")
	operation := func() error {
		callCount++
		return expectedErr
	}

	err := retryOperation(ctx, config, operation)

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 1, callCount, "Should not retry when disabled")
}

func TestRetryQueryOperation_Success(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	expectedRows := &sql.Rows{}
	callCount := 0
	operation := func() (*sql.Rows, error) {
		callCount++
		return expectedRows, nil
	}

	rows, err := retryQueryOperation(ctx, config, operation)

	assert.NoError(t, err)
	assert.Equal(t, expectedRows, rows)
	assert.Equal(t, 1, callCount, "Operation should be called once on success")
}

func TestRetryQueryOperation_SuccessAfterRetries(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	expectedRows := &sql.Rows{}
	callCount := 0
	operation := func() (*sql.Rows, error) {
		callCount++
		if callCount < 2 {
			return nil, errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "connection refused") // Retriable
		}
		return expectedRows, nil
	}

	rows, err := retryQueryOperation(ctx, config, operation)

	assert.NoError(t, err)
	assert.Equal(t, expectedRows, rows)
	assert.Equal(t, 2, callCount, "Operation should succeed on second attempt")
}

func TestRetryExecOperation_Success(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	callCount := 0
	operation := func() (sql.Result, error) {
		callCount++
		return &mockResultImpl{lastInsertId: 1, rowsAffected: 1}, nil
	}

	result, err := retryExecOperation(ctx, config, operation)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, callCount, "Operation should be called once on success")
}

// mockResultImpl implements sql.Result for testing
type mockResultImpl struct {
	lastInsertId int64
	rowsAffected int64
}

func (m *mockResultImpl) LastInsertId() (int64, error) {
	return m.lastInsertId, nil
}

func (m *mockResultImpl) RowsAffected() (int64, error) {
	return m.rowsAffected, nil
}

func TestRetryExecOperation_SuccessAfterRetries(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	callCount := 0
	operation := func() (sql.Result, error) {
		callCount++
		if callCount < 3 {
			return nil, errors.New(errors.ERR_STORAGE_ERROR, "deadlock detected") // Retriable
		}
		return &mockResultImpl{lastInsertId: 1, rowsAffected: 1}, nil
	}

	result, err := retryExecOperation(ctx, config, operation)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, callCount, "Operation should succeed on third attempt")
}

// TestIsRetriableErrorMessages tests various error message patterns
func TestIsRetriableErrorMessages(t *testing.T) {
	retriableMessages := []string{
		"Connection refused",
		"CONNECTION RESET BY PEER",
		"broken pipe",
		"I/O Timeout",
		"database is locked",
		"Deadlock Detected",
		"lock timeout exceeded",
		"too many connections",
		"could not connect to server",
		"unable to connect to database",
	}

	for _, msg := range retriableMessages {
		t.Run(msg, func(t *testing.T) {
			err := errors.New(errors.ERR_ERROR, msg)
			assert.True(t, isRetriable(err), "Error message '%s' should be retriable", msg)
		})
	}

	nonRetriableMessages := []string{
		"syntax error",
		"invalid input",
		"constraint violation",
		"duplicate key",
		"permission denied",
		"relation does not exist",
	}

	for _, msg := range nonRetriableMessages {
		t.Run(msg, func(t *testing.T) {
			err := errors.New(errors.ERR_ERROR, msg)
			assert.False(t, isRetriable(err), "Error message '%s' should not be retriable", msg)
		})
	}
}

// BenchmarkCalculateBackoff benchmarks the backoff calculation
func BenchmarkCalculateBackoff(b *testing.B) {
	baseDelay := 100 * time.Millisecond

	for i := 0; i < b.N; i++ {
		calculateBackoff(i%3, baseDelay)
	}
}

// BenchmarkIsRetriable benchmarks the error detection
func BenchmarkIsRetriable(b *testing.B) {
	err := errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "connection refused")

	for i := 0; i < b.N; i++ {
		isRetriable(err)
	}
}

// TestRetryOperation_PostgreSQLErrors tests various PostgreSQL error scenarios
func TestRetryOperation_PostgreSQLErrors(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 2,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     true,
	}

	retriablePGErrors := []string{
		"08000", "08003", "08006", // Connection errors
		"40001", "40P01", // Transaction conflicts
		"55P03", "57P03", // Lock/connect errors
	}

	for _, code := range retriablePGErrors {
		t.Run(fmt.Sprintf("PG_%s", code), func(t *testing.T) {
			callCount := 0
			operation := func() error {
				callCount++
				if callCount < 2 {
					return &pq.Error{Code: pq.ErrorCode(code)}
				}
				return nil
			}

			err := retryOperation(ctx, config, operation)
			assert.NoError(t, err)
			assert.Equal(t, 2, callCount, "Should retry PostgreSQL error %s", code)
		})
	}
}

// TestRetryOperation_ExponentialBackoffTiming verifies backoff timing
func TestRetryOperation_ExponentialBackoffTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timing test in short mode")
	}

	ctx := context.Background()
	config := RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   50 * time.Millisecond,
		Enabled:     true,
	}

	callCount := 0
	operation := func() error {
		callCount++
		return errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "connection refused")
	}

	start := time.Now()
	_ = retryOperation(ctx, config, operation)
	duration := time.Since(start)

	// Expected minimum: 50ms + 100ms + 200ms = 350ms
	// With jitter, could be less (down to ~262ms) or more (up to ~437ms)
	// We test for at least 250ms to account for jitter and timing variance
	assert.GreaterOrEqual(t, duration, 250*time.Millisecond,
		"Total duration should reflect exponential backoff")
	assert.LessOrEqual(t, duration, 500*time.Millisecond,
		"Total duration should not be excessive")
}
