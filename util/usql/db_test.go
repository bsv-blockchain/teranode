package usql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_SetGetRetryConfig(t *testing.T) {
	// Open an in-memory SQLite database for testing
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Test default config
	defaultConfig := db.GetRetryConfig()
	assert.Equal(t, 3, defaultConfig.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, defaultConfig.BaseDelay)
	assert.True(t, defaultConfig.Enabled)

	// Set custom config
	customConfig := RetryConfig{
		MaxAttempts: 5,
		BaseDelay:   200 * time.Millisecond,
		Enabled:     false,
	}
	db.SetRetryConfig(customConfig)

	// Verify custom config
	retrievedConfig := db.GetRetryConfig()
	assert.Equal(t, customConfig.MaxAttempts, retrievedConfig.MaxAttempts)
	assert.Equal(t, customConfig.BaseDelay, retrievedConfig.BaseDelay)
	assert.Equal(t, customConfig.Enabled, retrievedConfig.Enabled)
}

func TestDB_QueryWithRetry(t *testing.T) {
	// Open an in-memory SQLite database
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'test1'), (2, 'test2')")
	require.NoError(t, err)

	// Test Query
	rows, err := db.Query("SELECT id, name FROM test ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, 2, count, "Should retrieve 2 rows")
}

func TestDB_QueryContextWithRetry(t *testing.T) {
	// Open an in-memory SQLite database
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'test1')")
	require.NoError(t, err)

	// Test QueryContext
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT id, name FROM test")
	require.NoError(t, err)
	defer rows.Close()

	assert.True(t, rows.Next(), "Should have at least one row")
}

func TestDB_ExecWithRetry(t *testing.T) {
	// Open an in-memory SQLite database
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	result, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Insert test data
	result, err = db.Exec("INSERT INTO test (id, name) VALUES (?, ?)", 1, "test1")
	require.NoError(t, err)
	
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected, "Should insert 1 row")
}

func TestDB_ExecContextWithRetry(t *testing.T) {
	// Open an in-memory SQLite database
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Insert test data
	result, err := db.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (?, ?)", 1, "test1")
	require.NoError(t, err)
	
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected, "Should insert 1 row")
}

func TestDB_QueryRowWithRetry(t *testing.T) {
	// Open an in-memory SQLite database
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'test1')")
	require.NoError(t, err)

	// Test QueryRow
	var id int
	var name string
	row := db.QueryRow("SELECT id, name FROM test WHERE id = ?", 1)
	err = row.Scan(&id, &name)
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.Equal(t, "test1", name)
}

func TestDB_QueryRowContextWithRetry(t *testing.T) {
	// Open an in-memory SQLite database
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'test1')")
	require.NoError(t, err)

	// Test QueryRowContext
	ctx := context.Background()
	var id int
	var name string
	row := db.QueryRowContext(ctx, "SELECT id, name FROM test WHERE id = ?", 1)
	err = row.Scan(&id, &name)
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.Equal(t, "test1", name)
}

func TestDB_RetryDisabled(t *testing.T) {
	// Open an in-memory SQLite database
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Disable retry
	db.SetRetryConfig(RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		Enabled:     false,
	})

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Query with invalid SQL should fail immediately without retry
	_, err = db.Query("SELECT * FROM nonexistent_table")
	assert.Error(t, err, "Should fail on invalid query")
}

func TestDB_ContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping context cancellation test in short mode")
	}

	// Open an in-memory SQLite database
	db, err := Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Set retry config with longer delays
	db.SetRetryConfig(RetryConfig{
		MaxAttempts: 5,
		BaseDelay:   100 * time.Millisecond,
		Enabled:     true,
	})

	// Create a context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// This query should respect context cancellation
	// We use a non-existent table to trigger an error, but since it's not retriable,
	// it should fail immediately
	_, err = db.QueryContext(ctx, "SELECT * FROM nonexistent_table")
	assert.Error(t, err)
}

// BenchmarkDB_QueryWithRetry benchmarks query performance with retry enabled
func BenchmarkDB_QueryWithRetry(b *testing.B) {
	db, err := Open("sqlite", ":memory:")
	require.NoError(b, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(b, err)

	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'test1')")
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, name FROM test WHERE id = ?", 1)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

// BenchmarkDB_QueryWithoutRetry benchmarks query performance with retry disabled
func BenchmarkDB_QueryWithoutRetry(b *testing.B) {
	db, err := Open("sqlite", ":memory:")
	require.NoError(b, err)
	defer db.Close()

	// Disable retry
	db.SetRetryConfig(RetryConfig{
		MaxAttempts: 0,
		BaseDelay:   0,
		Enabled:     false,
	})

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(b, err)

	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'test1')")
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, name FROM test WHERE id = ?", 1)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}
