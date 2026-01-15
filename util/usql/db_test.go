package usql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
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

// MockDriver is a mock SQL driver for testing
type MockDriver struct {
	openFunc func(name string) (driver.Conn, error)
}

func (d *MockDriver) Open(name string) (driver.Conn, error) {
	if d.openFunc != nil {
		return d.openFunc(name)
	}
	return &MockConn{}, nil
}

// MockConn is a mock database connection
type MockConn struct {
	prepareFunc func(query string) (driver.Stmt, error)
	closeFunc   func() error
	beginFunc   func() (driver.Tx, error)
}

func (c *MockConn) Prepare(query string) (driver.Stmt, error) {
	if c.prepareFunc != nil {
		return c.prepareFunc(query)
	}
	return &MockStmt{}, nil
}

func (c *MockConn) Close() error {
	if c.closeFunc != nil {
		return c.closeFunc()
	}
	return nil
}

func (c *MockConn) Begin() (driver.Tx, error) {
	if c.beginFunc != nil {
		return c.beginFunc()
	}
	return &MockTx{}, nil
}

// MockStmt is a mock prepared statement
type MockStmt struct {
	closeFunc    func() error
	numInputFunc func() int
	execFunc     func(args []driver.Value) (driver.Result, error)
	queryFunc    func(args []driver.Value) (driver.Rows, error)
}

func (s *MockStmt) Close() error {
	if s.closeFunc != nil {
		return s.closeFunc()
	}
	return nil
}

func (s *MockStmt) NumInput() int {
	if s.numInputFunc != nil {
		return s.numInputFunc()
	}
	return -1
}

func (s *MockStmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.execFunc != nil {
		return s.execFunc(args)
	}
	return &MockResult{}, nil
}

func (s *MockStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.queryFunc != nil {
		return s.queryFunc(args)
	}
	return &MockRows{}, nil
}

// MockTx is a mock transaction
type MockTx struct {
	commitFunc   func() error
	rollbackFunc func() error
}

func (tx *MockTx) Commit() error {
	if tx.commitFunc != nil {
		return tx.commitFunc()
	}
	return nil
}

func (tx *MockTx) Rollback() error {
	if tx.rollbackFunc != nil {
		return tx.rollbackFunc()
	}
	return nil
}

// MockResult is a mock query result
type MockResult struct {
	lastInsertIdFunc func() (int64, error)
	rowsAffectedFunc func() (int64, error)
}

func (r *MockResult) LastInsertId() (int64, error) {
	if r.lastInsertIdFunc != nil {
		return r.lastInsertIdFunc()
	}
	return 0, nil
}

func (r *MockResult) RowsAffected() (int64, error) {
	if r.rowsAffectedFunc != nil {
		return r.rowsAffectedFunc()
	}
	return 0, nil
}

// MockRows is a mock rows result
type MockRows struct {
	columns []string
	data    [][]driver.Value
	pos     int
}

func (r *MockRows) Columns() []string {
	return r.columns
}

func (r *MockRows) Close() error {
	return nil
}

func (r *MockRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return errors.New(errors.ERR_NOT_FOUND, "EOF")
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

func TestOpen(t *testing.T) {
	// Register mock driver
	driverName := "mock-driver-open"
	sql.Register(driverName, &MockDriver{})

	t.Run("successful open", func(t *testing.T) {
		db, err := Open(driverName, "test-dsn")
		require.NoError(t, err)
		require.NotNil(t, db)
		assert.NotNil(t, db.DB)

		// Clean up
		err = db.Close()
		assert.NoError(t, err)
	})

	t.Run("open error", func(t *testing.T) {
		// Use invalid driver name
		db, err := Open("invalid-driver", "test-dsn")
		assert.Error(t, err)
		assert.Nil(t, db)
	})
}

func TestDB_Query(t *testing.T) {
	// Register mock driver
	driverName := "mock-driver-query"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	require.NoError(t, err)
	defer db.Close()

	t.Run("successful query", func(t *testing.T) {
		query := "SELECT * FROM users"
		rows, err := db.Query(query)
		assert.NoError(t, err)
		assert.NotNil(t, rows)
		rows.Close()
	})

	t.Run("query with args", func(t *testing.T) {
		query := "SELECT * FROM users WHERE id = ?"
		rows, err := db.Query(query, 1)
		assert.NoError(t, err)
		assert.NotNil(t, rows)
		rows.Close()
	})

	t.Run("timing statistics", func(t *testing.T) {
		// Test that query execution time is measured
		query := "SELECT * FROM users"
		start := time.Now()
		rows, err := db.Query(query)
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.NotNil(t, rows)
		assert.Greater(t, duration.Nanoseconds(), int64(0))
		rows.Close()
	})
}

func TestDB_QueryContext(t *testing.T) {
	// Register mock driver
	driverName := "mock-driver-query-context"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	require.NoError(t, err)
	defer db.Close()

	t.Run("successful query with context", func(t *testing.T) {
		ctx := context.Background()
		query := "SELECT * FROM users"
		rows, err := db.QueryContext(ctx, query)
		assert.NoError(t, err)
		assert.NotNil(t, rows)
		rows.Close()
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		query := "SELECT * FROM users"
		rows, err := db.QueryContext(ctx, query)
		// The behavior depends on the driver implementation
		// Some drivers might return an error, others might not
		if err == nil && rows != nil {
			rows.Close()
		}
	})
}

func TestDB_QueryRow(t *testing.T) {
	// Register mock driver
	driverName := "mock-driver-query-row"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	require.NoError(t, err)
	defer db.Close()

	t.Run("query row", func(t *testing.T) {
		query := "SELECT id FROM users WHERE email = ?"
		row := db.QueryRow(query, "test@example.com")
		assert.NotNil(t, row)

		// Note: We can't easily test Scan without a real database
		// or more complex mocking
	})

	t.Run("timing statistics", func(t *testing.T) {
		query := "SELECT COUNT(*) FROM users"
		start := time.Now()
		row := db.QueryRow(query)
		duration := time.Since(start)

		assert.NotNil(t, row)
		assert.Greater(t, duration.Nanoseconds(), int64(0))
	})
}

func TestDB_QueryRowContext(t *testing.T) {
	// Register mock driver
	driverName := "mock-driver-query-row-context"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	require.NoError(t, err)
	defer db.Close()

	t.Run("query row with context", func(t *testing.T) {
		ctx := context.Background()
		query := "SELECT id FROM users WHERE email = ?"
		row := db.QueryRowContext(ctx, query, "test@example.com")
		assert.NotNil(t, row)
	})

	t.Run("with timeout context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		query := "SELECT id FROM users"
		row := db.QueryRowContext(ctx, query)
		assert.NotNil(t, row)
	})
}

func TestDB_Exec(t *testing.T) {
	// Register mock driver
	driverName := "mock-driver-exec"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	require.NoError(t, err)
	defer db.Close()

	t.Run("successful exec", func(t *testing.T) {
		query := "INSERT INTO users (name, email) VALUES (?, ?)"
		result, err := db.Exec(query, "John Doe", "john@example.com")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("update query", func(t *testing.T) {
		query := "UPDATE users SET name = ? WHERE id = ?"
		result, err := db.Exec(query, "Jane Doe", 1)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("delete query", func(t *testing.T) {
		query := "DELETE FROM users WHERE id = ?"
		result, err := db.Exec(query, 1)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("timing statistics", func(t *testing.T) {
		query := "INSERT INTO logs (message) VALUES (?)"
		start := time.Now()
		result, err := db.Exec(query, "test message")
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, duration.Nanoseconds(), int64(0))
	})
}

func TestDB_ExecContext(t *testing.T) {
	// Register mock driver
	driverName := "mock-driver-exec-context"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	require.NoError(t, err)
	defer db.Close()

	t.Run("exec with context", func(t *testing.T) {
		ctx := context.Background()
		query := "INSERT INTO users (name) VALUES (?)"
		result, err := db.ExecContext(ctx, query, "Test User")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("exec with deadline", func(t *testing.T) {
		deadline := time.Now().Add(1 * time.Second)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		query := "UPDATE users SET updated_at = NOW()"
		result, err := db.ExecContext(ctx, query)
		if err == nil {
			assert.NotNil(t, result)
		}
	})
}

func TestDB_ConcurrentOperations(t *testing.T) {
	// Register mock driver
	driverName := "mock-driver-concurrent"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	require.NoError(t, err)
	defer db.Close()

	t.Run("concurrent queries", func(t *testing.T) {
		const numGoroutines = 10
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				query := "SELECT * FROM users WHERE id = ?"
				rows, err := db.Query(query, id)
				assert.NoError(t, err)
				if rows != nil {
					rows.Close()
				}
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < numGoroutines; i++ {
			select {
			case <-done:
				// Success
			case <-time.After(1 * time.Second):
				t.Fatal("Timeout waiting for concurrent queries")
			}
		}
	})
}

// BenchmarkQuery benchmarks the Query method
func BenchmarkQuery(b *testing.B) {
	// Register mock driver
	driverName := "mock-driver-bench"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users WHERE id = ?", i)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

// BenchmarkExec benchmarks the Exec method
func BenchmarkExec(b *testing.B) {
	// Register mock driver
	driverName := "mock-driver-bench-exec"
	sql.Register(driverName, &MockDriver{})

	db, err := Open(driverName, "test-dsn")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO logs (id, message) VALUES (?, ?)", i, "test")
		if err != nil {
			b.Fatal(err)
		}
	}
}
