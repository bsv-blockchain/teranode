package pruner

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/usql"
)

var (
	mockDriverOnce sync.Once
)

// MockLogger is a mock implementation of ulogger.Logger interface
type MockLogger struct {
	DebugfFunc      func(format string, args ...interface{})
	InfofFunc       func(format string, args ...interface{})
	WarnfFunc       func(format string, args ...interface{})
	ErrorfFunc      func(format string, args ...interface{})
	FatalfFunc      func(format string, args ...interface{})
	LogLevelFunc    func() int
	SetLogLevelFunc func(level string)
	NewFunc         func(service string, options ...ulogger.Option) ulogger.Logger
	DuplicateFunc   func(options ...ulogger.Option) ulogger.Logger
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	if m.DebugfFunc != nil {
		m.DebugfFunc(format, args...)
	}
}

func (m *MockLogger) Infof(format string, args ...interface{}) {
	if m.InfofFunc != nil {
		m.InfofFunc(format, args...)
	}
}

func (m *MockLogger) Warnf(format string, args ...interface{}) {
	if m.WarnfFunc != nil {
		m.WarnfFunc(format, args...)
	}
}

func (m *MockLogger) Errorf(format string, args ...interface{}) {
	if m.ErrorfFunc != nil {
		m.ErrorfFunc(format, args...)
	}
}

func (m *MockLogger) Fatalf(format string, args ...interface{}) {
	if m.FatalfFunc != nil {
		m.FatalfFunc(format, args...)
	}
}

func (m *MockLogger) LogLevel() int {
	if m.LogLevelFunc != nil {
		return m.LogLevelFunc()
	}
	return 1 // INFO
}

func (m *MockLogger) SetLogLevel(level string) {
	if m.SetLogLevelFunc != nil {
		m.SetLogLevelFunc(level)
	}
}

func (m *MockLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	if m.NewFunc != nil {
		return m.NewFunc(service, options...)
	}
	return &MockLogger{}
}

func (m *MockLogger) Duplicate(options ...ulogger.Option) ulogger.Logger {
	if m.DuplicateFunc != nil {
		return m.DuplicateFunc(options...)
	}
	return &MockLogger{}
}

func (m *MockLogger) WithTraceContext(_ context.Context) ulogger.Logger {
	return m
}

// MockDB is a mock implementation that satisfies usql.DB interface
type MockDB struct {
	DB       *usql.DB
	ExecFunc func(query string, args ...interface{}) (sql.Result, error)
	mockStmt *MockStmt
}

// NewMockDB creates a new mock DB with an in-memory database
func NewMockDB() *MockDB {
	// Register a mock driver for testing only once
	mockDriverOnce.Do(func() {
		sql.Register("test", &MockDriver{})
	})

	sqlDB, _ := sql.Open("test", "")
	mockDB := &MockDB{
		DB: &usql.DB{DB: sqlDB},
	}

	// Set up the mock statement to use our exec function
	mockDB.mockStmt = &MockStmt{mockDB: mockDB}

	return mockDB
}

// Exec overrides the Exec method to use the mock function if provided
func (m *MockDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	if m.ExecFunc != nil {
		return m.ExecFunc(query, args...)
	}
	// Fallback to the underlying DB if no mock function is provided
	return m.DB.Exec(query, args...)
}

// ExecContext overrides the ExecContext method to use the mock function if provided
func (m *MockDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if m.ExecFunc != nil {
		return m.ExecFunc(query, args...)
	}
	// Fallback to the underlying DB if no mock function is provided
	return m.DB.ExecContext(ctx, query, args...)
}

// Query delegates to the underlying DB
func (m *MockDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return m.DB.Query(query, args...)
}

// QueryContext delegates to the underlying DB
func (m *MockDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return m.DB.QueryContext(ctx, query, args...)
}

// QueryRow delegates to the underlying DB
func (m *MockDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return m.DB.QueryRow(query, args...)
}

// QueryRowContext delegates to the underlying DB
func (m *MockDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return m.DB.QueryRowContext(ctx, query, args...)
}

// Close delegates to the underlying DB
func (m *MockDB) Close() error {
	return m.DB.Close()
}

// Ping delegates to the underlying DB
func (m *MockDB) Ping() error {
	return m.DB.Ping()
}

// PingContext delegates to the underlying DB
func (m *MockDB) PingContext(ctx context.Context) error {
	return m.DB.PingContext(ctx)
}

// SetMaxIdleConns delegates to the underlying DB
func (m *MockDB) SetMaxIdleConns(n int) {
	m.DB.SetMaxIdleConns(n)
}

// SetMaxOpenConns delegates to the underlying DB
func (m *MockDB) SetMaxOpenConns(n int) {
	m.DB.SetMaxOpenConns(n)
}

// SetConnMaxLifetime delegates to the underlying DB
func (m *MockDB) SetConnMaxLifetime(d time.Duration) {
	m.DB.SetConnMaxLifetime(d)
}

// SetConnMaxIdleTime delegates to the underlying DB
func (m *MockDB) SetConnMaxIdleTime(d time.Duration) {
	m.DB.SetConnMaxIdleTime(d)
}

// Stats delegates to the underlying DB
func (m *MockDB) Stats() sql.DBStats {
	return m.DB.Stats()
}

// MockResult is a mock implementation of sql.Result
type MockResult struct {
	lastInsertId int64
	rowsAffected int64
	err          error
}

func (m *MockResult) LastInsertId() (int64, error) {
	return m.lastInsertId, m.err
}

func (m *MockResult) RowsAffected() (int64, error) {
	return m.rowsAffected, m.err
}

// MockDriver is a mock driver for testing
type MockDriver struct{}

func (m *MockDriver) Open(name string) (driver.Conn, error) {
	return &MockConn{}, nil
}

// MockConn is a mock connection
type MockConn struct{}

func (m *MockConn) Prepare(query string) (driver.Stmt, error) {
	// This won't work directly because we don't have access to MockDB from here
	return &MockStmt{}, nil
}

func (m *MockConn) Close() error {
	return nil
}

func (m *MockConn) Begin() (driver.Tx, error) {
	return &MockTx{}, nil
}

// MockStmt is a mock statement
type MockStmt struct {
	mockDB *MockDB
}

func (m *MockStmt) Close() error {
	return nil
}

func (m *MockStmt) NumInput() int {
	return -1 // -1 means don't check argument count
}

func (m *MockStmt) Exec(args []driver.Value) (driver.Result, error) {
	if m.mockDB != nil && m.mockDB.ExecFunc != nil {
		// Convert driver.Value to interface{} for consistency
		interfaceArgs := make([]interface{}, len(args))
		for i, arg := range args {
			interfaceArgs[i] = arg
		}

		result, err := m.mockDB.ExecFunc("", interfaceArgs...)
		if err != nil {
			return nil, err
		}

		// Convert sql.Result to driver.Result
		if sqlResult, ok := result.(*MockResult); ok {
			return &MockDriverResult{
				lastInsertId: sqlResult.lastInsertId,
				rowsAffected: sqlResult.rowsAffected,
			}, nil
		}
	}
	return &MockDriverResult{rowsAffected: 1}, nil
}

func (m *MockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

// MockTx is a mock transaction
type MockTx struct{}

func (m *MockTx) Commit() error {
	return nil
}

func (m *MockTx) Rollback() error {
	return nil
}

// MockDriverResult is a mock driver result
type MockDriverResult struct {
	lastInsertId int64
	rowsAffected int64
}

func (m *MockDriverResult) LastInsertId() (int64, error) {
	return m.lastInsertId, nil
}

func (m *MockDriverResult) RowsAffected() (int64, error) {
	return m.rowsAffected, nil
}

// FailingMockSQLDB is a sql.DB replacement that always returns errors
type FailingMockSQLDB struct {
	err error
}

func (f *FailingMockSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return nil, f.err
}

func (f *FailingMockSQLDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, f.err
}

func (f *FailingMockSQLDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return nil, f.err
}

func (f *FailingMockSQLDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, f.err
}

func (f *FailingMockSQLDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return nil
}

func (f *FailingMockSQLDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return nil
}

func (f *FailingMockSQLDB) Prepare(query string) (*sql.Stmt, error) {
	return nil, f.err
}

func (f *FailingMockSQLDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return nil, f.err
}

func (f *FailingMockSQLDB) Close() error {
	return f.err
}

func (f *FailingMockSQLDB) Begin() (*sql.Tx, error) {
	return nil, f.err
}

func (f *FailingMockSQLDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, f.err
}

func (f *FailingMockSQLDB) Ping() error {
	return f.err
}

func (f *FailingMockSQLDB) PingContext(ctx context.Context) error {
	return f.err
}

func (f *FailingMockSQLDB) SetMaxIdleConns(n int)              {}
func (f *FailingMockSQLDB) SetMaxOpenConns(n int)              {}
func (f *FailingMockSQLDB) SetConnMaxLifetime(d time.Duration) {}
func (f *FailingMockSQLDB) SetConnMaxIdleTime(d time.Duration) {}
func (f *FailingMockSQLDB) Stats() sql.DBStats                 { return sql.DBStats{} }

// MockSQLDB is a mock implementation of sql.DB that delegates to MockDB
type MockSQLDB struct {
	mockDB *MockDB
}

func (m *MockSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return m.mockDB.Exec(query, args...)
}

func (m *MockSQLDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return m.mockDB.ExecContext(ctx, query, args...)
}

func (m *MockSQLDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil // Not used in tests
}

func (m *MockSQLDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil // Not used in tests
}

func (m *MockSQLDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return nil // Not used in tests
}

func (m *MockSQLDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return nil // Not used in tests
}

func (m *MockSQLDB) Prepare(query string) (*sql.Stmt, error) {
	return nil, nil // Not used in tests
}

func (m *MockSQLDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return nil, nil // Not used in tests
}

func (m *MockSQLDB) Close() error {
	return nil
}

func (m *MockSQLDB) Begin() (*sql.Tx, error) {
	return nil, nil // Not used in tests
}

func (m *MockSQLDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, nil // Not used in tests
}

func (m *MockSQLDB) Ping() error {
	return nil
}

func (m *MockSQLDB) PingContext(ctx context.Context) error {
	return nil
}

func (m *MockSQLDB) SetMaxIdleConns(n int)              {}
func (m *MockSQLDB) SetMaxOpenConns(n int)              {}
func (m *MockSQLDB) SetConnMaxLifetime(d time.Duration) {}
func (m *MockSQLDB) SetConnMaxIdleTime(d time.Duration) {}
func (m *MockSQLDB) Stats() sql.DBStats                 { return sql.DBStats{} }
