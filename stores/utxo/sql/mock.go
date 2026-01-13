package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/usql"
	pq "github.com/lib/pq"
	"github.com/stretchr/testify/mock"
)

// MockDB provides a mock implementation of *usql.DB for testing
type MockDB struct {
	mock.Mock
}

// Begin mocks the Begin method of *usql.DB
func (m *MockDB) Begin() (*sql.Tx, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sql.Tx), args.Error(1)
}

// QueryContext mocks the QueryContext method of *usql.DB
func (m *MockDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	mockArgs := m.Called(ctx, query, args)
	if mockArgs.Get(0) == nil {
		return nil, mockArgs.Error(1)
	}
	return mockArgs.Get(0).(*sql.Rows), mockArgs.Error(1)
}

// ExecContext mocks the ExecContext method of *usql.DB
func (m *MockDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	mockArgs := m.Called(ctx, query, args)
	if mockArgs.Get(0) == nil {
		return nil, mockArgs.Error(1)
	}
	return mockArgs.Get(0).(sql.Result), mockArgs.Error(1)
}

// Exec mocks the Exec method of *usql.DB (used by createPostgresSchema)
func (m *MockDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	mockArgs := m.Called(query, args)
	if mockArgs.Get(0) == nil {
		return nil, mockArgs.Error(1)
	}
	return mockArgs.Get(0).(sql.Result), mockArgs.Error(1)
}

// Close mocks the Close method of *usql.DB (called on error in createPostgresSchema)
func (m *MockDB) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockTx provides a mock implementation of *sql.Tx for testing
type MockTx struct {
	mock.Mock
}

// QueryContext mocks the QueryContext method of *sql.Tx
func (m *MockTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	mockArgs := m.Called(ctx, query, args)
	if mockArgs.Get(0) == nil {
		return nil, mockArgs.Error(1)
	}
	return mockArgs.Get(0).(*sql.Rows), mockArgs.Error(1)
}

// ExecContext mocks the ExecContext method of *sql.Tx
func (m *MockTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	mockArgs := m.Called(ctx, query, args)
	if mockArgs.Get(0) == nil {
		return nil, mockArgs.Error(1)
	}
	return mockArgs.Get(0).(sql.Result), mockArgs.Error(1)
}

// Commit mocks the Commit method of *sql.Tx
func (m *MockTx) Commit() error {
	args := m.Called()
	return args.Error(0)
}

// Rollback mocks the Rollback method of *sql.Tx
func (m *MockTx) Rollback() error {
	args := m.Called()
	return args.Error(0)
}

// MockRows provides a mock implementation of *sql.Rows for testing
type MockRows struct {
	mock.Mock
}

// Next mocks the Next method of *sql.Rows
func (m *MockRows) Next() bool {
	args := m.Called()
	return args.Bool(0)
}

// Scan mocks the Scan method of *sql.Rows
func (m *MockRows) Scan(dest ...interface{}) error {
	args := m.Called(dest)
	return args.Error(0)
}

// Close mocks the Close method of *sql.Rows
func (m *MockRows) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Err mocks the Err method of *sql.Rows
func (m *MockRows) Err() error {
	args := m.Called()
	return args.Error(0)
}

// MockResult provides a mock implementation of sql.Result for testing
type MockResult struct {
	mock.Mock
}

// LastInsertId mocks the LastInsertId method of sql.Result
func (m *MockResult) LastInsertId() (int64, error) {
	args := m.Called()
	return args.Get(0).(int64), args.Error(1)
}

// RowsAffected mocks the RowsAffected method of sql.Result
func (m *MockResult) RowsAffected() (int64, error) {
	args := m.Called()
	return args.Get(0).(int64), args.Error(1)
}

// CreateMockStore creates a Store instance with mocked database for testing setMinedMultiBulk
func CreateMockStore(logger ulogger.Logger) (*Store, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	if err != nil {
		panic("Failed to create sqlmock: " + err.Error())
	}

	// Wrap the sql.DB in usql.DB
	udb := &usql.DB{DB: db}

	store := &Store{
		db:     udb,
		logger: logger,
	}

	return store, mock
}

// SetupSetMinedMultiBulkMocks configures common mock expectations for setMinedMultiBulk testing
func SetupSetMinedMultiBulkMocks(mock sqlmock.Sqlmock, hashes []*chainhash.Hash, minedInfo utxo.MinedBlockInfo) {
	// Convert hashes to byte arrays for expectations
	hashBytes := make([][]byte, len(hashes))
	for i, hash := range hashes {
		hashBytes[i] = hash[:]
	}

	// Mock transaction begin
	mock.ExpectBegin()

	// Mock Step 1: Check which transactions exist
	existsRows := sqlmock.NewRows([]string{"hash"})
	for _, hashByte := range hashBytes {
		existsRows.AddRow(hashByte)
	}
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash = ANY\(\$1::bytea\[\]\)`).
		WithArgs(pq.Array(hashBytes)).
		WillReturnRows(existsRows)

	// Note: Step 2 varies based on UnsetMined flag - this mock assumes UnsetMined=false (insert)

	// Mock Step 2: Insert block_ids (use AnyArg for first param since hash order can change)
	mock.ExpectExec(`INSERT INTO block_ids \(transaction_id, block_id, block_height, subtree_idx\) SELECT t\.id, \$2, \$3, \$4 FROM transactions t WHERE t\.hash = ANY\(\$1::bytea\[\]\) ON CONFLICT DO NOTHING`).
		WithArgs(sqlmock.AnyArg(), minedInfo.BlockID, minedInfo.BlockHeight, minedInfo.SubtreeIdx).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))

	// Mock Step 3: Update transactions to mark as mined
	mock.ExpectExec(`UPDATE transactions SET locked = false, unmined_since = NULL WHERE hash = ANY\(\$1::bytea\[\]\)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))

	// Mock Step 4: Get block IDs for the transactions
	blockIDRows := sqlmock.NewRows([]string{"hash", "array_agg"})
	for _, hashByte := range hashBytes {
		blockIDRows.AddRow(hashByte, pq.Int32Array{int32(minedInfo.BlockID)}) // Mock block ID array
	}
	mock.ExpectQuery(`SELECT t\.hash, array_agg\(b\.block_id ORDER BY b\.block_id\) FROM transactions t LEFT JOIN block_ids b ON t\.id = b\.transaction_id WHERE t\.hash = ANY\(\$1::bytea\[\]\) GROUP BY t\.hash`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(blockIDRows)

	// Mock transaction commit
	mock.ExpectCommit()
}

// SetupSetMinedMultiBulkErrorMocks configures mock expectations for error scenarios
func SetupSetMinedMultiBulkErrorMocks(mock sqlmock.Sqlmock, errorType string) {
	switch errorType {
	case "begin_error":
		mock.ExpectBegin().WillReturnError(sql.ErrConnDone)

	case "check_exists_error":
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WillReturnError(sql.ErrNoRows)
		mock.ExpectRollback()

	case "insert_block_ids_error":
		mock.ExpectBegin()
		// First query succeeds with some results
		existsRows := sqlmock.NewRows([]string{"hash"})
		existsRows.AddRow([]byte("test-hash"))
		mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WillReturnRows(existsRows)
		// Insert block_ids fails
		mock.ExpectExec(`INSERT INTO block_ids \(transaction_id, block_id, block_height, subtree_idx\) SELECT t\.id, \$2, \$3, \$4 FROM transactions t WHERE t\.hash = ANY\(\$1::bytea\[\]\) ON CONFLICT DO NOTHING`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(sql.ErrConnDone)
		mock.ExpectRollback()

	case "update_error":
		mock.ExpectBegin()
		// First operations succeed
		existsRows := sqlmock.NewRows([]string{"hash"})
		existsRows.AddRow([]byte("test-hash"))
		mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WillReturnRows(existsRows)
		mock.ExpectExec(`INSERT INTO block_ids \(transaction_id, block_id, block_height, subtree_idx\) SELECT t\.id, \$2, \$3, \$4 FROM transactions t WHERE t\.hash = ANY\(\$1::bytea\[\]\) ON CONFLICT DO NOTHING`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		// Update fails
		mock.ExpectExec(`UPDATE transactions SET locked = false, unmined_since = NULL WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnError(sql.ErrTxDone)
		mock.ExpectRollback()

	case "get_block_ids_error":
		mock.ExpectBegin()
		// First three operations succeed
		existsRows := sqlmock.NewRows([]string{"hash"})
		existsRows.AddRow([]byte("test-hash"))
		mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WillReturnRows(existsRows)
		mock.ExpectExec(`INSERT INTO block_ids \(transaction_id, block_id, block_height, subtree_idx\) SELECT t\.id, \$2, \$3, \$4 FROM transactions t WHERE t\.hash = ANY\(\$1::bytea\[\]\) ON CONFLICT DO NOTHING`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`UPDATE transactions SET locked = false, unmined_since = NULL WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		// Get block IDs fails
		mock.ExpectQuery(`SELECT t\.hash, array_agg\(b\.block_id ORDER BY b\.block_id\) FROM transactions t LEFT JOIN block_ids b ON t\.id = b\.transaction_id WHERE t\.hash = ANY\(\$1::bytea\[\]\) GROUP BY t\.hash`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnError(sql.ErrConnDone)
		mock.ExpectRollback()

	case "commit_error":
		mock.ExpectBegin()
		// All operations succeed but commit fails
		existsRows := sqlmock.NewRows([]string{"hash"})
		existsRows.AddRow([]byte("test-hash"))
		mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WillReturnRows(existsRows)
		mock.ExpectExec(`INSERT INTO block_ids \(transaction_id, block_id, block_height, subtree_idx\) SELECT t\.id, \$2, \$3, \$4 FROM transactions t WHERE t\.hash = ANY\(\$1::bytea\[\]\) ON CONFLICT DO NOTHING`).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`UPDATE transactions SET locked = false, unmined_since = NULL WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		blockIDRows := sqlmock.NewRows([]string{"hash", "array_agg"})
		blockIDRows.AddRow([]byte("test"), pq.Int32Array{1})
		mock.ExpectQuery(`SELECT t\.hash, array_agg\(b\.block_id ORDER BY b\.block_id\) FROM transactions t LEFT JOIN block_ids b ON t\.id = b\.transaction_id WHERE t\.hash = ANY\(\$1::bytea\[\]\) GROUP BY t\.hash`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(blockIDRows)
		mock.ExpectCommit().WillReturnError(sql.ErrTxDone)

	case "rollback_error":
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash = ANY\(\$1::bytea\[\]\)`).
			WillReturnError(sql.ErrConnDone)
		mock.ExpectRollback().WillReturnError(sql.ErrConnDone)
	}
}

// CreateTestHashes creates a slice of test chainhash.Hash objects for testing
func CreateTestHashes(count int) []*chainhash.Hash {
	hashes := make([]*chainhash.Hash, count)
	for i := 0; i < count; i++ {
		hashStr := fmt.Sprintf("%064d", i+1) // Create unique 64-char hex strings
		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			panic("Failed to create test hash: " + err.Error())
		}
		hashes[i] = hash
	}
	return hashes
}

// CreateTestMinedBlockInfo creates a test MinedBlockInfo for testing
func CreateTestMinedBlockInfo() utxo.MinedBlockInfo {
	return utxo.MinedBlockInfo{
		BlockID:        1,
		BlockHeight:    100,
		SubtreeIdx:     0,
		UnsetMined:     false,
		OnLongestChain: true,
	}
}

// CreateMockDBForSchema creates a MockDB instance specifically for schema testing
func CreateMockDBForSchema() *MockDB {
	return &MockDB{}
}

// SetupCreatePostgresSchemaSuccessMocks configures mock expectations for successful schema creation
func SetupCreatePostgresSchemaSuccessMocks(mockDB *MockDB) {
	// Mock all 15+ DDL operations that createPostgresSchema performs

	// 1. CREATE TABLE transactions
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "CREATE TABLE IF NOT EXISTS transactions")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 2. CREATE INDEX ux_transactions_hash
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_hash")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 3. CREATE INDEX px_unmined_since_transactions
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "CREATE INDEX IF NOT EXISTS px_unmined_since_transactions")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 4. CREATE INDEX ux_transactions_delete_at_height
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "CREATE INDEX IF NOT EXISTS ux_transactions_delete_at_height")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 5. CREATE TABLE inputs
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "CREATE TABLE IF NOT EXISTS inputs")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 6. DROP CONSTRAINT inputs_transaction_id_fkey (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "inputs_transaction_id_fkey") && strings.Contains(query, "DROP CONSTRAINT")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 7. ADD CONSTRAINT inputs_transaction_id_fkey (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "inputs_transaction_id_fkey") && strings.Contains(query, "ADD CONSTRAINT")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 8. CREATE TABLE outputs
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "CREATE TABLE IF NOT EXISTS outputs")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 9. DROP CONSTRAINT outputs_transaction_id_fkey (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "outputs_transaction_id_fkey") && strings.Contains(query, "DROP CONSTRAINT")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 10. ADD CONSTRAINT outputs_transaction_id_fkey (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "outputs_transaction_id_fkey") && strings.Contains(query, "ADD CONSTRAINT")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 11. CREATE TABLE block_ids
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "CREATE TABLE IF NOT EXISTS block_ids")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 12. ADD COLUMN block_height and subtree_idx to block_ids (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "block_ids") && strings.Contains(query, "ADD COLUMN")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 13. ADD COLUMN unmined_since to transactions (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "unmined_since") && strings.Contains(query, "ADD COLUMN")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 14. ADD COLUMN preserve_until to transactions (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "preserve_until") && strings.Contains(query, "ADD COLUMN")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 15. DROP CONSTRAINT block_ids_transaction_id_fkey (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "block_ids_transaction_id_fkey") && strings.Contains(query, "DROP CONSTRAINT")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 16. ADD CONSTRAINT block_ids_transaction_id_fkey (DO $$ block)
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "DO $$") && strings.Contains(query, "block_ids_transaction_id_fkey") && strings.Contains(query, "ADD CONSTRAINT")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)

	// 17. CREATE TABLE conflicting_children
	mockDB.On("Exec", mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "CREATE TABLE IF NOT EXISTS conflicting_children")
	}), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)
}

// SetupCreatePostgresSchemaErrorMocks configures mock expectations for schema creation error at specific step
func SetupCreatePostgresSchemaErrorMocks(mockDB *MockDB, errorAtStep int) {
	// Define specific patterns to match each DDL operation in order
	stepMatchers := []func(string) bool{
		func(q string) bool { return strings.Contains(q, "CREATE TABLE IF NOT EXISTS transactions") },
		func(q string) bool {
			return strings.Contains(q, "CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_hash")
		},
		func(q string) bool {
			return strings.Contains(q, "CREATE INDEX IF NOT EXISTS px_unmined_since_transactions")
		},
		func(q string) bool {
			return strings.Contains(q, "CREATE INDEX IF NOT EXISTS ux_transactions_delete_at_height")
		},
		func(q string) bool { return strings.Contains(q, "CREATE TABLE IF NOT EXISTS inputs") },
		func(q string) bool {
			return strings.Contains(q, "inputs_transaction_id_fkey") && strings.Contains(q, "DROP CONSTRAINT")
		},
		func(q string) bool {
			return strings.Contains(q, "inputs_transaction_id_fkey") && strings.Contains(q, "ADD CONSTRAINT")
		},
		func(q string) bool { return strings.Contains(q, "CREATE TABLE IF NOT EXISTS outputs") },
		func(q string) bool {
			return strings.Contains(q, "outputs_transaction_id_fkey") && strings.Contains(q, "DROP CONSTRAINT")
		},
		func(q string) bool {
			return strings.Contains(q, "outputs_transaction_id_fkey") && strings.Contains(q, "ADD CONSTRAINT")
		},
		func(q string) bool { return strings.Contains(q, "CREATE TABLE IF NOT EXISTS block_ids") },
		func(q string) bool { return strings.Contains(q, "block_height") && strings.Contains(q, "ADD COLUMN") },
		func(q string) bool { return strings.Contains(q, "unmined_since") && strings.Contains(q, "ADD COLUMN") },
		func(q string) bool { return strings.Contains(q, "preserve_until") && strings.Contains(q, "ADD COLUMN") },
		func(q string) bool {
			return strings.Contains(q, "block_ids_transaction_id_fkey") && strings.Contains(q, "DROP CONSTRAINT")
		},
		func(q string) bool {
			return strings.Contains(q, "block_ids_transaction_id_fkey") && strings.Contains(q, "ADD CONSTRAINT")
		},
		func(q string) bool { return strings.Contains(q, "CREATE TABLE IF NOT EXISTS conflicting_children") },
	}

	// Setup successful mocks for steps before the error
	for i := 0; i < errorAtStep; i++ {
		if i < len(stepMatchers) {
			mockDB.On("Exec", mock.MatchedBy(stepMatchers[i]), mock.Anything).Return(sqlmock.NewResult(0, 0), nil)
		}
	}

	// Setup the error at the specified step
	if errorAtStep < len(stepMatchers) {
		mockDB.On("Exec", mock.MatchedBy(stepMatchers[errorAtStep]), mock.Anything).Return(nil, sql.ErrConnDone)
		// Expect Close() to be called after the error
		mockDB.On("Close").Return(nil)
	}
}
