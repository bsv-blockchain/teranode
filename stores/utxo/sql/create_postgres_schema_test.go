package sql

import (
	"database/sql"
	"testing"

	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/assert"
)

func TestCreatePostgresSchema_Success(t *testing.T) {
	// Create a mock database
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup successful mock expectations for all DDL operations
	SetupCreatePostgresSchemaSuccessMocks(mockDB)

	// Wrap the mock in usql.DB
	udb := &usql.DB{DB: nil} // We'll override the methods with our mock

	// Call the function under test
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	// Verify success
	assert.NoError(t, err)
}

func TestCreatePostgresSchema_ErrorAtTransactionsTable(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 0 (transactions table creation)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 0)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	// Verify error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create transactions table")
}

func TestCreatePostgresSchema_ErrorAtTransactionsHashIndex(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 1 (transactions hash index)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 1)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create ux_transactions_hash index")
}

func TestCreatePostgresSchema_ErrorAtUnminedSinceIndex(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 2 (unmined_since index)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 2)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create px_unmined_since_transactions index")
}

func TestCreatePostgresSchema_ErrorAtDeleteAtHeightIndex(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 3 (delete_at_height index)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 3)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create ux_transactions_delete_at_height index")
}

func TestCreatePostgresSchema_ErrorAtInputsTable(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 4 (inputs table creation)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 4)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create inputs table")
}

func TestCreatePostgresSchema_ErrorAtInputsConstraintEnsure(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 5 (inputs FK ensure CASCADE)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 5)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not ensure CASCADE foreign key on inputs table")
}

func TestCreatePostgresSchema_ErrorAtOutputsTable(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 6 (outputs table creation)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 6)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create outputs table")
}

func TestCreatePostgresSchema_ErrorAtOutputsConstraintEnsure(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 7 (outputs FK ensure CASCADE)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 7)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not ensure CASCADE foreign key on outputs table")
}

func TestCreatePostgresSchema_ErrorAtBlockIDsTable(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 8 (block_ids table creation)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 8)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create block_ids table")
}

func TestCreatePostgresSchema_ErrorAtBlockIDsColumnAdd(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 9 (block_ids column additions)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 9)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not add new columns to block_ids table")
}

func TestCreatePostgresSchema_ErrorAtUnminedSinceColumnAdd(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 10 (unmined_since column add)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 10)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not add unmined_since column to transactions table")
}

func TestCreatePostgresSchema_ErrorAtPreserveUntilColumnAdd(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 11 (preserve_until column add)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 11)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not add preserve_until column to transactions table")
}

func TestCreatePostgresSchema_ErrorAtBlockIDsConstraintEnsure(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 12 (block_ids FK ensure CASCADE)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 12)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not ensure CASCADE foreign key on block_ids table")
}

func TestCreatePostgresSchema_ErrorAtConflictingChildrenTable(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 13 (conflicting_children table creation)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 13)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create conflicting_children table")
}

func TestCreatePostgresSchema_ErrorAtUnspentByParentIndex(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	// Setup error at step 14 (px_outputs_unspent_by_parent partial index)
	SetupCreatePostgresSchemaErrorMocks(mockDB, 14)

	udb := &usql.DB{DB: nil}
	err := createPostgresSchemaWithMockDB(udb, mockDB)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create px_outputs_unspent_by_parent index")
}

// The ACTUAL solution to get coverage: Create a testable interface version
// and temporarily modify the original function to be testable

// Helper function that calls the ACTUAL extracted implementation
func createPostgresSchemaWithMockDB(_ *usql.DB, mockDB *MockDB) error {
	// Now we can call the actual implementation function with our mock!
	return createPostgresSchemaImpl(mockDB)
}

// createPostgresSchemaTestWrapper delegates to the real implementation.
func createPostgresSchemaTestWrapper(db interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Close() error
}) error {
	return createPostgresSchemaImpl(db)
}

// Direct test of our wrapper function to show coverage
func TestCreatePostgresSchemaTestWrapper_Success(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	SetupCreatePostgresSchemaSuccessMocks(mockDB)

	err := createPostgresSchemaTestWrapper(mockDB)
	assert.NoError(t, err)
}

func TestCreatePostgresSchemaTestWrapper_Error(t *testing.T) {
	mockDB := CreateMockDBForSchema()
	defer mockDB.AssertExpectations(t)

	SetupCreatePostgresSchemaErrorMocks(mockDB, 0)

	err := createPostgresSchemaTestWrapper(mockDB)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not create transactions table")
}
