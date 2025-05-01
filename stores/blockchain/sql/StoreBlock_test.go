package sql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	teranodeErrors "github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/lib/pq"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite" // Import for side-effect (driver registration)
)

func mockParseBlock() *model.Block {
	// Create a minimal block. The parseSQLError function only uses block.Hash().
	// Ensure the Header is initialized *fully enough* for Hash() to work.
	// Hash() calls Header.Hash() -> Header.Bytes(), which needs non-nil hashes and Bits.
	blk := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Unix(1234567890, 0).Unix()), // nolint:gosec
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          0,
		},
		// Note: TransactionCount, SizeInBytes etc. are not needed for Hash()
	}
	// We don't need to pre-compute the hash; Block.Hash() will calculate it.
	return blk
}

// generateSQLiteConstraintError creates an in-memory SQLite database,
// creates a table with a unique constraint, and triggers that constraint
// violation to return the specific error.
func generateSQLiteConstraintError() error {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return fmt.Errorf("failed to open in-memory sqlite db: %w", err) // nolint:forbidigo
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err) // nolint:forbidigo
	}

	_, err = db.Exec("INSERT INTO test (name) VALUES (?)", "unique_value")
	if err != nil {
		return fmt.Errorf("failed to insert initial value: %w", err) // nolint:forbidigo
	}

	// Attempt to insert the same value again to trigger the constraint violation
	_, err = db.Exec("INSERT INTO test (name) VALUES (?)", "unique_value")
	// We expect an error here
	if err == nil {
		return fmt.Errorf("expected a constraint violation error, but got nil") // nolint:forbidigo
	}

	return err // Return the constraint violation error
}

func TestParseSQLError(t *testing.T) {
	s := &SQL{}
	mockBlk := mockParseBlock()
	mockBlockHashStr := mockBlk.Hash().String()

	sqliteConstraintErr := generateSQLiteConstraintError()
	if sqliteConstraintErr == nil {
		t.Fatal("Failed to generate SQLite constraint error for testing")
	}

	testCases := []struct {
		name                string
		inputErr            error
		block               *model.Block
		expectedSentinelErr error
		expectMsg           string
	}{
		{
			name: "Postgres Duplicate Error",
			inputErr: &pq.Error{
				Code:    "23505",
				Message: "duplicate key value violates unique constraint",
			},
			block:               mockParseBlock(),
			expectedSentinelErr: teranodeErrors.ErrBlockExists,
			expectMsg:           fmt.Sprintf("block already exists in the database: %s", mockBlockHashStr),
		},
		{
			name:                "SQLite Duplicate Error",
			inputErr:            sqliteConstraintErr, // Use the generated error
			block:               mockParseBlock(),
			expectedSentinelErr: teranodeErrors.ErrBlockExists,
			expectMsg:           fmt.Sprintf("block already exists in the database: %s", mockBlockHashStr), // Match NewBlockExistsError format
		},
		{
			name:                "Generic SQL Error (e.g., connection)",
			inputErr:            sql.ErrConnDone,
			block:               mockParseBlock(),
			expectedSentinelErr: teranodeErrors.ErrStorageError,
			expectMsg:           "failed to store block", // Match NewStorageError format
		},
		{
			name:                "Other Generic Error",
			inputErr:            fmt.Errorf("some other random error"), // nolint:forbidigo
			block:               mockParseBlock(),
			expectedSentinelErr: teranodeErrors.ErrStorageError,
			expectMsg:           "failed to store block", // Match NewStorageError format
		},
		{
			name:                "Nil Error",
			inputErr:            nil,
			block:               mockParseBlock(),
			expectedSentinelErr: teranodeErrors.ErrStorageError, // Reverted code wraps nil with StorageError
			expectMsg:           "failed to store block",        // Match NewStorageError format
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsedErr := s.parseSQLError(tc.inputErr, tc.block)

			if tc.expectedSentinelErr != nil {
				assert.True(t, teranodeErrors.Is(parsedErr, tc.expectedSentinelErr), "Expected error to be of type %T", tc.expectedSentinelErr)
			} else {
				assert.Nil(t, parsedErr, "Expected nil error")
			}

			if tc.expectMsg != "" {
				assert.Contains(t, parsedErr.Error(), tc.expectMsg, "Error message mismatch")
			}
		})
	}
}
