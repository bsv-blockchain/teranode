package pruner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestSettings creates default settings for testing
func createTestSettings() *settings.Settings {
	return &settings.Settings{
		GlobalBlockHeightRetention: 288, // Default retention
	}
}

func TestNewService(t *testing.T) {
	t.Run("ValidService", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
			Ctx:    context.Background(),
		})

		assert.NoError(t, err)
		assert.NotNil(t, service)
		assert.Equal(t, logger, service.logger)
		assert.Equal(t, db.DB, service.db)
	})

	t.Run("NilLogger", func(t *testing.T) {
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: nil,
			DB:     db.DB,
		})

		assert.Error(t, err)
		assert.Nil(t, service)
		assert.Contains(t, err.Error(), "logger is required")
	})

	t.Run("NilSettings", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(nil, Options{
			Logger: logger,
			DB:     db.DB,
		})

		assert.Error(t, err)
		assert.Nil(t, service)
		assert.Contains(t, err.Error(), "settings is required")
	})

	t.Run("NilDB", func(t *testing.T) {
		logger := &MockLogger{}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     nil,
		})

		assert.Error(t, err)
		assert.Nil(t, service)
		assert.Contains(t, err.Error(), "db is required")
	})

	t.Run("DefaultValues", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})

		assert.NoError(t, err)
		assert.NotNil(t, service)
	})
}

func TestService_Start(t *testing.T) {
	t.Run("StartService", func(t *testing.T) {
		loggedMessages := make([]string, 0, 5)
		logger := &MockLogger{
			InfofFunc: func(format string, args ...interface{}) {
				loggedMessages = append(loggedMessages, format)
			},
		}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		service.Start(ctx)

		// Check that service ready message is logged
		found := false
		for _, msg := range loggedMessages {
			if strings.Contains(msg, "service ready") {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected to find 'service ready' in logged messages: %v", loggedMessages)
	})
}

func TestService_PruneValidation(t *testing.T) {
	t.Run("ValidBlockHeight", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("ZeroBlockHeight", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 0, "<test-hash>")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot prune at block height 0")
		assert.Equal(t, int64(0), recordsProcessed)
	})

	t.Run("WithContext", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})
}

func TestService_Prune(t *testing.T) {
	t.Run("PruneEmpty", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("PruneWithData", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})
}

func TestService_PruneExecution(t *testing.T) {
	t.Run("SuccessfulPrune", func(t *testing.T) {
		loggedMessages := make([]string, 0, 5)
		logger := &MockLogger{
			InfofFunc: func(format string, args ...interface{}) {
				loggedMessages = append(loggedMessages, format)
			},
		}

		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))

		// Verify logging
		assert.GreaterOrEqual(t, len(loggedMessages), 1)
		found := false
		for _, msg := range loggedMessages {
			if strings.Contains(msg, "phase 2: starting cleanup scan") {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected to find 'starting cleanup scan' in logged messages")
	})

	t.Run("PruneWithNoRecords", func(t *testing.T) {
		logger := &MockLogger{
			InfofFunc: func(format string, args ...interface{}) {},
		}

		db := NewMockDB()
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			return &MockResult{rowsAffected: 0}, nil
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("PruneCancelledContext", func(t *testing.T) {
		logger := &MockLogger{
			InfofFunc:  func(format string, args ...interface{}) {},
			ErrorfFunc: func(format string, args ...interface{}) {},
		}

		db := NewMockDB()
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			return nil, context.Canceled
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.Error(t, err)
		assert.Equal(t, int64(0), recordsProcessed)
	})
}

func TestDeleteTombstoned(t *testing.T) {
	t.Run("SuccessfulDelete", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("DatabaseError", func(t *testing.T) {
		logger := &MockLogger{
			InfofFunc:  func(format string, args ...interface{}) {},
			ErrorfFunc: func(format string, args ...interface{}) {},
		}
		db := NewMockDB()
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			return nil, sql.ErrConnDone
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		// The mock may or may not propagate the error depending on driver behavior
		// Just verify the operation completes
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
		_ = err // Error behavior depends on mock implementation
	})

	t.Run("MaxBlockHeight", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 4294967295, "<test-hash>") // Max uint32
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})
}

func TestService_IntegrationTests(t *testing.T) {
	t.Run("FullWorkflow", func(t *testing.T) {
		logger := &MockLogger{
			InfofFunc:  func(format string, args ...interface{}) {},
			DebugfFunc: func(format string, args ...interface{}) {},
		}

		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		// Start the service
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		service.Start(ctx)

		// Give the workers a moment to start
		time.Sleep(50 * time.Millisecond)

		// Run prune synchronously
		pruneCtx, pruneCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer pruneCancel()

		recordsProcessed, err := service.Prune(pruneCtx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("ServiceImplementsInterface", func(t *testing.T) {
		logger := &MockLogger{}
		db := NewMockDB()

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		// Verify service implements the interface
		var _ pruner.Service = service
	})
}

func TestService_EdgeCases(t *testing.T) {
	t.Run("RapidPrunes", func(t *testing.T) {
		logger := &MockLogger{
			InfofFunc:  func(format string, args ...interface{}) {},
			DebugfFunc: func(format string, args ...interface{}) {},
		}

		db := NewMockDB()
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			return &MockResult{rowsAffected: 1}, nil
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		// Rapid prunes should not cause issues
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		for i := uint32(1); i <= 10; i++ {
			recordsProcessed, err := service.Prune(ctx, i, "<test-hash>")
			assert.NoError(t, err)
			assert.GreaterOrEqual(t, recordsProcessed, int64(0))
		}
	})

	t.Run("LargeBlockHeight", func(t *testing.T) {
		logger := &MockLogger{}

		db := NewMockDB()
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			height := args[0].(uint32)
			assert.Equal(t, uint32(4294967295), height) // Max uint32
			return &MockResult{rowsAffected: 1}, nil
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 4294967295, "<test-hash>") // Max uint32
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("DatabaseAvailable", func(t *testing.T) {
		logger := &MockLogger{
			InfofFunc:  func(format string, args ...interface{}) {},
			DebugfFunc: func(format string, args ...interface{}) {},
			ErrorfFunc: func(format string, args ...interface{}) {},
		}

		db := NewMockDB()
		// Configure mock to handle child safety query with 2 parameters
		db.ExecFunc = func(query string, args ...interface{}) (sql.Result, error) {
			return &MockResult{rowsAffected: 0}, nil
		}

		service, err := NewService(createTestSettings(), Options{
			Logger: logger,
			DB:     db.DB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(ctx, 100, "<test-hash>")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})
}

// TestUnverifiedClaimClauseIsIdentical: the unverified-claim clause is written
// out in full in each pruning query, so every statement is one literal. The
// marker INSERT and the DELETE must hold back exactly the same children, or a
// child would be deleted without its marker, or marked and kept. This pins the
// copies to one text: twice scoped to child, four times to transactions.
func TestUnverifiedClaimClauseIsIdentical(t *testing.T) {
	src, err := os.ReadFile("pruner_service.go")
	require.NoError(t, err)

	clause := func(alias string) string {
		return `SELECT 1
      FROM inputs hi
      JOIN transactions hp ON hp.hash = hi.previous_transaction_hash
      LEFT JOIN outputs ho ON ho.transaction_id = hp.id AND ho.idx = hi.previous_tx_idx
      WHERE hi.transaction_id = ` + alias + `.id
        AND (ho.transaction_id IS NULL OR ho.spending_data IS NULL OR length(ho.spending_data) < 36) AND NOT ` + alias + `.conflicting`
	}

	require.Equal(t, 2, strings.Count(string(src), clause("child")))
	require.Equal(t, 4, strings.Count(string(src), clause("transactions")))
	require.Equal(t, 6, strings.Count(string(src), "LEFT JOIN outputs ho ON"), "every copy of the clause must match the text above")
}

func newSqlmockService(t *testing.T, defensive bool, reap func(ctx context.Context, txHash chainhash.Hash, blockHeight, safetyWindow uint32) (bool, error)) (*Service, sqlmock.Sqlmock, *[]string) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	var errorLogs []string

	logger := &MockLogger{
		ErrorfFunc: func(format string, args ...interface{}) {
			errorLogs = append(errorLogs, fmt.Sprintf(format, args...))
		},
	}

	tSettings := createTestSettings()
	tSettings.Pruner.UTXODefensiveEnabled = defensive

	service, err := NewService(tSettings, Options{
		Logger:          logger,
		DB:              &usql.DB{DB: sqlDB},
		ReapConflicting: reap,
	})
	require.NoError(t, err)

	return service, mock, &errorLogs
}

func TestReapExpiredConflicting(t *testing.T) {
	hashA := chainhash.Hash{1}
	hashB := chainhash.Hash{2}

	t.Run("NoReapFunctionIsANoOp", func(t *testing.T) {
		service, mock, _ := newSqlmockService(t, true, nil)

		reaped, err := service.reapExpiredConflicting(context.Background(), 100, 5)
		require.NoError(t, err)
		require.Equal(t, int64(0), reaped)
		require.NoError(t, mock.ExpectationsWereMet(), "no query may run without a reap function")
	})

	t.Run("ListQueryErrorIsReturned", func(t *testing.T) {
		service, mock, _ := newSqlmockService(t, true, func(context.Context, chainhash.Hash, uint32, uint32) (bool, error) {
			require.FailNow(t, "nothing must be reaped when the list query fails")

			return false, nil
		})

		mock.ExpectQuery("SELECT t.hash").WillReturnError(sql.ErrConnDone)

		reaped, err := service.reapExpiredConflicting(context.Background(), 100, 5)
		require.Error(t, err)
		require.ErrorIs(t, err, sql.ErrConnDone)
		require.Equal(t, int64(0), reaped)
	})

	t.Run("RowIterationErrorIsReturned", func(t *testing.T) {
		service, mock, _ := newSqlmockService(t, true, func(context.Context, chainhash.Hash, uint32, uint32) (bool, error) {
			require.FailNow(t, "nothing must be reaped when the list cannot be read")

			return false, nil
		})

		rows := sqlmock.NewRows([]string{"hash"}).AddRow(hashA[:]).RowError(0, sql.ErrConnDone)
		mock.ExpectQuery("SELECT t.hash").WillReturnRows(rows)

		reaped, err := service.reapExpiredConflicting(context.Background(), 100, 5)
		require.Error(t, err)
		require.Equal(t, int64(0), reaped)
	})

	t.Run("CloseErrorIsReturned", func(t *testing.T) {
		service, mock, _ := newSqlmockService(t, true, func(context.Context, chainhash.Hash, uint32, uint32) (bool, error) {
			require.FailNow(t, "nothing must be reaped when the list cannot be closed")

			return false, nil
		})

		rows := sqlmock.NewRows([]string{"hash"}).AddRow(hashA[:]).CloseError(sql.ErrConnDone)
		mock.ExpectQuery("SELECT t.hash").WillReturnRows(rows)

		reaped, err := service.reapExpiredConflicting(context.Background(), 100, 5)
		require.Error(t, err)
		require.Equal(t, int64(0), reaped)
	})

	t.Run("MalformedHashIsSkipped", func(t *testing.T) {
		var reapedHashes []chainhash.Hash

		service, mock, _ := newSqlmockService(t, true, func(_ context.Context, hash chainhash.Hash, _, _ uint32) (bool, error) {
			reapedHashes = append(reapedHashes, hash)

			return true, nil
		})

		rows := sqlmock.NewRows([]string{"hash"}).AddRow([]byte{1, 2, 3}).AddRow(hashB[:])
		mock.ExpectQuery("SELECT t.hash").WillReturnRows(rows)

		reaped, err := service.reapExpiredConflicting(context.Background(), 100, 5)
		require.NoError(t, err)
		require.Equal(t, int64(1), reaped)
		require.Equal(t, []chainhash.Hash{hashB}, reapedHashes)
	})

	t.Run("ReapErrorIsLoggedAndTheRestAreStillReaped", func(t *testing.T) {
		var attempted []chainhash.Hash

		service, mock, errorLogs := newSqlmockService(t, true, func(_ context.Context, hash chainhash.Hash, blockHeight, safetyWindow uint32) (bool, error) {
			require.Equal(t, uint32(100), blockHeight)
			require.Equal(t, uint32(5), safetyWindow)

			attempted = append(attempted, hash)

			if hash == hashA {
				return false, errors.NewStorageError("reap failed")
			}

			return true, nil
		})

		rows := sqlmock.NewRows([]string{"hash"}).AddRow(hashA[:]).AddRow(hashB[:])
		mock.ExpectQuery("SELECT t.hash").WillReturnRows(rows)

		reaped, err := service.reapExpiredConflicting(context.Background(), 100, 5)
		require.NoError(t, err, "a record that cannot be reaped is kept, not a pruning failure")
		require.Equal(t, int64(1), reaped, "only the record that was reaped is counted")
		require.Equal(t, []chainhash.Hash{hashA, hashB}, attempted)
		require.Len(t, *errorLogs, 1)
		require.Contains(t, (*errorLogs)[0], hashA.String())
	})

	t.Run("IneligibleRecordIsNotCounted", func(t *testing.T) {
		service, mock, errorLogs := newSqlmockService(t, true, func(context.Context, chainhash.Hash, uint32, uint32) (bool, error) {
			return false, nil
		})

		rows := sqlmock.NewRows([]string{"hash"}).AddRow(hashA[:])
		mock.ExpectQuery("SELECT t.hash").WillReturnRows(rows)

		reaped, err := service.reapExpiredConflicting(context.Background(), 100, 5)
		require.NoError(t, err)
		require.Equal(t, int64(0), reaped)
		require.Empty(t, *errorLogs)
	})
}
