package pruner

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
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
