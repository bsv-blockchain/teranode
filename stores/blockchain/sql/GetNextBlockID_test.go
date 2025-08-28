package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestGetNextBlockID_SQLite(t *testing.T) {
	t.Run("sequential ID generation from empty database", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Test that first call returns 1
		id1, err := s.GetNextBlockID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint64(1), id1)

		// Test that subsequent calls increment
		id2, err := s.GetNextBlockID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint64(2), id2)

		id3, err := s.GetNextBlockID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint64(3), id3)
	})

	t.Run("ID generation after storing blocks", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Store a block (which should use ID 1)
		id1, _, err := s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		assert.Equal(t, uint64(1), id1)

		// Get next ID should be the next available after what StoreBlock used
		nextID, err := s.GetNextBlockID(context.Background())
		require.NoError(t, err)
		// We expect this to be whatever comes next after the stored block
		assert.Greater(t, nextID, id1)

		// Store another block
		id2, _, err := s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		// The second block should get the next ID in sequence
		assert.Greater(t, id2, id1)

		// Get next ID should be greater than the last stored block
		nextID2, err := s.GetNextBlockID(context.Background())
		require.NoError(t, err)
		assert.Greater(t, nextID2, id2)
	})

	t.Run("concurrent ID generation", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Test concurrent access
		numGoroutines := 10
		results := make([]uint64, numGoroutines)
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(index int) {
				id, err := s.GetNextBlockID(context.Background())
				if err != nil {
					t.Errorf("Error getting next block ID: %v", err)
				}
				results[index] = id
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify all IDs are unique and within expected range
		idMap := make(map[uint64]bool)
		for _, id := range results {
			assert.False(t, idMap[id], "ID %d was generated more than once", id)
			idMap[id] = true
			assert.GreaterOrEqual(t, id, uint64(1))
			assert.LessOrEqual(t, id, uint64(numGoroutines))
		}
		assert.Len(t, idMap, numGoroutines, "Not all IDs were unique")
	})

	t.Run("context cancellation", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Create a context that will be cancelled
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err = s.GetNextBlockID(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("timeout context", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Create a context with very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Sleep to ensure timeout
		time.Sleep(1 * time.Millisecond)

		_, err = s.GetNextBlockID(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})
}

func TestGetNextBlockID_Postgres(t *testing.T) {
	ctx := context.Background()

	t.Run("sequential ID generation from empty database", func(t *testing.T) {
		// Start PostgreSQL container
		pgContainer, err := postgres.Run(ctx,
			"postgres:13",
			postgres.WithDatabase("testdb"),
			postgres.WithUsername("testuser"),
			postgres.WithPassword("testpass"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(5*time.Minute),
			),
		)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, pgContainer.Terminate(ctx))
		}()

		// Get connection string
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)

		dbURL, err := url.Parse(connStr)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Ensure we're using PostgreSQL
		assert.Equal(t, util.Postgres, s.engine)

		// Test that first call returns 1
		id1, err := s.GetNextBlockID(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), id1)

		// Test that subsequent calls increment
		id2, err := s.GetNextBlockID(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), id2)

		id3, err := s.GetNextBlockID(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), id3)
	})

	t.Run("ID generation after storing blocks", func(t *testing.T) {
		// Start PostgreSQL container
		pgContainer, err := postgres.Run(ctx,
			"postgres:13",
			postgres.WithDatabase("testdb"),
			postgres.WithUsername("testuser"),
			postgres.WithPassword("testpass"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(5*time.Minute),
			),
		)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, pgContainer.Terminate(ctx))
		}()

		// Get connection string
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)

		dbURL, err := url.Parse(connStr)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Store a block (which should use ID 1)
		id1, _, err := s.StoreBlock(ctx, block1, "")
		require.NoError(t, err)
		assert.Equal(t, uint64(1), id1)

		// Get next ID should be the next available after what StoreBlock used
		nextID, err := s.GetNextBlockID(ctx)
		require.NoError(t, err)
		// We expect this to be whatever comes next after the stored block
		assert.Greater(t, nextID, id1)

		// Store another block
		id2, _, err := s.StoreBlock(ctx, block2, "")
		require.NoError(t, err)
		// The second block should get the next ID in sequence
		assert.Greater(t, id2, id1)

		// Get next ID should be greater than the last stored block
		nextID2, err := s.GetNextBlockID(ctx)
		require.NoError(t, err)
		assert.Greater(t, nextID2, id2)
	})

	t.Run("concurrent ID generation", func(t *testing.T) {
		// Start PostgreSQL container
		pgContainer, err := postgres.Run(ctx,
			"postgres:13",
			postgres.WithDatabase("testdb"),
			postgres.WithUsername("testuser"),
			postgres.WithPassword("testpass"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(5*time.Minute),
			),
		)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, pgContainer.Terminate(ctx))
		}()

		// Get connection string
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)

		dbURL, err := url.Parse(connStr)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Test concurrent access
		numGoroutines := 10
		results := make([]uint64, numGoroutines)
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(index int) {
				id, err := s.GetNextBlockID(ctx)
				if err != nil {
					t.Errorf("Error getting next block ID: %v", err)
				}
				results[index] = id
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify all IDs are unique and within expected range
		idMap := make(map[uint64]bool)
		for _, id := range results {
			assert.False(t, idMap[id], "ID %d was generated more than once", id)
			idMap[id] = true
			assert.GreaterOrEqual(t, id, uint64(1))
			assert.LessOrEqual(t, id, uint64(numGoroutines))
		}
		assert.Len(t, idMap, numGoroutines, "Not all IDs were unique")
	})

	t.Run("context cancellation", func(t *testing.T) {
		// Start PostgreSQL container
		pgContainer, err := postgres.Run(ctx,
			"postgres:13",
			postgres.WithDatabase("testdb"),
			postgres.WithUsername("testuser"),
			postgres.WithPassword("testpass"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(5*time.Minute),
			),
		)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, pgContainer.Terminate(ctx))
		}()

		// Get connection string
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)

		dbURL, err := url.Parse(connStr)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Create a context that will be cancelled
		testCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err = s.GetNextBlockID(testCtx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

func TestGetNextBlockID_DatabaseEngineDetection(t *testing.T) {
	t.Run("SQLite engine detection", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Verify SQLite engine is detected (could be Sqlite or SqliteMemory)
		assert.True(t, s.engine == util.Sqlite || s.engine == util.SqliteMemory)

		// Test that GetNextBlockID calls the SQLite implementation
		id, err := s.GetNextBlockID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint64(1), id)
	})
}
