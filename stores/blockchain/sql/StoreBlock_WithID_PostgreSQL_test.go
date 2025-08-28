package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestStoreBlockWithID_Postgres(t *testing.T) {
	ctx := context.Background()

	t.Run("store block with custom ID", func(t *testing.T) {
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

		customID := uint64(42)

		// Store block1 with custom ID
		id1, height1, err := s.StoreBlock(ctx, block1, "", options.WithID(customID))
		require.NoError(t, err)
		assert.Equal(t, customID, id1)
		assert.Equal(t, uint32(1), height1)

		// Verify the block can be retrieved by the custom ID
		retrievedBlock, err := s.GetBlockByID(ctx, customID)
		require.NoError(t, err)
		assert.Equal(t, block1.Hash().String(), retrievedBlock.Hash().String())
		assert.Equal(t, uint32(customID), retrievedBlock.ID)
	})

	t.Run("mix custom ID and auto-increment", func(t *testing.T) {
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

		// Store first block with custom ID 100
		customID1 := uint64(100)
		id1, _, err := s.StoreBlock(ctx, block1, "", options.WithID(customID1))
		require.NoError(t, err)
		assert.Equal(t, customID1, id1)

		// Store second block without custom ID (should use auto-increment)
		id2, _, err := s.StoreBlock(ctx, block2, "")
		require.NoError(t, err)
		// The auto-increment should continue from where it left off
		assert.Greater(t, id2, uint64(0))

		// Store third block with another custom ID
		customID3 := uint64(500)
		id3, _, err := s.StoreBlock(ctx, block3, "", options.WithID(customID3))
		require.NoError(t, err)
		assert.Equal(t, customID3, id3)

		// Verify all blocks can be retrieved
		block1Retrieved, err := s.GetBlockByID(ctx, customID1)
		require.NoError(t, err)
		assert.Equal(t, block1.Hash().String(), block1Retrieved.Hash().String())

		block2Retrieved, err := s.GetBlockByID(ctx, id2)
		require.NoError(t, err)
		assert.Equal(t, block2.Hash().String(), block2Retrieved.Hash().String())

		block3Retrieved, err := s.GetBlockByID(ctx, customID3)
		require.NoError(t, err)
		assert.Equal(t, block3.Hash().String(), block3Retrieved.Hash().String())
	})

	t.Run("genesis block cannot have custom ID", func(t *testing.T) {
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

		// Since the genesis block is automatically inserted when the table is created,
		// we need to retrieve it from the database to get the exact same block
		// that would be recognized as genesis by the coinbase TX ID matching logic
		genesisBlockFromDB, err := s.GetBlockByHeight(ctx, 0)
		require.NoError(t, err)
		require.NotNil(t, genesisBlockFromDB)

		// Try to store the genesis block again with custom ID - should fail
		customID := uint64(12345)
		_, _, err = s.StoreBlock(ctx, genesisBlockFromDB, "", options.WithID(customID))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "genesis block cannot have custom ID")
	})
}
