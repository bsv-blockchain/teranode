package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreBlockWithID_SQLite(t *testing.T) {
	t.Run("store block with custom ID", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		customID := uint64(42)

		// Store block1 with custom ID
		id1, height1, err := s.StoreBlock(context.Background(), block1, "", options.WithID(customID))
		require.NoError(t, err)
		assert.Equal(t, customID, id1)
		assert.Equal(t, uint32(1), height1)

		// Verify the block can be retrieved by the custom ID
		retrievedBlock, err := s.GetBlockByID(context.Background(), customID)
		require.NoError(t, err)
		assert.Equal(t, block1.Hash().String(), retrievedBlock.Hash().String())
		assert.Equal(t, uint32(customID), retrievedBlock.ID)
	})

	t.Run("store block without custom ID uses auto-increment", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Store block1 without custom ID (should use auto-increment)
		// Note: Genesis block is auto-inserted during initialization with ID=0,
		// so first user block gets ID=1
		id1, height1, err := s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		assert.Equal(t, uint64(1), id1)
		assert.Equal(t, uint32(1), height1)

		// Store block2 without custom ID (should use auto-increment)
		id2, height2, err := s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		assert.Equal(t, uint64(2), id2)
		assert.Equal(t, uint32(2), height2)
	})

	t.Run("mix custom ID and auto-increment", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Store first block with custom ID 100
		customID1 := uint64(100)
		id1, _, err := s.StoreBlock(context.Background(), block1, "", options.WithID(customID1))
		require.NoError(t, err)
		assert.Equal(t, customID1, id1)

		// Store second block without custom ID (should use auto-increment)
		id2, _, err := s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		// The auto-increment should continue from where it left off
		assert.Greater(t, id2, uint64(0))

		// Store third block with another custom ID
		customID3 := uint64(500)
		id3, _, err := s.StoreBlock(context.Background(), block3, "", options.WithID(customID3))
		require.NoError(t, err)
		assert.Equal(t, customID3, id3)

		// Verify all blocks can be retrieved
		block1Retrieved, err := s.GetBlockByID(context.Background(), customID1)
		require.NoError(t, err)
		assert.Equal(t, block1.Hash().String(), block1Retrieved.Hash().String())

		block2Retrieved, err := s.GetBlockByID(context.Background(), id2)
		require.NoError(t, err)
		assert.Equal(t, block2.Hash().String(), block2Retrieved.Hash().String())

		block3Retrieved, err := s.GetBlockByID(context.Background(), customID3)
		require.NoError(t, err)
		assert.Equal(t, block3.Hash().String(), block3Retrieved.Hash().String())
	})

	t.Run("duplicate custom ID should fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		customID := uint64(42)

		// Store first block with custom ID
		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithID(customID))
		require.NoError(t, err)

		// Try to store second block with same custom ID - should fail
		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithID(customID))
		assert.Error(t, err)
		// Error should indicate duplicate key constraint violation
	})

	t.Run("WithID with zero value should use auto-increment", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Store block with WithID(0) should behave like no WithID option
		// Genesis block is auto-inserted with ID=0, so first user block gets ID=1
		id1, _, err := s.StoreBlock(context.Background(), block1, "", options.WithID(0))
		require.NoError(t, err)
		assert.Equal(t, uint64(1), id1)

		// Store another block normally
		id2, _, err := s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		assert.Equal(t, uint64(2), id2)
	})

	t.Run("WithID works with other options", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		customID := uint64(999)

		// Store block with custom ID and other options
		id1, _, err := s.StoreBlock(context.Background(), block1, "",
			options.WithID(customID),
			options.WithMinedSet(true),
			options.WithSubtreesSet(true),
		)
		require.NoError(t, err)
		assert.Equal(t, customID, id1)

		// Verify the block was stored correctly
		retrievedBlock, err := s.GetBlockByID(context.Background(), customID)
		require.NoError(t, err)
		assert.Equal(t, uint32(customID), retrievedBlock.ID)
	})

	t.Run("genesis block cannot have custom ID", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)
		defer s.Close()

		// Since the genesis block is automatically inserted when the table is created,
		// we need to retrieve it from the database to get the exact same block
		// that would be recognized as genesis by the coinbase TX ID matching logic
		genesisBlockFromDB, err := s.GetBlockByHeight(context.Background(), 0)
		require.NoError(t, err)
		require.NotNil(t, genesisBlockFromDB)

		// Try to store the genesis block again with custom ID - should fail
		customID := uint64(12345)
		_, _, err = s.StoreBlock(context.Background(), genesisBlockFromDB, "", options.WithID(customID))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "genesis block cannot have custom ID")
	})
}
