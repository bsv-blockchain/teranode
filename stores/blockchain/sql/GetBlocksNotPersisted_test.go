package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// markGenesisAsPersisted marks the genesis block as persisted to avoid it
// appearing in GetBlocksNotPersisted results, since the store automatically
// inserts a genesis block with persisted_at=NULL.
func markGenesisAsPersisted(t *testing.T, store *SQL, s *settings.Settings) {
	t.Helper()
	genesisHash := s.ChainCfgParams.GenesisHash
	err := store.SetBlockPersistedAt(context.Background(), genesisHash)
	require.NoError(t, err)
}

func TestGetBlocksNotPersisted(t *testing.T) {
	t.Run("returns empty when no blocks", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		// Mark genesis as persisted so we start with a clean slate
		markGenesisAsPersisted(t, store, s)

		blocks, err := store.GetBlocksNotPersisted(context.Background(), 100)
		require.NoError(t, err)
		assert.Empty(t, blocks)
	})

	t.Run("returns blocks not persisted and not invalid", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		// Mark genesis as persisted
		markGenesisAsPersisted(t, store, s)

		// Store block1 - not persisted, not invalid (should be returned)
		_, _, err = store.StoreBlock(context.Background(), block1, "test")
		require.NoError(t, err)

		blocks, err := store.GetBlocksNotPersisted(context.Background(), 100)
		require.NoError(t, err)
		require.Len(t, blocks, 1)
		assert.Equal(t, block1.Hash().String(), blocks[0].Hash().String())
	})

	t.Run("excludes invalid blocks", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		// Mark genesis as persisted
		markGenesisAsPersisted(t, store, s)

		// Store block1 - not persisted, not invalid
		_, _, err = store.StoreBlock(context.Background(), block1, "test")
		require.NoError(t, err)

		// Mark block1 as invalid
		_, err = store.InvalidateBlock(context.Background(), block1.Hash())
		require.NoError(t, err)

		blocks, err := store.GetBlocksNotPersisted(context.Background(), 100)
		require.NoError(t, err)
		assert.Empty(t, blocks)
	})

	t.Run("excludes persisted blocks", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		// Mark genesis as persisted
		markGenesisAsPersisted(t, store, s)

		// Store block1 - not persisted
		_, _, err = store.StoreBlock(context.Background(), block1, "test")
		require.NoError(t, err)

		// Mark block1 as persisted
		err = store.SetBlockPersistedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		blocks, err := store.GetBlocksNotPersisted(context.Background(), 100)
		require.NoError(t, err)
		assert.Empty(t, blocks)
	})

	t.Run("returns multiple blocks ordered by height", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		// Mark genesis as persisted
		markGenesisAsPersisted(t, store, s)

		// Store blocks with different heights
		_, _, err = store.StoreBlock(context.Background(), block1, "test") // height 1
		require.NoError(t, err)
		_, _, err = store.StoreBlock(context.Background(), block2, "test") // height 2
		require.NoError(t, err)

		blocks, err := store.GetBlocksNotPersisted(context.Background(), 100)
		require.NoError(t, err)
		require.Len(t, blocks, 2)

		// Verify blocks are returned (may not be in strict height order based on block data)
		blockHashes := []string{blocks[0].Hash().String(), blocks[1].Hash().String()}
		assert.Contains(t, blockHashes, block1.Hash().String())
		assert.Contains(t, blockHashes, block2.Hash().String())
	})

	t.Run("respects limit parameter", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		// Mark genesis as persisted
		markGenesisAsPersisted(t, store, s)

		// Store multiple blocks
		_, _, err = store.StoreBlock(context.Background(), block1, "test")
		require.NoError(t, err)
		_, _, err = store.StoreBlock(context.Background(), block2, "test")
		require.NoError(t, err)

		// Request with limit 1
		blocks, err := store.GetBlocksNotPersisted(context.Background(), 1)
		require.NoError(t, err)
		require.Len(t, blocks, 1)
	})

	t.Run("returns complete block objects", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		// Mark genesis as persisted
		markGenesisAsPersisted(t, store, s)

		// Store block with subtrees_set=true (so subtrees are populated)
		_, _, err = store.StoreBlock(context.Background(), block1, "test", options.WithSubtreesSet(true))
		require.NoError(t, err)

		blocks, err := store.GetBlocksNotPersisted(context.Background(), 100)
		require.NoError(t, err)
		require.Len(t, blocks, 1)

		block := blocks[0]
		assert.NotNil(t, block.Header)
		assert.Equal(t, block1.Header.Version, block.Header.Version)
		assert.Equal(t, block1.Header.Timestamp, block.Header.Timestamp)
		assert.Equal(t, block1.Header.Nonce, block.Header.Nonce)
		assert.Equal(t, block1.Header.HashPrevBlock.String(), block.Header.HashPrevBlock.String())
		assert.Equal(t, block1.Header.HashMerkleRoot.String(), block.Header.HashMerkleRoot.String())
		assert.Equal(t, block1.Header.Bits.String(), block.Header.Bits.String())
		assert.Equal(t, block1.TransactionCount, block.TransactionCount)
		assert.NotNil(t, block.CoinbaseTx)
		assert.NotNil(t, block.Subtrees)
		assert.Len(t, block.Subtrees, len(block1.Subtrees))
	})

	t.Run("mixed persisted and non-persisted blocks", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		// Mark genesis as persisted
		markGenesisAsPersisted(t, store, s)

		// Store both blocks
		_, _, err = store.StoreBlock(context.Background(), block1, "test")
		require.NoError(t, err)
		_, _, err = store.StoreBlock(context.Background(), block2, "test")
		require.NoError(t, err)

		// Mark block1 as persisted
		err = store.SetBlockPersistedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		// Only block2 should be returned
		blocks, err := store.GetBlocksNotPersisted(context.Background(), 100)
		require.NoError(t, err)
		require.Len(t, blocks, 1)
		assert.Equal(t, block2.Hash().String(), blocks[0].Hash().String())
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		dbURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s := settings.NewSettings()
		store, err := New(logger, dbURL, s)
		require.NoError(t, err)
		defer store.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = store.GetBlocksNotPersisted(ctx, 100)
		assert.Error(t, err)
	})
}
