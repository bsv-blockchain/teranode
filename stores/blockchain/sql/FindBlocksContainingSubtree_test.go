package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindBlocksContainingSubtree_Success(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	blocks, err := s.FindBlocksContainingSubtree(ctx, subtree, 0)

	require.NoError(t, err)
	assert.NotNil(t, blocks)
	assert.Greater(t, len(blocks), 0)

	for _, block := range blocks {
		found := false
		for _, st := range block.Subtrees {
			if st.IsEqual(subtree) {
				found = true
				break
			}
		}
		assert.True(t, found, "Block should contain the searched subtree")
	}
}

func TestFindBlocksContainingSubtree_NotFound(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	nonExistentSubtree := block3Hash

	blocks, err := s.FindBlocksContainingSubtree(ctx, nonExistentSubtree, 0)

	require.NoError(t, err)
	assert.Empty(t, blocks)
}

func TestFindBlocksContainingSubtree_MaxBlocks(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block3, "test_peer")
	require.NoError(t, err)

	blocks, err := s.FindBlocksContainingSubtree(ctx, subtree, 2)

	require.NoError(t, err)
	assert.NotNil(t, blocks)
	assert.LessOrEqual(t, len(blocks), 2, "Should respect maxBlocks limit")
}

func TestFindBlocksContainingSubtree_NilSubtreeHash(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	blocks, err := s.FindBlocksContainingSubtree(ctx, nil, 0)

	assert.Error(t, err)
	assert.Nil(t, blocks)
}

func TestFindBlocksContainingSubtree_ContextCancellation(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.FindBlocksContainingSubtree(ctx, subtree, 0)

	assert.Error(t, err)
}

func TestFindBlocksContainingSubtree_OrderedByHeight(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	blocks, err := s.FindBlocksContainingSubtree(ctx, subtree, 0)

	require.NoError(t, err)
	assert.Greater(t, len(blocks), 1)

	for i := 1; i < len(blocks); i++ {
		assert.LessOrEqual(t, blocks[i-1].Height, blocks[i].Height, "Blocks should be ordered by height (ASC)")
	}
}

func TestFindBlocksContainingSubtree_CompleteBlockData(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	blocks, err := s.FindBlocksContainingSubtree(ctx, subtree, 0)

	require.NoError(t, err)
	require.Greater(t, len(blocks), 0)

	block := blocks[0]
	assert.NotNil(t, block.Header)
	assert.NotNil(t, block.Header.HashPrevBlock)
	assert.NotNil(t, block.Header.HashMerkleRoot)
	assert.Greater(t, block.Header.Version, uint32(0))
	assert.Greater(t, block.Height, uint32(0))
	assert.Greater(t, block.ID, uint32(0))
	assert.NotNil(t, block.CoinbaseTx)
	assert.Greater(t, block.TransactionCount, uint64(0))
	assert.NotNil(t, block.Subtrees)
	assert.Greater(t, len(block.Subtrees), 0)
}

func TestFindBlocksContainingSubtree_SubtreesIntegrity(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	blocks, err := s.FindBlocksContainingSubtree(ctx, subtree, 0)

	require.NoError(t, err)
	require.Greater(t, len(blocks), 0)

	block := blocks[0]
	assert.NotNil(t, block.Subtrees)

	for _, st := range block.Subtrees {
		assert.NotNil(t, st)
	}
}

func TestFindBlocksContainingSubtree_ZeroMaxBlocks(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	blocksUnlimited, err := s.FindBlocksContainingSubtree(ctx, subtree, 0)
	require.NoError(t, err)

	blocksLimited, err := s.FindBlocksContainingSubtree(ctx, subtree, 1)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, len(blocksUnlimited), len(blocksLimited), "Zero maxBlocks should return all matching blocks")
}

func TestFindBlocksContainingSubtree_MultipleBlocks(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	blocks, err := s.FindBlocksContainingSubtree(ctx, subtree, 0)

	require.NoError(t, err)

	if len(blocks) > 1 {
		for i := 1; i < len(blocks); i++ {
			assert.NotEqual(t, blocks[i-1].ID, blocks[i].ID, "Each block should have unique ID")
			assert.NotEqual(t, blocks[i-1].Hash().String(), blocks[i].Hash().String(), "Each block should have unique hash")
		}
	}
}

func TestFindBlocksContainingSubtreePostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	t.Run("postgres container - basic subtree search", func(t *testing.T) {
		connStr, teardown, err := postgres.SetupTestPostgresContainer()
		if err != nil {
			t.Skipf("PostgreSQL container not available: %v", err)
		}
		defer func() {
			if teardownErr := teardown(); teardownErr != nil {
				t.Logf("Failed to teardown PostgreSQL container: %v", teardownErr)
			}
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		ctx := context.Background()

		_, _, err = s.StoreBlock(ctx, block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(ctx, block2, "test_peer")
		require.NoError(t, err)

		blocks, err := s.FindBlocksContainingSubtree(ctx, subtree, 0)

		require.NoError(t, err)
		assert.NotNil(t, blocks)
		assert.Greater(t, len(blocks), 0, "Should find at least one block containing the subtree")

		for _, block := range blocks {
			found := false
			for _, st := range block.Subtrees {
				if st.IsEqual(subtree) {
					found = true
					break
				}
			}
			assert.True(t, found, "Block should contain the searched subtree")
		}
	})

	t.Run("postgres container - not found", func(t *testing.T) {
		connStr, teardown, err := postgres.SetupTestPostgresContainer()
		if err != nil {
			t.Skipf("PostgreSQL container not available: %v", err)
		}
		defer func() {
			if teardownErr := teardown(); teardownErr != nil {
				t.Logf("Failed to teardown PostgreSQL container: %v", teardownErr)
			}
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		ctx := context.Background()

		_, _, err = s.StoreBlock(ctx, block1, "test_peer")
		require.NoError(t, err)

		nonExistentSubtree := block3Hash

		blocks, err := s.FindBlocksContainingSubtree(ctx, nonExistentSubtree, 0)

		require.NoError(t, err)
		assert.NotNil(t, blocks)
		assert.Equal(t, 0, len(blocks), "Should not find any blocks with non-existent subtree")
	})

	t.Run("postgres container - max blocks limit", func(t *testing.T) {
		connStr, teardown, err := postgres.SetupTestPostgresContainer()
		if err != nil {
			t.Skipf("PostgreSQL container not available: %v", err)
		}
		defer func() {
			if teardownErr := teardown(); teardownErr != nil {
				t.Logf("Failed to teardown PostgreSQL container: %v", teardownErr)
			}
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		ctx := context.Background()

		_, _, err = s.StoreBlock(ctx, block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(ctx, block2, "test_peer")
		require.NoError(t, err)

		blocks, err := s.FindBlocksContainingSubtree(ctx, subtree, 1)

		require.NoError(t, err)
		assert.NotNil(t, blocks)
		assert.LessOrEqual(t, len(blocks), 1, "Should respect max blocks limit")
	})
}
