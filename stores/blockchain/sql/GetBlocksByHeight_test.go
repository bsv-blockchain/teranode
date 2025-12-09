package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBlocksByHeight_Success(t *testing.T) {
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

	blocks, err := s.GetBlocksByHeight(ctx, 1, 2)

	require.NoError(t, err)
	assert.NotNil(t, blocks)
	assert.Greater(t, len(blocks), 0)

	for i := 1; i < len(blocks); i++ {
		assert.LessOrEqual(t, blocks[i-1].Height, blocks[i].Height)
	}

	for _, block := range blocks {
		assert.GreaterOrEqual(t, block.Height, uint32(1))
		assert.LessOrEqual(t, block.Height, uint32(2))
	}

	for _, block := range blocks {
		assert.NotNil(t, block.Header)
		assert.NotNil(t, block.Header.HashPrevBlock)
		assert.NotNil(t, block.Header.HashMerkleRoot)
		assert.Greater(t, block.Header.Version, uint32(0))
		assert.Greater(t, block.Height, uint32(0))
		assert.Greater(t, block.ID, uint32(0))
	}
}

func TestGetBlocksByHeight_EmptyResult(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	blocks, err := s.GetBlocksByHeight(ctx, 100, 200)

	require.NoError(t, err)
	assert.Empty(t, blocks)
}

func TestGetBlocksByHeight_SameHeight(t *testing.T) {
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

	blocks, err := s.GetBlocksByHeight(ctx, 2, 2)

	require.NoError(t, err)
	assert.NotEmpty(t, blocks)

	for _, block := range blocks {
		assert.Equal(t, uint32(2), block.Height)
	}
}

func TestGetBlocksByHeight_ReverseRange(t *testing.T) {
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

	blocks, err := s.GetBlocksByHeight(ctx, 2, 1)

	require.NoError(t, err)
	assert.Empty(t, blocks)
}

func TestGetBlocksByHeight_ContextCancellation(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.GetBlocksByHeight(ctx, 1, 10)

	assert.Error(t, err)
}

func TestGetBlocksByHeight_ZeroHeight(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	blocks, err := s.GetBlocksByHeight(ctx, 0, 0)

	require.NoError(t, err)
	assert.NotEmpty(t, blocks)

	for _, block := range blocks {
		assert.Equal(t, uint32(0), block.Height)
	}
}

func TestGetBlocksByHeight_LargeRange(t *testing.T) {
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

	blocks, err := s.GetBlocksByHeight(ctx, 0, 1000000)

	require.NoError(t, err)
	assert.NotNil(t, blocks)
	assert.Greater(t, len(blocks), 0)
	assert.LessOrEqual(t, len(blocks), 10)
}

func TestGetBlocksByHeight_InvalidBlocks(t *testing.T) {
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

	blocksBefore, err := s.GetBlocksByHeight(ctx, 0, 10)
	require.NoError(t, err)
	countBefore := len(blocksBefore)

	_, err = s.InvalidateBlock(ctx, block2.Header.Hash())
	require.NoError(t, err)

	blocksAfter, err := s.GetBlocksByHeight(ctx, 0, 10)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(blocksAfter), countBefore)

	for _, block := range blocksAfter {
		assert.NotNil(t, block)
	}
}

func TestGetBlocksByHeight_MaxValues(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	blocks, err := s.GetBlocksByHeight(ctx, 4294967290, 4294967295)

	require.NoError(t, err)
	assert.Empty(t, blocks)
}

func TestGetBlocksByHeight_DataConsistency(t *testing.T) {
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

	blocks, err := s.GetBlocksByHeight(ctx, 0, 5)

	require.NoError(t, err)

	for _, block := range blocks {
		assert.NotNil(t, block, "Block should not be nil")
		assert.NotNil(t, block.Header, "Header should not be nil")
		assert.NotNil(t, block.Header.HashPrevBlock, "HashPrevBlock should be set")
		assert.NotNil(t, block.Header.HashMerkleRoot, "HashMerkleRoot should be set")
	}
}

func TestGetBlocksByHeight_CapacityCalculation(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	blocks, err := s.GetBlocksByHeight(ctx, 10, 5)

	require.NoError(t, err)
	assert.Empty(t, blocks)
}

func TestGetBlocksByHeight_Ordering(t *testing.T) {
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

	blocks, err := s.GetBlocksByHeight(ctx, 0, 10)

	require.NoError(t, err)
	assert.Greater(t, len(blocks), 1)

	for i := 1; i < len(blocks); i++ {
		assert.LessOrEqual(t, blocks[i-1].Height, blocks[i].Height, "Blocks should be ordered by height (ASC)")
	}
}

func TestGetBlocksByHeight_CompleteBlockData(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	blocks, err := s.GetBlocksByHeight(ctx, 1, 1)

	require.NoError(t, err)
	require.Len(t, blocks, 1)

	block := blocks[0]
	assert.Equal(t, uint32(1), block.Height)
	assert.NotNil(t, block.CoinbaseTx)
	assert.Greater(t, block.TransactionCount, uint64(0))
	assert.NotNil(t, block.Subtrees)
	assert.Greater(t, len(block.Subtrees), 0)
}

func TestGetBlocksByHeight_SubtreesIntegrity(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	blocks, err := s.GetBlocksByHeight(ctx, 1, 1)

	require.NoError(t, err)
	require.Len(t, blocks, 1)

	block := blocks[0]
	assert.NotNil(t, block.Subtrees)

	for _, subtree := range block.Subtrees {
		assert.NotNil(t, subtree)
	}
}
