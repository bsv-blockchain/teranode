package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
)

// TestMTPCacheIntegration_RangeConsultsCache verifies that GetMedianTimePastRange
// returns pre-populated cache values without hitting the store.
func TestMTPCacheIntegration_RangeConsultsCache(t *testing.T) {
	ctx := setup(t)
	ctx.server.settings.ChainCfgParams.CSVHeight = 0

	// Pre-populate the cache with known MTP values for heights 1..5.
	// Heights 0..10 have zero MTP by BIP113 (below MedianTimeBlocks), but the
	// cache zero-as-miss sentinel only fires for height >= 11. Use non-zero
	// values below 11 to make the assertion unambiguous.
	knownMTPs := []uint32{100, 200, 300, 400, 500}
	ctx.server.mtpCache.putRange(1, knownMTPs)

	// Call the public method. It should hit the cache and return the values we
	// stored without consulting the store (which has no data for these heights
	// beyond the genesis block at height 0).
	got, err := ctx.server.GetMedianTimePastRange(context.Background(), 1, 5)
	require.NoError(t, err)
	require.Equal(t, knownMTPs, got, "GetMedianTimePastRange must return pre-populated cache values")
}

// TestMTPCacheIntegration_RangePopulatesCache verifies that a store fetch on a
// cache miss causes the persisted-block portion of the range to be stored in the
// cache for subsequent calls.
func TestMTPCacheIntegration_RangePopulatesCache(t *testing.T) {
	ctx := setup(t)
	ctx.server.settings.ChainCfgParams.CSVHeight = 0

	// Build a chain of 15 blocks so MTP is non-trivial (heights 1–14, genesis=0).
	for i := 1; i <= 14; i++ {
		blk := createTestBlockAtHeight(ctx, t, uint32(i), uint32(1000000+i*600))
		_, _, err := ctx.server.store.StoreBlock(context.Background(), blk, "")
		require.NoError(t, err)
	}

	// First call — cache miss, must hit the store and populate the cache.
	result, err := ctx.server.GetMedianTimePastRange(context.Background(), 11, 14)
	require.NoError(t, err)
	require.Len(t, result, 4)

	// Now the cache should hold the same values for [11,14].
	for i, h := uint32(0), uint32(11); h <= 14; h, i = h+1, i+1 {
		cached, ok := ctx.server.mtpCache.get(h)
		require.True(t, ok, "height %d must be present in cache after store fetch", h)
		require.Equal(t, result[i], cached, "cache value at height %d must match store result", h)
	}
}

// TestMTPCacheIntegration_SpeculativeTopExcluded verifies that if toHeight is not
// yet persisted (the "speculative top"), that slot is not written to the cache.
func TestMTPCacheIntegration_SpeculativeTopExcluded(t *testing.T) {
	ctx := setup(t)
	ctx.server.settings.ChainCfgParams.CSVHeight = 0

	// Store blocks 1..13 (heights 1–13, genesis=0). Height 14 is NOT stored.
	for i := 1; i <= 13; i++ {
		blk := createTestBlockAtHeight(ctx, t, uint32(i), uint32(1000000+i*600))
		_, _, err := ctx.server.store.StoreBlock(context.Background(), blk, "")
		require.NoError(t, err)
	}

	// Request up to height 14 (which is not persisted).
	_, err := ctx.server.GetMedianTimePastRange(context.Background(), 11, 14)
	require.NoError(t, err)

	// Height 13 (the highest persisted block in range) must be in the cache.
	_, ok := ctx.server.mtpCache.get(13)
	require.True(t, ok, "height 13 (highest persisted) must be cached")

	// Height 14 (not persisted / speculative top) must NOT be in the cache.
	_, ok = ctx.server.mtpCache.get(14)
	require.False(t, ok, "height 14 (speculative top, not yet persisted) must not be cached")
}

// TestMTPCacheIntegration_AddBlockTruncatesCache verifies that AddBlock truncates
// the MTP cache at the new block's height, so stale pre-populated slots are dropped.
func TestMTPCacheIntegration_AddBlockTruncatesCache(t *testing.T) {
	ctx := setup(t)

	// Store blocks 1 and 2 so we have a valid prev-block for block 3.
	prevHash := ctx.server.settings.ChainCfgParams.GenesisHash
	for i := 1; i <= 2; i++ {
		blk := createTestBlockAtHeight(ctx, t, uint32(i), uint32(1000000+i*600))
		_, _, err := ctx.server.store.StoreBlock(context.Background(), blk, "")
		require.NoError(t, err)
		prevHash = blk.Header.Hash()
	}

	// Pre-populate the cache with values at heights 3 and 4 (stale speculative entries).
	ctx.server.mtpCache.putRange(3, []uint32{999, 888})

	_, ok := ctx.server.mtpCache.get(3)
	require.True(t, ok, "precondition: height 3 must be in cache before AddBlock")

	// Build block 3 and add it via the service path (which calls truncate).
	blk3 := createTestBlockAtHeight(ctx, t, 3, uint32(1000000+3*600))
	blk3.Header.HashPrevBlock = prevHash

	addReq := &blockchain_api.AddBlockRequest{
		Header:           blk3.Header.Bytes(),
		CoinbaseTx:       blk3.CoinbaseTx.Bytes(),
		SubtreeHashes:    [][]byte{blk3.Subtrees[0].CloneBytes()},
		TransactionCount: blk3.TransactionCount,
		SizeInBytes:      blk3.SizeInBytes,
		PeerId:           "test-peer",
	}
	_, err := ctx.server.AddBlock(context.Background(), addReq)
	require.NoError(t, err)

	// Height 3 must have been truncated from the cache.
	_, ok = ctx.server.mtpCache.get(3)
	require.False(t, ok, "height 3 must be dropped from cache after AddBlock at that height")
}

// TestMTPCacheIntegration_InvalidateBlockTruncatesCache verifies that InvalidateBlock
// truncates the MTP cache from the invalidated block's height, leaving lower entries
// intact.
func TestMTPCacheIntegration_InvalidateBlockTruncatesCache(t *testing.T) {
	ctx := setup(t)
	ctx.server.settings.ChainCfgParams.CSVHeight = 0

	// Store blocks 1..3.
	for i := 1; i <= 3; i++ {
		blk := createTestBlockAtHeight(ctx, t, uint32(i), uint32(1000000+i*600))
		_, _, err := ctx.server.store.StoreBlock(context.Background(), blk, "")
		require.NoError(t, err)
	}

	// Pre-populate cache for heights 1..9 (below MedianTimeBlocks so zero is valid).
	ctx.server.mtpCache.putRange(1, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9})

	// Retrieve block 2's hash so we can pass it to InvalidateBlock.
	headers, _, err := ctx.server.store.GetBlockHeadersByHeight(context.Background(), 2, 2)
	require.NoError(t, err)
	require.Len(t, headers, 1)
	blockHash := headers[0].Hash()

	// InvalidateBlock should truncate at height 2, leaving height 1 intact.
	_, err = ctx.server.InvalidateBlock(context.Background(), &blockchain_api.InvalidateBlockRequest{
		BlockHash: blockHash.CloneBytes(),
	})
	require.NoError(t, err)

	// Height 1 (below the invalidated height 2) must still be cached.
	mtp1, ok := ctx.server.mtpCache.get(1)
	require.True(t, ok, "height 1 (below invalidated) must remain in cache")
	require.Equal(t, uint32(1), mtp1)

	// Height 2 and above must be gone.
	_, ok = ctx.server.mtpCache.get(2)
	require.False(t, ok, "height 2 (invalidated block) must be truncated from cache")

	_, ok = ctx.server.mtpCache.get(3)
	require.False(t, ok, "height 3 (above invalidated) must be truncated from cache")
}
