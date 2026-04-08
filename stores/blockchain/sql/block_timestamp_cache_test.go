package sql

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Unit tests for blockTimestampCache
// ============================================================

func TestMTPCache_AddAndGetRange(t *testing.T) {
	c := newBlockTimestampCache()

	for h := uint32(0); h < 11; h++ {
		c.Add(h, 1000+h)
	}

	got := c.GetRange(0, 10)
	require.NotNil(t, got)
	require.Len(t, got, 11)
	for i, ts := range got {
		assert.Equal(t, uint32(1000+i), ts)
	}
}

func TestMTPCache_GetRange_Miss(t *testing.T) {
	c := newBlockTimestampCache()

	// Add heights 5-10, request 0-10 → miss because 0-4 are absent
	for h := uint32(5); h <= 10; h++ {
		c.Add(h, 1000+h)
	}

	got := c.GetRange(0, 10)
	assert.Nil(t, got)
}

func TestMTPCache_GetRange_EmptyCache(t *testing.T) {
	c := newBlockTimestampCache()
	got := c.GetRange(0, 10)
	assert.Nil(t, got)
}

func TestMTPCache_ForkDetection(t *testing.T) {
	c := newBlockTimestampCache()

	// Build a chain: heights 0-15
	for h := uint32(0); h <= 15; h++ {
		c.Add(h, 1000+h)
	}
	assert.Equal(t, 16, c.Len())

	// Fork at height 10: store a different block at height 10.
	// This should evict heights 10-15 and store the new value at 10.
	c.Add(10, 2000)

	// Heights 0-9 should still be cached
	got := c.GetRange(0, 9)
	require.NotNil(t, got)
	require.Len(t, got, 10)
	for i, ts := range got {
		assert.Equal(t, uint32(1000+i), ts)
	}

	// Height 10 should have the new timestamp
	got = c.GetRange(10, 10)
	require.NotNil(t, got)
	assert.Equal(t, uint32(2000), got[0])

	// Heights 11-15 should be gone
	got = c.GetRange(10, 15)
	assert.Nil(t, got, "heights 11-15 should have been evicted by fork at 10")

	// Total entries: 0-9 + 10 = 11
	assert.Equal(t, 11, c.Len())
}

func TestMTPCache_InvalidateFrom(t *testing.T) {
	c := newBlockTimestampCache()

	for h := uint32(0); h < 20; h++ {
		c.Add(h, 1000+h)
	}

	c.InvalidateFrom(10)

	// Heights 0-9 intact
	got := c.GetRange(0, 9)
	require.NotNil(t, got)
	require.Len(t, got, 10)

	// Height 10+ gone
	got = c.GetRange(10, 10)
	assert.Nil(t, got)

	assert.Equal(t, 10, c.Len())
}

func TestMTPCache_Clear(t *testing.T) {
	c := newBlockTimestampCache()

	for h := uint32(0); h < 10; h++ {
		c.Add(h, 1000+h)
	}
	assert.Equal(t, 10, c.Len())

	c.Clear()
	assert.Equal(t, 0, c.Len())

	got := c.GetRange(0, 0)
	assert.Nil(t, got)
}

func TestMTPCache_Pruning(t *testing.T) {
	c := newBlockTimestampCache()

	// Add more entries than blockTimestampCacheCapacity
	for h := uint32(0); h < blockTimestampCacheCapacity+30; h++ {
		c.Add(h, 1000+h)
	}

	// Cache should be bounded
	assert.LessOrEqual(t, c.Len(), blockTimestampCacheCapacity+1, "cache should not grow unbounded")

	// Recent entries should still be present
	maxH := uint32(blockTimestampCacheCapacity + 29)
	got := c.GetRange(maxH-10, maxH)
	require.NotNil(t, got, "recent entries should be cached")
	require.Len(t, got, 11)
}

func TestMTPCache_ConcurrentAccess(t *testing.T) {
	c := newBlockTimestampCache()

	var wg sync.WaitGroup
	// Writers
	for i := range 10 {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for h := uint32(0); h < 100; h++ {
				c.Add(h+uint32(offset*100), 1000+h)
			}
		}(i)
	}
	// Readers
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = c.GetRange(0, 10)
			}
		}()
	}

	wg.Wait()
	// No race detector failures = pass
}

// ============================================================
// Integration tests with SQL store
// ============================================================

// makeMTPTestBlock creates a test block with a deterministic coinbase based on
// height, so each block has a unique hash.
func makeMTPTestBlock(t *testing.T, height uint32, prevHash *chainhash.Hash, timestamp uint32) *model.Block {
	t.Helper()

	cb := bt.NewTx()

	// Coinbase input: null prev txid, output index 0xFFFFFFFF
	input := &bt.Input{
		PreviousTxOutIndex: 0xFFFFFFFF,
		SequenceNumber:     0xFFFFFFFF,
	}
	require.NoError(t, input.PreviousTxIDAdd(&chainhash.Hash{}))

	// BIP34 script: push 3 bytes encoding the height
	script := []byte{3, byte(height), byte(height >> 8), byte(height >> 16)}
	input.UnlockingScript = bscript.NewFromBytes(script)
	cb.Inputs = append(cb.Inputs, input)

	// Simple output
	outScript := bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0x88, 0xac})
	cb.AddOutput(&bt.Output{
		Satoshis:      5000000000,
		LockingScript: outScript,
	})

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        2,
			Timestamp:      timestamp,
			Nonce:          height,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: cb.TxIDChainHash(),
			Bits:           *bits,
		},
		Height:           height,
		CoinbaseTx:       cb,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
}

// newMTPTestStore creates an in-memory SQLite store with CSVHeight=0
// so that MTP calculation is active from block 11 onward.
func newMTPTestStore(t *testing.T) *SQL {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.CSVHeight = 0

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// storeMTPChain stores a linear chain of `count` blocks starting from genesis.
// Returns the slice of block hashes in order (index 0 = block at height 1).
func storeMTPChain(t *testing.T, s *SQL, count int, baseTimestamp uint32) []*chainhash.Hash {
	t.Helper()
	ctx := context.Background()

	genesisHeader, _, err := s.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	prevHash := genesisHeader.Hash()
	hashes := make([]*chainhash.Hash, 0, count)

	for i := 1; i <= count; i++ {
		block := makeMTPTestBlock(t, uint32(i), prevHash, baseTimestamp+uint32(i))
		_, _, err := s.StoreBlock(ctx, block, "test")
		require.NoError(t, err, "failed to store block at height %d", i)
		h := block.Hash()
		hashes = append(hashes, h)
		prevHash = h
	}

	return hashes
}

// getBlockIDAtHeight returns the database ID of the block at the given height.
func getBlockIDAtHeight(t *testing.T, s *SQL, height uint32) uint64 {
	t.Helper()
	var id uint64
	err := s.db.QueryRow("SELECT id FROM blocks WHERE height = $1 ORDER BY chain_work DESC LIMIT 1", height).Scan(&id)
	require.NoError(t, err, "failed to get block ID at height %d", height)
	return id
}

func TestMTPCache_SequentialInserts_CacheHit(t *testing.T) {
	s := newMTPTestStore(t)

	// Store 15 blocks — MTP kicks in at height 11 (needs 11 previous blocks)
	storeMTPChain(t, s, 15, 1600000000)

	assert.Greater(t, s.blockTimestampCache.Len(), 0, "cache should have entries after sequential inserts")

	// heights 4-14 needed for MTP of block 15
	cached := s.blockTimestampCache.GetRange(4, 14)
	require.NotNil(t, cached, "cache should have all timestamps needed for MTP of block 15")
	require.Len(t, cached, 11)
}

func TestMTPCache_MTPValueCorrectness(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	storeMTPChain(t, s, 15, 1600000000)

	// Get the parent block ID (block at height 14) for chain-walking MTP
	parentBlockID := getBlockIDAtHeight(t, s, 14)

	// Clear cache and compute MTP from DB (ground truth)
	s.blockTimestampCache.Clear()
	mtpFromDB, err := s.calculateMedianTimePastForHeight(ctx, 15, parentBlockID)
	require.NoError(t, err)

	// Re-populate cache with the same timestamps the blocks were stored with
	for h := uint32(1); h <= 14; h++ {
		s.blockTimestampCache.Add(h, 1600000000+h)
	}

	// Compute MTP from cache (parentBlockID not used when cache hits)
	mtpFromCache, err := s.calculateMedianTimePastForHeight(ctx, 15, parentBlockID)
	require.NoError(t, err)

	assert.Equal(t, mtpFromDB, mtpFromCache, "MTP from cache should match MTP from database")
}

func TestMTPCache_DBFallbackOnCacheMiss(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	storeMTPChain(t, s, 15, 1600000000)

	// Get parent block ID for chain-walking MTP
	parentBlockID := getBlockIDAtHeight(t, s, 14)

	// Clear cache to force DB fallback
	s.blockTimestampCache.Clear()

	mtp, err := s.calculateMedianTimePastForHeight(ctx, 15, parentBlockID)
	require.NoError(t, err)
	assert.Greater(t, mtp, uint32(0), "MTP should be non-zero for height 15")
}

func TestMTPCache_InvalidateBlockClearsCache(t *testing.T) {
	s := newMTPTestStore(t)

	hashes := storeMTPChain(t, s, 15, 1600000000)
	assert.Greater(t, s.blockTimestampCache.Len(), 0)

	// Invalidate block at height 10
	_, err := s.InvalidateBlock(context.Background(), hashes[9])
	require.NoError(t, err)

	assert.Equal(t, 0, s.blockTimestampCache.Len(), "InvalidateBlock should clear MTP cache")
}

func TestMTPCache_RevalidateBlockClearsCache(t *testing.T) {
	s := newMTPTestStore(t)

	hashes := storeMTPChain(t, s, 15, 1600000000)
	_, err := s.InvalidateBlock(context.Background(), hashes[9])
	require.NoError(t, err)

	// Manually repopulate cache
	s.blockTimestampCache.Add(1, 1000)
	s.blockTimestampCache.Add(2, 2000)
	assert.Greater(t, s.blockTimestampCache.Len(), 0)

	// Revalidate
	err = s.RevalidateBlock(context.Background(), hashes[9])
	require.NoError(t, err)

	assert.Equal(t, 0, s.blockTimestampCache.Len(), "RevalidateBlock should clear MTP cache")
}

func TestMTPCache_ForkHandling_NewBlockAtExistingHeight(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	// Store main chain: genesis → 1 → 2 → 3 → 4 → 5
	hashes := storeMTPChain(t, s, 5, 1600000000)

	// Cache should have heights 1-5
	cached := s.blockTimestampCache.GetRange(1, 5)
	require.NotNil(t, cached)

	// Store a fork block at height 2 (parent = block 1)
	forkBlock := makeMTPTestBlock(t, 2, hashes[0], 1700000000)
	_, _, err := s.StoreBlock(ctx, forkBlock, "fork-peer")
	require.NoError(t, err)

	// Cache should have invalidated heights 2+ due to fork detection in Add()
	cached = s.blockTimestampCache.GetRange(3, 5)
	assert.Nil(t, cached, "heights above fork point should be evicted")

	// Height 1 should still be cached
	cached = s.blockTimestampCache.GetRange(1, 1)
	require.NotNil(t, cached)
	assert.Equal(t, uint32(1600000001), cached[0])

	// Height 2 should have the fork block's timestamp
	cached = s.blockTimestampCache.GetRange(2, 2)
	require.NotNil(t, cached)
	assert.Equal(t, uint32(1700000000), cached[0])
}

func TestMTPCache_BelowCSVHeight_NoCache(t *testing.T) {
	// Use default CSVHeight (576 for regtest) so MTP returns 0
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	storeMTPChain(t, s, 15, 1600000000)

	// parentBlockID=0 is fine here since the function returns early (below CSVHeight)
	mtp, err := s.calculateMedianTimePastForHeight(ctx, 15, 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), mtp, "MTP should be 0 below CSVHeight")
}

func TestMTPCache_HeightBelow11_ReturnsZero(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	storeMTPChain(t, s, 10, 1600000000)

	// parentBlockID=0 is fine here since the function returns early (height < 11)
	mtp, err := s.calculateMedianTimePastForHeight(ctx, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), mtp, "MTP should be 0 for height < 11")
}

// TestMTPCache_ForkBlocks_ParentChainWalk verifies that MTP calculation
// correctly walks the parent chain instead of querying by height range.
// This is a regression test for a bug where fork blocks (two valid blocks
// at the same height) caused "expected N timestamps, got N+1" errors.
func TestMTPCache_ForkBlocks_ParentChainWalk(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	// Store a main chain of 14 blocks (heights 1-14)
	mainHashes := storeMTPChain(t, s, 14, 1600000000)

	// Store a fork block at height 5 (same parent as main-chain block 5)
	// This creates two valid (non-invalid) blocks at height 5
	forkBlock := makeMTPTestBlock(t, 5, mainHashes[3], 1700000000)
	_, _, err := s.StoreBlock(ctx, forkBlock, "fork-peer")
	require.NoError(t, err)

	// Get the main-chain parent block ID (block at height 14)
	mainParentID := getBlockIDAtHeight(t, s, 14)

	// Clear cache to force the DB fallback path
	s.blockTimestampCache.Clear()

	// This would fail with the old height-range query ("expected 11 timestamps, got 12")
	// because height 5 has two valid blocks. The parent chain walk correctly follows
	// only the main chain.
	block15 := makeMTPTestBlock(t, 15, mainHashes[13], 1600000015)
	_, _, err = s.StoreBlock(ctx, block15, "test")
	require.NoError(t, err)

	// Also verify direct call works
	s.blockTimestampCache.Clear()
	mtp, err := s.calculateMedianTimePastForHeight(ctx, 15, mainParentID)
	require.NoError(t, err)
	assert.Greater(t, mtp, uint32(0), "MTP should be non-zero")
}

func TestCalculateMTPFromTimestamps(t *testing.T) {
	timestamps := []uint32{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100}

	mtp, err := calculateMTPFromTimestamps(timestamps)
	require.NoError(t, err)

	// Median of 11 sorted values is the 6th (index 5) = 600
	assert.Equal(t, uint32(600), mtp)
}

func TestCalculateMTPFromTimestamps_UnsortedInput(t *testing.T) {
	// Bitcoin timestamps can be slightly out of order
	timestamps := []uint32{500, 300, 700, 100, 900, 200, 800, 400, 600, 1000, 1100}

	mtp, err := calculateMTPFromTimestamps(timestamps)
	require.NoError(t, err)

	// After sorting: 100,200,300,400,500,600,700,800,900,1000,1100 → median = 600
	assert.Equal(t, uint32(600), mtp)
}
