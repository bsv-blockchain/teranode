package sql

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Unit tests for blockTimestampCache
// ============================================================

// Two blocks at the same height on one parent each keep their own entry, and each one's
// ancestors follow its own parent link.
func TestBlockTimeCache_SiblingsDoNotCollide(t *testing.T) {
	c := newBlockTimestampCache()
	c.Add(24, 23, 2400)
	c.Add(25, 24, 2500)
	c.Add(125, 24, 1)

	require.Equal(t, []uint32{2400, 2500}, c.Ancestors(25, 2))
	require.Equal(t, []uint32{2400, 1}, c.Ancestors(125, 2))
}

func TestBlockTimeCache_MissReturnsNil(t *testing.T) {
	c := newBlockTimestampCache()
	c.Add(24, 23, 2400)
	c.Add(25, 24, 2500)

	require.Nil(t, c.Ancestors(25, 3), "block 23 is not cached")
	require.Nil(t, c.Ancestors(99, 1))
}

func TestBlockTimeCache_EvictsOldestFirst(t *testing.T) {
	c := newBlockTimestampCache()
	for id := uint64(1); id <= blockTimestampCacheCapacity+1; id++ {
		c.Add(id, id-1, uint32(id)) //nolint:gosec // test IDs are small
	}

	require.Equal(t, blockTimestampCacheCapacity, c.Len())
	require.Nil(t, c.Ancestors(1, 1), "the first block added is dropped first")
	require.NotNil(t, c.Ancestors(2, 1))
	require.Nil(t, c.Ancestors(blockTimestampCacheCapacity+1, blockTimestampCacheCapacity+1))
}

func TestBlockTimeCache_ReAddIsANoOp(t *testing.T) {
	c := newBlockTimestampCache()
	c.Add(7, 6, 700)
	c.Add(7, 6, 701)

	require.Equal(t, 1, c.Len())
	require.Equal(t, []uint32{700}, c.Ancestors(7, 1))
}

func TestBlockTimeCache_Clear(t *testing.T) {
	c := newBlockTimestampCache()
	c.Add(1, 0, 100)
	c.Clear()

	require.Zero(t, c.Len())
	require.Nil(t, c.Ancestors(1, 1))

	c.Add(1, 0, 100)
	require.Equal(t, []uint32{100}, c.Ancestors(1, 1), "the cache works again after a clear")
}

func TestBlockTimeCache_ConcurrentAddAndAncestors(t *testing.T) {
	c := newBlockTimestampCache()

	var wg sync.WaitGroup

	for g := uint64(0); g < 8; g++ {
		wg.Add(2)

		go func() {
			defer wg.Done()

			for i := uint64(1); i <= 200; i++ {
				id := g*1_000 + i
				c.Add(id, id-1, uint32(id)) //nolint:gosec // test IDs are small
			}
		}()

		go func() {
			defer wg.Done()

			for i := uint64(1); i <= 200; i++ {
				_ = c.Ancestors(g*1_000+i, 2)
			}
		}()
	}

	wg.Wait()
	require.LessOrEqual(t, c.Len(), blockTimestampCacheCapacity)
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

	return newMTPTestStoreAt(t, "sqlitememory:///")
}

// newMTPTestStoreAt opens the store at rawURL with BIP113 on from height 0 and the in-memory
// chain check off, its default.
func newMTPTestStoreAt(t *testing.T, rawURL string) *SQL {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.CSVHeight = 0
	tSettings.BlockChain.UseInMemoryChainCheck = false

	storeURL, err := url.Parse(rawURL)
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// storeMTPChain stores mtpTestChain's blocks 1 to count.
// Returns the slice of block hashes in order (index 0 = block at height 1).
func storeMTPChain(t *testing.T, s *SQL, count int) []*chainhash.Hash {
	t.Helper()
	ctx := context.Background()

	chain := mtpTestChain(t, ctx, s, uint32(count))
	requireStored(t, ctx, s, chain[1:]...)

	hashes := make([]*chainhash.Hash, 0, count)
	for _, block := range chain[1:] {
		hashes = append(hashes, block.Hash())
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
	storeMTPChain(t, s, 15)

	require.Positive(t, s.blockTimestampCache.Len(), "cache should have entries after sequential inserts")

	// blocks 4-14, followed from block 14 by parent links, are what block 15's median reads
	cached := s.blockTimestampCache.Ancestors(getBlockIDAtHeight(t, s, 14), 11)
	require.NotNil(t, cached, "cache should have all timestamps needed for MTP of block 15")
	require.Len(t, cached, 11)
}

func TestMTPCache_MTPValueCorrectness(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	storeMTPChain(t, s, 15)

	// Get the parent block ID (block at height 14) for chain-walking MTP
	parentBlockID := getBlockIDAtHeight(t, s, 14)

	// Clear cache and compute MTP from DB (ground truth); the walk fills the cache
	s.blockTimestampCache.Clear()
	mtpFromDB, err := s.calculateMedianTimePastForHeight(ctx, 15, parentBlockID)
	require.NoError(t, err)
	require.NotNil(t, s.blockTimestampCache.Ancestors(parentBlockID, 11), "the SQL walk fills the cache")

	mtpFromCache, err := s.calculateMedianTimePastForHeight(ctx, 15, parentBlockID)
	require.NoError(t, err)

	require.Equal(t, mtpFromDB, mtpFromCache, "MTP from cache should match MTP from database")
}

func TestMTPCache_DBFallbackOnCacheMiss(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	storeMTPChain(t, s, 15)

	// Get parent block ID for chain-walking MTP
	parentBlockID := getBlockIDAtHeight(t, s, 14)

	// Clear cache to force DB fallback
	s.blockTimestampCache.Clear()

	mtp, err := s.calculateMedianTimePastForHeight(ctx, 15, parentBlockID)
	require.NoError(t, err)
	assert.Greater(t, mtp, uint32(0), "MTP should be non-zero for height 15")
}

// Invalidating and revalidating a block changes neither its timestamp nor its parent, so the
// median-time cache keeps its entries.
func TestMTPCache_InvalidateAndRevalidateKeepTheCache(t *testing.T) {
	s := newMTPTestStore(t)

	hashes := storeMTPChain(t, s, 15)
	before := s.blockTimestampCache.Len()
	require.Positive(t, before)

	_, err := s.InvalidateBlock(context.Background(), hashes[9])
	require.NoError(t, err)
	require.Equal(t, before, s.blockTimestampCache.Len(), "InvalidateBlock keeps the cache")

	require.NoError(t, s.RevalidateBlock(context.Background(), hashes[9]))
	require.Equal(t, before, s.blockTimestampCache.Len(), "RevalidateBlock keeps the cache")
}

func TestMTPCache_ForkHandling_NewBlockAtExistingHeight(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	// Store main chain: genesis → 1 → 2 → 3 → 4 → 5
	hashes := storeMTPChain(t, s, 5)

	mainTip := getBlockIDAtHeight(t, s, 5)
	mainChain := s.blockTimestampCache.Ancestors(mainTip, 5)
	require.Equal(t, []uint32{1600000001, 1600000002, 1600000003, 1600000004, 1600000005}, mainChain)

	// Store a fork block at height 2 (parent = block 1)
	forkBlock := makeMTPTestBlock(t, 2, hashes[0], 1700000000)
	forkID, _, err := s.StoreBlock(ctx, forkBlock, "fork-peer")
	require.NoError(t, err)

	// The fork block has its own entry; the main chain's are untouched
	require.Equal(t, mainChain, s.blockTimestampCache.Ancestors(mainTip, 5), "a fork block does not disturb the main chain")
	require.Equal(t, []uint32{1600000001, 1700000000}, s.blockTimestampCache.Ancestors(forkID, 2))
}

func TestMTPCache_BelowCSVHeight_NoCache(t *testing.T) {
	// Use default CSVHeight (576 for regtest) so MTP returns 0
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	ctx := context.Background()

	storeMTPChain(t, s, 15)

	// parentBlockID=0 is fine here since the function returns early (below CSVHeight)
	mtp, err := s.calculateMedianTimePastForHeight(ctx, 15, 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), mtp, "MTP should be 0 below CSVHeight")
}

func TestMTPCache_HeightBelow11_ReturnsZero(t *testing.T) {
	s := newMTPTestStore(t)
	ctx := context.Background()

	storeMTPChain(t, s, 10)

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
	mainHashes := storeMTPChain(t, s, 14)

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

// runMedianTimeTests runs fn against SQLite in memory and, unless -short, PostgreSQL, with BIP113
// on from height 0 and the in-memory chain check off, its default. fn's context ends after two
// minutes, so a hang fails by name.
func runMedianTimeTests(t *testing.T, name string, fn func(t *testing.T, ctx context.Context, s *SQL)) {
	t.Helper()

	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(name+"/"+backend, func(t *testing.T) {
			storeURL := "sqlitememory:///"

			if backend == "postgres" {
				if testing.Short() {
					t.Skip("skipping postgres-backed test in -short mode")
				}

				connStr, teardown, pgErr := postgres.SetupTestPostgresContainer()
				if pgErr != nil {
					t.Skipf("PostgreSQL container not available: %v", pgErr)
				}

				t.Cleanup(func() { _ = teardown() })

				storeURL = connStr
			}

			s := newMTPTestStoreAt(t, storeURL)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			fn(t, ctx, s)
		})
	}
}

// mtpTestChain builds blocks 1 to n on the store's genesis, block h timed 1,600,000,000 + h, so
// the median of the 11 blocks below height h is 1,600,000,000 + h - 6. Index i holds height i.
func mtpTestChain(t *testing.T, ctx context.Context, s *SQL, n uint32) []*model.Block {
	t.Helper()

	genesis, _, err := s.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	chain := make([]*model.Block, n+1)
	prev := genesis.Hash()

	for h := uint32(1); h <= n; h++ {
		chain[h] = makeMTPTestBlock(t, h, prev, 1_600_000_000+h)
		prev = chain[h].Hash()
	}

	return chain
}

func requireStored(t *testing.T, ctx context.Context, s *SQL, blocks ...*model.Block) {
	t.Helper()

	for _, block := range blocks {
		_, _, err := s.StoreBlock(ctx, block, "test")
		require.NoError(t, err)
	}
}

func requireChainMedians(t *testing.T, ctx context.Context, s *SQL, chain []*model.Block, from, to uint32) {
	t.Helper()

	for h := from; h <= to; h++ {
		_, meta, err := s.GetBlockHeader(ctx, chain[h].Hash())
		require.NoError(t, err)
		require.Equal(t, 1_600_000_000+h-6, meta.MedianTimePast, "height %d", h)
	}
}

// A side block stored at height 25, between the main chain's blocks 25 and 26, must not enter
// the medians saved for the main chain's blocks above it.
func TestStoreBlockMedianTimeFollowsItsOwnBranch(t *testing.T) {
	runMedianTimeTests(t, "own branch", func(t *testing.T, ctx context.Context, s *SQL) {
		chain := mtpTestChain(t, ctx, s, 40)
		side := makeMTPTestBlock(t, 25, chain[24].Hash(), 1_000_000_000) // older than every block on the chain

		requireStored(t, ctx, s, chain[1:26]...)
		requireStored(t, ctx, s, side)
		requireStored(t, ctx, s, chain[26:]...)

		requireChainMedians(t, ctx, s, chain, 12, 40)

		_, meta, err := s.GetBlockHeader(ctx, chain[5].Hash())
		require.NoError(t, err)
		require.Zero(t, meta.MedianTimePast, "a block with fewer than 11 below it saves 0")
	})
}

// After a restart the cache is empty: the next block falls back to the parent walk in SQL,
// saves the same median, and fills the cache for the blocks after it.
func TestStoreBlockMedianTimeSurvivesAnEmptyCache(t *testing.T) {
	runMedianTimeTests(t, "empty cache", func(t *testing.T, ctx context.Context, s *SQL) {
		chain := mtpTestChain(t, ctx, s, 30)

		requireStored(t, ctx, s, chain[1:21]...)
		s.blockTimestampCache.Clear()

		walks := s.parentWalkQueries.Load()
		requireStored(t, ctx, s, chain[21])
		require.Equal(t, walks+1, s.parentWalkQueries.Load(), "an empty cache falls back to one SQL walk")
		require.GreaterOrEqual(t, s.blockTimestampCache.Len(), 11, "the fallback fills the cache")

		requireStored(t, ctx, s, chain[22:]...)
		require.Equal(t, walks+1, s.parentWalkQueries.Load(), "the blocks after it read the cache, not SQL")
		requireChainMedians(t, ctx, s, chain, 12, 30)
	})
}
