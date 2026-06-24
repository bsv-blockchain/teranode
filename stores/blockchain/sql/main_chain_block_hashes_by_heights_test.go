package sql

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// pgFlatInt64 builds a pgtype.FlatArray for the EXPLAIN test's ANY($2) parameter.
func pgFlatInt64(vals ...int64) pgtype.FlatArray[int64] {
	arr := make(pgtype.FlatArray[int64], len(vals))
	copy(arr, vals)
	return arr
}

func TestMainChainBlockHashesByHeights_FastPath(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)
	ctx := context.Background()

	res, ok, err := s.MainChainBlockHashesByHeights(ctx, block3.Hash(), []uint32{3, 2, 1})
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, res, 3)
	require.Equal(t, block3.Hash().String(), res[3].String())
	require.Equal(t, block2.Hash().String(), res[2].String())
	require.Equal(t, block1.Hash().String(), res[1].String())
}

func TestMainChainBlockHashesByHeights_CeilingFromHashHeight(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)
	ctx := context.Background()

	// Start hash is block2 (height 2). Requesting height 3 (above block2) must be
	// excluded because the ceiling is derived from the start hash's OWN height,
	// not trusted from the caller. The result is therefore incomplete (height 3
	// missing) and the method falls back rather than returning a wrong locator.
	res, ok, err := s.MainChainBlockHashesByHeights(ctx, block2.Hash(), []uint32{3, 2, 1})
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, res)
}

func TestMainChainBlockHashesByHeights_ForkTipFallsBack(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2)
	ctx := context.Background()

	res, ok, err := s.MainChainBlockHashesByHeights(ctx, blockAlternative2.Hash(), []uint32{2, 1})
	require.NoError(t, err)
	require.False(t, ok, "a fork-tip start hash must signal fallback")
	require.Nil(t, res)
}

func TestMainChainBlockHashesByHeights_RebuildingFallsBack(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)
	ctx := context.Background()

	s.mainChainRebuilding.Add(1)
	defer s.mainChainRebuilding.Add(-1)

	res, ok, err := s.MainChainBlockHashesByHeights(ctx, block3.Hash(), []uint32{3, 2, 1})
	require.NoError(t, err)
	require.False(t, ok, "mid-rebuild must signal fallback")
	require.Nil(t, res)
}

func TestMainChainBlockHashesByHeights_MissingHeightFallsBack(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)
	ctx := context.Background()

	res, ok, err := s.MainChainBlockHashesByHeights(ctx, block3.Hash(), []uint32{3, 99})
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, res)
}

func TestMainChainBlockHashesByHeights_EmptyInputsFallBack(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)
	ctx := context.Background()

	res, ok, err := s.MainChainBlockHashesByHeights(ctx, block3.Hash(), nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, res)
}

func TestMainChainBlockHashesByHeights_AgreesWithCTEWalk(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)
	ctx := context.Background()

	heights := []uint32{3, 2, 1}
	res, ok, err := s.MainChainBlockHashesByHeights(ctx, block3.Hash(), heights)
	require.NoError(t, err)
	require.True(t, ok)

	for _, h := range heights {
		blk, _, err := s.GetBlockInChainByHeightHash(ctx, h, block3.Hash())
		require.NoError(t, err, "height %d", h)
		require.Equal(t, blk.Header.Hash().String(), res[h].String(), "fast path must match CTE walk at height %d", h)
	}
}

func TestMainChainBlockHashesByHeights_PostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	t.Run("fast path matches CTE walk", func(t *testing.T) {
		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()
		waitForStartupRebuild(t, s)
		storeTestBlocks(t, s) // stores block1, block2 (heights 1, 2)
		ctx := t.Context()

		heights := []uint32{2, 1}
		res, ok, err := s.MainChainBlockHashesByHeights(ctx, block2.Hash(), heights)
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, res, 2)

		for _, h := range heights {
			blk, _, err := s.GetBlockInChainByHeightHash(ctx, h, block2.Hash())
			require.NoError(t, err, "height %d", h)
			require.Equal(t, blk.Header.Hash().String(), res[h].String(), "height %d", h)
		}
	})

	t.Run("query uses idx_on_main_chain_height", func(t *testing.T) {
		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()
		waitForStartupRebuild(t, s)
		storeTestBlocks(t, s)
		ctx := t.Context()

		// Disable seq scan so the planner is forced onto an index path: this
		// asserts the fast-path query CAN be served by idx_on_main_chain_height,
		// rather than depending on cost estimates that favour a seq scan on a
		// tiny test table. SET LOCAL requires a transaction.
		//
		// The EXPLAIN query text below must match the Postgres branch of
		// MainChainBlockHashesByHeights exactly — it intentionally pins the
		// query shape so a future change that stops using the index fails here.
		tx, err := s.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		_, err = tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off")
		require.NoError(t, err)

		rows, err := tx.QueryContext(ctx, `
			EXPLAIN (FORMAT TEXT)
			SELECT height, hash FROM blocks
			WHERE on_main_chain = true AND height <= $1 AND height = ANY($2)`,
			int64(2), pgFlatInt64(2, 1))
		require.NoError(t, err)
		defer rows.Close()

		var plan strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			plan.WriteString(line)
			plan.WriteString("\n")
		}
		require.NoError(t, rows.Err())
		require.Contains(t, plan.String(), "idx_on_main_chain_height",
			"fast-path query must use the partial index; plan was:\n%s", plan.String())
	})

	t.Run("fork tip falls back", func(t *testing.T) {
		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()
		waitForStartupRebuild(t, s)
		storeTestBlocks(t, s)
		ctx := t.Context()

		altBlock2 := createAlternativeBlock2()
		_, _, err := s.StoreBlock(ctx, altBlock2, "test_peer")
		require.NoError(t, err)

		res, ok, err := s.MainChainBlockHashesByHeights(ctx, altBlock2.Hash(), []uint32{2, 1})
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})
}

// newOnMainChainTestStoreForBench mirrors newOnMainChainTestStore for benchmarks
// (sqlitememory store, waits for the background startup rebuild to complete).
func newOnMainChainTestStoreForBench(b *testing.B) *SQL {
	b.Helper()

	tSettings := test.CreateBaseTestSettings(b)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(b, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(b, err)

	b.Cleanup(func() { _ = s.Close() })

	waitForStartupRebuild(b, s)

	return s
}

// benchBuildMainChain stores a linear main chain of n blocks (heights 1..n) off
// the seeded genesis, each linked to its predecessor by HashPrevBlock so the
// store forms real parent_id links and marks them on_main_chain. Returns the tip
// hash and height.
func benchBuildMainChain(b *testing.B, s *SQL, n uint32) (*chainhash.Hash, uint32) {
	b.Helper()

	ctx := context.Background()

	_, _, err := s.StoreBlock(ctx, block1, "")
	require.NoError(b, err)
	prevHash := block1.Hash()

	for h := uint32(2); h <= n; h++ {
		blk := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1729259727,
				Nonce:          h, // vary to keep block hashes distinct
				HashPrevBlock:  prevHash,
				HashMerkleRoot: hashMerkleRoot,
				Bits:           *bits,
			},
			Height:           h,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree},
		}

		_, _, err := s.StoreBlock(ctx, blk, "")
		require.NoError(b, err)
		prevHash = blk.Hash()
	}

	return prevHash, n
}

// benchLocatorHeights mirrors services/blockchain.computeLocatorHeights (kept
// local to avoid importing the service package from the store package). The
// slice capacity differs (fixed 64 vs. the production computed maxEntries),
// which is irrelevant to the benchmark; if the production height formula
// changes, update this copy to match.
func benchLocatorHeights(tipHeight uint32) []uint32 {
	heights := make([]uint32, 0, 64)
	step := uint32(1)
	height := tipHeight
	for {
		heights = append(heights, height)
		if height == 0 {
			break
		}
		if step > height {
			step = height
		}
		height -= step
		if len(heights) > 10 {
			step *= 2
		}
	}
	return heights
}

func BenchmarkBlockLocatorFetch(b *testing.B) {
	const chainLen = 5_000

	s := newOnMainChainTestStoreForBench(b)
	tipHash, tipHeight := benchBuildMainChain(b, s, chainLen)
	ctx := context.Background()

	heights := benchLocatorHeights(tipHeight)

	// Both paths are measured COLD: GetBlockInChainByHeightHash caches its
	// result in s.responseCache, and in production that cache is invalidated by
	// StoreBlock on every new block (so each locator build against the advancing
	// tip is effectively cold). Clearing the cache each iteration — outside the
	// timer — keeps the comparison honest. MainChainBlockHashesByHeights is
	// uncached, so the clear is a no-op for it but kept for symmetry.
	b.Run("fast-path-batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			s.responseCache.DeleteAll()
			b.StartTimer()

			_, ok, err := s.MainChainBlockHashesByHeights(ctx, tipHash, heights)
			require.NoError(b, err)
			require.True(b, ok)
		}
	})

	b.Run("per-height-cte-walk", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			s.responseCache.DeleteAll()
			b.StartTimer()

			hash := tipHash
			for _, h := range heights {
				blk, _, err := s.GetBlockInChainByHeightHash(ctx, h, hash)
				require.NoError(b, err)
				hash = blk.Header.Hash()
			}
		}
	})
}
