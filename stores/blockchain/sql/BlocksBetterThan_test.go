package sql

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// forEachBestTipEngine runs fn on a fresh SQLite store and, when a container is available, a
// fresh PostgreSQL store. The best-tip order and the low-water mark are written in SQL both
// engines must accept, and the peer id order is the database's own.
func forEachBestTipEngine(t *testing.T, fn func(t *testing.T, s *SQL)) {
	t.Run("sqlite", func(t *testing.T) {
		fn(t, setupTestStore(t))
	})

	t.Run("postgres", func(t *testing.T) {
		if testing.Short() {
			t.Skip("postgres container test")
		}

		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()

		fn(t, s)
	})
}

func storeOn(t *testing.T, s *SQL, block *model.Block, peer string, opts ...options.StoreBlockOption) {
	t.Helper()

	_, _, err := s.StoreBlock(t.Context(), block, peer, opts...)
	require.NoError(t, err)
}

func keyOf(t *testing.T, s *SQL, hash *chainhash.Hash) model.BestTipKey {
	t.Helper()

	header, meta, err := s.GetBlockHeader(t.Context(), hash)
	require.NoError(t, err)

	return model.NewBestTipKey(header, meta)
}

func requireBest(t *testing.T, s *SQL, want *model.Block) {
	t.Helper()

	best, _, err := s.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, want.Hash().String(), best.Hash().String())
}

func requireBetter(t *testing.T, s *SQL, after model.BestTipKey, from *model.BestTipKey, minHeight, limit uint32, want ...*model.Block) {
	t.Helper()

	headers, metas, err := s.BlocksBetterThan(t.Context(), after, from, minHeight, limit)
	require.NoError(t, err)
	require.Len(t, metas, len(headers))

	got := make([]string, len(headers))
	for i, h := range headers {
		got[i] = h.Hash().String()
	}

	wanted := make([]string, len(want))
	for i, b := range want {
		wanted[i] = b.Hash().String()
	}

	require.Equal(t, wanted, got)
}

// lowWater reads the low-water mark; ok is false when there is none.
func lowWater(t *testing.T, s *SQL) (mark model.BestTipLowWater, data []byte, ok bool) {
	t.Helper()

	data, err := s.GetState(t.Context(), model.BestTipLowWaterStateKey)
	if err != nil {
		require.ErrorIs(t, err, sql.ErrNoRows)
		return model.BestTipLowWater{}, nil, false
	}

	mark, err = model.NewBestTipLowWaterFromBytes(data)
	require.NoError(t, err)

	return mark, data, true
}

// TestBlocksBetterThan_BrieflyBestInOrder: a reader at A1 sees every block that was the best tip
// since, worst first. B2's id was reserved before A2 was stored, so B2 has the lower id although
// it was written later; it ties A2 on work and peer and wins on id, so it was the best tip until
// A3 arrived. A read in id order from A2 would never see it.
func TestBlocksBetterThan_BrieflyBestInOrder(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		ctx := t.Context()
		genesis := s.chainParams.GenesisHash

		a1 := createTestBlock(t, 101, genesis)
		storeOn(t, s, a1, "p")

		// B1 ties A1 and loses on id: never the best.
		b1 := createTestBlock(t, 201, genesis)
		storeOn(t, s, b1, "p")
		requireBest(t, s, a1)

		b2 := createTestBlock(t, 202, b1.Hash())
		b2ID, err := s.AssignBlockID(ctx, b2.Hash())
		require.NoError(t, err)

		a2 := createTestBlock(t, 102, a1.Hash())
		storeOn(t, s, a2, "p")
		requireBest(t, s, a2)

		storeOn(t, s, b2, "p", options.WithID(b2ID))
		requireBest(t, s, b2)
		require.Less(t, keyOf(t, s, b2.Hash()).ID, keyOf(t, s, a2.Hash()).ID, "B2 holds the lower id although it was written after A2")

		a3 := createTestBlock(t, 103, a2.Hash())
		storeOn(t, s, a3, "p")
		requireBest(t, s, a3)

		pos := keyOf(t, s, a1.Hash())
		require.Len(t, pos.ChainWork, 32, "chain work is stored fixed-length big-endian, so bytewise order is numeric order")

		requireBetter(t, s, pos, nil, 0, 100, a2, b2, a3)

		// Paging continues from the last block returned.
		requireBetter(t, s, pos, nil, 0, 2, a2, b2)
		requireBetter(t, s, keyOf(t, s, b2.Hash()), nil, 0, 2, a3)
		requireBetter(t, s, keyOf(t, s, a3.Hash()), nil, 0, 100)

		// The height floor drops blocks below it.
		requireBetter(t, s, pos, nil, 3, 100, a3)

		// An invalid block is never returned.
		_, err = s.InvalidateBlock(ctx, a3.Hash())
		require.NoError(t, err)
		requireBetter(t, s, pos, nil, 0, 100, a2, b2)
	})
}

// TestBlocksBetterThan_PeerTieSwitch: a sibling with the same work and a lower peer id becomes
// the best tip even though it arrived later with a higher block id, and the read returns it.
func TestBlocksBetterThan_PeerTieSwitch(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		a1 := createTestBlock(t, 301, s.chainParams.GenesisHash)
		storeOn(t, s, a1, "b")

		a2 := createTestBlock(t, 302, a1.Hash())
		storeOn(t, s, a2, "b")

		c2 := createTestBlock(t, 402, a1.Hash())
		storeOn(t, s, c2, "a")
		requireBest(t, s, c2)

		// D2 has the higher peer id: it sorts below A2 and was never the best.
		d2 := createTestBlock(t, 502, a1.Hash())
		storeOn(t, s, d2, "c")
		requireBest(t, s, c2)

		requireBetter(t, s, keyOf(t, s, a2.Hash()), nil, 0, 100, c2)
		requireBetter(t, s, keyOf(t, s, a1.Hash()), nil, 0, 100, d2, a2, c2)
	})
}

// TestBlocksBetterThan_FromIsInclusive: with a from key below after, the read starts at from
// itself; with one above, it adds nothing.
func TestBlocksBetterThan_FromIsInclusive(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		chain := make([]*model.Block, 0, 4)
		parent := s.chainParams.GenesisHash

		for i := uint32(0); i < 4; i++ {
			b := createTestBlock(t, 600+i, parent)
			storeOn(t, s, b, "p")
			chain = append(chain, b)
			parent = b.Hash()
		}

		after := keyOf(t, s, chain[2].Hash())
		low := keyOf(t, s, chain[0].Hash())
		high := keyOf(t, s, chain[3].Hash())

		requireBetter(t, s, after, nil, 0, 100, chain[3])
		requireBetter(t, s, after, &low, 0, 100, chain...)
		requireBetter(t, s, after, &high, 0, 100, chain[3])
	})
}

// TestBestTipLowWater_WrittenByDrops: only a write that moves the best tip down records the
// mark, the mark keeps the lowest place and every block that was the best tip when it dropped,
// every write gives it a new generation, and the conditional clear removes only the bytes the
// caller read.
func TestBestTipLowWater_WrittenByDrops(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		ctx := t.Context()

		a1 := createTestBlock(t, 701, s.chainParams.GenesisHash)
		a2 := createTestBlock(t, 702, a1.Hash())
		a3 := createTestBlock(t, 703, a2.Hash())
		side := createTestBlock(t, 802, a1.Hash())

		for _, b := range []*model.Block{a1, a2, a3, side} {
			storeOn(t, s, b, "p")
		}

		requireBest(t, s, a3)

		_, _, ok := lowWater(t, s)
		require.False(t, ok, "extending the chain records no drop")

		// Invalidating a block off the best chain does not move the tip.
		_, err := s.InvalidateBlock(ctx, side.Hash())
		require.NoError(t, err)

		_, _, ok = lowWater(t, s)
		require.False(t, ok, "an invalidation that leaves the tip records no drop")

		// Invalidating the tip drops it to A2.
		_, err = s.InvalidateBlock(ctx, a3.Hash())
		require.NoError(t, err)

		mark, first, ok := lowWater(t, s)
		require.True(t, ok)
		require.Equal(t, a2.Hash().String(), mark.Key.Hash.String())
		require.Equal(t, keyOf(t, s, a2.Hash()).ChainWork, mark.Key.ChainWork)
		require.Equal(t, []chainhash.Hash{*a3.Hash()}, mark.DroppedTips, "the mark names the tip that was invalidated")
		require.False(t, mark.DroppedTipsOverflow)

		// A deeper drop lowers the mark to A1.
		_, err = s.InvalidateBlock(ctx, a2.Hash())
		require.NoError(t, err)

		mark, second, ok := lowWater(t, s)
		require.True(t, ok)
		require.Equal(t, a1.Hash().String(), mark.Key.Hash.String())
		require.Equal(t, []chainhash.Hash{*a3.Hash(), *a2.Hash()}, mark.DroppedTips, "two drops before a read keep both tips")

		// Revalidation only moves the tip up and writes nothing.
		require.NoError(t, s.RevalidateBlock(ctx, a2.Hash()))
		require.NoError(t, s.RevalidateBlock(ctx, a3.Hash()))
		requireBest(t, s, a3)

		_, unchanged, ok := lowWater(t, s)
		require.True(t, ok)
		require.Equal(t, second, unchanged)

		// A drop to a place above the mark keeps the lower key but changes the bytes.
		_, err = s.InvalidateBlock(ctx, a3.Hash())
		require.NoError(t, err)

		mark, third, ok := lowWater(t, s)
		require.True(t, ok)
		require.Equal(t, a1.Hash().String(), mark.Key.Hash.String(), "the mark keeps the lower of the two")
		require.NotEqual(t, second, third, "a reader holding the older bytes must not clear this drop")
		require.Equal(t, []chainhash.Hash{*a3.Hash(), *a2.Hash()}, mark.DroppedTips, "a tip that drops again is listed once")

		// The clear is conditional on the bytes read.
		require.NoError(t, s.ClearBestTipLowWater(ctx, first))
		require.NoError(t, s.ClearBestTipLowWater(ctx, second))

		_, still, ok := lowWater(t, s)
		require.True(t, ok, "a clear with stale bytes is a no-op")
		require.Equal(t, third, still)

		require.NoError(t, s.ClearBestTipLowWater(ctx, third))

		_, _, ok = lowWater(t, s)
		require.False(t, ok)
	})
}

// TestBestTipLowWater_DeleteBest: deleting the best block drops the tip and records the mark;
// deleting a block off the best chain does not.
func TestBestTipLowWater_DeleteBest(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		ctx := t.Context()

		a1 := createTestBlock(t, 901, s.chainParams.GenesisHash)
		a2 := createTestBlock(t, 902, a1.Hash())
		side := createTestBlock(t, 1002, a1.Hash())

		for _, b := range []*model.Block{a1, a2, side} {
			storeOn(t, s, b, "p")
		}

		require.NoError(t, s.DeleteBlock(ctx, side.Hash()))

		_, _, ok := lowWater(t, s)
		require.False(t, ok)

		require.NoError(t, s.DeleteBlock(ctx, a2.Hash()))
		requireBest(t, s, a1)

		mark, _, ok := lowWater(t, s)
		require.True(t, ok)
		require.Equal(t, a1.Hash().String(), mark.Key.Hash.String())
		require.Equal(t, []chainhash.Hash{*a2.Hash()}, mark.DroppedTips, "the mark names the deleted tip")
	})
}

// TestBestTipLowWater_UnreadableMarkResets: a drop merged into a mark this binary cannot read
// loses whatever tips that mark listed, so the new mark asks the reader to reset regardless.
func TestBestTipLowWater_UnreadableMarkResets(t *testing.T) {
	forEachBestTipEngine(t, func(t *testing.T, s *SQL) {
		ctx := t.Context()

		a1 := createTestBlock(t, 1101, s.chainParams.GenesisHash)
		a2 := createTestBlock(t, 1102, a1.Hash())

		for _, b := range []*model.Block{a1, a2} {
			storeOn(t, s, b, "p")
		}

		require.NoError(t, s.SetState(ctx, model.BestTipLowWaterStateKey, []byte{0xee}))

		_, err := s.InvalidateBlock(ctx, a2.Hash())
		require.NoError(t, err)

		mark, _, ok := lowWater(t, s)
		require.True(t, ok)
		require.Equal(t, a1.Hash().String(), mark.Key.Hash.String())
		require.Equal(t, []chainhash.Hash{*a2.Hash()}, mark.DroppedTips)
		require.True(t, mark.DroppedTipsOverflow, "the tips the unreadable mark held are lost, so the reader must reset")
	})
}

// TestBlocksBetterThan_PostgresUsesTheBestTipIndex checks the plan of both read queries on
// PostgreSQL. Left to itself on a tiny table the planner picks a bitmap scan of
// idx_chain_work_valid bounded by chain_work, then sorts the few rows it finds; the test logs
// that plan and checks the index bound. With sequential and bitmap scans disabled it must find
// the index scan read backwards with no sort, which shows the index serves both the bound and
// the order, so the read stays a bounded index range on a table of a million blocks.
func TestBlocksBetterThan_PostgresUsesTheBestTipIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("postgres container test")
	}

	s, cleanup := setupPostgresTestStore(t)
	defer cleanup()

	require.Equal(t, util.Postgres, s.engine)

	a1 := createTestBlock(t, 1101, s.chainParams.GenesisHash)
	storeOn(t, s, a1, "p")

	key := keyOf(t, s, a1.Hash())

	plan := func(forceIndex bool, query string, args ...any) string {
		tx, err := s.db.BeginTx(t.Context(), nil)
		require.NoError(t, err)

		defer func() { _ = tx.Rollback() }()

		if forceIndex {
			_, err = tx.ExecContext(t.Context(), `SET LOCAL enable_seqscan = off`)
			require.NoError(t, err)

			_, err = tx.ExecContext(t.Context(), `SET LOCAL enable_bitmapscan = off`)
			require.NoError(t, err)
		}

		rows, err := tx.QueryContext(t.Context(), "EXPLAIN "+query, args...)
		require.NoError(t, err)

		defer rows.Close()

		var lines []string

		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			lines = append(lines, line)
		}

		require.NoError(t, rows.Err())

		return strings.Join(lines, "\n")
	}

	afterArgs := []any{key.ChainWork, key.ChainWork, key.PeerID, int64(key.ID), 0, 100}
	fromArgs := []any{key.ChainWork, key.ChainWork, key.PeerID, int64(key.ID), key.ChainWork, key.PeerID, int64(key.ID), 0, 100}

	for name, q := range map[string]struct {
		query string
		args  []any
	}{
		"after": {blocksBetterThanQuery, afterArgs},
		"from":  {blocksBetterThanOrFromQuery, fromArgs},
	} {
		free := plan(false, q.query, q.args...)
		t.Logf("%s plan, planner's choice:\n%s", name, free)
		require.Contains(t, free, "idx_chain_work_valid", name)
		require.Contains(t, free, "Index Cond: (chain_work >= ", name)

		forced := plan(true, q.query, q.args...)
		t.Logf("%s plan, index scan:\n%s", name, forced)
		require.Contains(t, forced, "Index Scan Backward using idx_chain_work_valid", name)
		require.Contains(t, forced, "Index Cond: (chain_work >= ", name)
		require.NotContains(t, forced, "Sort", name)
	}
}
