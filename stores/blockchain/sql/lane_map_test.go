package sql

import (
	"context"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func holedRows(t *testing.T, laneNum uint8, positions ...uint64) []lane.MapRow {
	t.Helper()

	bm := roaring64.New()
	bm.AddMany(positions)

	rows, err := lane.BuildMapRows(laneNum, bm)
	require.NoError(t, err)

	return rows
}

func TestLaneMapRowsInsertAndRead(t *testing.T) {
	runLaneHandoutTests(t, "map rows", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		// Block 1,000 takes 1 to 90,000 of lane 7 whole and a holed stretch of lane 200 (top bit set).
		full := roaring64.New()
		full.AddRange(1, 90_001)
		lane7, err := lane.BuildMapRows(7, full)
		require.NoError(t, err)

		rows := append(holedRows(t, 200, 5_000, 5_001, 5_003, 7_000), lane7...)
		require.NoError(t, s.InsertLaneMapRows(ctx, 1_000, rows))

		got, err := s.GetLaneMapRows(ctx, 1_000)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, lane7[0], got[0], "sorted by lane")
		require.Equal(t, uint8(200), got[1].Lane)
		require.Equal(t, uint64(5_002), got[1].FirstHole)
		require.Equal(t, rows[0].HoleData, got[1].HoleData)

		// A retry with the same rows, in another order, writes nothing and succeeds.
		require.NoError(t, s.InsertLaneMapRows(ctx, 1_000, []lane.MapRow{rows[1], rows[0]}))

		// A retry that differs fails and changes nothing.
		require.Error(t, s.InsertLaneMapRows(ctx, 1_000, holedRows(t, 200, 5_000, 5_002, 7_000)))
		require.Error(t, s.InsertLaneMapRows(ctx, 1_000, lane7), "a missing row is a difference")

		again, err := s.GetLaneMapRows(ctx, 1_000)
		require.NoError(t, err)
		require.Equal(t, got, again)

		// Unknown block: no rows.
		none, err := s.GetLaneMapRows(ctx, 999)
		require.NoError(t, err)
		require.Empty(t, none)
	})
}

func TestLaneMapRowsRefuseInvalidRows(t *testing.T) {
	runLaneHandoutTests(t, "invalid rows", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		bad := []lane.MapRow{{Lane: 1, First: 5, Last: 4, Tag: lane.RowTagRange}}
		require.Error(t, s.InsertLaneMapRows(ctx, 1, bad))

		overlap := []lane.MapRow{{Lane: 1, First: 1, Last: 10, Tag: lane.RowTagRange}, {Lane: 1, First: 11, Last: 20, Tag: lane.RowTagRange}}
		require.Error(t, s.InsertLaneMapRows(ctx, 1, overlap), "two rows of one lane in one chunk")

		rows, err := s.GetLaneMapRows(ctx, 1)
		require.NoError(t, err)
		require.Empty(t, rows)

		// A stored row that a fault corrupted is refused on read, not returned.
		_, err = s.db.ExecContext(ctx, `INSERT INTO lane_map (block_id, lane, first, last, first_hole, tag, hole_data) VALUES (2, 1, 1, 10, 3, 0, NULL)`)
		require.NoError(t, err)

		_, err = s.GetLaneMapRows(ctx, 2)
		require.Error(t, err)
	})
}

// The reader query for a floor: rows of a lane ending above a position, from every block.
func TestGetLaneMapRowsEndingAbove(t *testing.T) {
	runLaneHandoutTests(t, "ending above", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		require.NoError(t, s.InsertLaneMapRows(ctx, 10, []lane.MapRow{{Lane: 7, First: 1, Last: 1_000, Tag: lane.RowTagRange}}))
		require.NoError(t, s.InsertLaneMapRows(ctx, 11, []lane.MapRow{{Lane: 7, First: 1_001, Last: 2_000, Tag: lane.RowTagRange}, {Lane: 8, First: 1, Last: 5_000, Tag: lane.RowTagRange}}))
		// A late-mined transaction below the floor: its row covers only position 600.
		require.NoError(t, s.InsertLaneMapRows(ctx, 12, []lane.MapRow{{Lane: 7, First: 600, Last: 600, Tag: lane.RowTagRange}}))

		got, err := s.GetLaneMapRowsEndingAbove(ctx, 7, 999)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, uint32(10), got[0].BlockID)
		require.Equal(t, uint32(11), got[1].BlockID)

		got, err = s.GetLaneMapRowsEndingAbove(ctx, 7, 500)
		require.NoError(t, err)
		require.Len(t, got, 3)

		_, err = s.GetLaneMapRowsEndingAbove(ctx, 7, lane.MaxPosition+1)
		require.Error(t, err)
	})
}

func testSegment(id uint32, first, prefixLast, last uint64) lane.ConversionSegment {
	return lane.ConversionSegment{
		ID: id, First: first, PrefixLast: prefixLast, Last: last,
		SourceTipBlockID: 900_000, SourceTipHash: chainhash.Hash{0xEE}, ManifestDigest: chainhash.Hash{byte(id)},
	}
}

// The design's example: block 700,000 holds A, B, C at 1,000,001 to 1,000,003, block 700,001 holds
// D and E at 1,000,004 and 1,000,005, and the suffix holds one unmined record at 1,000,006.
func TestLaneConversionSegmentLifecycle(t *testing.T) {
	runLaneHandoutTests(t, "conversion", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		seg := testSegment(1, 1_000_001, 1_000_005, 1_000_006)

		require.NoError(t, s.InsertLaneConversionSegment(ctx, seg))
		require.NoError(t, s.InsertLaneConversionSegment(ctx, seg), "the same segment again is a no-op")

		changed := seg
		changed.Last++
		require.Error(t, s.InsertLaneConversionSegment(ctx, changed), "a segment's bounds are immutable")
		require.Error(t, s.InsertLaneConversionSegment(ctx, testSegment(2, 1_000_006, 1_000_006, 1_000_010)), "overlap")

		b0 := lane.ConversionBlock{Seq: 0, BlockID: 700_000, Height: 700_000, First: 1_000_001, Count: 3}
		b1 := lane.ConversionBlock{Seq: 1, BlockID: 700_001, Height: 700_001, First: 1_000_004, Count: 2}

		require.Error(t, s.CompleteLaneConversionSegment(ctx, 1, chainhash.Hash{1}), "no blocks yet")

		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b0}))

		// A resume repeats what is stored and continues.
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b0, b1}))

		wrong := b1
		wrong.Count = 3
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{wrong}), "a recorded block is immutable")
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 3, BlockID: 1, Height: 700_003, First: 1_000_006}}), "a gap in the sequence")
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 9, []lane.ConversionBlock{b0}), "unknown segment")

		blocks, err := s.GetLaneConversionBlocks(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, []lane.ConversionBlock{b0, b1}, blocks)

		complete, err := s.LaneConversionComplete(ctx, 1)
		require.NoError(t, err)
		require.False(t, complete)

		require.NoError(t, s.CompleteLaneConversionSegment(ctx, 1, chainhash.Hash{1}))
		require.NoError(t, s.CompleteLaneConversionSegment(ctx, 1, chainhash.Hash{1}), "same digest again")
		require.Error(t, s.CompleteLaneConversionSegment(ctx, 1, chainhash.Hash{2}))
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b0}), "a complete segment is fixed")

		complete, err = s.LaneConversionComplete(ctx, 1)
		require.NoError(t, err)
		require.True(t, complete)

		segs, err := s.GetLaneConversionSegments(ctx)
		require.NoError(t, err)
		require.Equal(t, []lane.ConversionSegment{seg}, segs)

		// A later segment appends above; its block heights may be older than the first segment's.
		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(2, 1_000_007, 1_000_007, 1_000_007)))
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 2, []lane.ConversionBlock{{Seq: 0, BlockID: 650_000, Height: 650_000, First: 1_000_007, Count: 1}}))
		require.NoError(t, s.CompleteLaneConversionSegment(ctx, 2, chainhash.Hash{3}))
	})
}

func TestLaneConversionBlocksMustNotPassThePrefix(t *testing.T) {
	runLaneHandoutTests(t, "prefix end", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(1, 2, 4, 4)))
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 0, BlockID: 1, Height: 1, First: 2, Count: 4}}))
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 0, BlockID: 1, Height: 1, First: 3, Count: 1}}), "must start at the segment's first")
	})
}

// A blockchain database from before this change has the lease tables but none of the map tables.
// Schema setup adds them and touches nothing else.
func TestLaneMapTablesAreAddedToAnOlderSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	s, err := New(ulogger.TestLogger{}, dbURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, err = db.Exec(`INSERT INTO lane_handout (lane, first, last, holder, height, handed_at) VALUES (3, 1, 10, 'old', 1, now())`)
	require.NoError(t, err)

	for _, table := range laneMapPostgresTables {
		if table.name == "lane_map_lane_last" {
			continue
		}

		_, err = db.Exec(`DROP TABLE ` + table.name)
		require.NoError(t, err, table.name)
	}

	require.NoError(t, createPostgresSchema(ulogger.TestLogger{}, db, true))

	for _, table := range laneMapPostgresTables {
		var exists bool
		require.NoError(t, db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table.name).Scan(&exists))
		require.True(t, exists, table.name)
	}

	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM lane_handout`).Scan(&n))
	require.Equal(t, 1, n, "existing hand-outs are kept")
}

// A writer of a block's rows takes that block's lane write lock before it looks at what is
// stored, so a second writer waits for the first and then compares with its rows, instead of both
// seeing none and inserting different ones. Held here from another transaction, the lock must hold
// the insert back until it is released.
func TestLaneMapRowsWaitForTheBlockLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	s, cleanup := setupLanePostgresStore(t)
	defer cleanup()

	ctx := t.Context()

	holder, err := s.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback() }()

	_, err = holder.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, laneMapLockClass, int32(77))
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		done <- s.InsertLaneMapRows(ctx, 77, []lane.MapRow{{Lane: 1, First: 1, Last: 10, Tag: lane.RowTagRange}})
	}()

	select {
	case err = <-done:
		require.Failf(t, "the insert did not wait for the block lock", "returned %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	// Another block's rows are not held back.
	require.NoError(t, s.InsertLaneMapRows(ctx, 78, []lane.MapRow{{Lane: 1, First: 1, Last: 10, Tag: lane.RowTagRange}}))

	require.NoError(t, holder.Rollback())
	require.NoError(t, <-done)
}
