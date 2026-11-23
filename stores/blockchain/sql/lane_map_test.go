package sql

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
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

		got, err := s.LaneMapRows(ctx, 1_000)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, lane7[0], got[0], "sorted by lane")
		require.Equal(t, uint8(200), got[1].Lane)
		require.Equal(t, uint64(5_002), got[1].FirstHole)
		require.Equal(t, rows[0].HoleData, got[1].HoleData)

		// A retry with the same rows writes nothing and succeeds, in the order given (lane 200 before
		// lane 7, not the stored order) and in the stored order.
		require.Equal(t, uint8(200), rows[0].Lane, "the retry is out of the stored order")
		require.NoError(t, s.InsertLaneMapRows(ctx, 1_000, rows))
		require.NoError(t, s.InsertLaneMapRows(ctx, 1_000, []lane.MapRow{rows[1], rows[0]}))

		// A retry that differs fails and changes nothing: other holes in as many rows, a row
		// fewer, or one row alone.
		require.Error(t, s.InsertLaneMapRows(ctx, 1_000, append(holedRows(t, 200, 5_000, 5_002, 7_000), lane7...)), "same row count, different holes")
		require.Error(t, s.InsertLaneMapRows(ctx, 1_000, append(append([]lane.MapRow(nil), lane7...), holedRows(t, 200, 5_000, 5_002, 7_000)...)), "same row count, different holes, out of order")
		require.Error(t, s.InsertLaneMapRows(ctx, 1_000, holedRows(t, 200, 5_000, 5_002, 7_000)))
		require.Error(t, s.InsertLaneMapRows(ctx, 1_000, lane7), "a missing row is a difference")

		again, err := s.LaneMapRows(ctx, 1_000)
		require.NoError(t, err)
		require.Equal(t, got, again)

		// Unknown block: no rows.
		none, err := s.LaneMapRows(ctx, 999)
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

		rows, err := s.LaneMapRows(ctx, 1)
		require.NoError(t, err)
		require.Empty(t, rows)

		// A stored row that a fault corrupted is refused on read, not returned.
		_, err = s.db.ExecContext(ctx, `INSERT INTO lane_map (block_id, lane, first, last, first_hole, tag, hole_data) VALUES (2, 1, 1, 10, 3, 0, NULL)`)
		require.NoError(t, err)

		_, err = s.LaneMapRows(ctx, 2)
		require.Error(t, err)
	})
}

// The reader query for a floor: rows of a lane ending above a position, from every block.
func TestLaneMapRowsEndingAbove(t *testing.T) {
	runLaneHandoutTests(t, "ending above", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		require.NoError(t, s.InsertLaneMapRows(ctx, 10, []lane.MapRow{{Lane: 7, First: 1, Last: 1_000, Tag: lane.RowTagRange}}))
		require.NoError(t, s.InsertLaneMapRows(ctx, 11, []lane.MapRow{{Lane: 7, First: 1_001, Last: 2_000, Tag: lane.RowTagRange}, {Lane: 8, First: 1, Last: 5_000, Tag: lane.RowTagRange}}))
		// A late-mined transaction below the floor: its row covers only position 600.
		require.NoError(t, s.InsertLaneMapRows(ctx, 12, []lane.MapRow{{Lane: 7, First: 600, Last: 600, Tag: lane.RowTagRange}}))

		got, err := s.LaneMapRowsEndingAbove(ctx, 7, 999)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, uint32(10), got[0].BlockID)
		require.Equal(t, uint32(11), got[1].BlockID)

		got, err = s.LaneMapRowsEndingAbove(ctx, 7, 500)
		require.NoError(t, err)
		require.Len(t, got, 3)

		_, err = s.LaneMapRowsEndingAbove(ctx, 7, lane.MaxPosition+1)
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

		blocks, err := s.LaneConversionBlocks(ctx, 1)
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

		_, err = s.LaneConversionComplete(ctx, 9)
		require.Error(t, err, "an unrecorded segment is an error, not false")

		segs, err := s.LaneConversionSegments(ctx)
		require.NoError(t, err)
		require.Equal(t, []lane.ConversionSegment{seg}, segs)

		// A later segment appends above; its block heights may be older than the first segment's.
		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(2, 1_000_007, 1_000_007, 1_000_007)))
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 2, []lane.ConversionBlock{{Seq: 0, BlockID: 650_000, Height: 650_000, First: 1_000_007, Count: 1}}))
		require.NoError(t, s.CompleteLaneConversionSegment(ctx, 2, chainhash.Hash{3}))
	})
}

// The prefix ends at 4 and the suffix runs to 10, so a block reaching 5 stays inside the segment and
// only the prefix-end check refuses it.
func TestLaneConversionBlocksMustNotPassThePrefix(t *testing.T) {
	runLaneHandoutTests(t, "prefix end", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(1, 2, 4, 10)))
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 0, BlockID: 1, Height: 1, First: 2, Count: 4}}))
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 0, BlockID: 1, Height: 1, First: 3, Count: 1}}), "must start at the segment's first")

		// Across calls too: the second block may not run past the prefix end either.
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 0, BlockID: 1, Height: 1, First: 2, Count: 2}}))
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 1, BlockID: 2, Height: 2, First: 4, Count: 2}}))
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 1, BlockID: 2, Height: 2, First: 4, Count: 1}}))
		require.NoError(t, s.CompleteLaneConversionSegment(ctx, 1, chainhash.Hash{1}))
	})
}

// A block added in a later call must continue the last stored block: the next position, a higher
// height and another block. The stored list is read only from its last block, so each of these is
// checked against that block.
func TestLaneConversionBlocksContinueAcrossCalls(t *testing.T) {
	runLaneHandoutTests(t, "continue", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(1, 2, 20, 20)))

		b0 := lane.ConversionBlock{Seq: 0, BlockID: 100, Height: 100, First: 2, Count: 3}
		b1 := lane.ConversionBlock{Seq: 1, BlockID: 101, Height: 101, First: 5, Count: 0}
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b0, b1}))

		for name, b := range map[string]lane.ConversionBlock{
			"height not rising": {Seq: 2, BlockID: 102, Height: 101, First: 5, Count: 1},
			"same block again":  {Seq: 2, BlockID: 101, Height: 102, First: 5, Count: 1},
			"position gap":      {Seq: 2, BlockID: 102, Height: 102, First: 6, Count: 1},
			"position overlap":  {Seq: 2, BlockID: 102, Height: 102, First: 4, Count: 1},
			"sequence gap":      {Seq: 3, BlockID: 102, Height: 102, First: 5, Count: 1},
		} {
			require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b}), name)
		}

		// Repeating an earlier block, out of order with new ones, compares it and continues.
		b2 := lane.ConversionBlock{Seq: 2, BlockID: 102, Height: 102, First: 5, Count: 2}
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b0, b2}))

		changed := b0
		changed.BlockID = 99
		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{changed, b1}), "a repeated block that differs, with a matching one")

		blocks, err := s.LaneConversionBlocks(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, []lane.ConversionBlock{b0, b1, b2}, blocks)

		// A resume that repeats three stored blocks out of order, the lowest neither first nor
		// last, and adds one: every repeat is compared and the new block goes in.
		b3 := lane.ConversionBlock{Seq: 3, BlockID: 103, Height: 103, First: 7, Count: 1}
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b1, b0, b2, b3}))

		blocks, err = s.LaneConversionBlocks(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, []lane.ConversionBlock{b0, b1, b2, b3}, blocks)

		// A block ID listed in two calls, not adjacent, is caught when the segment completes.
		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 4, BlockID: 100, Height: 104, First: 8, Count: 13}}))
		require.Error(t, s.CompleteLaneConversionSegment(ctx, 1, chainhash.Hash{1}))
	})
}

// Within one call each new block chains onto the block before it in that call, on a first call
// and after stored blocks, and a call with one bad block stores none of its blocks.
func TestLaneConversionBlocksChainWithinOneCall(t *testing.T) {
	runLaneHandoutTests(t, "one call", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(1, 2, 20, 20)))

		b0 := lane.ConversionBlock{Seq: 0, BlockID: 100, Height: 100, First: 2, Count: 3}
		b1 := lane.ConversionBlock{Seq: 1, BlockID: 101, Height: 101, First: 5, Count: 2}

		bad := func(seq uint32, after lane.ConversionBlock) map[string]lane.ConversionBlock {
			next := after.First + after.Count

			return map[string]lane.ConversionBlock{
				"height not rising": {Seq: seq, BlockID: after.BlockID + 50, Height: after.Height, First: next, Count: 1},
				"same block":        {Seq: seq, BlockID: after.BlockID, Height: after.Height + 1, First: next, Count: 1},
				"position gap":      {Seq: seq, BlockID: after.BlockID + 50, Height: after.Height + 1, First: next + 1, Count: 1},
			}
		}

		for name, b := range bad(1, b0) {
			require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b0, b}), "first call: %s", name)

			blocks, err := s.LaneConversionBlocks(ctx, 1)
			require.NoError(t, err)
			require.Empty(t, blocks, name)
		}

		require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b0}))

		for name, b := range bad(2, b1) {
			require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{b1, b}), "after stored blocks: %s", name)

			blocks, err := s.LaneConversionBlocks(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, []lane.ConversionBlock{b0}, blocks, name)
		}
	})
}

// A stored block list that already runs past the prefix end, which only a fault could leave, is
// refused rather than continued.
func TestLaneConversionBlocksRefuseAStoredListPastThePrefix(t *testing.T) {
	runLaneHandoutTests(t, "stored past prefix", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(1, 2, 4, 10)))

		_, err := s.db.ExecContext(ctx, `INSERT INTO lane_conversion_block (segment, seq, block_id, height, first, count) VALUES (1, 0, 100, 100, 2, 5)`)
		require.NoError(t, err)

		require.Error(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 1, BlockID: 101, Height: 101, First: 7, Count: 0}}))
	})
}

// The readers refuse a stored integer outside its Go type's range before casting it, rather than
// return a truncated value that looks valid: lane 257 would read as lane 1, block -1 as block
// 4294967295. Each case stores one valid row with one value out of range, and names the error the
// range check gives, so no later check can stand in for it.
func TestLaneReadersRefuseOutOfRangeStoredValues(t *testing.T) {
	const tooWide = int64(1) << 32

	runLaneHandoutTests(t, "out of range", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		mapRow := func(blockID, laneNum, first, last, firstHole, tag int64) {
			t.Helper()

			_, err := s.db.ExecContext(ctx, `DELETE FROM lane_map`)
			require.NoError(t, err)

			_, err = s.db.ExecContext(ctx, `INSERT INTO lane_map (block_id, lane, first, last, first_hole, tag, hole_data) VALUES ($1, $2, $3, $4, $5, $6, NULL)`,
				blockID, laneNum, first, last, firstHole, tag)
			require.NoError(t, err)
		}

		mapRow(5, 1, 1, 10, 0, 0)
		rows, err := s.LaneMapRows(ctx, 5)
		require.NoError(t, err)
		require.Len(t, rows, 1, "the base row is valid")

		for name, c := range map[string][6]int64{
			"lane below 0":        {5, -1, 1, 10, 0, 0},
			"lane above 255":      {5, 257, 1, 10, 0, 0},
			"tag below 0":         {5, 1, 1, 10, 0, -1},
			"tag above 255":       {5, 1, 1, 10, 0, 257},
			"negative first":      {5, 1, -1, 10, 0, 0},
			"negative last":       {5, 1, 1, -1, 0, 0},
			"negative first hole": {5, 1, 1, 10, -1, 0},
		} {
			mapRow(c[0], c[1], c[2], c[3], c[4], c[5])

			_, err := s.LaneMapRows(ctx, 5)
			require.ErrorContains(t, err, "out-of-range values", name)
		}

		// A block ID out of range cannot be asked for by block, so it is read by lane.
		for name, blockID := range map[string]int64{"block below 0": -1, "block above 2^32-1": tooWide} {
			mapRow(blockID, 1, 1, 10, 0, 0)

			_, err := s.LaneMapRowsEndingAbove(ctx, 1, 0)
			require.ErrorContains(t, err, "out-of-range values", name)
		}

		hash := make([]byte, chainhash.HashSize)

		segment := func(id, tip int64, tipHash, digest []byte) {
			t.Helper()

			_, err := s.db.ExecContext(ctx, `DELETE FROM lane_conversion_segment`)
			require.NoError(t, err)

			_, err = s.db.ExecContext(ctx, `INSERT INTO lane_conversion_segment (segment, first, prefix_last, last, source_tip_block_id, source_tip_hash, manifest_digest, created_at) VALUES ($1, 2, 4, 10, $2, $3, $4, CURRENT_TIMESTAMP)`,
				id, tip, tipHash, digest)
			require.NoError(t, err)
		}

		segment(1, 900_000, hash, hash)
		segs, err := s.LaneConversionSegments(ctx)
		require.NoError(t, err)
		require.Len(t, segs, 1, "the base segment is valid")

		for name, c := range map[string]struct {
			id, tip         int64
			tipHash, digest []byte
		}{
			"segment 0":             {0, 900_000, hash, hash},
			"segment above 2^32-1":  {tooWide, 900_000, hash, hash},
			"tip below 0":           {1, -1, hash, hash},
			"tip above 2^32-1":      {1, tooWide, hash, hash},
			"short tip hash":        {1, 900_000, hash[:31], hash},
			"short manifest digest": {1, 900_000, hash, hash[:31]},
		} {
			segment(c.id, c.tip, c.tipHash, c.digest)

			_, err := s.LaneConversionSegments(ctx)
			require.ErrorContains(t, err, "is malformed", name)
		}

		block := func(seq, blockID, height, first, count int64) {
			t.Helper()

			_, err := s.db.ExecContext(ctx, `DELETE FROM lane_conversion_block`)
			require.NoError(t, err)

			_, err = s.db.ExecContext(ctx, `INSERT INTO lane_conversion_block (segment, seq, block_id, height, first, count) VALUES (1, $1, $2, $3, $4, $5)`,
				seq, blockID, height, first, count)
			require.NoError(t, err)
		}

		block(0, 100, 100, 2, 1)
		blocks, err := s.LaneConversionBlocks(ctx, 1)
		require.NoError(t, err)
		require.Len(t, blocks, 1, "the base block is valid")

		for name, c := range map[string][5]int64{
			"seq below 0":         {-1, 100, 100, 2, 1},
			"seq above 2^32-1":    {tooWide, 100, 100, 2, 1},
			"block below 0":       {0, -1, 100, 2, 1},
			"block above 2^32-1":  {0, tooWide, 100, 2, 1},
			"height below 0":      {0, 100, -1, 2, 1},
			"height above 2^32-1": {0, 100, tooWide, 2, 1},
			"negative first":      {0, 100, 100, -1, 1},
			"negative count":      {0, 100, 100, 2, -1},
		} {
			block(c[0], c[1], c[2], c[3], c[4])

			_, err := s.LaneConversionBlocks(ctx, 1)
			require.ErrorContains(t, err, "is malformed", name)
		}
	})
}

// A segment or completion insert that the database refuses is reported, and nothing is recorded.
// A SQLite trigger stands in for the refusal, such as a role without INSERT.
func TestLaneConversionInsertsReportARefusedInsert(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	ctx := t.Context()

	_, err = s.db.ExecContext(ctx, `CREATE TRIGGER refuse_segment BEFORE INSERT ON lane_conversion_segment BEGIN SELECT RAISE(ABORT, 'refused'); END`)
	require.NoError(t, err)

	require.Error(t, s.InsertLaneConversionSegment(ctx, testSegment(1, 2, 4, 4)))

	segs, err := s.LaneConversionSegments(ctx)
	require.NoError(t, err)
	require.Empty(t, segs)

	_, err = s.db.ExecContext(ctx, `DROP TRIGGER refuse_segment`)
	require.NoError(t, err)

	require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(1, 2, 4, 4)))
	require.NoError(t, s.InsertLaneConversionBlocks(ctx, 1, []lane.ConversionBlock{{Seq: 0, BlockID: 100, Height: 100, First: 2, Count: 3}}))

	_, err = s.db.ExecContext(ctx, `CREATE TRIGGER refuse_complete BEFORE INSERT ON lane_conversion_complete BEGIN SELECT RAISE(ABORT, 'refused'); END`)
	require.NoError(t, err)

	require.Error(t, s.CompleteLaneConversionSegment(ctx, 1, chainhash.Hash{1}))

	complete, err := s.LaneConversionComplete(ctx, 1)
	require.NoError(t, err)
	require.False(t, complete)
}

// A new segment appends: an ID below a recorded one, or positions not above every recorded
// segment's, is refused even when nothing overlaps. Recording a segment again is a no-op, and an
// invalid segment is refused before anything is stored.
func TestLaneConversionSegmentsAppend(t *testing.T) {
	runLaneHandoutTests(t, "append", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(5, 100, 110, 120)))
		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(5, 100, 110, 120)), "the same segment again")

		require.Error(t, s.InsertLaneConversionSegment(ctx, testSegment(4, 200, 210, 220)), "a lower ID above the positions")
		require.Error(t, s.InsertLaneConversionSegment(ctx, testSegment(6, 10, 20, 30)), "a higher ID below the positions")
		require.Error(t, s.InsertLaneConversionSegment(ctx, testSegment(6, 120, 125, 130)), "starting at the last position")

		require.Error(t, s.InsertLaneConversionSegment(ctx, testSegment(0, 200, 210, 220)), "segment ID 0")
		require.Error(t, s.InsertLaneConversionSegment(ctx, testSegment(7, 300, 310, 305)), "prefix past the last position")

		require.NoError(t, s.InsertLaneConversionSegment(ctx, testSegment(6, 121, 125, 130)))

		segs, err := s.LaneConversionSegments(ctx)
		require.NoError(t, err)
		require.Equal(t, []lane.ConversionSegment{testSegment(5, 100, 110, 120), testSegment(6, 121, 125, 130)}, segs)
	})
}

// Every lane_map index the reader needs exists on both engines, by its own name.
func TestLaneMapLaneLastIndexExists(t *testing.T) {
	runLaneHandoutTests(t, "index", func(t *testing.T, s *SQL) {
		var n int

		if s.engine == util.Postgres {
			require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM pg_indexes WHERE tablename = 'lane_map' AND indexname = 'lane_map_lane_last' AND indexdef LIKE '%(lane, last)%'`).Scan(&n))
		} else {
			require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND tbl_name = 'lane_map' AND name = 'lane_map_lane_last'`).Scan(&n))
		}

		require.Equal(t, 1, n)
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

	// The holder writes its own row for block 77 under the lock.
	_, err = holder.ExecContext(ctx, `INSERT INTO lane_map (block_id, lane, first, last, first_hole, tag, hole_data) VALUES (77, 1, 1, 10, 0, 0, NULL)`)
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		done <- s.InsertLaneMapRows(ctx, 77, []lane.MapRow{{Lane: 2, First: 1, Last: 10, Tag: lane.RowTagRange}})
	}()

	select {
	case err = <-done:
		require.Failf(t, "the insert did not wait for the block lock", "returned %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	// Another block's rows are not held back.
	require.NoError(t, s.InsertLaneMapRows(ctx, 78, []lane.MapRow{{Lane: 1, First: 1, Last: 10, Tag: lane.RowTagRange}}))

	// Once the holder commits, the waiting writer reads what it wrote and refuses its own,
	// different row instead of adding it beside the holder's.
	require.NoError(t, holder.Commit())
	require.Error(t, <-done)

	got, err := s.LaneMapRows(ctx, 77)
	require.NoError(t, err)
	require.Equal(t, []lane.MapRow{{Lane: 1, First: 1, Last: 10, Tag: lane.RowTagRange}}, got)
}

// The segment tables are written under one lock, taken before the writer reads them: on Postgres
// the segment advisory lock, on SQLite laneWriteMu. Held here while the holder records segment 1,
// the lock keeps a second writer from reading too early and recording segment 2 over the same
// positions; once the holder is done the writer sees segment 1 and refuses.
func TestLaneConversionSegmentWaitsForTheSegmentLock(t *testing.T) {
	runLaneHandoutTests(t, "segment lock", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		const insertSegment1 = `INSERT INTO lane_conversion_segment (segment, first, prefix_last, last, source_tip_block_id, source_tip_hash, manifest_digest, created_at)
			VALUES (1, 100, 110, 120, 900000, $1, $2, CURRENT_TIMESTAMP)`

		seg1 := testSegment(1, 100, 110, 120)

		var (
			release func() error
			insert  func() error
		)

		if s.engine == util.Postgres {
			holder, err := s.db.BeginTx(ctx, nil)
			require.NoError(t, err)

			defer func() { _ = holder.Rollback() }()

			_, err = holder.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, laneSegmentLockClass, int32(0))
			require.NoError(t, err)

			insert = func() error {
				_, err := holder.ExecContext(ctx, insertSegment1, seg1.SourceTipHash[:], seg1.ManifestDigest[:])
				return err
			}
			release = holder.Commit
		} else {
			laneWriteMu.Lock()

			var unlockOnce sync.Once
			unlock := func() { unlockOnce.Do(laneWriteMu.Unlock) }
			t.Cleanup(unlock) // a failed step below must not leave the package-wide mutex held

			insert = func() error {
				_, err := s.db.ExecContext(ctx, insertSegment1, seg1.SourceTipHash[:], seg1.ManifestDigest[:])
				return err
			}
			release = func() error {
				unlock()
				return nil
			}
		}

		done := make(chan error, 1)

		go func() {
			done <- s.InsertLaneConversionSegment(ctx, testSegment(2, 115, 118, 125))
		}()

		select {
		case err := <-done:
			require.Failf(t, "the segment insert did not wait for the segment lock", "returned %v", err)
		case <-time.After(500 * time.Millisecond):
		}

		require.NoError(t, insert())
		require.NoError(t, release())
		require.Error(t, <-done, "segment 2 overlaps segment 1, which the holder recorded")

		segs, err := s.LaneConversionSegments(ctx)
		require.NoError(t, err)
		require.Equal(t, []lane.ConversionSegment{seg1}, segs)
	})
}

// On Postgres hole_data is stored out of line with no compression attempt, on a new database and
// on one whose lane_map an earlier build created with the default storage.
func TestLaneMapHoleDataIsStoredExternal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	s, err := New(ulogger.TestLogger{}, dbURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	storage := func() string {
		var st string
		require.NoError(t, db.QueryRow(`SELECT attstorage::text FROM pg_attribute WHERE attrelid = 'lane_map'::regclass AND attname = 'hole_data'`).Scan(&st))

		return st
	}

	require.Equal(t, "e", storage(), "a new lane_map")

	_, err = db.Exec(`ALTER TABLE lane_map ALTER COLUMN hole_data SET STORAGE EXTENDED`)
	require.NoError(t, err)
	require.Equal(t, "x", storage())

	require.NoError(t, createPostgresSchema(ulogger.TestLogger{}, db, true))
	require.Equal(t, "e", storage(), "an earlier build's lane_map")
}

// The storage setting is a performance setting and never stops startup. A session holding
// lane_map makes startup wait at most laneMapStorageLockTimeout, then start with the default
// storage; a role that does not own lane_map starts too.
func TestLaneMapHoleDataStorageNeverBlocksStartup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	s, err := New(ulogger.TestLogger{}, dbURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	storage := func() string {
		var st string
		require.NoError(t, db.QueryRow(`SELECT attstorage::text FROM pg_attribute WHERE attrelid = 'lane_map'::regclass AND attname = 'hole_data'`).Scan(&st))

		return st
	}

	_, err = db.Exec(`ALTER TABLE lane_map ALTER COLUMN hole_data SET STORAGE EXTENDED`)
	require.NoError(t, err)

	holder, err := db.Begin()
	require.NoError(t, err)

	_, err = holder.Exec(`LOCK TABLE lane_map IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)

	// The wait is the 2s the godoc and the PR state. The storage step alone is timed, so the
	// bound is tight: a raised timeout fails here even where startup as a whole stays quick.
	require.Equal(t, 2*time.Second, laneMapStorageLockTimeout)

	started := time.Now()
	setLaneMapHoleDataStorage(ulogger.TestLogger{}, db)
	waited := time.Since(started)
	require.GreaterOrEqual(t, waited, laneMapStorageLockTimeout-100*time.Millisecond, "it waits for the lock, up to the timeout")
	require.Less(t, waited, laneMapStorageLockTimeout+time.Second, "then gives up, not waiting for the holder")

	started = time.Now()
	require.NoError(t, createPostgresSchema(ulogger.TestLogger{}, db, true), "a held lane_map lock does not fail startup")
	require.Less(t, time.Since(started), laneMapStorageLockTimeout+10*time.Second, "startup waits about the lock timeout, not for the holder")
	require.NoError(t, holder.Rollback())
	require.Equal(t, "x", storage(), "the default storage stays while the lock was held")

	// A role that does not own lane_map, on a schema its owner created with the default storage.
	for _, stmt := range []string{
		`CREATE ROLE lane_storage_node LOGIN PASSWORD 'lane_storage_node'`,
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
		`GRANT USAGE ON SCHEMA public TO lane_storage_node`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO lane_storage_node`,
	} {
		_, err = db.Exec(stmt)
		require.NoError(t, err, stmt)
	}

	nodeURL := *dbURL
	nodeURL.User = url.UserPassword("lane_storage_node", "lane_storage_node")

	nodeDB, err := usql.Open("postgres", nodeURL.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = nodeDB.Close() })

	require.NoError(t, createPostgresSchema(ulogger.TestLogger{}, nodeDB, true), "a non-owner starts with the default storage")
	require.Equal(t, "x", storage())

	// The owner's next start applies it.
	require.NoError(t, createPostgresSchema(ulogger.TestLogger{}, db, true))
	require.Equal(t, "e", storage())

	// Once the storage is set, a start runs no DDL on lane_map: the catalog check returns first, so
	// a session holding lane_map costs it nothing. The ALTER would take its lock before looking at
	// the current value, and wait out the whole timeout here.
	held, err := db.Begin()
	require.NoError(t, err)

	_, err = held.Exec(`LOCK TABLE lane_map IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)

	started = time.Now()
	setLaneMapHoleDataStorage(ulogger.TestLogger{}, db)
	require.Less(t, time.Since(started), 500*time.Millisecond, "a current schema does not wait for the lane_map lock")
	require.NoError(t, held.Rollback())
	require.Equal(t, "e", storage())
}

// The literal lock_timeout statement names the same wait as laneMapStorageLockTimeout.
func TestLaneMapStorageLockTimeoutSQLMatchesTheConstant(t *testing.T) {
	require.Equal(t, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", laneMapStorageLockTimeout.Milliseconds()), laneMapStorageLockTimeoutSQL)
}
