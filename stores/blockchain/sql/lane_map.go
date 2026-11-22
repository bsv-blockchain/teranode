package sql

import (
	"bytes"
	"context"
	"database/sql"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The map and its neighbours, in the blockchain database.
//
// lane_map holds the map rows: per block, one row per lane the block took positions from, split
// over whole chunks when a row's hole data would pass 64 MB. On Postgres startup asks for
// hole_data STORAGE EXTERNAL, so hole data large enough to leave the row is stored out of line
// without a compression attempt; small hole data stays inline. See setLaneMapHoleDataStorage. Positions are the seven-byte position
// within the lane. Rows are insert-only and kept for ever, including after the block is
// invalidated; nothing references blocks by foreign key, so creating these tables takes no lock on
// blocks. The index on (lane, last) serves the reader query for rows ending above a lane floor. It
// is an index of the blockchain database, not of a UTXO store.
//
// lane_floor holds the per-lane floors: below floor every position is settled on chains holding
// the anchor block, except the listed positions, a portable 64-bit Roaring bitmap (NULL when none).
// lane_gap holds the stretches a restore procedure jumps over. Both are insert-only. Their writers
// come later; this build installs them so the schema exists before any historical numbering.
//
// lane_conversion_segment, lane_conversion_block and lane_conversion_complete record the
// historical conversion: each segment's bounds and frozen source, its main-chain prefix block by
// block with original heights, and its completion once every audit passed. All insert-only.
//
// The tables are created with the lane lease tables, behind the same catalog check, so a node role
// without CREATE starts on a schema its owner installed. Creating a new table takes no lock on an
// existing one.

var laneMapPostgresTables = []struct{ name, ddl string }{
	{"lane_map", `
		CREATE TABLE IF NOT EXISTS lane_map (
			block_id   BIGINT   NOT NULL,
			lane       SMALLINT NOT NULL,
			first      BIGINT   NOT NULL,
			last       BIGINT   NOT NULL,
			first_hole BIGINT   NOT NULL,
			tag        SMALLINT NOT NULL,
			hole_data  BYTEA,
			PRIMARY KEY (block_id, lane, first)
		);`},
	{"lane_map_lane_last", `CREATE INDEX IF NOT EXISTS lane_map_lane_last ON lane_map (lane, last);`},
	{"lane_floor", `
		CREATE TABLE IF NOT EXISTS lane_floor (
			lane            SMALLINT    NOT NULL,
			anchor_block_id BIGINT      NOT NULL,
			floor           BIGINT      NOT NULL,
			listed          BYTEA,
			written_at      TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (lane, anchor_block_id)
		);`},
	{"lane_gap", `
		CREATE TABLE IF NOT EXISTS lane_gap (
			lane        SMALLINT    NOT NULL,
			first       BIGINT      NOT NULL,
			last        BIGINT      NOT NULL,
			backup_time TIMESTAMPTZ NOT NULL,
			recorded_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (lane, first)
		);`},
	{"lane_conversion_segment", `
		CREATE TABLE IF NOT EXISTS lane_conversion_segment (
			segment             BIGINT      PRIMARY KEY,
			first               BIGINT      NOT NULL,
			prefix_last         BIGINT      NOT NULL,
			last                BIGINT      NOT NULL,
			source_tip_block_id BIGINT      NOT NULL,
			source_tip_hash     BYTEA       NOT NULL,
			manifest_digest     BYTEA       NOT NULL,
			created_at          TIMESTAMPTZ NOT NULL
		);`},
	{"lane_conversion_block", `
		CREATE TABLE IF NOT EXISTS lane_conversion_block (
			segment  BIGINT NOT NULL,
			seq      BIGINT NOT NULL,
			block_id BIGINT NOT NULL,
			height   BIGINT NOT NULL,
			first    BIGINT NOT NULL,
			count    BIGINT NOT NULL,
			PRIMARY KEY (segment, seq)
		);`},
	{"lane_conversion_complete", `
		CREATE TABLE IF NOT EXISTS lane_conversion_complete (
			segment      BIGINT      PRIMARY KEY,
			audit_digest BYTEA       NOT NULL,
			completed_at TIMESTAMPTZ NOT NULL
		);`},
}

// Advisory lock classes that serialise lane map and conversion writes on Postgres, in the
// two-key form pg_advisory_xact_lock(class, key), so they cannot collide with the single-key
// hand-out lock. A retry racing the first insert then waits for it and finds its rows.
const (
	laneMapLockClass     int32 = 726_573
	laneSegmentLockClass int32 = 726_574
)

// laneWriteMu does the same on SQLite, where one process owns the file.
var laneWriteMu sync.Mutex

// lockLaneProcess holds laneWriteMu on SQLite and returns its release; on Postgres the advisory
// lock does the job, per block, and this does nothing.
func (s *SQL) lockLaneProcess() func() {
	if s.engine == util.Postgres {
		return func() {}
	}

	laneWriteMu.Lock()

	return laneWriteMu.Unlock
}

// lockLaneWrite takes the lock for one block's rows or for the segment tables, for the rest of tx
// on Postgres. On SQLite the caller holds laneWriteMu instead.
func (s *SQL) lockLaneWrite(ctx context.Context, tx *sql.Tx, class int32, key uint32) error {
	if s.engine != util.Postgres {
		return nil
	}

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, class, int32(key)); err != nil { //nolint:gosec // the key is a block ID, bit for bit
		return errors.NewStorageError("take the lane write lock", err)
	}

	return nil
}

// laneMapSqliteDDL is the same schema for SQLite.
const laneMapSqliteDDL = `
	CREATE TABLE IF NOT EXISTS lane_map (
		block_id   INTEGER NOT NULL,
		lane       INTEGER NOT NULL,
		first      INTEGER NOT NULL,
		last       INTEGER NOT NULL,
		first_hole INTEGER NOT NULL,
		tag        INTEGER NOT NULL,
		hole_data  BLOB,
		PRIMARY KEY (block_id, lane, first)
	);
	CREATE INDEX IF NOT EXISTS lane_map_lane_last ON lane_map (lane, last);
	CREATE TABLE IF NOT EXISTS lane_floor (
		lane            INTEGER   NOT NULL,
		anchor_block_id INTEGER   NOT NULL,
		floor           INTEGER   NOT NULL,
		listed          BLOB,
		written_at      TIMESTAMP NOT NULL,
		PRIMARY KEY (lane, anchor_block_id)
	);
	CREATE TABLE IF NOT EXISTS lane_gap (
		lane        INTEGER   NOT NULL,
		first       INTEGER   NOT NULL,
		last        INTEGER   NOT NULL,
		backup_time TIMESTAMP NOT NULL,
		recorded_at TIMESTAMP NOT NULL,
		PRIMARY KEY (lane, first)
	);
	CREATE TABLE IF NOT EXISTS lane_conversion_segment (
		segment             INTEGER   PRIMARY KEY,
		first               INTEGER   NOT NULL,
		prefix_last         INTEGER   NOT NULL,
		last                INTEGER   NOT NULL,
		source_tip_block_id INTEGER   NOT NULL,
		source_tip_hash     BLOB      NOT NULL,
		manifest_digest     BLOB      NOT NULL,
		created_at          TIMESTAMP NOT NULL
	);
	CREATE TABLE IF NOT EXISTS lane_conversion_block (
		segment  INTEGER NOT NULL,
		seq      INTEGER NOT NULL,
		block_id INTEGER NOT NULL,
		height   INTEGER NOT NULL,
		first    INTEGER NOT NULL,
		count    INTEGER NOT NULL,
		PRIMARY KEY (segment, seq)
	);
	CREATE TABLE IF NOT EXISTS lane_conversion_complete (
		segment      INTEGER   PRIMARY KEY,
		audit_digest BLOB      NOT NULL,
		completed_at TIMESTAMP NOT NULL
	);
`

// The two timestamped inserts are written out whole, once per engine, and each call site passes
// the constant itself, so no query text is built or chosen through a helper at run time: each
// reads the database clock in the form that engine stores.
const (
	insertConversionSegmentPostgres = `INSERT INTO lane_conversion_segment (segment, first, prefix_last, last, source_tip_block_id, source_tip_hash, manifest_digest, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, clock_timestamp())`
	insertConversionSegmentSQLite = `INSERT INTO lane_conversion_segment (segment, first, prefix_last, last, source_tip_block_id, source_tip_hash, manifest_digest, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, strftime('%Y-%m-%d %H:%M:%f', 'now'))`

	insertConversionCompletePostgres = `INSERT INTO lane_conversion_complete (segment, audit_digest, completed_at) VALUES ($1, $2, clock_timestamp())`
	insertConversionCompleteSQLite   = `INSERT INTO lane_conversion_complete (segment, audit_digest, completed_at) VALUES ($1, $2, strftime('%Y-%m-%d %H:%M:%f', 'now'))`
)

// InsertLaneMapRows records the map rows of one block. Rows are insert-only: the first call for a
// block inserts all of them in one transaction; a retry with rows that say the same thing, by
// lane.SameRow, succeeds and writes nothing; a call whose rows differ from what is stored, by one
// row or by the set of rows, fails and writes nothing. The rows are checked with
// lane.CheckMapRows first. A block with no rows records nothing, so a later call with rows for it
// is a first insert: a live block always has rows, at least its coinbase's, and a historical block
// with none has no readable transaction to place.
func (s *SQL) InsertLaneMapRows(ctx context.Context, blockID uint32, rows []lane.MapRow) error {
	if err := lane.CheckMapRows(rows); err != nil {
		return errors.NewProcessingError("block %d", blockID, err)
	}

	defer s.lockLaneProcess()()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewStorageError("begin lane map insert for block %d", blockID, err)
	}

	defer func() { _ = tx.Rollback() }()

	if err = s.insertLaneMapRowsTx(ctx, tx, blockID, rows); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return errors.NewStorageError("commit lane map rows of block %d", blockID, err)
	}

	return nil
}

// insertLaneMapRowsTx is InsertLaneMapRows inside a caller's transaction, so the block gate can
// commit a block's rows with the block itself. On Postgres it takes the block's lane write lock
// first, held to the end of tx, so two writers of one block never both see no rows and insert
// different ones. On SQLite the lock is laneWriteMu, which only the caller can hold for the whole
// transaction: the caller takes s.lockLaneProcess() before beginning tx and releases it after the
// commit, as InsertLaneMapRows and StoreBlock do. StoreBlock calls it through
// insertLaneMapRowsWithBlock, under a savepoint in the block's own transaction.
func (s *SQL) insertLaneMapRowsTx(ctx context.Context, tx *sql.Tx, blockID uint32, rows []lane.MapRow) error {
	if err := s.lockLaneWrite(ctx, tx, laneMapLockClass, blockID); err != nil {
		return err
	}

	existing, err := queryLaneMapRows(ctx, tx, `SELECT block_id, lane, first, last, first_hole, tag, hole_data FROM lane_map WHERE block_id = $1 ORDER BY lane, first`, int64(blockID))
	if err != nil {
		return err
	}

	if len(existing) > 0 {
		return sameRows(blockID, existing, rows)
	}

	for _, r := range rows {
		var holeData any
		if r.HoleData != nil {
			holeData = r.HoleData
		}

		if _, err = tx.ExecContext(ctx,
			`INSERT INTO lane_map (block_id, lane, first, last, first_hole, tag, hole_data) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			int64(blockID), int(r.Lane), positionArg(r.First), positionArg(r.Last), positionArg(r.FirstHole), int(r.Tag), holeData,
		); err != nil {
			return errors.NewStorageError("insert lane %d map row %d to %d of block %d", r.Lane, r.First, r.Last, blockID, err)
		}
	}

	return nil
}

// sameRows compares a retry with the rows already stored for a block.
func sameRows(blockID uint32, stored []lane.BlockMapRow, rows []lane.MapRow) error {
	sorted := append([]lane.MapRow(nil), rows...)
	sortRows(sorted)

	if len(stored) != len(sorted) {
		return errors.NewProcessingError("block %d already has %d lane map rows, not the %d given", blockID, len(stored), len(sorted))
	}

	for i := range sorted {
		same, err := lane.SameRow(stored[i].MapRow, sorted[i])
		if err != nil {
			return errors.NewProcessingError("block %d lane map row %d", blockID, i, err)
		}

		if !same {
			return errors.NewProcessingError("block %d already has a different lane %d map row starting %d", blockID, stored[i].Lane, stored[i].First)
		}
	}

	return nil
}

func sortRows(rows []lane.MapRow) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && (rows[j].Lane < rows[j-1].Lane || rows[j].Lane == rows[j-1].Lane && rows[j].First < rows[j-1].First); j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}

// LaneMapRows returns one block's map rows by lane and first position, each checked.
func (s *SQL) LaneMapRows(ctx context.Context, blockID uint32) ([]lane.MapRow, error) {
	got, err := queryLaneMapRows(ctx, s.db, `SELECT block_id, lane, first, last, first_hole, tag, hole_data FROM lane_map WHERE block_id = $1 ORDER BY lane, first`, int64(blockID))
	if err != nil {
		return nil, err
	}

	rows := make([]lane.MapRow, len(got))
	for i := range got {
		rows[i] = got[i].MapRow
	}

	return rows, nil
}

// LaneMapRowsEndingAbove returns every map row of a lane whose range ends above a position,
// from every block, by block and first position, each checked. A reader loads these with a floor:
// rows below the floor's position that a late-mined transaction left are still needed.
func (s *SQL) LaneMapRowsEndingAbove(ctx context.Context, laneNum uint8, above uint64) ([]lane.BlockMapRow, error) {
	if above > lane.MaxPosition {
		return nil, errors.NewProcessingError("position %d is wider than seven bytes", above)
	}

	return queryLaneMapRows(ctx, s.db, `SELECT block_id, lane, first, last, first_hole, tag, hole_data FROM lane_map WHERE lane = $1 AND last > $2 ORDER BY block_id, first`, int(laneNum), positionArg(above))
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func queryLaneMapRows(ctx context.Context, q queryer, query string, args ...any) ([]lane.BlockMapRow, error) {
	res, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.NewStorageError("query lane map rows", err)
	}

	defer func() { _ = res.Close() }()

	var out []lane.BlockMapRow

	for res.Next() {
		var (
			blockID, first, last, firstHole int64
			laneNum, tag                    int
			holeData                        []byte
		)

		if err = res.Scan(&blockID, &laneNum, &first, &last, &firstHole, &tag, &holeData); err != nil {
			return nil, errors.NewStorageError("scan lane map row", err)
		}

		if blockID < 0 || blockID > int64(^uint32(0)) || laneNum < 0 || laneNum > lane.MaxLane || tag < 0 || tag > 255 || first < 0 || last < 0 || firstHole < 0 {
			return nil, errors.NewProcessingError("lane map row of block %d lane %d holds out-of-range values", blockID, laneNum)
		}

		row := lane.BlockMapRow{
			BlockID: uint32(blockID),
			MapRow: lane.MapRow{
				Lane:      uint8(laneNum),
				First:     uint64(first),
				Last:      uint64(last),
				FirstHole: uint64(firstHole),
				Tag:       lane.RowTag(tag),
				HoleData:  holeData,
			},
		}

		if len(row.HoleData) == 0 {
			row.HoleData = nil
		}

		if _, err = row.Check(); err != nil {
			return nil, errors.NewProcessingError("stored lane map row of block %d", blockID, err)
		}

		out = append(out, row)
	}

	if err = res.Err(); err != nil {
		return nil, errors.NewStorageError("read lane map rows", err)
	}

	return out, nil
}

// positionArg is a seven-byte position as the signed integer the database keeps. It is never
// negative: positions are below 2^56.
func positionArg(p uint64) int64 {
	return int64(p) //nolint:gosec // positions are checked to be below 2^56
}

// InsertLaneConversionSegment records a conversion segment's bounds and source. It is
// insert-only: recording the same segment again is a no-op, a different segment under an ID
// already used fails, and a new segment must append, with an ID above every recorded segment's and
// a first position above every recorded segment's last.
func (s *SQL) InsertLaneConversionSegment(ctx context.Context, seg lane.ConversionSegment) error {
	if err := seg.Check(); err != nil {
		return err
	}

	defer s.lockLaneProcess()()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewStorageError("begin conversion segment %d", seg.ID, err)
	}

	defer func() { _ = tx.Rollback() }()

	if err = s.lockLaneWrite(ctx, tx, laneSegmentLockClass, 0); err != nil {
		return err
	}

	segments, err := querySegments(ctx, tx)
	if err != nil {
		return err
	}

	for _, other := range segments {
		if other.ID == seg.ID {
			if other != seg {
				return errors.NewProcessingError("conversion segment %d is already recorded differently", seg.ID)
			}

			return nil
		}
	}

	// A new segment appends: its ID and its positions are above every recorded segment's.
	for _, other := range segments {
		if seg.ID < other.ID {
			return errors.NewProcessingError("conversion segment %d is below the recorded segment %d; a new segment appends above", seg.ID, other.ID)
		}

		if seg.First <= other.Last {
			return errors.NewProcessingError("conversion segment %d starts at %d, not above segment %d's last position %d", seg.ID, seg.First, other.ID, other.Last)
		}
	}

	args := []any{int64(seg.ID), positionArg(seg.First), positionArg(seg.PrefixLast), positionArg(seg.Last), int64(seg.SourceTipBlockID), seg.SourceTipHash[:], seg.ManifestDigest[:]}

	if s.engine == util.Postgres {
		_, err = tx.ExecContext(ctx, insertConversionSegmentPostgres, args...)
	} else {
		_, err = tx.ExecContext(ctx, insertConversionSegmentSQLite, args...)
	}

	if err != nil {
		return errors.NewStorageError("insert conversion segment %d", seg.ID, err)
	}

	if err = tx.Commit(); err != nil {
		return errors.NewStorageError("commit conversion segment %d", seg.ID, err)
	}

	return nil
}

// LaneConversionSegments returns every recorded segment by ID.
func (s *SQL) LaneConversionSegments(ctx context.Context) ([]lane.ConversionSegment, error) {
	return querySegments(ctx, s.db)
}

func querySegments(ctx context.Context, q queryer) ([]lane.ConversionSegment, error) {
	res, err := q.QueryContext(ctx, `SELECT segment, first, prefix_last, last, source_tip_block_id, source_tip_hash, manifest_digest FROM lane_conversion_segment ORDER BY segment`)
	if err != nil {
		return nil, errors.NewStorageError("query conversion segments", err)
	}

	defer func() { _ = res.Close() }()

	var out []lane.ConversionSegment

	for res.Next() {
		var (
			id, first, prefixLast, last, tip int64
			tipHash, digest                  []byte
		)

		if err = res.Scan(&id, &first, &prefixLast, &last, &tip, &tipHash, &digest); err != nil {
			return nil, errors.NewStorageError("scan conversion segment", err)
		}

		if len(tipHash) != chainhash.HashSize || len(digest) != chainhash.HashSize || id <= 0 || id > int64(^uint32(0)) || tip < 0 || tip > int64(^uint32(0)) {
			return nil, errors.NewProcessingError("stored conversion segment %d is malformed", id)
		}

		seg := lane.ConversionSegment{
			ID:               uint32(id),
			First:            uint64(first),      //nolint:gosec // checked by seg.Check
			PrefixLast:       uint64(prefixLast), //nolint:gosec // checked by seg.Check
			Last:             uint64(last),       //nolint:gosec // checked by seg.Check
			SourceTipBlockID: uint32(tip),
		}
		copy(seg.SourceTipHash[:], tipHash)
		copy(seg.ManifestDigest[:], digest)

		if err = seg.Check(); err != nil {
			return nil, errors.NewProcessingError("stored conversion segment", err)
		}

		out = append(out, seg)
	}

	if err = res.Err(); err != nil {
		return nil, errors.NewStorageError("read conversion segments", err)
	}

	return out, nil
}

// InsertLaneConversionBlocks records part of a segment's prefix block list, in order. A resumed
// conversion calls it again from where it stopped: a block already recorded must match exactly,
// and a new block must continue the stored list without a gap: the next sequence number, the next
// position, a height above the block before it and not past the prefix end. It fails for an
// unknown or completed segment. It reads only the last stored block and the stored blocks from the
// lowest to the highest sequence number the call repeats, so recording a long segment a few blocks
// at a time costs what each call holds. Each new block is checked against the block before it;
// that a block ID is not listed twice anywhere else, within a call or across calls, is checked
// once, over the whole list, by CompleteLaneConversionSegment.
func (s *SQL) InsertLaneConversionBlocks(ctx context.Context, segment uint32, blocks []lane.ConversionBlock) error {
	defer s.lockLaneProcess()()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewStorageError("begin conversion blocks of segment %d", segment, err)
	}

	defer func() { _ = tx.Rollback() }()

	if err = s.lockLaneWrite(ctx, tx, laneSegmentLockClass, 0); err != nil {
		return err
	}

	seg, complete, err := segmentState(ctx, tx, segment)
	if err != nil {
		return err
	}

	if complete {
		return errors.NewProcessingError("conversion segment %d is complete; its blocks are fixed", segment)
	}

	last, haveLast, err := lastConversionBlock(ctx, tx, segment)
	if err != nil {
		return err
	}

	// stored is how many blocks are recorded: sequence numbers are 0 to stored-1, with no gap.
	stored := uint64(0)
	if haveLast {
		stored = uint64(last.Seq) + 1
	}

	repeatLo, repeatHi, repeats := uint32(0), uint32(0), false

	var fresh []lane.ConversionBlock

	for _, b := range blocks {
		switch {
		case uint64(b.Seq) < stored:
			if !repeats || b.Seq < repeatLo {
				repeatLo = b.Seq
			}

			if !repeats || b.Seq > repeatHi {
				repeatHi = b.Seq
			}

			repeats = true
		case uint64(b.Seq) == stored+uint64(len(fresh)):
			fresh = append(fresh, b)
		default:
			return errors.NewProcessingError("conversion segment %d block %d does not follow block %d", segment, b.Seq, int64(stored)+int64(len(fresh))-1) //nolint:gosec // a segment has far fewer than 2^63 blocks
		}
	}

	if repeats {
		recorded, err := queryConversionBlockRange(ctx, tx, segment, repeatLo, repeatHi)
		if err != nil {
			return err
		}

		for _, b := range blocks {
			if uint64(b.Seq) >= stored {
				continue
			}

			if r, ok := recorded[b.Seq]; !ok || r != b {
				return errors.NewProcessingError("conversion segment %d block %d is already recorded differently", segment, b.Seq)
			}
		}
	}

	var prev *lane.ConversionBlock
	if haveLast {
		prev = &last
	}

	if err = checkBlocksFollow(seg, prev, fresh); err != nil {
		return err
	}

	for _, b := range fresh {
		if _, err = tx.ExecContext(ctx,
			`INSERT INTO lane_conversion_block (segment, seq, block_id, height, first, count) VALUES ($1, $2, $3, $4, $5, $6)`,
			int64(segment), int64(b.Seq), int64(b.BlockID), int64(b.Height), positionArg(b.First), positionArg(b.Count),
		); err != nil {
			return errors.NewStorageError("insert conversion segment %d block %d", segment, b.Seq, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return errors.NewStorageError("commit conversion blocks of segment %d", segment, err)
	}

	return nil
}

// checkBlocksFollow checks blocks that continue a segment's stored list after prev, the last stored
// block, or from the start when prev is nil: each starts where the one before it ends, rises in
// height, is not the block before it, and none runs past the prefix end. The sequence numbers are
// checked by the caller.
func checkBlocksFollow(seg lane.ConversionSegment, prev *lane.ConversionBlock, blocks []lane.ConversionBlock) error {
	next := seg.First
	if prev != nil {
		next = prev.First + prev.Count
	}

	if next > seg.PrefixLast+1 {
		return errors.NewProcessingError("conversion segment %d: stored blocks end at %d, past the prefix end %d", seg.ID, next-1, seg.PrefixLast)
	}

	for i := range blocks {
		b := blocks[i]

		if prev != nil {
			if b.Height <= prev.Height {
				return errors.NewProcessingError("conversion segment %d: block %d at height %d does not rise above %d", seg.ID, b.Seq, b.Height, prev.Height)
			}

			if b.BlockID == prev.BlockID {
				return errors.NewProcessingError("conversion segment %d: block %d repeats block ID %d", seg.ID, b.Seq, b.BlockID)
			}
		}

		if b.First != next {
			return errors.NewProcessingError("conversion segment %d: block %d starts at %d, want %d", seg.ID, b.Seq, b.First, next)
		}

		if b.Count > seg.PrefixLast+1-next {
			return errors.NewProcessingError("conversion segment %d: block %d's %d positions from %d run past the prefix end %d", seg.ID, b.Seq, b.Count, next, seg.PrefixLast)
		}

		next += b.Count
		prev = &blocks[i]
	}

	return nil
}

// lastConversionBlock returns a segment's last stored block, if it has any.
func lastConversionBlock(ctx context.Context, q queryer, segment uint32) (lane.ConversionBlock, bool, error) {
	res, err := q.QueryContext(ctx, `SELECT seq, block_id, height, first, count FROM lane_conversion_block WHERE segment = $1 ORDER BY seq DESC LIMIT 1`, int64(segment))
	if err != nil {
		return lane.ConversionBlock{}, false, errors.NewStorageError("query the last conversion block of segment %d", segment, err)
	}

	blocks, err := scanConversionBlocks(res, segment)
	if err != nil || len(blocks) == 0 {
		return lane.ConversionBlock{}, false, err
	}

	return blocks[0], true, nil
}

// queryConversionBlockRange returns a segment's stored blocks with sequence numbers lo to hi, by
// sequence number.
func queryConversionBlockRange(ctx context.Context, q queryer, segment, lo, hi uint32) (map[uint32]lane.ConversionBlock, error) {
	res, err := q.QueryContext(ctx, `SELECT seq, block_id, height, first, count FROM lane_conversion_block WHERE segment = $1 AND seq >= $2 AND seq <= $3 ORDER BY seq`, int64(segment), int64(lo), int64(hi))
	if err != nil {
		return nil, errors.NewStorageError("query conversion blocks %d to %d of segment %d", lo, hi, segment, err)
	}

	blocks, err := scanConversionBlocks(res, segment)
	if err != nil {
		return nil, err
	}

	out := make(map[uint32]lane.ConversionBlock, len(blocks))
	for _, b := range blocks {
		out[b.Seq] = b
	}

	return out, nil
}

// LaneConversionBlocks returns a segment's recorded prefix blocks in order.
func (s *SQL) LaneConversionBlocks(ctx context.Context, segment uint32) ([]lane.ConversionBlock, error) {
	return queryConversionBlocks(ctx, s.db, segment)
}

func queryConversionBlocks(ctx context.Context, q queryer, segment uint32) ([]lane.ConversionBlock, error) {
	res, err := q.QueryContext(ctx, `SELECT seq, block_id, height, first, count FROM lane_conversion_block WHERE segment = $1 ORDER BY seq`, int64(segment))
	if err != nil {
		return nil, errors.NewStorageError("query conversion blocks of segment %d", segment, err)
	}

	return scanConversionBlocks(res, segment)
}

// scanConversionBlocks reads conversion block rows and closes res.
func scanConversionBlocks(res *sql.Rows, segment uint32) ([]lane.ConversionBlock, error) {
	defer func() { _ = res.Close() }()

	var out []lane.ConversionBlock

	for res.Next() {
		var seq, blockID, height, first, count int64

		if err := res.Scan(&seq, &blockID, &height, &first, &count); err != nil {
			return nil, errors.NewStorageError("scan conversion block", err)
		}

		if seq < 0 || seq > int64(^uint32(0)) || blockID < 0 || blockID > int64(^uint32(0)) || height < 0 || height > int64(^uint32(0)) || first < 0 || count < 0 {
			return nil, errors.NewProcessingError("stored conversion block %d of segment %d is malformed", seq, segment)
		}

		out = append(out, lane.ConversionBlock{Seq: uint32(seq), BlockID: uint32(blockID), Height: uint32(height), First: uint64(first), Count: uint64(count)})
	}

	if err := res.Err(); err != nil {
		return nil, errors.NewStorageError("read conversion blocks of segment %d", segment, err)
	}

	return out, nil
}

// segmentState returns a segment and whether it is complete.
func segmentState(ctx context.Context, q queryer, segment uint32) (lane.ConversionSegment, bool, error) {
	segments, err := querySegments(ctx, q)
	if err != nil {
		return lane.ConversionSegment{}, false, err
	}

	for _, seg := range segments {
		if seg.ID != segment {
			continue
		}

		res, err := q.QueryContext(ctx, `SELECT 1 FROM lane_conversion_complete WHERE segment = $1`, int64(segment))
		if err != nil {
			return seg, false, errors.NewStorageError("query completion of segment %d", segment, err)
		}

		complete := res.Next()
		err = res.Err()
		_ = res.Close()

		if err != nil {
			return seg, false, errors.NewStorageError("read completion of segment %d", segment, err)
		}

		return seg, complete, nil
	}

	return lane.ConversionSegment{}, false, errors.NewProcessingError("conversion segment %d is not recorded", segment)
}

// CompleteLaneConversionSegment records that a segment passed every audit. It requires the
// segment's complete block list, tiling its prefix exactly; completing it again with the same
// digest is a no-op, with another digest an error. The conversion publishes this last.
func (s *SQL) CompleteLaneConversionSegment(ctx context.Context, segment uint32, auditDigest chainhash.Hash) error {
	defer s.lockLaneProcess()()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewStorageError("begin completion of segment %d", segment, err)
	}

	defer func() { _ = tx.Rollback() }()

	if err = s.lockLaneWrite(ctx, tx, laneSegmentLockClass, 0); err != nil {
		return err
	}

	seg, complete, err := segmentState(ctx, tx, segment)
	if err != nil {
		return err
	}

	if complete {
		var digest []byte
		if err = tx.QueryRowContext(ctx, `SELECT audit_digest FROM lane_conversion_complete WHERE segment = $1`, int64(segment)).Scan(&digest); err != nil {
			return errors.NewStorageError("read completion of segment %d", segment, err)
		}

		if !bytes.Equal(digest, auditDigest[:]) {
			return errors.NewProcessingError("conversion segment %d is already complete with another audit digest", segment)
		}

		return nil
	}

	blocks, err := queryConversionBlocks(ctx, tx, segment)
	if err != nil {
		return err
	}

	if err = lane.CheckConversionBlocks(seg, blocks); err != nil {
		return errors.NewProcessingError("conversion segment %d cannot complete", segment, err)
	}

	if s.engine == util.Postgres {
		_, err = tx.ExecContext(ctx, insertConversionCompletePostgres, int64(segment), auditDigest[:])
	} else {
		_, err = tx.ExecContext(ctx, insertConversionCompleteSQLite, int64(segment), auditDigest[:])
	}

	if err != nil {
		return errors.NewStorageError("insert completion of segment %d", segment, err)
	}

	if err = tx.Commit(); err != nil {
		return errors.NewStorageError("commit completion of segment %d", segment, err)
	}

	return nil
}

// LaneConversionComplete reports whether a segment is complete.
func (s *SQL) LaneConversionComplete(ctx context.Context, segment uint32) (bool, error) {
	_, complete, err := segmentState(ctx, s.db, segment)

	return complete, err
}
