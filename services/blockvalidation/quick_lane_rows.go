package blockvalidation

import (
	"context"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// Lane rows for a block stored by quick validation, at utxostore_laneMigration shadow (lanes
// Phase 1). Full validation builds a peer block's rows from the lane bitmaps subtree validation
// saved beside each subtree (laneRows). Quick validation runs no subtree validation, so it builds
// them from the records it writes itself. A record utxoset or Aerospike creates for the block comes
// back with the lane position it was written with; one that comes back without a valid position, as
// a SQL-store create does, is left out and counted. A record that already existed, written by an
// earlier attempt of the block or stored unmined before it, is read back for its position, because
// the position the store took for this attempt was burned, not written. The rows are the ones P2
// builds: one Roaring bitmap per lane, turned into map rows by lane.BuildBlockRows, with the
// coinbase left out, since block assembly creates it. A redone block reads back every record the
// first attempt wrote, so on utxoset and Aerospike it gets the same rows, and no record takes a
// second position.

// quickLanePendingMax is how many positions a block's collection holds unsorted, at 8 bytes each,
// before it adds them to its per-lane bitmaps. The bitmaps' own size depends on how the positions
// spread: those a block's creates take are near-consecutive runs, but a record read back keeps the
// position it was first written with, anywhere in any lane.
const quickLanePendingMax = 1 << 16

// quickLaneReadChunk is how many records one read of existing records' positions asks for, and
// quickLaneReadConcurrency how many such reads run at once across every block (quickLaneReadSlots).
const (
	quickLaneReadChunk       = 1024
	quickLaneReadConcurrency = 8
)

// quickLaneReadTimeout bounds a batch's read-backs of positions together: past it the block is
// stored without rows, whether or not the store has answered (readLanePositions). A variable so a
// test can shorten it.
var quickLaneReadTimeout = 30 * time.Second

// quickLanePositions collects the lane positions of one quick-validation attempt of one block. A
// nil collection, which is what an attempt at off has, collects nothing.
type quickLanePositions struct {
	mu sync.Mutex
	// pending are positions not yet added to lanes, by lane.
	pending  map[uint8][]uint64
	npending int
	lanes    lane.LaneBitmaps
	// unpositioned counts records stored with no valid position: written before shadow, or by a
	// store that returned none.
	unpositioned int
	// noRecord counts transactions the block stores no record for: those whose create is skipped
	// as unspendable (shouldSkipUnspendableCreate) and that were not stored before. They take no
	// position, and nothing spends them.
	noRecord int
	// failed is the first read of existing records that failed; the block then goes without rows.
	failed error
}

// quickLanePositionsKey is the context key of an attempt's collection. The collection travels on
// the context because an attempt reaches the store through three batch drivers and their stages,
// all of which derive their contexts from the attempt's, and a block retried is a new attempt with
// a new collection.
type quickLanePositionsKey struct{}

// withQuickLanePositions returns ctx carrying a new, empty collection for one quick-validation
// attempt, or ctx unchanged when lanes are off.
func (u *BlockValidation) withQuickLanePositions(ctx context.Context) context.Context {
	if u.settings == nil || !u.settings.UtxoStore.LaneMigration.Stamps() {
		return ctx
	}

	return context.WithValue(ctx, quickLanePositionsKey{}, &quickLanePositions{
		pending: map[uint8][]uint64{},
		lanes:   lane.LaneBitmaps{},
	})
}

// quickLanePositionsFrom is the collection ctx carries, or nil.
func quickLanePositionsFrom(ctx context.Context) *quickLanePositions {
	q, _ := ctx.Value(quickLanePositionsKey{}).(*quickLanePositions)

	return q
}

// add records the positions of records the block is stored with.
func (q *quickLanePositions) add(positions ...uint64) {
	if q == nil || len(positions) == 0 {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, v := range positions {
		q.addLocked(v)
	}
}

// addLocked records one position, adding the pending ones to the bitmaps at the bound.
func (q *quickLanePositions) addLocked(v uint64) {
	if !lane.Valid(v) {
		q.unpositioned++
		return
	}

	l, p := lane.Unpack(v)
	q.pending[l] = append(q.pending[l], p)
	q.npending++

	if q.npending >= quickLanePendingMax {
		q.flushLocked()
	}
}

// addCreated records the position of every record a list's results say this call created. A
// transaction reported existing is not one of them: the position taken for it was burned, so its
// record is read back instead (readExisting).
func (q *quickLanePositions) addCreated(results []utxo.SpendAndCreateMultiResult) {
	if q == nil {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, r := range results {
		if r.Status != utxo.MultiTxCreated {
			continue
		}

		position := lane.None
		if r.Meta != nil {
			position = r.Meta.LanePosition
		}

		q.addLocked(position)
	}
}

// addNoRecord counts n transactions the block is stored without a record for.
func (q *quickLanePositions) addNoRecord(n int) {
	if q == nil || n <= 0 {
		return
	}

	q.mu.Lock()
	q.noRecord += n
	q.mu.Unlock()
}

// fail records a read that failed, so the block is stored without rows.
func (q *quickLanePositions) fail(err error) {
	if q == nil {
		return
	}

	q.mu.Lock()
	if q.failed == nil {
		q.failed = err
	}
	q.mu.Unlock()
}

// hasFailed reports whether a read has failed.
func (q *quickLanePositions) hasFailed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.failed != nil
}

// flushLocked adds the pending positions to the per-lane bitmaps.
func (q *quickLanePositions) flushLocked() {
	for l, positions := range q.pending {
		bm := q.lanes[l]
		if bm == nil {
			bm = roaring64.New()
			q.lanes[l] = bm
		}

		bm.AddMany(positions)

		q.pending[l] = positions[:0]
	}

	q.npending = 0
}

// readExistingLanePositions reads back the lane position of each transaction in hashes, ones whose
// record already existed, and records it. A record not found, or one without a valid position,
// counts as unpositioned. A read that fails, whole or for one record, is recorded and the block goes
// without rows: at shadow nothing may stop a block.
func (u *BlockValidation) readExistingLanePositions(ctx context.Context, q *quickLanePositions, hashes []*chainhash.Hash) {
	u.readLanePositions(ctx, q, hashes, false)
}

// readSkippedLanePositions is readExistingLanePositions for transactions whose create was skipped as
// unspendable: one stored before, by an earlier attempt or while the skip was off, keeps its record
// and position, and one not found has no record (addNoRecord).
func (u *BlockValidation) readSkippedLanePositions(ctx context.Context, q *quickLanePositions, hashes []*chainhash.Hash) {
	u.readLanePositions(ctx, q, hashes, true)
}

// laneChunkRead is one chunk's read-back: the positions read, the records not found when those
// count as none, and the read's error.
type laneChunkRead struct {
	positions []uint64
	missing   int
	err       error
}

// readLanePositions reads hashes' positions in chunks of quickLaneReadChunk and returns by
// quickLaneReadTimeout, or when ctx ends, whether or not every read has. It does not wait for a
// read past the deadline, because the Aerospike client does not follow its context: such a read
// keeps its goroutine and its slot of the pool every block shares (quickLaneReadSlots) until the
// store answers, its answer is dropped, and the block goes without rows. notFoundIsNoRecord counts
// a record not found as none rather than as one without a position.
func (u *BlockValidation) readLanePositions(ctx context.Context, q *quickLanePositions, hashes []*chainhash.Hash, notFoundIsNoRecord bool) {
	// Once a read has failed the block goes without rows, so nothing more is read for it.
	if q == nil || len(hashes) == 0 || q.hasFailed() {
		return
	}

	readCtx, cancel := context.WithTimeout(ctx, quickLaneReadTimeout)
	defer cancel()

	chunks := (len(hashes) + quickLaneReadChunk - 1) / quickLaneReadChunk

	// Room for every chunk, so a read that answers after this call returned never blocks.
	results := make(chan laneChunkRead, chunks)

	go u.dispatchLaneReads(readCtx, hashes, notFoundIsNoRecord, results)

	for got := 0; got < chunks; got++ {
		select {
		case r := <-results:
			// A result and the deadline can be ready together; one past the deadline does not count.
			if err := readCtx.Err(); err != nil {
				q.fail(err)

				return
			}

			q.add(r.positions...)
			q.addNoRecord(r.missing)

			if r.err != nil {
				q.fail(r.err)

				return
			}
		case <-readCtx.Done():
			q.fail(readCtx.Err())

			return
		}
	}
}

// quickLaneReadSlots is the slot pool every block's position reads share, quickLaneReadConcurrency
// slots, so reads stuck in the store hold at most that many: a later block finds no free slot,
// times out and goes without rows.
func (u *BlockValidation) quickLaneReadSlots() chan struct{} {
	u.quickLaneSlotsOnce.Do(func() { u.quickLaneSlots = make(chan struct{}, quickLaneReadConcurrency) })

	return u.quickLaneSlots
}

// dispatchLaneReads starts one read per chunk of hashes, each once it holds a slot, and stops when
// readCtx ends. Each read checks readCtx before the store call; a read that passed that check just
// before the deadline can still reach the store, at most one per slot.
func (u *BlockValidation) dispatchLaneReads(readCtx context.Context, hashes []*chainhash.Hash, notFoundIsNoRecord bool, results chan<- laneChunkRead) {
	slots := u.quickLaneReadSlots()

	for start := 0; start < len(hashes); start += quickLaneReadChunk {
		chunk := hashes[start:min(start+quickLaneReadChunk, len(hashes))]

		select {
		case slots <- struct{}{}:
		case <-readCtx.Done():
			return
		}

		go func() {
			defer func() { <-slots }()

			if err := readCtx.Err(); err != nil {
				results <- laneChunkRead{err: err}

				return
			}

			results <- u.readLaneChunk(readCtx, chunk, notFoundIsNoRecord)
		}()
	}
}

// readLaneChunk reads one chunk's positions.
func (u *BlockValidation) readLaneChunk(readCtx context.Context, chunk []*chainhash.Hash, notFoundIsNoRecord bool) laneChunkRead {
	items := make([]*utxo.UnresolvedMetaData, 0, len(chunk))
	for i, h := range chunk {
		items = append(items, &utxo.UnresolvedMetaData{Hash: *h, Idx: i})
	}

	if err := u.utxoStore.BatchDecorate(readCtx, items, fields.LanePosition); err != nil {
		return laneChunkRead{err: err}
	}

	r := laneChunkRead{positions: make([]uint64, 0, len(items))}

	for _, it := range items {
		switch {
		case it.Err != nil && !errors.Is(it.Err, errors.ErrTxNotFound):
			if r.err == nil {
				r.err = it.Err
			}
		case it.Err != nil && notFoundIsNoRecord:
			r.missing++
		case it.Err == nil && it.Data != nil:
			r.positions = append(r.positions, it.Data.LanePosition)
		default:
			r.positions = append(r.positions, lane.None)
		}
	}

	return r
}

// quickLaneRows builds the lane map rows of block from the positions its quick validation
// collected on ctx, or returns nil when lanes are off, the block has no transaction besides its
// coinbase, or the rows cannot be built, which is counted and logged.
func (u *BlockValidation) quickLaneRows(ctx context.Context, block *model.Block) []lane.MapRow {
	q := quickLanePositionsFrom(ctx)
	if q == nil || len(block.Subtrees) == 0 {
		return nil
	}

	start := time.Now()

	q.mu.Lock()
	q.flushLocked()
	failed, unpositioned, noRecord := q.failed, q.unpositioned, q.noRecord
	q.mu.Unlock()

	lane.CountTxsWithoutPosition(laneQuickValidationService, unpositioned)

	if failed != nil {
		lane.CountRowInsertFailure(lane.BlockPeer)
		u.logger.Warnf("[quickLaneRows][%s] storing the block without lane rows: the lane positions of its existing records could not be read: %v", block.Hash().String(), failed)

		return nil
	}

	rows, err := lane.BuildBlockRows([]lane.LaneBitmaps{q.lanes})
	if err != nil {
		lane.CountRowInsertFailure(lane.BlockPeer)
		u.logger.Warnf("[quickLaneRows][%s] storing the block without lane rows, which could not be built: %v", block.Hash().String(), err)

		return nil
	}

	lane.ObserveGate(lane.BlockPeer, lane.GateStageBuild, time.Since(start))
	lane.ObserveBlockRows(lane.BlockPeer, rows)
	u.checkLaneRowCount(block, rows.Members, uint64(noRecord)) //nolint:gosec // a count is never negative

	return rows.Rows
}

// laneQuickValidationService is the service label quick validation counts its records stored
// without a lane position under.
const laneQuickValidationService = "blockvalidation"

// quickLaneLateInsertTimeout bounds insertLaneRowsOfStoredBlock. A variable so a test can shorten it.
var quickLaneLateInsertTimeout = 10 * time.Second

// insertLaneRowsOfStoredBlock stores rows, the lane map rows this attempt built, for a block its
// AddBlock found already stored: by this attempt, whose AddBlock then failed, or by another route,
// which may have stored the block without rows. The insert is insert-only, so it writes the rows
// only where the stored block has none, and rows that differ from those stored are refused and
// counted in teranode_lane_row_insert_failures_total, as is any other failure. It runs detached
// from ctx, as commitBlock's probe does, so a caller that has given up does not cut it short.
func (u *BlockValidation) insertLaneRowsOfStoredBlock(ctx context.Context, block *model.Block, rows []lane.MapRow, caller string) {
	insertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quickLaneLateInsertTimeout)
	defer cancel()

	if err := u.blockchainClient.InsertLaneMapRows(insertCtx, block.Hash(), rows); err != nil {
		lane.CountRowInsertFailure(lane.BlockPeer)
		u.logger.Warnf("[%s][%s] the block is stored without this attempt's lane map rows, whose insert after it failed: %v", caller, block.Hash().String(), err)
	}
}
