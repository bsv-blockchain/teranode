package utxoset

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// THE STAMP. Once per containment window, when the window's last block is stampDepth deep, this
// pass writes the block that won at each height onto the UTXOs of the transactions that were
// seen before their block, and deletes their identity rows. It is the ONLY order-sensitive
// decision about a UTXO's block anywhere in the store, and it is made from a chain answer the
// pruner service hands in as a value: the store makes no chain call.
//
// Four calls, one transaction each, so the caller can re-read the chain between them.
//
//  1. BeginWindow. Preconditions, then step 1: find the window's losing containment rows with NO
//     lock held, take the exclusive fence lock, raise stamp_fence to the window's upper bound,
//     delete the known losers, re-check for rows that landed since the scan, commit. After this
//     commit the window is frozen and every row left in it names a winner (invariant I3).
//  2. StampPage, 256 times. A page is every winning containment row of the window whose txid
//     begins with one byte, read from the primary key in txid order; its identity rows live in
//     one identity leaf and its UTXOs in one UTXO leaf, so a page touches one partition of each.
//     Stamp the UTXOs, then delete the identity rows, in ONE statement, so a crash can never
//     leave a UTXO at (0,0) with no identity row behind it.
//  3. CompleteWindow. A sampled audit, then the completion record and the completion floor in
//     one transaction. The record carries the tip at which the window was stamped, and the drop
//     waits 1,728 blocks past it, which is what keeps a UTXO spent before its stamp answerable
//     for as long as its undo copy can live.
//
// The work list is the identity table (decision 2): only a transaction seen before its block can
// own a UTXO at (0,0). Below the checkpoint every create carries its block, tx_ident is empty,
// and a window costs one loser scan and one completion record. That is what lets the pruner keep
// up during sync.
//
// The page statement lives in ONE place, stampPageSQL, so the page driver can be swapped. This
// build drives pages by txid slab. The bench measured a heap-order driver 25% faster on writes
// and 3x heavier on reads; which wins on the mainnet box depends on whether the identity key and
// the UTXO index fit its buffer pool, and that is unmeasured, so the slab driver ships first.

// StampDepthFor is the depth, in blocks, at which a window may be stamped: twice the coinbase
// maturity, rounded up to a whole window. Maturity is the depth past which the node already
// refuses a chain switch, so twice it is room for an operator invalidation or a block on an
// uncapped path to reach deeper than maturity and be admitted, and a whole window because the
// stamp works one partition at a time. On every network with a maturity of 100 that is 288.
func StampDepthFor(coinbaseMaturity uint32) uint32 {
	depth := 2 * coinbaseMaturity

	return (depth + TxMinedPartitionBlocks - 1) / TxMinedPartitionBlocks * TxMinedPartitionBlocks
}

// stampMarginBlocks is added to the tip at which a window completes to give stamped_at: one undo
// partition, which is how far above the tip a spend's undo copy can be journalled while the
// window is being published. It is defined as the undo width so the two change together.
const stampMarginBlocks = SpendJournalPartitionBlocks

// undoMaxLifeBlocks is the longest an undo copy can live with an on-time pruner: retention plus
// one partition width. A window drops no earlier than stamped_at plus this.
const undoMaxLifeBlocks = DefaultSpendJournalRetentionBlocks + SpendJournalPartitionBlocks

// The three fixed advisory-lock keys, all in the two-integer form. The store's per-transaction
// locks are single bigints keyed on the first eight bytes of a txid, and postgres keeps the two
// forms in separate key spaces, so these cannot collide with a transaction.
const (
	// The fence lock. Writers of containment take it shared as the first statement of their
	// transaction; the stamp's step 1 takes it exclusive, so once granted every earlier
	// containment write has committed and the loser scan under it is exact.
	fenceLockKey1, fenceLockKey2 = 0x75747873, 0x66656e63 // "utxs", "fenc"
	// The stamp worker's session lock, held on one dedicated connection for the whole drain,
	// so two drains cannot run at once. Always the try form: a second drain is skipped, never
	// queued.
	stampSessionLockKey1, stampSessionLockKey2 = 0x75747873, 0x7374616d // "utxs", "stam"
)

// fenceLockTimeout is how long the stamp's step 1 waits for the exclusive fence lock before
// giving up and retrying from fenceRetryBackoffMin, doubling to fenceRetryBackoffMax. While it
// keeps timing out the block path is stalled for at most the timeout in every timeout plus
// backoff, about 17% at the first retry and less as the backoff doubles. Constants, not
// settings: an operator has no information to set them by. Neither was tuned against real
// create batches, which the design says openly.
const (
	fenceLockTimeout     = 200 * time.Millisecond
	fenceRetryBackoffMin = time.Second
	fenceRetryBackoffMax = 30 * time.Second
)

// stampPagesPerWindow is the slab count: one page per value of the txid's first byte.
const stampPagesPerWindow = 256

// stampAuditSampleRows is how many rows of the window the completion audit reads, in
// stampAuditSampleRuns runs from random starting txids. A judgement, not a measurement.
const (
	stampAuditSampleRows = 1_000
	stampAuditSampleRuns = 8
)

// ErrStampDrainBusy is returned by OpenDrain while another drain holds the session lock. It is
// the interface's error, named here so the package's own tests read naturally.
var ErrStampDrainBusy = pruner.ErrStampDrainBusy

var _ pruner.Stamper = (*Store)(nil)

// floorsSQL reads the three floors. floor is a window number and the other two are heights.
const floorsSQL = `SELECT floor, stamp_fence, stamp_complete_floor FROM tx_mined_floor WHERE id = 0`

// StampDepth is the depth, in blocks, below the tip at which this store's UTXOs get their block
// written permanently. Nothing may change the chain below it; the rewind tool asks.
func (s *Store) StampDepth() uint32 { return s.stampDepth }

// WindowBlocks is the containment window width.
func (s *Store) WindowBlocks() uint32 { return TxMinedPartitionBlocks }

// Floors is one read of the tx_mined_floor row, as heights.
func (s *Store) Floors(ctx context.Context) (pruner.StampFloors, error) {
	return readFloors(ctx, s.pool)
}

func readFloors(ctx context.Context, q querier) (pruner.StampFloors, error) {
	var floor, fence, complete int32

	if err := q.QueryRow(ctx, floorsSQL).Scan(&floor, &fence, &complete); err != nil {
		return pruner.StampFloors{}, errors.NewStorageError("[utxoset][stamp] read floors", err)
	}

	return pruner.StampFloors{
		DroppedFloor:       uint32(floor) * TxMinedPartitionBlocks, //nolint:gosec // a window number is never negative
		StampFence:         uint32(fence),                          //nolint:gosec // a height is never negative
		StampCompleteFloor: uint32(complete),                       //nolint:gosec // a height is never negative
	}, nil
}

// stampDrain is one drain: the dedicated connection holding the session lock, and nothing else.
// Every transaction of the pass runs on ordinary pool connections.
type stampDrain struct {
	store *Store
	conn  *pgxpool.Conn
}

// OpenDrain takes the session lock on a dedicated connection and runs the completion-record
// check. A session-level advisory lock belongs to the connection that took it, not to a
// transaction, and a pooled connection is not released to the pool while this drain holds it,
// so the lock cannot leak to another caller and cannot be released by one.
func (s *Store) OpenDrain(ctx context.Context) (pruner.StampDrain, pruner.StampFloors, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, pruner.StampFloors{}, errors.NewStorageError("[utxoset][stamp] acquire drain connection", err)
	}

	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, $2)`, int32(stampSessionLockKey1), int32(stampSessionLockKey2)).Scan(&got); err != nil {
		conn.Release()

		return nil, pruner.StampFloors{}, errors.NewStorageError("[utxoset][stamp] take session lock", err)
	}

	if !got {
		conn.Release()
		stampDrainsSkipped.WithLabelValues("lock_held").Inc()

		return nil, pruner.StampFloors{}, ErrStampDrainBusy
	}

	d := &stampDrain{store: s, conn: conn}

	floors, err := s.checkCompletionRecords(ctx)
	if err != nil {
		_ = d.Close()

		return nil, pruner.StampFloors{}, err
	}

	return d, floors, nil
}

// checkCompletionRecords is the alarm for the impossible state: an attached window whose upper
// bound is at or below stamp_complete_floor and which has no completion record. The completion
// record and the floor advance commit together, so this can only mean a hand edit or a bug, and
// such a window can never drop. It counts and carries on; the drop refuses the window on its own.
func (s *Store) checkCompletionRecords(ctx context.Context) (pruner.StampFloors, error) {
	floors, err := s.Floors(ctx)
	if err != nil {
		return floors, err
	}

	windows, err := s.listTxMinedWindows(ctx)
	if err != nil {
		return floors, err
	}

	records, err := s.completionRecords(ctx)
	if err != nil {
		return floors, err
	}

	for _, w := range windows {
		if !w.attached {
			continue
		}

		hi := (w.window + 1) * TxMinedPartitionBlocks
		if hi > floors.StampCompleteFloor {
			continue
		}

		if _, ok := records[w.window*TxMinedPartitionBlocks]; !ok {
			stampCompletionMissing.Inc()
			s.logger.Errorf("[utxoset][stamp] window %s is below the stamp-complete floor %d and has no completion record; it can never drop", w.name, floors.StampCompleteFloor)
		}
	}

	return floors, nil
}

// completionRecords reads every completion record: window start height to stamped_at.
func (s *Store) completionRecords(ctx context.Context) (map[uint32]uint32, error) {
	rows, err := s.pool.Query(ctx, `SELECT window_start, stamped_at FROM tx_mined_stamped`)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][stamp] read completion records", err)
	}

	defer rows.Close()

	out := map[uint32]uint32{}

	for rows.Next() {
		var start, at int32
		if err := rows.Scan(&start, &at); err != nil {
			return nil, errors.NewStorageError("[utxoset][stamp] scan completion record", err)
		}

		out[uint32(start)] = uint32(at) //nolint:gosec // heights are never negative
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][stamp] read completion records", err)
	}

	return out, nil
}

// PagesPerWindow is the slab count.
func (d *stampDrain) PagesPerWindow() int { return stampPagesPerWindow }

// Close releases the session lock and the connection. If the unlock fails, or answers false,
// the connection is closed rather than returned, because closing a connection releases every
// session lock it holds and a pooled connection with a stray lock would serialise nothing.
func (d *stampDrain) Close() error {
	if d.conn == nil {
		return nil
	}

	conn := d.conn
	d.conn = nil

	var released bool

	err := conn.QueryRow(context.Background(), `SELECT pg_advisory_unlock($1, $2)`,
		int32(stampSessionLockKey1), int32(stampSessionLockKey2)).Scan(&released)
	if err != nil || !released {
		_ = conn.Hijack().Close(context.Background())

		if err != nil {
			return errors.NewStorageError("[utxoset][stamp] release session lock; connection closed instead", err)
		}

		return errors.NewStorageError("[utxoset][stamp] session lock was not held at close; connection closed instead")
	}

	conn.Release()

	return nil
}

// findLosersSQL names every row of the window whose (height, block id) is not the ancestry's
// pair for that height. A row naming block id 0 names no block and is neither a winner nor a
// loser. The window bounds are scalars so the planner reads one partition.
const findLosersSQL = `
SELECT m.txid, m.mined_height, m.block_id
  FROM tx_mined m
 WHERE m.mined_height >= $1::int AND m.mined_height < $2::int
   AND m.block_id <> 0
   AND NOT EXISTS (SELECT 1 FROM unnest($3::int[], $4::int[]) AS a(h, b)
                    WHERE a.h = m.mined_height AND a.b = m.block_id)`

// deleteLosersSQL deletes a known list of rows by their full primary key.
const deleteLosersSQL = `
DELETE FROM tx_mined m
 USING unnest($1::bytea[], $2::int[], $3::int[]) AS k(txid, h, b)
 WHERE m.txid = k.txid AND m.mined_height = k.h AND m.block_id = k.b`

// loser is one losing containment row.
type loser struct {
	txid   []byte
	height int32
	block  int32
}

// BeginWindow checks the preconditions and runs step 1. See the file comment for the steps.
func (d *stampDrain) BeginWindow(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry) (pruner.StampWindowState, error) {
	s := d.store

	if wLo%TxMinedPartitionBlocks != 0 {
		return 0, errors.NewProcessingError("[utxoset][stamp] %d is not a window start", wLo)
	}

	wEnd := wLo + TxMinedPartitionBlocks - 1
	wHi := wLo + TxMinedPartitionBlocks

	floors, err := s.Floors(ctx)
	if err != nil {
		return 0, err
	}

	// Precondition 1: the lowest window with no completion record, and never a skip.
	if wLo != floors.StampCompleteFloor {
		return 0, errors.NewProcessingError("[utxoset][stamp] window %d is not the next to stamp; the stamp-complete floor is %d", wLo, floors.StampCompleteFloor)
	}

	// Precondition 2: deep enough. The anchor's height is the tip the ancestry was built from.
	if anc.Hi() < wEnd+s.stampDepth {
		return pruner.StampWindowNotDeep, nil
	}

	// Precondition 3: the ancestry speaks for the whole window.
	if !anc.Covers(wLo) || !anc.Covers(wEnd) {
		return 0, errors.NewProcessingError("[utxoset][stamp] ancestry [%d, %d] does not cover window %d-%d", anc.Lo(), anc.Hi(), wLo, wEnd)
	}

	// Precondition 4: every main-chain block of the window has its containment recorded.
	if h, ok := anc.NotMined(wLo, wEnd); ok {
		stampNotMinedAborts.Inc()

		return 0, errors.NewProcessingError("[utxoset][stamp] block at height %d of window %d has mined_set false; its containment may be incomplete, so the window is not stamped this drain", h, wLo)
	}

	// Precondition 5: the partition is attached. A window with no table at all is empty by
	// decision: both stamp floors move past it, so a seeded store that predates the floor
	// write, or one whose floors were lost, still prunes. It is counted and logged because a
	// missing table above the dropped floor can also be a window dropped by mistake.
	state, err := s.txMinedWindowState(ctx, wLo/TxMinedPartitionBlocks)
	if err != nil {
		return 0, err
	}

	switch {
	case state == nil:
		if err := s.skipMissingWindow(ctx, wLo, wHi); err != nil {
			return 0, err
		}

		return pruner.StampWindowSkipped, nil
	case !state.attached && !state.detachPending:
		return 0, errors.NewProcessingError("[utxoset][stamp] window %s is detached and has no completion record, which the drop rule makes impossible", state.name)
	}

	// Resume: the fence is only ever the completion floor or one window above it. At or above
	// this window's upper bound means step 1 committed for it in an earlier drain.
	if floors.StampFence >= wHi {
		return pruner.StampWindowResumed, nil
	}

	if err := d.raiseFenceAndDeleteLosers(ctx, wLo, wHi, anc); err != nil {
		return 0, err
	}

	return pruner.StampWindowReady, nil
}

// skipMissingWindow advances both stamp floors past a window that has no table. The completion
// floor is advanced by compare-and-set against the value BeginWindow read, so a second drain
// that somehow got past the session lock cannot advance it twice.
func (s *Store) skipMissingWindow(ctx context.Context, wLo, wHi uint32) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tx_mined_floor
		   SET stamp_fence          = GREATEST(stamp_fence, $1::int),
		       stamp_complete_floor = $1::int
		 WHERE id = 0 AND stamp_complete_floor = $2::int`,
		int32(wHi), int32(wLo)) //nolint:gosec // heights fit int32
	if err != nil {
		return errors.NewStorageError("[utxoset][stamp] advance floors past missing window %d", wLo, err)
	}

	if tag.RowsAffected() != 1 {
		return errors.NewProcessingError("[utxoset][stamp] the stamp-complete floor moved from %d under this drain", wLo)
	}

	stampMissingWindows.Inc()
	s.logger.Warnf("[utxoset][stamp] window %d-%d has no table and is treated as empty; both stamp floors advanced to %d", wLo, wHi-1, wHi)

	return nil
}

// raiseFenceAndDeleteLosers is step 1. The scan for losers runs with NO lock held, so the block
// path does not wait for it. The exclusive lock is held only to raise the fence, delete the
// known list, and re-check: the re-check is the same scan run again, exact now because the
// fence is up and every earlier writer has committed, and warm because the unlocked scan has
// just read the same index pages. During sync there are no losers and the whole hold is the
// warm scan, measured at 0.43 s on a 5.57M-row window and 1.25 s at 16.1M.
func (d *stampDrain) raiseFenceAndDeleteLosers(ctx context.Context, wLo, wHi uint32, anc *chainancestry.Ancestry) error {
	s := d.store

	heights, ids := anc.Pairs(wLo, wHi-1)

	known, err := s.findLosers(ctx, s.pool, wLo, wHi, heights, ids)
	if err != nil {
		return err
	}

	backoff := fenceRetryBackoffMin

	for {
		deleted, err := d.tryRaiseFence(ctx, wLo, wHi, heights, ids, known)
		if err == nil {
			if deleted > 0 {
				stampLosersDeleted.Add(float64(deleted))
				s.logger.Infof("[utxoset][stamp] window %d-%d: fence raised to %d and %d losing containment rows deleted", wLo, wHi-1, wHi, deleted)
			}

			return s.countForkOnly(ctx, wLo, wHi, heights, ids, known)
		}

		if !isLockTimeout(err) {
			return err
		}

		stampFenceLockTimeouts.Inc()
		s.logger.Warnf("[utxoset][stamp] window %d-%d: fence lock not granted within %s, retrying in %s", wLo, wHi-1, fenceLockTimeout, backoff)

		select {
		case <-ctx.Done():
			return errors.NewProcessingError("[utxoset][stamp] cancelled while waiting for the fence lock", ctx.Err())
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > fenceRetryBackoffMax {
			backoff = fenceRetryBackoffMax
		}
	}
}

// forkOnlySQL counts, among the transactions whose losing rows were just deleted, the ones with
// no winner left in the window and an identity row whose unmined marker is set. That is the
// transaction only a fork block contained, still waiting for a block: normal, and gauged so a
// pass that resumes after a crash, which skips step 1, is the only one that misses it.
const forkOnlySQL = `
SELECT count(*)
  FROM (SELECT DISTINCT txid FROM unnest($1::bytea[]) AS k(txid)) AS k
 WHERE EXISTS (SELECT 1 FROM tx_ident i
                WHERE i.leaf = (get_byte(k.txid, 0) & 7)::smallint AND i.txid = k.txid
                  AND i.off_chain_since IS NOT NULL)
   AND NOT EXISTS (SELECT 1 FROM tx_mined m
                    WHERE m.txid = k.txid
                      AND m.mined_height >= $2::int AND m.mined_height < $3::int
                      AND EXISTS (SELECT 1 FROM unnest($4::int[], $5::int[]) AS a(h, b)
                                   WHERE a.h = m.mined_height AND a.b = m.block_id)
                   OFFSET 0)`

// countForkOnly gauges the first no-winner case of the design: a transaction only a losing block
// contained, whose marker is set. The other case, marker NULL, is judged page by page as a
// suspect. Rows naming block id 0 never reach here, because they are never losers.
func (s *Store) countForkOnly(ctx context.Context, wLo, wHi uint32, heights, ids []int32, losers []loser) error {
	if len(losers) == 0 {
		return nil
	}

	txids := make([][]byte, 0, len(losers))
	for _, l := range losers {
		txids = append(txids, l.txid)
	}

	var n int64
	if err := s.pool.QueryRow(ctx, forkOnlySQL, txids, int32(wLo), int32(wHi), heights, ids).Scan(&n); err != nil { //nolint:gosec // heights fit int32
		return errors.NewStorageError("[utxoset][stamp] count fork-only transactions", err)
	}

	stampNoWinner.WithLabelValues("set").Add(float64(n))

	return nil
}

// isLockTimeout reports whether err is postgres refusing a lock wait past lock_timeout.
func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// tryRaiseFence is one attempt at step 1's transaction. It returns the number of losers deleted.
func (d *stampDrain) tryRaiseFence(ctx context.Context, wLo, wHi uint32, heights, ids []int32, known []loser) (int64, error) {
	s := d.store

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][stamp] begin step 1", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	if _, err := dbTx.Exec(ctx, fmt.Sprintf(`SET LOCAL lock_timeout = '%dms'`, fenceLockTimeout.Milliseconds())); err != nil {
		return 0, errors.NewStorageError("[utxoset][stamp] set lock timeout", err)
	}

	// Its own statement, so the statements after it take their snapshots after the lock is
	// granted, which is after every earlier writer committed.
	if _, err := dbTx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(fenceLockKey1), int32(fenceLockKey2)); err != nil {
		return 0, err
	}

	if _, err := dbTx.Exec(ctx, `UPDATE tx_mined_floor SET stamp_fence = GREATEST(stamp_fence, $1::int) WHERE id = 0`,
		int32(wHi)); err != nil { //nolint:gosec // a height fits int32
		return 0, errors.NewStorageError("[utxoset][stamp] raise fence", err)
	}

	deleted, err := s.deleteLosers(ctx, dbTx, known)
	if err != nil {
		return 0, err
	}

	// The re-check: rows that landed in the window between the unlocked scan and the lock.
	late, err := s.findLosers(ctx, dbTx, wLo, wHi, heights, ids)
	if err != nil {
		return 0, err
	}

	n, err := s.deleteLosers(ctx, dbTx, late)
	if err != nil {
		return 0, err
	}

	deleted += n

	if err := dbTx.Commit(ctx); err != nil {
		return 0, errors.NewStorageError("[utxoset][stamp] commit step 1", err)
	}

	return deleted, nil
}

func (s *Store) findLosers(ctx context.Context, q querier, wLo, wHi uint32, heights, ids []int32) ([]loser, error) {
	rows, err := q.Query(ctx, findLosersSQL, int32(wLo), int32(wHi), heights, ids) //nolint:gosec // heights fit int32
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][stamp] find losers in window %d", wLo, err)
	}

	defer rows.Close()

	var out []loser

	for rows.Next() {
		var l loser
		if err := rows.Scan(&l.txid, &l.height, &l.block); err != nil {
			return nil, errors.NewStorageError("[utxoset][stamp] scan loser", err)
		}

		out = append(out, l)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset][stamp] find losers in window %d", wLo, err)
	}

	return out, nil
}

func (s *Store) deleteLosers(ctx context.Context, q querier, losers []loser) (int64, error) {
	if len(losers) == 0 {
		return 0, nil
	}

	txids := make([][]byte, 0, len(losers))
	heights := make([]int32, 0, len(losers))
	blocks := make([]int32, 0, len(losers))

	for _, l := range losers {
		txids = append(txids, l.txid)
		heights = append(heights, l.height)
		blocks = append(blocks, l.block)
	}

	tag, err := q.Exec(ctx, deleteLosersSQL, txids, heights, blocks)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][stamp] delete losers", err)
	}

	return tag.RowsAffected(), nil
}

// stampPageSQL is the page statement, and the ONE place the page shape lives.
//
// $1 and $2 are the page's txid bounds, $3 its leaf, $4 and $5 the window's 288 (height, block
// id) pairs from the ancestry, $6 and $7 the window's height bounds. The scan of m joins the
// window's rows to the pairs, so only winners are in the page; losers were deleted by step 1
// and a stray survivor cannot put a wrong pair on a UTXO here either. The identity delete
// RETURNING is the work list: a transaction with no identity row is not stamped, because under
// decision 2 only a transaction with one can own a UTXO at (0,0). The UTXO read is the fenced
// shape stampUTXOsSQL uses, with the packed-key range derived in SQL from the txid, and the
// UPDATE matches on the exact row the read found and rechecks the full txid, because the
// packed key is a non-unique prefix. mined_height = 0 is the guard that leaves a UTXO born
// with its pair, or already stamped, untouched.
//
// The last column is the count of distinct transactions in the work list. If it is below the
// row count, one transaction had two winners in the window, which consensus forbids outside
// the two historic duplicate coinbases, and those have no identity row. The caller rolls the
// page back on it.
const stampPageSQL = `
WITH win AS MATERIALIZED (
    SELECT h, b FROM unnest($4::int[], $5::int[]) AS t(h, b)
),
m AS MATERIALIZED (
    SELECT m.txid, m.mined_height, m.block_id
      FROM tx_mined m
      JOIN win ON win.h = m.mined_height AND win.b = m.block_id
     WHERE m.mined_height >= $6::int AND m.mined_height < $7::int
       AND m.txid >= $1::bytea AND m.txid <= $2::bytea
     ORDER BY m.txid
),
del AS (
    DELETE FROM tx_ident i
     WHERE i.leaf = $3::smallint
       AND i.txid = ANY (ARRAY(SELECT txid FROM m))
    RETURNING i.txid, (i.off_chain_since IS NOT NULL) AS marker_set
),
w AS MATERIALIZED (
    SELECT m.txid, m.mined_height AS h, m.block_id AS b, del.marker_set
      FROM del
      JOIN m ON m.txid = del.txid
),
hit AS MATERIALIZED (
    SELECT c.ukey, w.txid, w.h, w.b
      FROM w
     CROSS JOIN LATERAL (
        SELECT u.ukey
          FROM utxo u
         WHERE u.leaf = $3::smallint
           AND u.ukey >= (encode(substring(w.txid from 1 for 12), 'hex') || '00000000')::uuid
           AND u.ukey <= (encode(substring(w.txid from 1 for 12), 'hex') || 'ffffffff')::uuid
           AND u.txid = w.txid
           AND u.mined_height = 0
        OFFSET 0
     ) AS c
),
upd AS (
    UPDATE utxo u SET mined_height = hit.h, block_id = hit.b
      FROM hit
     WHERE u.leaf = $3::smallint AND u.ukey = hit.ukey AND u.txid = hit.txid
       AND u.mined_height = 0
    RETURNING 1
)
SELECT (SELECT count(*) FROM m),
       (SELECT count(*) FROM w),
       (SELECT count(*) FROM upd),
       (SELECT count(*) FROM w WHERE marker_set),
       (SELECT count(DISTINCT txid) FROM w)`

// suspectsInSlabSQL names the identity rows of one page's slab that have no winner in this
// window, believe themselves mined (marker NULL), and were first seen before the window. It
// runs after the page's delete in the same transaction, so a row stamped by this page is gone.
const suspectsInSlabSQL = `
SELECT i.txid
  FROM tx_ident i
 WHERE i.leaf = $3::smallint
   AND i.txid >= $1::bytea AND i.txid <= $2::bytea
   AND i.off_chain_since IS NULL
   AND i.created_height < $4::int`

// judgeSuspectsSQL keeps the suspects that have NO containment row the chain confirms: every
// row below $3, the window's upper bound, is a winner because losers below the fence are
// deleted; a row above $6, the anchor, is not judged on this pass; a row between is judged
// against the ancestry's pairs. $2 is the dropped floor as a height.
const judgeSuspectsSQL = `
SELECT k.txid
  FROM unnest($1::bytea[]) AS k(txid)
 WHERE NOT EXISTS (
       SELECT 1 FROM tx_mined m
        WHERE m.txid = k.txid
          AND m.mined_height >= $2::int
          AND (m.mined_height < $3::int
               OR m.mined_height > $6::int
               OR EXISTS (SELECT 1 FROM unnest($4::int[], $5::int[]) AS a(h, b)
                           WHERE a.h = m.mined_height AND a.b = m.block_id))
       OFFSET 0)`

// slabBounds is the txid range of one page: every txid whose first byte is the page number.
func slabBounds(page int) (lo, hi []byte, leaf int16) {
	lo = make([]byte, 32)
	hi = make([]byte, 32)

	for i := range hi {
		hi[i] = 0xff
	}

	lo[0] = byte(page)
	hi[0] = byte(page)

	return lo, hi, int16(page & (NumLeaves - 1)) //nolint:gosec // a leaf index fits
}

// StampPage runs one page as one transaction, then the page hook. See stampPageSQL.
func (d *stampDrain) StampPage(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry, page int) (pruner.StampPageResult, error) {
	s := d.store

	if page < 0 || page >= stampPagesPerWindow {
		return pruner.StampPageResult{}, errors.NewProcessingError("[utxoset][stamp] page %d out of range", page)
	}

	wHi := wLo + TxMinedPartitionBlocks
	lo, hi, leaf := slabBounds(page)
	heights, ids := anc.Pairs(wLo, wHi-1)

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return pruner.StampPageResult{}, errors.NewStorageError("[utxoset][stamp] begin page", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	var res pruner.StampPageResult
	var distinct int64

	if err := dbTx.QueryRow(ctx, stampPageSQL, lo, hi, leaf, heights, ids, int32(wLo), int32(wHi)). //nolint:gosec // heights fit int32
													Scan(&res.Rows, &res.Transactions, &res.UTXOs, &res.MarkerSetWinners, &distinct); err != nil {
		return pruner.StampPageResult{}, errors.NewStorageError("[utxoset][stamp] page %d of window %d", page, wLo, err)
	}

	if distinct != res.Transactions {
		stampTwoWinners.Inc()

		return pruner.StampPageResult{}, errors.NewStorageError("[utxoset][stamp] page %d of window %d: a transaction has two winning containment rows in one window; the page is rolled back", page, wLo)
	}

	marked, err := d.judgeSuspects(ctx, dbTx, wLo, wHi, anc, lo, hi, leaf)
	if err != nil {
		return pruner.StampPageResult{}, err
	}

	res.SuspectsMarked = marked

	if err := dbTx.Commit(ctx); err != nil {
		return pruner.StampPageResult{}, errors.NewStorageError("[utxoset][stamp] commit page %d of window %d", page, wLo, err)
	}

	stampPages.Inc()
	stampUTXOs.Add(float64(res.UTXOs))
	stampIdentityDeleted.Add(float64(res.Transactions))
	stampWinnerMarkerSet.Add(float64(res.MarkerSetWinners))

	if s.stampPageHook != nil {
		if err := s.stampPageHook(wLo, page); err != nil {
			return res, err
		}
	}

	return res, nil
}

// judgeSuspects is the second case of a transaction with no winner: an identity row that
// believes itself mined, was first seen before this window, and has no containment row the
// chain confirms anywhere. Left alone it leaks for ever, because the stamp never matches it and
// the unmined iterator never sees it. The pass sets its marker so block assembly picks it up
// again, and alarms. It runs only in a drain whose ancestry reads mined_set true from the
// window's upper bound to the anchor; otherwise a record-mined call for a block in that span
// may still be running, its rows not all visible, and the step is deferred to a later drain.
func (d *stampDrain) judgeSuspects(ctx context.Context, dbTx pgx.Tx, wLo, wHi uint32, anc *chainancestry.Ancestry, lo, hi []byte, leaf int16) (int64, error) {
	s := d.store

	rows, err := dbTx.Query(ctx, suspectsInSlabSQL, lo, hi, leaf, int32(wLo)) //nolint:gosec // a height fits int32
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][stamp] list suspects", err)
	}

	var suspects [][]byte

	for rows.Next() {
		var txid []byte
		if err := rows.Scan(&txid); err != nil {
			rows.Close()

			return 0, errors.NewStorageError("[utxoset][stamp] scan suspect", err)
		}

		suspects = append(suspects, txid)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset][stamp] list suspects", err)
	}

	if len(suspects) == 0 {
		return 0, nil
	}

	if wHi <= anc.Hi() {
		if _, notMined := anc.NotMined(wHi, anc.Hi()); notMined {
			stampSuspectsDeferred.Add(float64(len(suspects)))

			return 0, nil
		}
	}

	floors, err := readFloors(ctx, dbTx)
	if err != nil {
		return 0, err
	}

	heights, ids := anc.Pairs(wHi, anc.Hi())

	judged, err := queryTxids(ctx, dbTx, judgeSuspectsSQL, suspects,
		int32(floors.DroppedFloor), int32(wHi), heights, ids, int32(anc.Hi())) //nolint:gosec // heights fit int32
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][stamp] judge suspects", err)
	}

	if len(judged) == 0 {
		return 0, nil
	}

	txids := make([][]byte, 0, len(judged))
	for i := range judged {
		txids = append(txids, judged[i][:])
	}

	tag, err := dbTx.Exec(ctx, `
		UPDATE tx_ident i SET off_chain_since = $3::int
		 WHERE i.leaf = $1::smallint AND i.txid = ANY($2::bytea[]) AND i.off_chain_since IS NULL`,
		leaf, txids, int32(anc.Hi())) //nolint:gosec // a height fits int32
	if err != nil {
		return 0, errors.NewStorageError("[utxoset][stamp] mark suspects unmined", err)
	}

	stampNoWinner.WithLabelValues("null").Add(float64(tag.RowsAffected()))
	s.logger.Warnf("[utxoset][stamp] window %d-%d: %d transactions believed mined have no chain-confirmed containment row anywhere; their unmined marker is set so block assembly reconsiders them", wLo, wHi-1, tag.RowsAffected())

	return tag.RowsAffected(), nil
}

// CompleteWindow runs the sampled audit and then publishes the window: the completion record
// and the completion floor in one transaction, or neither.
func (d *stampDrain) CompleteWindow(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry, liveTip uint32) error {
	s := d.store
	wHi := wLo + TxMinedPartitionBlocks

	if err := s.auditWindow(ctx, wLo, wHi, anc); err != nil {
		return err
	}

	if s.stampPageHook != nil {
		if err := s.stampPageHook(wLo, -1); err != nil {
			return err
		}
	}

	// T is the larger of the tip the service read uncached after the last page and the store's
	// own height. Reading it after the last page's commit can only make it larger, which is
	// the safe direction: every UTXO of the window now carries a pair, so no later spend can
	// copy (0,0) into an undo row above T.
	tip := liveTip
	if h := s.GetBlockHeight(); h > tip {
		tip = h
	}

	stampedAt := tip + stampMarginBlocks

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][stamp] begin completion", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	if _, err := dbTx.Exec(ctx, `INSERT INTO tx_mined_stamped (window_start, stamped_at) VALUES ($1::int, $2::int)`,
		int32(wLo), int32(stampedAt)); err != nil { //nolint:gosec // heights fit int32
		return errors.NewStorageError("[utxoset][stamp] write completion record for window %d", wLo, err)
	}

	tag, err := dbTx.Exec(ctx, `UPDATE tx_mined_floor SET stamp_complete_floor = $1::int WHERE id = 0 AND stamp_complete_floor = $2::int`,
		int32(wHi), int32(wLo)) //nolint:gosec // heights fit int32
	if err != nil {
		return errors.NewStorageError("[utxoset][stamp] advance completion floor to %d", wHi, err)
	}

	if tag.RowsAffected() != 1 {
		stampCompletionMissing.Inc()

		return errors.NewProcessingError("[utxoset][stamp] the stamp-complete floor moved from %d under this drain; the completion is rolled back", wLo)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][stamp] commit completion of window %d", wLo, err)
	}

	stampWindows.Inc()
	s.logger.Infof("[utxoset][stamp] window %d-%d complete at tip %d, stamped_at %d", wLo, wHi-1, tip, stampedAt)

	// Statistics refreshed after the writes. Stale statistics flipped the UTXO probe to a
	// bitmap plan 5.7x slower in the bench. ANALYZE samples, so it is cheap on any size.
	if _, err := s.pool.Exec(ctx, `ANALYZE tx_ident; ANALYZE utxo`); err != nil {
		s.logger.Warnf("[utxoset][stamp] analyze after window %d: %v", wLo, err)
	}

	return nil
}

// auditSampleSQL reads a short run of the window's primary key from a random starting txid.
const auditSampleSQL = `
SELECT m.txid, m.mined_height, m.block_id, m.flags
  FROM tx_mined m
 WHERE m.mined_height >= $1::int AND m.mined_height < $2::int
   AND m.txid >= $3::bytea
 ORDER BY m.txid
 LIMIT $4`

// auditPairsSQL reads the pair of every live UTXO of the sampled transactions.
const auditPairsSQL = `
SELECT k.txid, u.mined_height, u.block_id
  FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[]) AS k(leaf, txid, lo, hi)
 CROSS JOIN LATERAL (
   SELECT u.mined_height, u.block_id
     FROM utxo u
    WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND u.ukey <= k.hi AND u.txid = k.txid
   OFFSET 0
 ) AS u`

// auditWindow is the sampled detector for invariant I1, run after the last page and before the
// completion record. For each sampled winner it checks that no identity row is left and that
// every live UTXO carries the winner's pair, or (h, 0) with h above zero, which is the
// legitimate "restored, known on chain" value. A UTXO at (0,0) is an I1 violation: it alarms
// and the window does not complete, because a window dropped over such a UTXO would leave it
// with nothing behind it. Any other pair is counted as a disagreement, except on a coinbase,
// whose pair is written at birth and can name the first of two blocks that carry it. The audit
// repairs nothing.
func (s *Store) auditWindow(ctx context.Context, wLo, wHi uint32, anc *chainancestry.Ancestry) error {
	type sampled struct {
		txid   []byte
		height int32
		block  int32
		flags  int16
	}

	var rows []sampled

	perRun := stampAuditSampleRows / stampAuditSampleRuns

	for run := 0; run < stampAuditSampleRuns; run++ {
		start := make([]byte, 32)
		if _, err := rand.Read(start); err != nil {
			return errors.NewProcessingError("[utxoset][stamp] audit random start", err)
		}

		rs, err := s.pool.Query(ctx, auditSampleSQL, int32(wLo), int32(wHi), start, perRun) //nolint:gosec // heights fit int32
		if err != nil {
			return errors.NewStorageError("[utxoset][stamp] audit sample", err)
		}

		for rs.Next() {
			var r sampled
			if err := rs.Scan(&r.txid, &r.height, &r.block, &r.flags); err != nil {
				rs.Close()

				return errors.NewStorageError("[utxoset][stamp] audit scan", err)
			}

			rows = append(rows, r)
		}

		rs.Close()

		if err := rs.Err(); err != nil {
			return errors.NewStorageError("[utxoset][stamp] audit sample", err)
		}
	}

	if len(rows) == 0 {
		return nil
	}

	winner := map[string]sampled{}
	txids := make([][]byte, 0, len(rows))

	for _, r := range rows {
		id, ok := anc.BlockID(uint32(r.height)) //nolint:gosec // a height is never negative
		if !ok || int32(id) != r.block {        //nolint:gosec // a block id fits int32
			continue
		}

		if _, dup := winner[string(r.txid)]; dup {
			continue
		}

		winner[string(r.txid)] = r
		txids = append(txids, r.txid)
	}

	if len(txids) == 0 {
		return nil
	}

	// No identity row may be left for a winner.
	for _, g := range leafGroups(txids) {
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_ident i WHERE i.leaf = $1::smallint AND i.txid = ANY($2::bytea[])`,
			g.leaf, g.txids).Scan(&n); err != nil {
			return errors.NewStorageError("[utxoset][stamp] audit identity", err)
		}

		if n > 0 {
			stampAuditViolations.Inc()

			return errors.NewProcessingError("[utxoset][stamp] audit of window %d: %d sampled winners still have an identity row; the window does not complete", wLo, n)
		}
	}

	leaves, ids, los, his := liveUTXOArgs(txids)

	prs, err := s.pool.Query(ctx, auditPairsSQL, leaves, ids, los, his)
	if err != nil {
		return errors.NewStorageError("[utxoset][stamp] audit pairs", err)
	}

	defer prs.Close()

	var violations, disagreements int

	for prs.Next() {
		var (
			txid          []byte
			height, block int32
		)

		if err := prs.Scan(&txid, &height, &block); err != nil {
			return errors.NewStorageError("[utxoset][stamp] audit pair scan", err)
		}

		w := winner[string(txid)]

		switch {
		case height == 0:
			violations++
		case height == w.height && block == w.block:
		case block == 0:
		case w.flags&FlagCoinbase != 0:
		default:
			disagreements++
		}
	}

	if err := prs.Err(); err != nil {
		return errors.NewStorageError("[utxoset][stamp] audit pairs", err)
	}

	if disagreements > 0 {
		stampAuditDisagreements.Add(float64(disagreements))
		s.logger.Errorf("[utxoset][stamp] audit of window %d: %d live UTXOs of sampled winners carry a pair that is not the winner's", wLo, disagreements)
	}

	if violations > 0 {
		stampAuditViolations.Add(float64(violations))

		return errors.NewProcessingError("[utxoset][stamp] audit of window %d: %d live UTXOs of sampled winners are still at (0,0) with no identity row; the window does not complete", wLo, violations)
	}

	return nil
}

// SetStampFloorsForSeed is the seeding tool's hook. A seeded store holds UTXOs at historic
// heights with no containment window behind them, so without this the pass would start at
// window 0 and find no table for about 3,280 windows. The tool calls it once its last UTXO is
// written, with the seed height, and every floor moves to the window holding the first block
// that will be applied after the seed. GREATEST makes a re-run harmless.
func (s *Store) SetStampFloorsForSeed(ctx context.Context, seedHeight uint32) error {
	window := (seedHeight + 1) / TxMinedPartitionBlocks
	height := window * TxMinedPartitionBlocks

	if _, err := s.pool.Exec(ctx, `
		UPDATE tx_mined_floor
		   SET floor                = GREATEST(floor, $1::int),
		       stamp_fence          = GREATEST(stamp_fence, $2::int),
		       stamp_complete_floor = GREATEST(stamp_complete_floor, $2::int)
		 WHERE id = 0`,
		int32(window), int32(height)); err != nil { //nolint:gosec // heights fit int32
		return errors.NewStorageError("[utxoset][stamp] set floors for a seed at height %d", seedHeight, err)
	}

	s.minedWindow.Store(0)

	s.logger.Infof("[utxoset][stamp] floors set for a seed at height %d: the stamp and the drop begin at window %d, height %d", seedHeight, window, height)

	return nil
}
