package utxoset

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// stampUTXOsSQL writes a block onto every live UTXO of the listed transactions that is still at
// the unconfirmed sentinel. See liveUTXOArgs for the leaf/lo/hi shape; $5 and $6 are the
// parallel height and block-id arrays keyed to the same transaction.
//
// NOTHING RUNS IT YET. It is the write half of the deep stamp of build step 5, kept because
// that stamp reuses it as it stands: the retirement stamp that used to drive it, which took a
// transaction's earliest containment row by an insertion counter as the winner, is deleted
// with the counter, because insertion order is not chain order and it stamped reorg losers
// onto UTXOs. The stamp that replaces it is told the winner by the pruner service.
//
// mined_height = 0 is what makes this idempotent and leaves a block-path UTXO -- written with
// its pair at create, never at the sentinel -- untouched. A UTXO that already carries a pair
// must not be touched a second time: a non-zero pair is final.
//
// The fenced read runs FIRST, in a CTE, and the UPDATE then matches on the exact (leaf, ukey)
// it returns -- the same shape resetUTXOsSQL uses, and for the identical reason. A plain
// `UPDATE ... FROM unnest(...) AS k WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND ...` was
// measured first, on 40,000 seeded UTXOs across all eight leaves with 500 keys: a Hash Join
// against a Seq Scan of every one of the eight UTXO partitions, because the planner can build
// a hash on (leaf, txid) from the whole table more cheaply than it can cost 500 per-key index
// probes, and the ukey range only applies as a post-join filter rather than an index
// condition. The CROSS JOIN LATERAL with an OFFSET 0 fence is what forces an index scan per
// key instead: an UPDATE cannot laterally reference its own target, so the read has to happen
// in a CTE the UPDATE then joins on the row identity it found, not on the search predicate.
//
// EVERY live UTXO of the transaction is stamped, not just one, so the LATERAL carries no
// LIMIT -- unlike utxoFactsSQL, which only ever needs one.
//
// The UPDATE rechecks the FULL TXID and not only the (leaf, ukey) the read found the row by.
// ukey is a 96-bit prefix and NON-UNIQUE by design -- see Pack -- so a stranger's UTXO in the
// same leaf can share it, and it is a UTXO at the SENTINEL that this statement is looking for,
// which is exactly what a colliding row is most likely to be. Stamping it would hand a UTXO the
// facts of a block that does not contain its transaction, and once this window is dropped there
// is nothing left to correct it from. resetUTXOsSQL carries the identical recheck.
const stampUTXOsSQL = `
WITH hit AS (
    SELECT c.leaf, c.ukey, k.txid, k.h, k.b
      FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[], $5::int[], $6::int[])
           AS k(leaf, txid, lo, hi, h, b)
     CROSS JOIN LATERAL (
       SELECT u.leaf, u.ukey
         FROM utxo u
        WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND u.ukey <= k.hi AND u.txid = k.txid
          AND u.mined_height = 0
       OFFSET 0
     ) AS c
)
UPDATE utxo u SET mined_height = hit.h, block_id = hit.b
  FROM hit
 WHERE u.leaf = hit.leaf AND u.ukey = hit.ukey AND u.txid = hit.txid`

// TxMinedPartitionBlocks is the width of one membership window.
//
// 288, not the journal's 48. A lookup by transaction id with no height probes every live
// window, and six probes at 288 cost about 50 microseconds against 31 at 48 costing 260.
// Nothing needs 48-block drop granularity here.
const TxMinedPartitionBlocks = 288

// ensureTxMinedPartition creates the membership window covering height, if absent.
//
// It MUST be called before the caller opens its transaction, for the reason
// ensureTxBodyPartition must: the DDL needs its own pool connection.
//
// It REFUSES a window at or below the floor. The floor is the highest window ever dropped
// plus one, and a create below it can only be a block re-offered after its window retired.
// Recreating the window would claim every transaction in that block afresh and double every
// UTXO that is still live. Failing loudly here is the guard.
func (s *Store) ensureTxMinedPartition(ctx context.Context, height uint32) error {
	window := height / TxMinedPartitionBlocks

	if s.minedWindow.Load() == window+1 {
		return nil
	}

	s.minedDDL.Lock()
	defer s.minedDDL.Unlock()

	if s.minedWindow.Load() == window+1 {
		return nil
	}

	floor, err := s.txMinedFloor(ctx)
	if err != nil {
		return err
	}

	if window < floor {
		return errors.NewProcessingError("[utxoset] refusing to recreate dropped membership window %d for height %d (floor %d)", window, height, floor)
	}

	lo := window * TxMinedPartitionBlocks
	hi := lo + TxMinedPartitionBlocks

	// Built standalone and attached, so the block path never takes the parent's strongest
	// lock at a window boundary. See ensureAttachedPartition for the measurement behind it.
	child := fmt.Sprintf("tx_mined_w%d", window)
	if err := s.ensureAttachedPartition(ctx, partitionSpec{
		parent: "tx_mined",
		child:  child,
		key:    "mined_height",
		lo:     lo,
		hi:     hi,
		after:  []string{fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN tx_inpoints SET STORAGE EXTERNAL`, child)},
	}); err != nil {
		return errors.NewStorageError("[utxoset] create tx_mined window %d", window, err)
	}

	s.minedWindow.Store(window + 1)

	return nil
}

// txMinedFloor returns the highest dropped window index plus one; 0 when nothing was dropped.
func (s *Store) txMinedFloor(ctx context.Context) (uint32, error) {
	var floor int32
	if err := s.pool.QueryRow(ctx, `SELECT floor FROM tx_mined_floor WHERE id = 0`).Scan(&floor); err != nil {
		return 0, errors.NewStorageError("[utxoset] read tx_mined floor", err)
	}

	return uint32(floor), nil //nolint:gosec // a window index is never negative
}

// txMinedWindowSQL lists the containment windows in whichever of the three crash states they
// are in; see txBodyWindowSQL for the states and why the join is LEFT.
const txMinedWindowSQL = `
SELECT c.relname,
       c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = 'tx_mined'::regclass
 WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'tx_mined'::regclass)
   AND c.relkind  = 'r'
   AND c.relname ~ '^tx_mined_w[0-9]+$'`

// windowState is one containment window's table and which crash state it is in.
type windowState struct {
	name          string
	window        uint32
	attached      bool
	detachPending bool
}

// listTxMinedWindows reads every containment window's table, in ascending window order.
func (s *Store) listTxMinedWindows(ctx context.Context) ([]windowState, error) {
	rows, err := s.pool.Query(ctx, txMinedWindowSQL)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset] list tx_mined windows", err)
	}

	var windows []windowState

	for rows.Next() {
		var w windowState
		if err := rows.Scan(&w.name, &w.attached, &w.detachPending); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("[utxoset] scan tx_mined window", err)
		}

		if _, err := fmt.Sscanf(w.name, "tx_mined_w%d", &w.window); err != nil {
			continue
		}

		windows = append(windows, w)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset] list tx_mined windows", err)
	}

	sort.Slice(windows, func(i, j int) bool { return windows[i].window < windows[j].window })

	return windows, nil
}

// txMinedWindowState is one window's state, or nil when no table of its name exists.
func (s *Store) txMinedWindowState(ctx context.Context, window uint32) (*windowState, error) {
	windows, err := s.listTxMinedWindows(ctx)
	if err != nil {
		return nil, err
	}

	for i := range windows {
		if windows[i].window == window {
			return &windows[i], nil
		}
	}

	return nil, nil
}

// preDropCheckSQL is the last of the four drop conditions: no identity row may still be joined to
// a containment row of the window. After the stamp every row of the window with a non-zero block
// id is a winner, so any row is the right test, and a loser that survived step 1 also blocks the
// drop, loudly. The window is addressed by name so the answer is the same whether it is
// attached, pending detach or already detached.
const preDropCheckSQL = `
SELECT 1
  FROM tx_ident i
 WHERE EXISTS (SELECT 1 FROM %s m WHERE m.txid = i.txid AND m.block_id <> 0 OFFSET 0)
 LIMIT 1`

// detachTimeout bounds the wait for DETACH CONCURRENTLY, which waits for every open transaction
// on the parent. A stamp page is such a transaction, so a drop can wait for at most one page;
// past the timeout the pass stops dropping containment windows and tries again next block. A
// judgement, not a measurement.
const detachTimeout = 10 * time.Second

// dropStampedTxMinedWindows drops every containment window the four-part rule allows at height,
// oldest first, and returns how many. It stops at the first window that fails, because the
// dropped floor is one number and a hole below it must not exist.
//
// Window W drops when all four hold. (a) W has a completion record, written by the stamp when
// it finished W. (b) height is at least stamped_at(W) plus the longest an undo copy can live,
// 1,728 blocks: a UTXO of W spent before its stamp left an undo copy at (0,0), and the window is
// the only thing that can answer for it while that copy lives. (c) No attached undo partition
// covers any height below stamped_at(W), read from the catalog rather than inferred from the
// height, because an undo drop can fail or be skipped; this is what keeps (b) true with a late
// pruner. (d) The pre-drop check finds no identity row joined to a row of W.
//
// A window already detached, or pending detach, passed the rule in an earlier pass that was
// interrupted; it is finished without judging the rule again, because none of the four
// conditions can become false once it held.
//
// The retain-indefinitely setting skips every containment drop. The stamp still runs.
func (s *Store) dropStampedTxMinedWindows(ctx context.Context, height uint32) (int, error) {
	if s.retainIndefinitely {
		return 0, nil
	}

	windows, err := s.listTxMinedWindows(ctx)
	if err != nil {
		return 0, err
	}

	records, err := s.completionRecords(ctx)
	if err != nil {
		return 0, err
	}

	undoFloor, err := s.oldestUndoPartitionStart(ctx)
	if err != nil {
		return 0, err
	}

	dropped := 0

	for _, w := range windows {
		wLo := w.window * TxMinedPartitionBlocks

		if w.attached && !w.detachPending {
			stampedAt, ok := records[wLo]
			if !ok {
				return dropped, nil
			}

			if height < stampedAt+undoMaxLifeBlocks {
				return dropped, nil
			}

			if undoFloor.attached && undoFloor.start < stampedAt {
				dropHeldByUndo.Inc()

				return dropped, nil
			}

			refused, err := s.preDropCheck(ctx, w.name)
			if err != nil {
				return dropped, err
			}

			if refused {
				dropRefused.Inc()
				s.logger.Errorf("[utxoset] refusing to drop window %s: an identity row is still joined to one of its containment rows, so the stamp missed it", w.name)

				return dropped, nil
			}
		}

		switch {
		case w.detachPending:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_mined DETACH PARTITION %s FINALIZE`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] finalize detach of tx_mined window %s", w.name, err)
			}

			dropDetachRecovered.Inc()

		case w.attached:
			// On its own connection with its own deadline, because it waits for every open
			// transaction on the parent. A timeout is a skip, not an error of the session.
			dctx, cancel := context.WithTimeout(ctx, detachTimeout)
			_, err := s.pool.Exec(dctx,
				fmt.Sprintf(`ALTER TABLE tx_mined DETACH PARTITION %s CONCURRENTLY`, w.name))
			cancel()

			if err != nil {
				if errors.Is(dctx.Err(), context.DeadlineExceeded) {
					dropDetachWaits.Inc()
					s.logger.Warnf("[utxoset] detach of window %s did not finish within %s; it is retried next block", w.name, detachTimeout)

					return dropped, nil
				}

				return dropped, errors.NewStorageError("[utxoset] detach tx_mined window %s", w.name, err)
			}

		default:
			// Already standalone after an interrupted session: finish the job.
			dropDetachRecovered.Inc()
		}

		if s.dropHook != nil {
			s.dropHook(w.name)
		}

		// The drop, the completion record's delete and the floor advance are ONE statement,
		// not three Execs. All three run inside postgres's implicit transaction for a
		// multi-statement Exec, and DROP TABLE is fully transactional, so a crash between them
		// cannot happen. Two separate calls would let a crash drop the window for good while
		// the floor stayed pointed below it, and ensureTxMinedPartition would then recreate
		// the very window this loop just destroyed, doubling every UTXO still claimed by a
		// transaction in it. Every statement is idempotent, and the delete tolerates a
		// completion record that is already gone, which is what lets an interrupted drop be
		// finished without judging the rule again. window is a regex-filtered catalog name, so
		// folding it into the literal carries no injection risk.
		ddl := fmt.Sprintf(`DROP TABLE IF EXISTS %[1]s;
DELETE FROM tx_mined_stamped WHERE window_start = %[2]d;
UPDATE tx_mined_floor SET floor = GREATEST(floor, %[3]d) WHERE id = 0;`, w.name, wLo, w.window+1)

		if _, err := s.pool.Exec(ctx, ddl); err != nil {
			return dropped, errors.NewStorageError("[utxoset] drop tx_mined window %s and advance its floor", w.name, err)
		}

		dropped++

		// The ensure cache can be holding the window just destroyed, and would then let a
		// later create at that height skip the floor read entirely. Clear it so the next
		// ensure re-reads the floor and refuses loudly instead of failing on a missing
		// partition. See ensureTxMinedPartition.
		s.minedWindow.Store(0)
	}

	return dropped, nil
}

// preDropCheck reports whether any identity row is still joined to a containment row of the
// named window.
func (s *Store) preDropCheck(ctx context.Context, window string) (bool, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(preDropCheckSQL, window))
	if err != nil {
		return false, errors.NewStorageError("[utxoset] pre-drop check of %s", window, err)
	}

	defer rows.Close()

	found := rows.Next()

	return found, rows.Err()
}

// undoPartitionFloor is the first height of the oldest undo partition that is attached or
// pending detach, and whether there is one. A partition already fully detached and waiting for
// its DROP is ignored: Unspend reads undo copies through the parent table and cannot see it.
type undoPartitionFloor struct {
	attached bool
	start    uint32
}

func (s *Store) oldestUndoPartitionStart(ctx context.Context) (undoPartitionFloor, error) {
	leaves, err := s.listPartitionLeaves(ctx, "spend_journal")
	if err != nil {
		return undoPartitionFloor{}, err
	}

	var out undoPartitionFloor

	for _, l := range leaves {
		if !l.attached && !l.detachPending {
			continue
		}

		start := l.leaf * SpendJournalPartitionBlocks
		if !out.attached || start < out.start {
			out = undoPartitionFloor{attached: true, start: start}
		}
	}

	return out, nil
}
