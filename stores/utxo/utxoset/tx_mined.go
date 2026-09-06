package utxoset

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/teranode/errors"
)

// stampRetiringWindowChunk is the page size for reading a retiring window's rows. 20,000 keeps
// one round trip's argument arrays and the chunk's own working set modest, while still being
// far wider than an ordinary block, so a window with millions of transactions is read in a
// bounded number of round trips rather than one per transaction.
const stampRetiringWindowChunk = 20_000

// retiringWindowRowsSQL lists a window's rows in (txid, seq) order, to name WHICH transactions
// the coin stamp has to visit. It deliberately does not read mined_height or block_id: this
// window's own row is not necessarily the transaction's earliest, so the block facts come from
// firstMinedRowSQL instead.
//
// Paged by a KEYSET on (txid, seq) rather than OFFSET, so the plan stays an index scan at every
// page rather than degrading as the offset grows: an OFFSET-based page has to skip every row
// before it, which is O(rows read so far) per page over a window that can hold tens of millions
// of rows. Ordering by txid also keeps one transaction's rows contiguous across page
// boundaries, which is what makes the dedup below correct.
const retiringWindowRowsSQL = `
SELECT txid, seq FROM %[1]s
 WHERE (txid, seq) > ($1::bytea, $2::bigint)
 ORDER BY txid, seq
 LIMIT $3`

// firstMinedRowSQL resolves, for each listed transaction, the block named by its EARLIEST
// membership row across EVERY LIVE WINDOW -- not just the one that is retiring.
//
// It has to look across windows because a transaction's rows do not all live in one. A window
// is keyed by mined_height, and a transaction mined at height h can be fork-stamped at h-1 or
// h+1 by appendMinedSQL, which straddles a 288 boundary about one block in 288. The older
// window then retires first, and stamping from the row IT holds would hand the coin a FORK
// block whenever that is the row it has -- permanently, because the mined_height = 0 guard
// skips the coin when the other window retires and nothing survives to correct it from.
//
// The earliest row by seq is the transaction's LONGEST-CHAIN stamp, and that is a rule rather
// than an ordering accident. Since task 9 a transaction only reaches the membership table by a
// longest-chain stamp or a block-path create; a fork stamp gets there only by appendMinedSQL,
// which copies an EXISTING row's payload, so it can never be the first row. seq is a table-wide
// identity, so "earliest" is well defined across windows without comparing heights -- which
// would be the wrong test anyway, as this statement's own fork case shows.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, the shape minedByTxidSQL and
// appendMinedSQL use: one primary-key descent per key per live window. A plain
// `JOIN tx_mined m ON m.txid = ANY(...)` lets the planner hash the keys against the windows and
// read them whole, which is what the measurements behind minedByTxidSQL found at 500 keys.
const firstMinedRowSQL = `
SELECT k.txid, m.mined_height, m.block_id
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT m.mined_height, m.block_id
     FROM tx_mined m
    WHERE m.txid = k.txid
    ORDER BY m.seq
    LIMIT 1 OFFSET 0
 ) AS m`

// stampCoinsSQL stamps every live (unconfirmed) coin of the listed transactions with the
// block their FIRST surviving tx_mined row named. See liveCoinArgs for the leaf/lo/hi shape;
// $5 and $6 are the parallel height and block-id arrays keyed to the same transaction.
//
// mined_height = 0 is what makes this idempotent and leaves a block-path coin -- stamped with
// its facts at create, never at the sentinel -- untouched. A coin that has already been
// stamped this way, or created directly by the block path, must not be touched a second time:
// nothing here can tell "already correct" from "a later, wrong window" apart except that guard.
//
// The fenced read runs FIRST, in a CTE, and the UPDATE then matches on the exact (leaf, ukey)
// it returns -- the same shape resetCoinsSQL uses, and for the identical reason. A plain
// `UPDATE ... FROM unnest(...) AS k WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND ...` was
// measured first, on 40,000 seeded coins across all eight leaves with 500 keys: a Hash Join
// against a Seq Scan of every one of the eight coin partitions, because the planner can build
// a hash on (leaf, txid) from the whole table more cheaply than it can cost 500 per-key index
// probes, and the ukey range only applies as a post-join filter rather than an index
// condition. The CROSS JOIN LATERAL with an OFFSET 0 fence is what forces an index scan per
// key instead: an UPDATE cannot laterally reference its own target, so the read has to happen
// in a CTE the UPDATE then joins on the row identity it found, not on the search predicate.
//
// EVERY live coin of the transaction is stamped, not just one, so the LATERAL carries no
// LIMIT -- unlike coinFactsSQL, which only ever needs one.
//
// The UPDATE rechecks the FULL TXID and not only the (leaf, ukey) the read found the row by.
// ukey is a 96-bit prefix and NON-UNIQUE by design -- see Pack -- so a stranger's coin in the
// same leaf can share it, and it is a coin at the SENTINEL that this statement is looking for,
// which is exactly what a colliding row is most likely to be. Stamping it would hand a coin the
// facts of a block that does not contain its transaction, and once this window is dropped there
// is nothing left to correct it from. resetCoinsSQL carries the identical recheck.
const stampCoinsSQL = `
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
// coin that is still live. Failing loudly here is the guard.
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

	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS tx_mined_w%[1]d PARTITION OF tx_mined
  FOR VALUES FROM (%[2]d) TO (%[3]d);
ALTER TABLE tx_mined_w%[1]d ALTER COLUMN tx_inpoints SET STORAGE EXTERNAL;`, window, lo, hi)

	if _, err := s.pool.Exec(ctx, ddl); err != nil {
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

// txMinedWindowSQL lists the membership windows in whichever of the three crash states they
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

// dropTxMinedWindowsBelow drops every membership window whose upper bound is below
// cutoffHeight, oldest first, and advances the floor past each. Returns the count dropped.
//
// This IS identity reclaim in this design: no work list, no probes, no row deletes. The
// coins of transactions in a retiring window are stamped from the window's list first in
// stage 2; in stage 1 every coin was written with its block facts at create.
func (s *Store) dropTxMinedWindowsBelow(ctx context.Context, cutoffHeight uint32) (int, error) {
	cutoff := cutoffHeight / TxMinedPartitionBlocks

	rows, err := s.pool.Query(ctx, txMinedWindowSQL)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_mined windows", err)
	}

	type windowState struct {
		name          string
		window        uint32
		attached      bool
		detachPending bool
	}

	var windows []windowState

	for rows.Next() {
		var w windowState
		if err := rows.Scan(&w.name, &w.attached, &w.detachPending); err != nil {
			rows.Close()
			return 0, errors.NewStorageError("[utxoset] scan tx_mined window", err)
		}

		if _, err := fmt.Sscanf(w.name, "tx_mined_w%d", &w.window); err != nil {
			continue
		}

		windows = append(windows, w)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_mined windows", err)
	}

	sort.Slice(windows, func(i, j int) bool { return windows[i].window < windows[j].window })

	dropped := 0

	for _, w := range windows {
		if w.window >= cutoff {
			continue
		}

		// The coin stamp runs BEFORE the detach, and a failure here returns without touching
		// the window at all. A window must never be dropped while any of its transactions'
		// surviving coins still carry the sentinel: once the window is gone, its rows are the
		// only place that block fact lived, and readCoinFacts would then answer with mined
		// height 0 for a parent that really does have a block.
		txCount, coinCount, err := s.stampRetiringWindowCoins(ctx, w.name)
		if err != nil {
			return dropped, err
		}

		s.logger.Infof("[utxoset] stamped %d live coins of %d transactions before dropping tx_mined window %s",
			coinCount, txCount, w.name)

		switch {
		case w.detachPending:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_mined DETACH PARTITION %s FINALIZE`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] finalize detach of tx_mined window %s", w.name, err)
			}

		case w.attached:
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_mined DETACH PARTITION %s CONCURRENTLY`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] detach tx_mined window %s", w.name, err)
			}

		default:
			// Already standalone after an interrupted session: finish the job.
		}

		// The drop and the floor advance are ONE statement, not two Execs. Both run inside
		// postgres's implicit transaction for a multi-statement Exec, and DROP TABLE is
		// fully transactional, so a crash or connection drop between them cannot happen: it
		// either lands with both effects or neither. Two separate calls would let a crash in
		// between drop the window from the catalog for good -- gone, so it never resurfaces
		// in txMinedWindowSQL's listing to retry -- while the floor stayed pointed below it,
		// and ensureTxMinedPartition would then recreate the very window this loop just
		// destroyed, doubling every coin still claimed by a transaction in it. window is a
		// regex-filtered catalog name (^tx_mined_w[0-9]+$), so folding it into the literal
		// with Sprintf carries no injection risk.
		ddl := fmt.Sprintf(`DROP TABLE IF EXISTS %[1]s;
UPDATE tx_mined_floor SET floor = GREATEST(floor, %[2]d) WHERE id = 0;`, w.name, w.window+1)

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

// stampRetiringWindowCoins is the lazy coin stamp: it reads the distinct transactions of the
// window about to be detached and stamps each one's still-unconfirmed coins with the block its
// EARLIEST membership row names, resolved across every live window. Returns the number of
// distinct transactions read and the number of coin rows the stamp touched.
//
// It reads in pages of stampRetiringWindowChunk rows, oldest (txid, seq) first, rather than one
// query for the whole window: a window can hold a full 288 blocks of mainnet membership, tens
// of millions of rows, and a single unbounded read would hold that whole result set in memory
// and in one round trip.
//
// Keyset pagination on (txid, seq) rather than OFFSET, so the plan is an index scan on every
// page and does not degrade as the window is worked through: an OFFSET-based page N has to skip
// the N-1 pages before it, at O(rows read so far) per page.
//
// The window's rows say WHICH transactions to stamp; firstMinedRowSQL says with WHAT. A
// transaction's rows are NOT all in one window -- see firstMinedRowSQL -- so the block cannot be
// taken from the retiring window's own row without risking a fork block on the coin. The two
// reads are separate for that reason, not for tidiness.
//
// Within this window a transaction's rows still sort together, because the pages are ordered by
// (txid, seq), so a transaction straddling a page boundary is deduplicated correctly:
// lastStampedTxid carries across pages rather than resetting with each one.
//
// The one case this cannot get right is a transaction whose FIRST row is a fork stamp, and it
// cannot arise: a fork stamp on a transaction that still has an identity row rewrites that row
// and stays in the mempool table, and a fork stamp on one that does not can only append to a
// membership row that already exists. So there is nothing to test there, and nothing to guard.
func (s *Store) stampRetiringWindowCoins(ctx context.Context, window string) (txCount, coinCount int, err error) {
	var (
		lastTxid       = []byte{}
		lastSeq  int64 = -1
		// lastStampedTxid is the most recent transaction id this window has already
		// collected a stamp for. Rows for one transaction are contiguous in (txid, seq)
		// order, so equality with the immediately preceding row's txid is enough to skip
		// every row after its first.
		lastStampedTxid []byte
	)

	query := fmt.Sprintf(retiringWindowRowsSQL, window)

	for {
		rows, qerr := s.pool.Query(ctx, query, lastTxid, lastSeq, stampRetiringWindowChunk)
		if qerr != nil {
			return txCount, coinCount, errors.NewStorageError("[utxoset] read retiring window %s", window, qerr)
		}

		var (
			txids [][]byte
			n     int
		)

		for rows.Next() {
			var (
				txid []byte
				seq  int64
			)

			if serr := rows.Scan(&txid, &seq); serr != nil {
				rows.Close()
				return txCount, coinCount, errors.NewStorageError("[utxoset] scan retiring window %s", window, serr)
			}

			n++
			lastTxid = txid
			lastSeq = seq

			// Only the first row seen for a transaction is its earliest stamp.
			if lastStampedTxid != nil && bytes.Equal(txid, lastStampedTxid) {
				continue
			}

			lastStampedTxid = txid
			txCount++

			txids = append(txids, txid)
		}

		rerr := rows.Err()

		rows.Close()

		if rerr != nil {
			return txCount, coinCount, errors.NewStorageError("[utxoset] read retiring window %s", window, rerr)
		}

		if len(txids) > 0 {
			stamped, serr := s.stampCoinsOf(ctx, txids)
			if serr != nil {
				return txCount, coinCount, errors.NewStorageError("[utxoset] stamp coins for retiring window %s", window, serr)
			}

			coinCount += stamped
		}

		if n < stampRetiringWindowChunk {
			break
		}
	}

	return txCount, coinCount, nil
}

// stampCoinsOf resolves each transaction's earliest membership row across every live window and
// stamps its still-unconfirmed coins with that row's block.
//
// A transaction whose rows have all gone -- nothing can produce that here, since the retiring
// window has not been detached yet -- is simply absent from the resolve and is not stamped, the
// CROSS JOIN LATERAL being an inner join.
func (s *Store) stampCoinsOf(ctx context.Context, txids [][]byte) (int, error) {
	rows, err := s.pool.Query(ctx, firstMinedRowSQL, txids)
	if err != nil {
		return 0, err
	}

	var (
		found   [][]byte
		heights []int32
		blockID []int32
	)

	for rows.Next() {
		var (
			txid   []byte
			height int32
			block  int32
		)

		if serr := rows.Scan(&txid, &height, &block); serr != nil {
			rows.Close()

			return 0, serr
		}

		found = append(found, txid)
		heights = append(heights, height)
		blockID = append(blockID, block)
	}

	rerr := rows.Err()

	rows.Close()

	if rerr != nil {
		return 0, rerr
	}

	if len(found) == 0 {
		return 0, nil
	}

	leaves, ids, los, his := liveCoinArgs(found)

	tag, err := s.pool.Exec(ctx, stampCoinsSQL, leaves, ids, los, his, heights, blockID)
	if err != nil {
		return 0, err
	}

	return int(tag.RowsAffected()), nil
}
