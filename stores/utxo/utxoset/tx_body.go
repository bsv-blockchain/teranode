package utxoset

import (
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/teranode/errors"
)

// TxBodyPartitionBlocks is the width of one body window. Reclaim drops whole windows, so
// retention is granular to this. It matches the spend journal's width deliberately: the two
// are reclaimed by the same pruner session and there is no reason for them to disagree.
const TxBodyPartitionBlocks = 48

// DefaultTxBodyRetentionBlocks is how long the serialized bytes are kept.
//
// 288, which is global_blockHeightRetention, and it is not a number this store chose. It is
// the horizon at which subtree files are deleted, past which the node physically cannot
// un-mine a block because the un-mine path warns and skips a missing subtree file. Choosing
// 144 instead would not create a new failure class, it would narrow a range that already
// works and make depths 145 to 287 fail quietly inside a band where the rest of the node
// still functions.
const DefaultTxBodyRetentionBlocks = 288

// ensureTxBodyPartition creates the body window covering height, if absent.
//
// It MUST be called before the caller opens its transaction, for the same reason
// ensureSpendJournalPartition must: the DDL needs its own pool connection, and taking one
// while holding a transaction borrowed from the same pool is a nested acquire. At
// pool_max_conns concurrent writers every connection ends up held by a transaction waiting
// for a connection, and that deadlock has no timeout.
func (s *Store) ensureTxBodyPartition(ctx context.Context, height uint32) error {
	window := height / TxBodyPartitionBlocks

	// Only touch the catalog when the window actually changes. The cache holds window+1 so
	// its zero value means "nothing cached yet" rather than "window 0 is already created",
	// which would make window 0 permanently unreachable on a fresh store.
	if s.bodyWindow.Load() == window+1 {
		return nil
	}

	s.bodyDDL.Lock()
	defer s.bodyDDL.Unlock()

	if s.bodyWindow.Load() == window+1 {
		return nil
	}

	lo := window * TxBodyPartitionBlocks
	hi := lo + TxBodyPartitionBlocks

	// raw_tx is forced out of line on each partition. Keep it for the genuinely large tail,
	// but NOT for the reason the design doc gave: an inline body is not rewritten by an
	// update of another column, because postgres writes only what changed working in from
	// both ends of the row. Do not set toast_tuple_target to force more of it out -- at 128
	// the toaster does not stop at raw_tx, it keeps going and externalises txid too.
	//
	// Built standalone and attached, so the block path never takes the parent's strongest
	// lock at a window boundary. See ensureAttachedPartition.
	child := fmt.Sprintf("tx_body_w%d", window)
	if err := s.ensureAttachedPartition(ctx, partitionSpec{
		parent: "tx_body",
		child:  child,
		key:    "created_height",
		lo:     lo,
		hi:     hi,
		after:  []string{fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN raw_tx SET STORAGE EXTERNAL`, child)},
	}); err != nil {
		return errors.NewStorageError("[utxoset] create tx_body window %d", window, err)
	}

	// Only cache once the DDL has actually succeeded, or a transient failure becomes
	// permanent: every later write would see a hit, skip the retry, and fail on a partition
	// that was never created.
	s.bodyWindow.Store(window + 1)

	return nil
}

// txBodyWindowSQL lists the body windows this store owns, in whichever state they are in.
//
// This used to INNER JOIN pg_inherits, which made it blind to the very state a crash produces.
// The three states, all verified against PostgreSQL rather than assumed, and all of them handled
// twelve lines away in spend_journal.go for the journal's own partitions:
//
//   - ATTACHED. The normal case.
//   - ORPHANED. A crash between DETACH and DROP leaves a fully standalone table: relispartition
//     goes FALSE and the pg_inherits row is GONE. An inner join can never see it again, so its
//     disk is never returned and nothing reports it. Found here by name within tx_body's own
//     schema, which is all that still identifies it.
//   - DETACH PENDING. A crash DURING a concurrent detach leaves inhdetachpending set. PostgreSQL
//     then refuses every further ATTACH and DETACH on the parent, so the detach below fails on
//     every call, dropTxBodyWindowsBelow returns an error, and Prune returns before it reaches
//     the journal loop at all. That is the WHOLE cleanup stopped, not one table leaked, and it
//     would also fail the CREATE TABLE ... PARTITION OF that the create path runs at each
//     48-block rollover.
//
// Resolved through regclass so it finds windows of the SAME table the DROP below will name.
// Matching on a bare name would search every schema in the database, and an unqualified drop
// would then resolve against the search path and fail, aborting the loop before a single real
// window went.
const txBodyWindowSQL = `
SELECT c.relname,
       c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = 'tx_body'::regclass
 WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'tx_body'::regclass)
   AND c.relkind  = 'r'
   AND c.relname ~ '^tx_body_w[0-9]+$'`

// dropTxBodyWindowsBelow discards the serialized transaction bytes that have aged out.
//
// The height passed in is the pruner service's clock, which by default is the last height the
// block persister has archived rather than the chain tip. That matters: the persister is the
// only producer of the permanent archive for a block this node mined, and the store's copy of
// the bytes is its only source. Dropping ahead of it would wedge it permanently, with no
// fallback. If pruner_force_ignore_block_persister_height is ever set, that protection is
// gone and this becomes unsafe.
//
// Before any window goes, the bodies of transactions still waiting to be mined are carried into
// tx_body_carry (carryBodiesAhead), and after the drops the carried bodies that are no longer
// needed are deleted (reclaimCarriedBodies). A carry that fails stops the pass before anything
// is dropped: a dropped body cannot be brought back.
func (s *Store) dropTxBodyWindowsBelow(ctx context.Context, height uint32) (int, error) {
	carried, err := s.carryBodiesAhead(ctx, height, false)
	if err != nil {
		return 0, err
	}

	if height <= s.bodyRetention {
		return 0, nil
	}

	cutoff := (height - s.bodyRetention) / TxBodyPartitionBlocks

	rows, err := s.pool.Query(ctx, txBodyWindowSQL)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_body windows", err)
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
			return 0, errors.NewStorageError("[utxoset] scan tx_body window", err)
		}

		// A name carrying no window number is not one of ours. Parsing it here rather than in
		// the drop loop keeps it in one place, so the ordering below and the cutoff test can
		// never disagree about which window a table is.
		if _, err := fmt.Sscanf(w.name, "tx_body_w%d", &w.window); err != nil {
			continue
		}

		windows = append(windows, w)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset] list tx_body windows", err)
	}

	// Oldest first, for the reason the journal's own loop sorts: the listing query has no
	// ORDER BY, so the catalog returns whatever order it scanned, and with a backlog that
	// decides which disk comes back first and whether the oldest surviving window is a usable
	// progress measure at all.
	sort.Slice(windows, func(i, j int) bool { return windows[i].window < windows[j].window })

	// A drop is due, so carry again unless this pass already did: a transaction un-mined by a
	// reorg since the window entered the carry band is waiting again and needs its body.
	if !carried && len(windows) > 0 && windows[0].window < cutoff {
		if _, err := s.carryBodiesAhead(ctx, height, true); err != nil {
			return 0, err
		}
	}

	dropped := 0

	for _, w := range windows {
		if w.window >= cutoff {
			continue
		}

		switch {
		case w.detachPending:
			// FINALIZE is the only way out of this state, and until it runs no other window of
			// this table can be detached either, and no new one can be created.
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_body DETACH PARTITION %s FINALIZE`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] finalize detach of tx_body window %s", w.name, err)
			}

		case w.attached:
			// Detach without blocking readers, then drop the now-standalone table. A bare drop
			// on an attached partition briefly takes an exclusive lock on the parent, which
			// would stall every concurrent create.
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE tx_body DETACH PARTITION %s CONCURRENTLY`, w.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] detach tx_body window %s", w.name, err)
			}

		default:
			// Already standalone: a previous session was interrupted between its DETACH and its
			// DROP. Nothing to detach, just finish the job.
		}

		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, w.name)); err != nil {
			return dropped, errors.NewStorageError("[utxoset] drop tx_body window %s", w.name, err)
		}

		dropped++
	}

	if err := s.reclaimCarriedBodies(ctx, height); err != nil {
		return dropped, err
	}

	return dropped, nil
}

// carryBodiesAhead runs carryUnminedBodies for every window that drops within one window of
// the tip, once per new window, or now when force is set. It reports whether it ran.
//
// The bound is measured from the TIP, one window ahead of the drop, and that is what makes it
// safe against the persister lagging. The drop is measured from the pruner's height, which is
// the persister's archived height, so a waiting transaction mined in a block the persister
// has not reached yet has already lost its unmined marker by the time its window drops. A
// carry that looked only at drop time would let that body go and wedge the persister on the
// block. Carried while the tip enters the band, the transaction was still waiting, or it was
// mined at or below that tip, which is at least one window below the height its window
// drops at, so the persister has archived it by then.
//
// The band is entered once per 48 blocks, and that is the only time the unmined set is read
// for it; the drop pass reads it a second time (see dropTxBodyWindowsBelow).
func (s *Store) carryBodiesAhead(ctx context.Context, height uint32, force bool) (bool, error) {
	tip := max(s.GetBlockHeight(), height)

	if tip+TxBodyPartitionBlocks <= s.bodyRetention {
		return false, nil
	}

	// Every window below bound drops once the tip is one more window along.
	bound := (tip + TxBodyPartitionBlocks - s.bodyRetention) / TxBodyPartitionBlocks
	if bound == 0 || (!force && s.carryBound.Load() == bound+1) {
		return false, nil
	}

	if _, err := s.carryUnminedBodies(ctx, bound*TxBodyPartitionBlocks); err != nil {
		return false, err
	}

	// Cached only once the copy has committed, so a failure is retried on the next pass.
	s.carryBound.Store(bound + 1)

	return true, nil
}

// carryUnminedBodySQL copies, into tx_body_carry, the body of every transaction still waiting
// to be mined whose body lives below $1, a height.
//
// "Waiting" is the identity row's unmined marker, the predicate copyForwardUnminedSpends uses
// (unminedInpointsSQL), and the scan is driven by the marker's partial index for the same
// reason: it reads the waiting population and nothing else, whatever the size of the windows.
//
// Crash safety. The copy commits on its own and the drop comes later, so a crash between them
// carries the same window again on the next pass. The NOT EXISTS skips a transaction already
// carried before its body is read, and ON CONFLICT is the backstop: the table's key is the
// txid, so a second copy cannot be a second row. Until the window drops a carried body exists
// twice, once in its window and once here, and the reader takes the window's and consults this
// table only when that is gone (readCarriedBodies), so two copies are never two answers.
//
// The body is read through the parent tx_body, so only an attached window is carried from.
// That is enough: a window is detached only by a pass that has already carried it.
const carryUnminedBodySQL = `
INSERT INTO tx_body_carry (txid, raw_tx)
SELECT i.txid, b.raw_tx
  FROM tx_ident i
 CROSS JOIN LATERAL (
   SELECT b.raw_tx
     FROM tx_body b
    WHERE b.created_height = i.created_height
      AND b.txid = i.txid
   OFFSET 0
 ) AS b
 WHERE i.off_chain_since IS NOT NULL
   AND i.created_height < $1::int
   AND b.raw_tx IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM tx_body_carry c WHERE c.txid = i.txid)
ON CONFLICT (txid) DO NOTHING`

// carryUnminedBodies carries the body of every waiting transaction created below bound into
// tx_body_carry, and returns how many it copied. See carryUnminedBodySQL.
func (s *Store) carryUnminedBodies(ctx context.Context, bound uint32) (int64, error) {
	tag, err := s.pool.Exec(ctx, carryUnminedBodySQL, int32(bound)) //nolint:gosec // a height fits int32
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] carry bodies of unmined transactions below height %d", bound, err)
	}

	if n := tag.RowsAffected(); n > 0 {
		s.logger.Infof("[utxoset] carried the bodies of %d unmined transactions created below height %d past their window", n, bound)
	}

	return tag.RowsAffected(), nil
}

// reclaimCarriedBodySQL deletes the carried bodies nothing needs any more: the transaction is
// not waiting, and no block that contains it is above $1, the pruner's height less the body
// horizon. A carried transaction therefore keeps its body while it waits and for 288 blocks
// after it is mined, and a transaction deleted outright loses it here too.
//
// Both tests read one snapshot, and SetMined records the block and clears the marker in one
// transaction, so a transaction being mined as this runs matches one of them either way.
const reclaimCarriedBodySQL = `
DELETE FROM tx_body_carry c
 WHERE NOT EXISTS (
       SELECT 1 FROM tx_ident i
        WHERE i.leaf = (get_byte(c.txid, 0) & 7)::smallint
          AND i.txid = c.txid
          AND i.off_chain_since IS NOT NULL)
   AND NOT EXISTS (
       SELECT 1 FROM tx_mined m
        WHERE m.txid = c.txid
          AND m.mined_height > $1::int)`

// reclaimCarriedBodies applies reclaimCarriedBodySQL at the pruner height.
func (s *Store) reclaimCarriedBodies(ctx context.Context, height uint32) error {
	if height <= s.bodyRetention {
		return nil
	}

	tag, err := s.pool.Exec(ctx, reclaimCarriedBodySQL, int32(height-s.bodyRetention)) //nolint:gosec // a height fits int32
	if err != nil {
		return errors.NewStorageError("[utxoset] reclaim carried bodies at height %d", height, err)
	}

	if n := tag.RowsAffected(); n > 0 {
		s.logger.Infof("[utxoset] dropped %d carried bodies of transactions mined %d or more blocks ago", n, s.bodyRetention)
	}

	return nil
}
