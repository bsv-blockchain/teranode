package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// THE FENCE. Below stamp_fence, containment is read-only: the stamp has raised the fence and
// deleted the window's losing rows in one transaction, so from the moment a writer can see the
// raised fence, a row that exists below it is a winner and a row that does not exist is not
// (invariant I3). Nothing may add a row on the longest chain there, and nothing may delete one.
//
// Four writers take the fence lock SHARED as the first statement of their transaction: the
// block-path create, record mined, the un-mine and Unspend. Shared holders do not block each
// other. The stamp's first step takes it EXCLUSIVE for one short transaction, so once granted
// every earlier containment write has committed and its loser scan is exact. A transaction-level
// advisory lock is released at commit or rollback, so it cannot leak. The writer then reads the
// fence as its own second statement: the store runs at read committed, under which a read
// folded into the lock statement would see the fence as it was before the lock was granted.
//
// Spends never take it. Neither does the unmined create, mark-on or mark-off: none of them
// writes containment.
//
// Every fenced decision below is judged after the lock is granted and the fence is read. The
// dropped test comes first: "dropped" means the height is below the dropped floor, "fenced"
// means at or above it and below the fence.

// fenceState is what a writer sees after taking the fence lock shared.
type fenceState struct {
	droppedFloor uint32 // a HEIGHT: 288 x the stored window number
	fence        uint32 // a HEIGHT
}

// dropped reports whether height h is in a window that has been dropped.
func (f fenceState) dropped(h uint32) bool { return h < f.droppedFloor }

// fenced reports whether height h is below the fence and not dropped.
func (f fenceState) fenced(h uint32) bool { return h >= f.droppedFloor && h < f.fence }

// takeFenceShared takes the fence lock shared, as its own statement, then reads the fence.
func (s *Store) takeFenceShared(ctx context.Context, dbTx pgx.Tx) (fenceState, error) {
	if _, err := dbTx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, $2)`, int32(fenceLockKey1), int32(fenceLockKey2)); err != nil {
		return fenceState{}, errors.NewStorageError("[utxoset][fence] take shared", err)
	}

	floors, err := readFloors(ctx, dbTx)
	if err != nil {
		return fenceState{}, err
	}

	return fenceState{droppedFloor: floors.DroppedFloor, fence: floors.StampFence}, nil
}

// fencedRowsSQL counts the caller's transaction ids that have a row for one block. The height
// is a scalar, so the planner reads one partition: a point read reached only by a replay or a
// deep fork block.
const fencedRowsSQL = `
SELECT count(*) FROM tx_mined m
 WHERE m.txid = ANY($1::bytea[])
   AND m.mined_height = $2::int
   AND m.block_id = $3::int`

// fencedRowCount is the one count the three fenced decisions come from. n distinct ids in the
// call: a count of n means every row exists, zero means none does, anything between is a mixed
// batch, which is always the boundary error. A mixed batch is never split into a part that
// succeeds and a part that fails, because a half-applied record-mined call is exactly the state
// the mined-status worker cannot recover from.
func fencedRowCount(ctx context.Context, q querier, txids [][]byte, height, blockID uint32) (int64, error) {
	var n int64

	if err := q.QueryRow(ctx, fencedRowsSQL, txids, int32(height), int32(blockID)).Scan(&n); err != nil { //nolint:gosec // heights and ids fit int32
		return 0, errors.NewStorageError("[utxoset][fence] count rows below the fence", err)
	}

	return n, nil
}

// boundaryError is the refusal of a containment write that would change a stamped window.
// The design asks for one named error code that crosses gRPC so the receiver can tell it from
// a storage error and escalate; that code, and the escalation, belong with the cross-service
// half of the boundary and are not built here. Until then it is a storage error carrying a
// fixed token, counted by the site that raised it.
func boundaryError(site, format string, args ...any) error {
	boundaryRefusals.WithLabelValues(site).Inc()

	return errors.NewStorageError("[utxoset][boundary][%s] "+format, append([]any{site}, args...)...)
}

var (
	boundaryRefusals = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_boundary_refusals_total",
		Help: "Containment writes refused because they would add or delete a row in a window the stamp has fenced, by site",
	}, []string{"site"})

	// fenceNoops counts the quiet outcomes below the fence: an off-chain insert not made, an
	// un-mine of rows that do not exist, and a write skipped in a dropped window. Each returns
	// success so that a deep fork block's mined status can settle instead of retrying for ever.
	fenceNoops = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_fence_noops_total",
		Help: "Containment writes below the fence that were skipped and returned success, by kind",
	}, []string{"kind"})

	createAheadOfTip = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_create_ahead_of_tip_total",
		Help: "Block-path creates applying a block more than 287 heights above the store's height, which the window drop rule assumes never happens",
	})

	unspendRepaired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_unspend_repaired_total",
		Help: "UTXOs restored from a (0,0) undo copy with the pair of their one surviving containment row below the fence",
	})
	unspendRepairNoSource = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_unspend_repair_no_source_total",
		Help: "Unspend calls rolled back because a (0,0) undo copy had no containment row below the fence and no identity row",
	})
	unspendRepairAmbiguous = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_unspend_repair_ambiguous_total",
		Help: "Unspend calls rolled back because a (0,0) undo copy had more than one containment row below the fence",
	})

	lookupTier2Keys = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_lookup_tier2_keys_total",
		Help: "Transactions sent to the second read tier, by the trigger that sent them",
	}, []string{"trigger"})
	lookupTier2Answered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_lookup_tier2_answered_total",
		Help: "Transactions the second read tier answered with a containment row below the lookup floor",
	})
	lookupTier2Empty = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_lookup_tier2_empty_total",
		Help: "Transactions the second read tier found no containment row for",
	})
	lookupI1Violations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_lookup_i1_violations_total",
		Help: "A UTXO or undo copy at (0,0) with no identity row and no containment row in one snapshot: nothing can ever stamp it",
	})

	preserveNoSource = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_preserve_no_source_total",
		Help: "Parents named for preservation with no containment row, no identity row, no live UTXO and no undo copy; the retention arithmetic has a hole",
	})
	preserveWaiting = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_preserve_waiting_total",
		Help: "Parents named for preservation whose only containment row is in a window the stamp has not completed, retried next cycle",
	})
)
