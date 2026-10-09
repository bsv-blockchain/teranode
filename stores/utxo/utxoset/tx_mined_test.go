package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUTXORowsCarryTheirParentsBlockFacts pins the two columns the read path relies on once
// a transaction's containment window has been dropped: the height and block of the
// transaction that made the UTXO. Both fixed width, placed before the variable-length
// script so alignment costs nothing.
func TestUTXORowsCarryTheirParentsBlockFacts(t *testing.T) {
	s, ctx := newTestStore(t)

	var names []string
	rows, err := s.pool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		 WHERE table_name = 'utxo' ORDER BY ordinal_position`)
	require.NoError(t, err)

	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	rows.Close()

	require.Contains(t, names, "mined_height")
	require.Contains(t, names, "block_id")

	idx := func(n string) int {
		for i, x := range names {
			if x == n {
				return i
			}
		}
		return -1
	}
	require.Less(t, idx("block_id"), idx("script"), "fixed-width columns go before the script so the row needs no padding for them")
}

// TestMembershipTableIsKeyedByTransactionIdFirst pins the key order. PostgreSQL needs the
// partition key inside a partitioned table's primary key but not at its head, and a
// height-leading key cannot be probed by transaction id without a skip scan over every
// height in the partition.
func TestMembershipTableIsKeyedByTransactionIdFirst(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 700_000))

	var def string
	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		 WHERE tablename = 'tx_mined' AND indexname = 'tx_mined_pkey'`).Scan(&def))
	require.Contains(t, def, "(txid, mined_height, block_id)")

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = 'tx_mined'::regclass`).Scan(&n))
	require.Equal(t, 1, n, "one 288-block window for height 700,000")
}

// TestDroppedMembershipWindowsCannotComeBack pins the floor: once a window is dropped, a
// create for a height inside it fails instead of recreating the window, because a block
// re-offered after its window retired would otherwise claim every transaction in it afresh.
func TestDroppedMembershipWindowsCannotComeBack(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000))

	require.Equal(t, 1, retireWindows(t, s, ctx, 0, nil))

	floor, err := s.txMinedFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(1), floor, "window 0 (heights 0-287) was dropped, so the floor is window 1")

	err = s.ensureTxMinedPartition(ctx, 100)
	require.Error(t, err, "recreating a dropped window would let a stale block double its UTXOs")

	require.NoError(t, s.ensureTxMinedPartition(ctx, 1_000), "a live window is still fine")
}

// TestDropWritesOnlyTheDroppedFloor: the drop's transaction advances the dropped floor alone.
// The other two floors are already at or above the window's upper bound, because the drop's
// first condition is the completion record the stamp wrote when it advanced them.
func TestDropWritesOnlyTheDroppedFloor(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 400))
	require.NoError(t, s.ensureTxMinedPartition(ctx, 5_000))

	require.Equal(t, 2, retireWindows(t, s, ctx, 1, nil), "windows 0 and 1 go, window 17 stays")

	var floor, fence, complete int32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT floor, stamp_fence, stamp_complete_floor FROM tx_mined_floor WHERE id = 0`).
		Scan(&floor, &fence, &complete))

	require.Equal(t, int32(2), floor, "a window number: the highest dropped plus one")
	require.Equal(t, int32(2*TxMinedPartitionBlocks), complete, "a height: the stamped windows' upper bound")
	require.Equal(t, int32(2*TxMinedPartitionBlocks), fence)
}

// TestFloorRowRefusesAFloorAboveTheFence pins the constraint itself, so that a later build which
// raised floor alone would be refused by postgres rather than silently leaving the three values
// out of order.
func TestFloorRowRefusesAFloorAboveTheFence(t *testing.T) {
	s, ctx := newTestStore(t)

	_, err := s.pool.Exec(ctx, `UPDATE tx_mined_floor SET floor = 3 WHERE id = 0`)
	requireCheckViolation(t, err, "288 x floor above stamp_complete_floor must be refused")
}

// TestDroppingAWindowClearsTheEnsureCache pins the interaction between the two halves of
// window management, which is invisible until they disagree.
//
// ensureTxMinedPartition remembers the last window it created and returns early when the next
// create lands in the same one. That memory can outlive the window itself: a fork stamp at an
// old height caches it, the pruner then drops it, and a later stamp at that height hits the
// cache, skips the floor read, and the INSERT fails with "no partition of relation" instead of
// the loud, explained refusal the floor exists to give.
func TestDroppingAWindowClearsTheEnsureCache(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.ensureTxMinedPartition(ctx, 100))

	require.Equal(t, 1, retireWindows(t, s, ctx, 0, nil))

	require.Error(t, s.ensureTxMinedPartition(ctx, 100),
		"the cached window is gone, so the floor has to be re-read and the create refused")
}
