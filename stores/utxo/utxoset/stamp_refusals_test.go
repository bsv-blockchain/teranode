package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util/chainancestry/chainancestrytest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// mineAt creates a transaction on the identity route and records it mined at height with the
// given block id, so partition height/288 exists and holds one winner.
func mineAt(t *testing.T, s *Store, ctx context.Context, sats uint64, height, blockID uint32) *bt.Tx {
	t.Helper()

	return mineAtSeenAt(t, s, ctx, sats, height-1, height, blockID)
}

// mineAtSeenAt is mineAt for a transaction first seen at createdHeight, which may be far below
// the block that mines it.
func mineAtSeenAt(t *testing.T, s *Store, ctx context.Context, sats uint64, createdHeight, height, blockID uint32) *bt.Tx {
	t.Helper()

	tx := mkTx(t, 1, sats)
	_, err := s.Create(ctx, tx, createdHeight)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: height, OnLongestChain: true})
	require.NoError(t, err)

	return tx
}

// A drain is only ever asked for partitions the worker computed from its floors, so these three
// refusals guard against a worker bug, not a normal path: each refuses before it writes
// anything, and none moves the fence.
func TestStampBeginWindowRefusesWhatIsNotTheNextPartitionOrIsNotCovered(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)
	mineAt(t, s, ctx, 5_000, 100, 7)

	tip := stampDepthTip(s, 1)
	require.NoError(t, s.SetBlockHeight(tip))

	ids := map[uint32]uint32{100: 7}
	full := chainancestrytest.Chain(t, 0, tip, ids)

	drain, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, drain.Close()) }()

	t.Run("not a partition start", func(t *testing.T) {
		_, err := drain.BeginWindow(ctx, 100, full)
		require.ErrorContains(t, err, "is not a partition start")
	})

	t.Run("not the next partition", func(t *testing.T) {
		_, err := drain.BeginWindow(ctx, TxMinedPartitionBlocks, full)
		require.ErrorContains(t, err, "is not the next to stamp")
	})

	t.Run("ancestry starts above the partition", func(t *testing.T) {
		// Deep enough for partition 0, but the answer starts above its first block.
		short := chainancestrytest.Chain(t, 50, tip, ids)
		_, err := drain.BeginWindow(ctx, 0, short)
		require.ErrorContains(t, err, "does not cover partition")
	})

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), floors.StampFence, "every refusal came before step 1")
	require.Equal(t, uint32(0), floors.StampCompleteFloor)
}

// stampDepthTip is the lowest tip at which partition `window` is stampable.
func stampDepthTip(s *Store, window uint32) uint32 {
	return (window+1)*TxMinedPartitionBlocks - 1 + s.stampDepth
}

// A partition that is below the stamp-complete floor's successor but is no longer attached, with
// no completion record, is a state the drop rule makes impossible. The stamp refuses to touch it
// rather than write onto a table that is not part of tx_mined.
func TestStampRefusesADetachedPartitionWithNoCompletionRecord(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)
	mineAt(t, s, ctx, 5_000, 100, 7)

	_, err := s.pool.Exec(ctx, `ALTER TABLE tx_mined DETACH PARTITION tx_mined_w0`)
	require.NoError(t, err)

	tip := stampDepthTip(s, 0)
	require.NoError(t, s.SetBlockHeight(tip))

	drain, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, drain.Close()) }()

	_, err = drain.BeginWindow(ctx, 0, chainancestrytest.Chain(t, 0, tip, map[uint32]uint32{100: 7}))
	require.ErrorContains(t, err, "is detached and has no completion record")

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), floors.StampFence)
}

// OpenDrain alarms, and carries on, when an attached partition lies below the completion floor
// with no completion record. The floor and the record commit together, so this is only a hand
// edit or a bug; the counter is how an operator learns the partition can never drop.
func TestOpenDrainCountsAPartitionBelowTheFloorWithNoCompletionRecord(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)
	mineAt(t, s, ctx, 5_000, 100, 7)

	_, err := s.pool.Exec(ctx, `UPDATE tx_mined_floor SET stamp_fence = 288, stamp_complete_floor = 288 WHERE id = 0`)
	require.NoError(t, err)

	// Another test asserts this process-wide counter is still zero, so this one counts on a
	// private counter and puts the real one back untouched.
	real := stampCompletionMissing
	stampCompletionMissing = prometheus.NewCounter(prometheus.CounterOpts{Name: "test_completion_missing"})

	t.Cleanup(func() { stampCompletionMissing = real })

	before := testutil.ToFloat64(stampCompletionMissing)

	drain, floors, err := s.OpenDrain(ctx)
	require.NoError(t, err)
	require.NoError(t, drain.Close())

	require.Equal(t, uint32(288), floors.StampCompleteFloor)
	require.Equal(t, before+1, testutil.ToFloat64(stampCompletionMissing))
}

// containmentRows counts a transaction's tx_mined rows.
func containmentRows(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1`, hashBytes(tx)).Scan(&n))

	return n
}

type beginResult struct {
	state pruner.StampWindowState
	err   error
}

// Step 1 waits for the exclusive fence lock for at most fenceLockTimeout and then backs off and
// tries again, so a long writer stalls the block path for a fraction of the time and the stamp
// still gets through once the writer is gone. The lock is held here until the first timeout is
// counted, which proves the retry path ran, then released; the partition must then stamp with the
// fence raised and its loser deleted.
func TestStampWaitsOutAHeldFenceLockAndThenRaisesTheFence(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	winner := mineAt(t, s, ctx, 5_000, 100, 7)
	loser := mineAt(t, s, ctx, 6_000, 100, 8) // a fork block at the same height

	tip := stampDepthTip(s, 0)
	require.NoError(t, s.SetBlockHeight(tip))

	anc := chainancestrytest.Chain(t, 0, tip, map[uint32]uint32{100: 7})

	drain, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, drain.Close()) }()

	release := holdFenceExclusive(t, s, ctx)
	t.Cleanup(release)
	timeoutsBefore := testutil.ToFloat64(stampFenceLockTimeouts)
	losersBefore := testutil.ToFloat64(stampLosersDeleted)

	done := make(chan beginResult, 1)

	go func() {
		state, err := drain.BeginWindow(ctx, 0, anc)
		done <- beginResult{state, err}
	}()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(stampFenceLockTimeouts) > timeoutsBefore
	}, 10*time.Second, 10*time.Millisecond, "the held lock must be counted as a timeout")

	select {
	case r := <-done:
		t.Fatalf("BeginWindow returned while the fence lock was held: %v %v", r.state, r.err)
	default:
	}

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), floors.StampFence, "nothing raised while the lock is held")

	release()

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, pruner.StampWindowReady, r.state)
	case <-time.After(15 * time.Second):
		t.Fatal("BeginWindow did not finish after the fence lock was released")
	}

	floors, err = s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(TxMinedPartitionBlocks), floors.StampFence)
	require.Equal(t, losersBefore+1, testutil.ToFloat64(stampLosersDeleted))

	require.Equal(t, 1, containmentRows(t, s, ctx, winner), "the winner's row stands")
	require.Zero(t, containmentRows(t, s, ctx, loser), "the fork block's row was deleted")
}

// A drain whose context ends while it waits out the back-off stops with a cancellation error and
// leaves the fence where it was, so a worker shutting down never leaves a half-raised fence.
func TestStampStopsWaitingForTheFenceLockWhenCancelled(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)
	mineAt(t, s, ctx, 5_000, 100, 7)

	tip := stampDepthTip(s, 0)
	require.NoError(t, s.SetBlockHeight(tip))

	anc := chainancestrytest.Chain(t, 0, tip, map[uint32]uint32{100: 7})

	drain, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, drain.Close()) }()

	t.Cleanup(holdFenceExclusive(t, s, ctx))

	timeoutsBefore := testutil.ToFloat64(stampFenceLockTimeouts)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)

	go func() {
		_, err := drain.BeginWindow(cctx, 0, anc)
		done <- err
	}()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(stampFenceLockTimeouts) > timeoutsBefore
	}, 10*time.Second, 10*time.Millisecond)

	cancel()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "cancelled while waiting for the fence lock")
	case <-time.After(5 * time.Second):
		t.Fatal("BeginWindow ignored the cancellation")
	}

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), floors.StampFence)
}

func TestIsLockTimeoutOnlyMatchesALockNotAvailableError(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	t.Cleanup(holdFenceExclusive(t, s, ctx))

	dbTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = dbTx.Rollback(ctx) }()

	_, err = dbTx.Exec(ctx, `SET LOCAL lock_timeout = '50ms'`)
	require.NoError(t, err)

	_, err = dbTx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(fenceLockKey1), int32(fenceLockKey2))
	require.Error(t, err)
	require.True(t, isLockTimeout(err))
	require.False(t, isLockTimeout(context.Canceled))
	require.False(t, isLockTimeout(nil))
}
