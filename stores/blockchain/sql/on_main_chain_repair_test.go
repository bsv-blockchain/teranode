package sql

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// rebuildFault stands in for the on_main_chain rebuild transaction. While failing is set,
// or for the first failFirst calls, it returns an error without touching the database;
// otherwise it runs the real transaction. It records the full and floor arguments of every
// call, and whether slowPathMu was held, so a test can see which walk the repair asked for.
type rebuildFault struct {
	failing   atomic.Bool
	failFirst int32
	calls     atomic.Int32

	mu     sync.Mutex
	fulls  []bool
	floors []int64
	locked []bool
}

// installRebuildFault wires a rebuildFault into the store, failing every call until the
// test clears failing.
func installRebuildFault(s *SQL) *rebuildFault {
	f := &rebuildFault{}
	f.failing.Store(true)

	s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
		n := f.calls.Add(1)

		// TryLock fails exactly when someone, in practice the caller, holds slowPathMu.
		held := !s.slowPathMu.TryLock()
		if !held {
			s.slowPathMu.Unlock()
		}

		f.mu.Lock()
		f.fulls = append(f.fulls, full)
		f.floors = append(f.floors, floor)
		f.locked = append(f.locked, held)
		f.mu.Unlock()

		if f.failing.Load() || n <= f.failFirst {
			return errors.NewStorageError("injected rebuild failure")
		}

		return s.rebuildOnMainChainFlagTx(ctx, full, floor)
	}

	return f
}

func (f *rebuildFault) walks() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]bool(nil), f.fulls...)
}

func (f *rebuildFault) floorsSeen() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]int64(nil), f.floors...)
}

func (f *rebuildFault) lockHeld() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]bool(nil), f.locked...)
}

// setRepairBackoff shortens the background repair's first backoff so a test does not
// wait out the production second.
func setRepairBackoff(t *testing.T, s *SQL, d time.Duration) {
	t.Helper()

	s.onMainChainRepair.mu.Lock()
	s.onMainChainRepair.backoff = d
	s.onMainChainRepair.mu.Unlock()
}

// waitForRepair blocks until the store no longer owes a flag repair, which it signals by
// dropping mainChainRebuilding back to zero.
func waitForRepair(t *testing.T, s *SQL) {
	t.Helper()

	require.Eventually(t, func() bool {
		return s.mainChainRebuilding.Load() == 0
	}, 10*time.Second, time.Millisecond, "the background repair must release its guard exactly once")
}

// A successful operator rebuild can acknowledge the repair after the worker has
// selected its generation but before the worker accounts for its own success.
// The acknowledgment leaves the record to the running attempt, so the guard is
// released only once.
func TestOnMainChainRepairDoesNotClearAnAlreadyRepairedGeneration(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block1.Hash().CloneBytes())
	require.NoError(t, err)

	acknowledged := make(chan struct{})
	s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
		if err := s.rebuildOnMainChainFlagTx(ctx, full, floor); err != nil {
			return err
		}

		// Model the external acknowledgment in the worker's accounting window,
		// after a real full rebuild has repaired the persisted flags.
		s.onMainChainRepaired(true)
		close(acknowledged)

		return nil
	}

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))
	select {
	case <-acknowledged:
	case <-time.After(5 * time.Second):
		t.Fatal("the repair did not reach the acknowledgment")
	}

	// Join the worker before examining its final accounting, including underflow.
	close(s.backgroundDone)
	select {
	case <-s.onMainChainRepair.stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the repair loop did not stop")
	}

	require.Zero(t, s.mainChainRebuilding.Load(), "the same repair must not release its guard twice")
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "the persisted flag was repaired")
	require.Equal(t, gaugeBefore, repairPending(t), "the pending gauge is released once")
}

func repairPending(t *testing.T) float64 {
	t.Helper()

	return testutil.ToFloat64(prometheusOnMainChainRepairPending)
}

func writeFailures(t *testing.T, path string) float64 {
	t.Helper()

	return testutil.ToFloat64(prometheusOnMainChainWriteFailures.WithLabelValues(path))
}

// TestInvalidateBlockFailedRebuildIsRepairedInProcess is the reason the store keeps a
// record of a failed flag write.
//
// InvalidateBlock commits its UPDATE and then runs the full on_main_chain rebuild. When the
// rebuild fails it logs and returns success, and before this change nothing retried it: the
// guard dropped with the call, every flag reader went back to trusting the column, and the
// losing branch stayed flagged true while the winner stayed false until a restart or the
// next operator action. The block below the invalidated one is that stale true, and the
// fork that becomes the best chain is that stale false.
func TestInvalidateBlockFailedRebuildIsRepairedInProcess(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, 10*time.Millisecond)

	ctx := context.Background()

	// Main chain genesis -> 1 -> 2 -> 3 -> 4, and a fork 1 -> alt2 -> alt3 that loses.
	block4 := createBlock3OnFork(block3)
	alt3 := createBlock3OnFork(blockAlternative2)
	storeBlocks(t, s, block1, block2, block3, block4, blockAlternative2, alt3)
	require.True(t, getOnMainChain(t, s, block4.Hash().CloneBytes()), "block4 is the best block")
	require.False(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()), "alt3 is a fork")

	failuresBefore := writeFailures(t, "invalidate_block")
	gaugeBefore := repairPending(t)
	fault := installRebuildFault(s)

	// Invalidating block3 takes block4 with it, so the best chain is now 1 -> alt2 -> alt3.
	_, err := s.InvalidateBlock(ctx, block3.Hash())
	require.NoError(t, err, "InvalidateBlock logs a failed rebuild and still returns success")

	// The UPDATE committed and the rebuild did not run, so the flags are stale both ways.
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 is a stale true: it is no longer on the best chain")
	require.False(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()), "alt3 is a stale false: it is the best block")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block4.Hash().CloneBytes()))

	// The guard must stay up for as long as the repair is owed, so no reader trusts the
	// column, and the failure must be visible.
	require.Positive(t, s.mainChainRebuilding.Load(), "the guard stays raised while a repair is owed")
	require.Equal(t, gaugeBefore+1, repairPending(t), "the pending gauge counts this store's owed repair")
	require.Equal(t, failuresBefore+1, writeFailures(t, "invalidate_block"), "the failure is counted once")

	// Readers answer from the flag-free walk while the guard is up.
	onChain, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{getBlockID(t, s, block2.Hash().CloneBytes())})
	require.NoError(t, err)
	require.False(t, onChain, "block2 is flagged true but is not on the best chain")

	onChain, err = s.CheckBlockIsInCurrentChain(ctx, []uint32{getBlockID(t, s, alt3.Hash().CloneBytes())})
	require.NoError(t, err)
	require.True(t, onChain, "alt3 is flagged false but is the best block")

	byHeight, err := s.GetBlockByHeight(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, alt3.Hash().String(), byHeight.Hash().String(), "GetBlockByHeight walks the chain while the guard is up")

	// Let the database "recover" and the background repair land.
	fault.failing.Store(false)
	waitForRepair(t, s)

	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the repair clears the stale true")
	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "the repair flags the winning branch")
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()), "the repair flags the new best block")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block4.Hash().CloneBytes()))

	require.Equal(t, gaugeBefore, repairPending(t), "the pending gauge drops once the repair lands")
	require.GreaterOrEqual(t, fault.calls.Load(), int32(2), "at least one failed attempt and the one that landed")

	// With the guard down the flag fast path now agrees with the walk.
	byHeight, err = s.GetBlockByHeight(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, blockAlternative2.Hash().String(), byHeight.Hash().String())
}

// TestRevalidateBlockFailedRebuildIsRepairedInProcess covers the other operator path with
// the same shape: the UPDATE that makes a block valid again commits, the full rebuild that
// would move the flags onto the revalidated branch fails, and nothing retried it.
func TestRevalidateBlockFailedRebuildIsRepairedInProcess(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, 10*time.Millisecond)

	ctx := context.Background()

	// Main chain genesis -> 1 -> 2 -> 3, then a fork 1 -> alt2 -> alt3 -> alt4 that wins.
	alt3 := createBlock3OnFork(blockAlternative2)
	alt4 := createBlock3OnFork(alt3)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2, alt3, alt4)
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()), "alt4 won the reorg")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	// Park alt4 with a working rebuild. block3 and alt3 tie on work and block3 has the lower
	// id, so block3 is the best again.
	_, err := s.InvalidateBlock(ctx, alt4.Hash())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))

	failuresBefore := writeFailures(t, "revalidate_block")
	gaugeBefore := repairPending(t)
	fault := installRebuildFault(s)

	require.NoError(t, s.RevalidateBlock(ctx, alt4.Hash()), "RevalidateBlock logs a failed rebuild and still returns success")

	// alt4 is valid and the best block again, but the flags still describe block3's chain.
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 is a stale true")
	require.False(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()), "alt4 is a stale false")

	require.Positive(t, s.mainChainRebuilding.Load(), "the guard stays raised while a repair is owed")
	require.Equal(t, gaugeBefore+1, repairPending(t))
	require.Equal(t, failuresBefore+1, writeFailures(t, "revalidate_block"))

	onChain, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{getBlockID(t, s, block3.Hash().CloneBytes())})
	require.NoError(t, err)
	require.False(t, onChain, "block3 is flagged true but is not on the best chain")

	onChain, err = s.CheckBlockIsInCurrentChain(ctx, []uint32{getBlockID(t, s, alt4.Hash().CloneBytes())})
	require.NoError(t, err)
	require.True(t, onChain, "alt4 is flagged false but is the best block")

	fault.failing.Store(false)
	waitForRepair(t, s)

	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.Equal(t, gaugeBefore, repairPending(t))
}

// TestOnMainChainRepairKeepsTheRecordedFloorAsTheTipMoves pins the floor. A bounded
// repair's own window is measured from the tip at the time of the attempt, and blocks keep
// arriving while the repair keeps failing. Without the floor the mark recorded, a retry
// after the tip moved walks a window that no longer reaches the stale row, succeeds,
// releases the guard, and leaves the row wrong.
//
// CoinbaseMaturity 0 makes the bounded window the tip alone, so a single new block is
// enough to carry it past the stale row.
func TestOnMainChainRepairKeepsTheRecordedFloorAsTheTipMoves(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	setRepairBackoff(t, s, 20*time.Millisecond)

	storeBlocks(t, s, block1, block2)

	// block2, at the tip, is the row a failed StoreBlock write left wrong.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block2.Hash().CloneBytes())
	require.NoError(t, err)

	fault := installRebuildFault(s)

	s.markOnMainChainDirty(false, int64(block2.Height), "test", errors.NewStorageError("injected"))

	require.Eventually(t, func() bool { return fault.calls.Load() >= 1 }, 5*time.Second, time.Millisecond, "the first attempt runs")

	// The tip moves while the repair is failing.
	storeBlocks(t, s, block3)

	fault.failing.Store(false)
	waitForRepair(t, s)

	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the repair reaches the stale row below the moved window")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	for i, full := range fault.walks() {
		require.False(t, full, "attempt %d stays bounded: the floor covers the stale row", i+1)
	}

	for i, floor := range fault.floorsSeen() {
		require.EqualValues(t, block2.Height, floor, "attempt %d keeps the recorded floor", i+1)
	}

	for i, held := range fault.lockHeld() {
		require.True(t, held, "bounded attempt %d runs under slowPathMu", i+1)
	}
}

// TestOnMainChainRepairWithoutAFloorWalksToGenesis: a bounded mark that cannot name the
// lowest stale height cannot be covered by any bounded window, so it is owed a full walk.
// That walk holds slowPathMu on SQLite, where a long write would otherwise make StoreBlock
// fail on the busy timeout, and runs without it on PostgreSQL.
func TestOnMainChainRepairWithoutAFloorWalksToGenesis(t *testing.T) {
	for _, tc := range []struct {
		name     string
		unlocked bool
	}{
		{name: "sqlite holds the lock", unlocked: false},
		{name: "postgres runs without the lock", unlocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newOnMainChainTestStore(t)
			require.False(t, s.unlockedFullRepair, "a SQLite store holds the lock for a full repair")
			s.unlockedFullRepair = tc.unlocked

			setRepairBackoff(t, s, time.Millisecond)
			storeBlocks(t, s, block1, block2)

			fault := installRebuildFault(s)
			fault.failing.Store(false)

			s.markOnMainChainDirty(false, noRepairFloor, "test", errors.NewStorageError("injected"))
			waitForRepair(t, s)

			require.Equal(t, []bool{true}, fault.walks())
			require.Equal(t, []bool{!tc.unlocked}, fault.lockHeld())
		})
	}
}

// TestOnMainChainRepairRerunsWhenAChainWriteOverlapsIt: a full walk that runs without
// slowPathMu reads its rows from a snapshot taken when it began. RevalidateBlock can commit
// in the meantime, its own rebuild can land and acknowledge the repair, and then the walk
// commits its older view. On PostgreSQL the two touch different rows, so neither aborts.
// The repair must not release the guard on that walk. It must run again.
//
// SQLite cannot hold two writers open at once, so this test stands in for the stale
// commit: the loop's first attempt waits until RevalidateBlock has finished and then writes
// what a walk from the old snapshot would write. The PostgreSQL test below runs the real
// walk.
func TestOnMainChainRepairRerunsWhenAChainWriteOverlapsIt(t *testing.T) {
	s := newOnMainChainTestStore(t)
	s.unlockedFullRepair = true
	setRepairBackoff(t, s, time.Millisecond)

	ctx := context.Background()

	// Main chain genesis -> 1 -> 2 -> 3, and a fork 1 -> alt2 -> alt3 -> alt4 that wins
	// once alt4 is valid. Park alt4, so block3 is the best.
	alt3 := createBlock3OnFork(blockAlternative2)
	alt4 := createBlock3OnFork(alt3)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2, alt3, alt4)
	_, err := s.InvalidateBlock(ctx, alt4.Hash())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	var calls atomic.Int32

	started := make(chan struct{})
	release := make(chan struct{})

	s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
		if calls.Add(1) != 1 {
			return s.rebuildOnMainChainFlagTx(ctx, full, floor)
		}

		close(started)
		<-release

		// The walk's snapshot predates the revalidation: block3's chain was the best.
		_, err := s.db.ExecContext(ctx, `UPDATE blocks SET on_main_chain = (hash IN ($1, $2)) WHERE height IN (2, 3, 4)`,
			block2.Hash().CloneBytes(), block3.Hash().CloneBytes())

		return err
	}

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the repair's first attempt did not start")
	}

	// alt4 becomes the best again, and RevalidateBlock's own rebuild lands while the
	// loop's attempt is still in flight.
	require.NoError(t, s.RevalidateBlock(ctx, alt4.Hash()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
	require.Positive(t, s.mainChainRebuilding.Load(), "an acknowledgment during an attempt does not drop the guard")

	close(release)
	waitForRepair(t, s)

	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the rerun undoes the stale walk's write")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
	require.EqualValues(t, 3, calls.Load(), "the stale attempt, RevalidateBlock's rebuild, and the rerun")
	require.Equal(t, gaugeBefore, repairPending(t))
}

// TestStartupBoundedRebuildFailureKeepsItsWindowAsTheFloor: a failed bounded startup
// rebuild is owed a bounded repair that still reaches the bottom of the window the startup
// rebuild meant to cover, however far the tip moves before the repair lands.
func TestStartupBoundedRebuildFailureKeepsItsWindowAsTheFloor(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	setRepairBackoff(t, s, 20*time.Millisecond)

	storeBlocks(t, s, block1, block2)

	// block2, at the tip at startup, is the row the previous process left wrong.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block2.Hash().CloneBytes())
	require.NoError(t, err)

	floor := s.startupRepairFloor(context.Background())
	require.EqualValues(t, block2.Height, floor, "the window bottom with CoinbaseMaturity 0 is the tip")

	fault := installRebuildFault(s)

	s.startupRebuildOnMainChain(context.Background(), false, floor)
	require.Positive(t, s.mainChainRebuilding.Load(), "a repair is owed")

	require.Eventually(t, func() bool { return fault.calls.Load() >= 2 }, 5*time.Second, time.Millisecond, "the loop's first attempt runs")

	// The tip moves past the startup window while the repair is failing.
	storeBlocks(t, s, block3)

	fault.failing.Store(false)
	waitForRepair(t, s)

	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the repair reaches the startup window below the moved tip")

	walks := fault.walks()
	floors := fault.floorsSeen()

	// The first call is the startup rebuild itself; the rest are the loop's attempts.
	for i := 1; i < len(walks); i++ {
		require.False(t, walks[i], "attempt %d stays bounded", i)
		require.EqualValues(t, block2.Height, floors[i], "attempt %d keeps the startup window's bottom as its floor", i)
	}
}

// TestOnMainChainMarkDropsCachedAnswers: GetBlockByHeight answers from the response cache
// before it consults the guard, so a reply cached from the stale column before the mark
// would keep being served while the repair is owed.
func TestOnMainChainMarkDropsCachedAnswers(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, time.Hour)
	ctx := context.Background()

	storeBlocks(t, s, block1, block2, blockAlternative2)

	// A failed write leaves the fork flagged and the main-chain block not.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = (hash = $1) WHERE height = 2`, blockAlternative2.Hash().CloneBytes())
	require.NoError(t, err)

	cached, err := s.GetBlockByHeight(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, blockAlternative2.Hash().String(), cached.Hash().String(), "the stale column answers before the mark")

	installRebuildFault(s)
	s.markOnMainChainDirty(false, 2, "test", errors.NewStorageError("injected"))

	byHeight, err := s.GetBlockByHeight(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, block2.Hash().String(), byHeight.Hash().String(), "after the mark the answer comes from the flag-free walk")
}

// TestOnMainChainMarkIsSkippedWhileClosing: a write cancelled by Close is not a fault to
// repair. It must not raise the guard, count a failure or set the pending gauge.
func TestOnMainChainMarkIsSkippedWhileClosing(t *testing.T) {
	s := newOnMainChainTestStore(t)

	failuresBefore := writeFailures(t, "test_closing")
	gaugeBefore := repairPending(t)

	close(s.backgroundDone)
	s.markOnMainChainDirty(true, noRepairFloor, "test_closing", context.Canceled)

	require.Zero(t, s.mainChainRebuilding.Load())
	require.Equal(t, failuresBefore, writeFailures(t, "test_closing"))
	require.Equal(t, gaugeBefore, repairPending(t))
}

// TestALaterFullRebuildClearsThePendingRepair: a full rebuild that an operator path runs
// successfully while a repair is owed satisfies that repair, because it leaves every flag
// right. The background loop then has nothing left to do, and the store's shutdown must not
// wait for its backoff to run out.
func TestALaterFullRebuildClearsThePendingRepair(t *testing.T) {
	s := newOnMainChainTestStore(t)
	// Long enough that the loop cannot retry on its own during the test.
	setRepairBackoff(t, s, time.Hour)

	ctx := context.Background()

	block4 := createBlock3OnFork(block3)
	alt3 := createBlock3OnFork(blockAlternative2)
	storeBlocks(t, s, block1, block2, block3, block4, blockAlternative2, alt3)

	gaugeBefore := repairPending(t)
	fault := installRebuildFault(s)

	_, err := s.InvalidateBlock(ctx, block4.Hash())
	require.NoError(t, err)
	require.Positive(t, s.mainChainRebuilding.Load(), "a repair is owed")

	// Let the loop make its one attempt and go to sleep.
	deadline := time.Now().Add(5 * time.Second)
	for fault.calls.Load() < 2 {
		require.False(t, time.Now().After(deadline), "the loop never made its first attempt")
		time.Sleep(time.Millisecond)
	}

	require.Positive(t, s.mainChainRebuilding.Load(), "still owed after the failed attempt")

	// Now an operator invalidates block3 and the full rebuild works.
	fault.failing.Store(false)

	_, err = s.InvalidateBlock(ctx, block3.Hash())
	require.NoError(t, err)

	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "the successful full rebuild clears the owed repair at once")
	require.Equal(t, gaugeBefore, repairPending(t))
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))

	// Shutdown must not wait out the hour-long backoff. Close signals backgroundDone first,
	// so do that half here and leave the rest to the test cleanup's Close.
	close(s.backgroundDone)

	select {
	case <-s.onMainChainRepair.stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the repair loop did not stop on shutdown")
	}
}

// TestOnMainChainRepairRacingRevalidate_PostgreSQL runs the overlap with the real walk on
// the engine where the full repair runs without slowPathMu. The loop's walk is held after it
// has taken its snapshot, with block3's chain as the best and block3's flag left false by
// the failed write it is owed for. RevalidateBlock then makes alt4 the best and its own
// rebuild commits. The held walk resumes and sets block3 true from its old snapshot. The
// two rebuilds update different rows, so neither aborts, and without the rerun the table
// ends with both block3 and alt3 flagged at height 3.
func TestOnMainChainRepairRacingRevalidate_PostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	connStr, teardown, err := postgres.SetupTestPostgresContainer()
	if err != nil {
		t.Skipf("PostgreSQL container not available: %v", err)
	}

	t.Cleanup(func() { _ = teardown() })

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(context.Background()) })
	waitForStartupRebuild(t, s)

	require.True(t, s.unlockedFullRepair, "a PostgreSQL store runs the full repair without slowPathMu")
	setRepairBackoff(t, s, time.Millisecond)

	ctx := context.Background()

	alt3 := createBlock3OnFork(blockAlternative2)
	alt4 := createBlock3OnFork(alt3)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2, alt3, alt4)
	_, err = s.InvalidateBlock(ctx, alt4.Hash())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	// The stale flag the failed write left behind.
	_, err = s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block3.Hash().CloneBytes())
	require.NoError(t, err)

	var snapshots atomic.Int32

	started := make(chan struct{})
	release := make(chan struct{})

	s.rebuildSnapshotHook = func(bool) {
		if snapshots.Add(1) != 1 {
			return
		}

		close(started)
		<-release
	}

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the repair's walk did not reach its snapshot")
	}

	require.NoError(t, s.RevalidateBlock(ctx, alt4.Hash()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))

	close(release)
	waitForRepair(t, s)

	var atHeight3 int
	require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM blocks WHERE height = 3 AND on_main_chain = true`).Scan(&atHeight3))
	require.Equal(t, 1, atHeight3, "one block per height on the main chain")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the rerun undoes the held walk's write")
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
	require.GreaterOrEqual(t, snapshots.Load(), int32(3), "the held walk, RevalidateBlock's rebuild, and the rerun")
	require.Equal(t, gaugeBefore, repairPending(t))
}

// TestStartupRebuildOverlappedByAChainWriteRunsAgain: the startup rebuild runs without
// slowPathMu and reads its rows from a snapshot taken when it began. A chain write can
// commit while it runs and the rebuild can then commit its older view over it; on
// PostgreSQL the two touch different rows, so neither aborts. The startup goroutine then
// drops its guard and every reader trusts the stale column. The rebuild must record the
// overlap so the background repair walks again.
//
// As in TestOnMainChainRepairRerunsWhenAChainWriteOverlapsIt, SQLite cannot hold two
// writers open at once, so the startup rebuild's first call waits until RevalidateBlock
// has finished and then writes what a walk from the old snapshot would write.
func TestStartupRebuildOverlappedByAChainWriteRunsAgain(t *testing.T) {
	for _, tc := range []struct {
		name  string
		full  bool
		floor int64
	}{
		{name: "full startup rebuild", full: true, floor: noRepairFloor},
		{name: "bounded startup rebuild", full: false, floor: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newOnMainChainTestStore(t)
			setRepairBackoff(t, s, time.Millisecond)

			ctx := context.Background()

			alt3 := createBlock3OnFork(blockAlternative2)
			alt4 := createBlock3OnFork(alt3)
			storeBlocks(t, s, block1, block2, block3, blockAlternative2, alt3, alt4)
			_, err := s.InvalidateBlock(ctx, alt4.Hash())
			require.NoError(t, err)
			require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

			var calls atomic.Int32

			var fulls []bool

			var fullsMu sync.Mutex

			started := make(chan struct{})
			release := make(chan struct{})

			s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
				fullsMu.Lock()
				fulls = append(fulls, full)
				fullsMu.Unlock()

				if calls.Add(1) != 1 {
					return s.rebuildOnMainChainFlagTx(ctx, full, floor)
				}

				close(started)
				<-release

				// The startup rebuild's snapshot predates the revalidation.
				_, err := s.db.ExecContext(ctx, `UPDATE blocks SET on_main_chain = (hash IN ($1, $2)) WHERE height IN (2, 3, 4)`,
					block2.Hash().CloneBytes(), block3.Hash().CloneBytes())

				return err
			}

			gaugeBefore := repairPending(t)
			done := make(chan struct{})

			go func() {
				defer close(done)

				s.startupRebuildOnMainChain(ctx, tc.full, tc.floor)
			}()

			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("the startup rebuild did not start")
			}

			require.NoError(t, s.RevalidateBlock(ctx, alt4.Hash()))

			close(release)

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the startup rebuild did not return")
			}

			waitForRepair(t, s)

			require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the repair undoes the startup rebuild's stale write")
			require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
			require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
			require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
			require.EqualValues(t, 3, calls.Load(), "the startup rebuild, RevalidateBlock's rebuild, and the repair")

			fullsMu.Lock()
			require.Equal(t, tc.full, fulls[2], "the repair walks as far as the startup rebuild did")
			fullsMu.Unlock()

			require.Equal(t, gaugeBefore, repairPending(t))
		})
	}
}

// TestStartupBoundedRebuildOutOfTimeStaysBounded: the floor of a failed bounded startup
// rebuild is read before the rebuild and not on the rebuild's context. When the rebuild
// runs out of its budget, which is when a slow database fails it, the owed repair is
// still the bounded walk down to that floor and not a walk to genesis.
func TestStartupBoundedRebuildOutOfTimeStaysBounded(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	setRepairBackoff(t, s, time.Millisecond)

	storeBlocks(t, s, block1, block2)

	// block2, at the tip at startup, is the row the previous process left wrong.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block2.Hash().CloneBytes())
	require.NoError(t, err)

	floor := s.startupRepairFloor(context.Background())
	require.EqualValues(t, block2.Height, floor)

	fault := installRebuildFault(s)
	fault.failing.Store(false)

	spent, cancel := context.WithCancel(context.Background())
	cancel()

	s.startupRebuildOnMainChain(spent, false, floor)
	waitForRepair(t, s)

	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the repair lands")
	require.Equal(t, []bool{false, false}, fault.walks(), "the startup rebuild and one bounded repair, no walk to genesis")
	require.EqualValues(t, block2.Height, fault.floorsSeen()[1], "the repair keeps the startup window's bottom")
}

// TestStartupRepairFloorWithoutTheBestBlockOwesAFullWalk: the floor is the bottom of the
// window under the chain_work best block. When that block cannot be read there is no
// height known to be at or below it, since MAX(height) can sit above it when a longer fork
// has less work, so a failed bounded startup rebuild is owed a full walk.
func TestStartupRepairFloorWithoutTheBestBlockOwesAFullWalk(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 1
	})

	// Best chain genesis -> 1 -> 2 -> 3, so the window under it starts at 3 less 10.
	storeBlocks(t, s, block1, block2, block3)
	require.EqualValues(t, 0, s.startupRepairFloor(context.Background()), "clamped at genesis")

	spent, cancel := context.WithCancel(context.Background())
	cancel()

	require.Equal(t, noRepairFloor, s.startupRepairFloor(spent), "no best block read, no floor")

	fault := installRebuildFault(s)
	setRepairBackoff(t, s, time.Hour)

	s.startupRebuildOnMainChain(context.Background(), false, s.startupRepairFloor(spent))

	pending, full, floor := repairRecord(s)
	require.True(t, pending, "the failed bounded rebuild is owed a repair")
	require.True(t, full, "with no floor the owed repair walks to genesis")
	require.Equal(t, noRepairFloor, floor)
	require.Equal(t, []bool{false}, fault.walks()[:1], "the startup rebuild itself was bounded")
}

// repairRecord reads what the store owes.
func repairRecord(s *SQL) (pending, full bool, floor int64) {
	r := &s.onMainChainRepair

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.pending, r.full, r.floor
}

// requireRepairSettled checks the end state once a repair has landed: nothing owed, no
// guard held, the gauge back where it started and no watch left open.
func requireRepairSettled(t *testing.T, s *SQL, gaugeBefore float64) {
	t.Helper()

	pending, full, floor := repairRecord(s)
	require.False(t, pending, "nothing is owed")
	require.False(t, full)
	require.Equal(t, noRepairFloor, floor)
	require.Zero(t, s.mainChainRebuilding.Load(), "no guard is held")
	require.Equal(t, gaugeBefore, repairPending(t), "the pending gauge is back")

	s.onMainChainRepair.mu.Lock()
	open := len(s.onMainChainRepair.watches)
	s.onMainChainRepair.mu.Unlock()

	require.Zero(t, open, "every watch is closed")
}

// reorgFixture stores genesis -> 1 -> 2 -> 3 as the best chain and a fork 1 -> alt2, and
// returns alt3 and alt4, which extend the fork and take the tip once alt4 is stored.
func reorgFixture(t *testing.T, s *SQL) (alt3, alt4 *model.Block) {
	t.Helper()

	alt3 = createBlock3OnFork(blockAlternative2)
	alt4 = createBlock3OnFork(alt3)

	storeBlocks(t, s, block1, block2, block3, blockAlternative2)
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	return alt3, alt4
}

// reorgDuringWalk is a rebuild hook for the overlap tests. SQLite cannot hold two writers
// open at once, so on its first call it stands in for a walk overlapped by a reorg: it
// stores alt3 and alt4, which moves the best chain to the fork through StoreBlock's
// reconcile, and then writes what a walk from the snapshot before the reorg would write.
// Later calls run the real transaction.
func reorgDuringWalk(s *SQL, fault *rebuildFault, alt3, alt4 *model.Block) func(ctx context.Context, full bool, floor int64) error {
	recorded := s.rebuildTxHook
	var calls atomic.Int32

	return func(ctx context.Context, full bool, floor int64) error {
		if calls.Add(1) != 1 {
			return recorded(ctx, full, floor)
		}

		// Recorded like every other call, but without running the real transaction.
		fault.failing.Store(true)
		_ = recorded(ctx, full, floor)
		fault.failing.Store(false)

		for _, b := range []*model.Block{alt3, alt4} {
			if _, _, err := s.StoreBlock(ctx, b, "peer"); err != nil {
				return err
			}
		}

		_, err := s.db.ExecContext(ctx, `UPDATE blocks SET on_main_chain = (hash IN ($1, $2)) WHERE height IN (2, 3, 4)`,
			block2.Hash().CloneBytes(), block3.Hash().CloneBytes())

		return err
	}
}

// requireForkIsTheMainChain checks the flags once the fork 1 -> alt2 -> alt3 -> alt4 has
// taken the tip from genesis -> 1 -> 2 -> 3.
func requireForkIsTheMainChain(t *testing.T, s *SQL, alt3, alt4 *model.Block) {
	t.Helper()

	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the stale walk's write is undone")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the stale walk's write is undone")
	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt3.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))
}

// TestOnMainChainRepairNarrowsAnOverlappedFullWalkToTheWritesFloor: a full repair on
// PostgreSQL runs without slowPathMu, and a StoreBlock reorg that commits during it can
// leave the walk's older view in place. The reorg notes the lowest height it can have
// moved, and the walk left every row below that right, so the repair runs next as a bounded
// walk down to that height under slowPathMu, which no write can overlap. It must not run
// the full walk again, and it must never run a full walk under slowPathMu, which would stop
// block storage for up to 30 min.
func TestOnMainChainRepairNarrowsAnOverlappedFullWalkToTheWritesFloor(t *testing.T) {
	s := newOnMainChainTestStore(t)
	s.unlockedFullRepair = true
	setRepairBackoff(t, s, time.Hour)

	alt3, alt4 := reorgFixture(t, s)

	fault := installRebuildFault(s)
	fault.failing.Store(false)
	s.rebuildTxHook = reorgDuringWalk(s, fault, alt3, alt4)

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))
	waitForRepair(t, s)

	require.Equal(t, []bool{true, false}, fault.walks(), "the overlapped full walk, then one bounded walk")
	require.Equal(t, []bool{false, true}, fault.lockHeld(), "the full walk runs unlocked, the bounded one under slowPathMu")
	require.Equal(t, s.reconcileFloor(alt4.Height), fault.floorsSeen()[1], "the bounded walk reaches the reorg's floor")

	requireForkIsTheMainChain(t, s, alt3, alt4)
	requireRepairSettled(t, s, gaugeBefore)
}

// TestOnMainChainRepairRerunsAnOperatorOverlapInFullWithoutTheLock: InvalidateBlock and
// RevalidateBlock cannot name the height they moved the chain from, so a full repair they
// overlap runs again in full. It stays unlocked however many times it is overlapped: the
// repair waits out its backoff between walks rather than taking slowPathMu for a walk to
// genesis.
func TestOnMainChainRepairRerunsAnOperatorOverlapInFullWithoutTheLock(t *testing.T) {
	const overlapped = 4

	s := newOnMainChainTestStore(t)
	s.unlockedFullRepair = true
	setRepairBackoff(t, s, time.Millisecond)

	storeBlocks(t, s, block1, block2)

	fault := installRebuildFault(s)
	fault.failing.Store(false)

	recorded := s.rebuildTxHook
	s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
		err := recorded(ctx, full, floor)

		// An operator write commits during each of the first walks.
		if fault.calls.Load() <= overlapped && s.slowPathMu.TryLock() {
			s.noteOnMainChainWrite(noRepairFloor)
			s.slowPathMu.Unlock()
		}

		return err
	}

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))
	waitForRepair(t, s)

	require.Equal(t, []bool{true, true, true, true, true}, fault.walks(), "every walk is full")
	require.Equal(t, []bool{false, false, false, false, false}, fault.lockHeld(), "no walk to genesis holds slowPathMu")
	requireRepairSettled(t, s, gaugeBefore)
}

// TestStoreBlockNotesOnlyChainWritesThatMoveMembership: an unlocked walk reruns when a
// chain write is noted during it, so a write that cannot have been overwritten must not be
// noted. A fork block that leaves the tip where it is, and an invalid record, add a row no
// older snapshot can see and change none it can. A reorg moves rows and is noted, with the
// bottom of its reconcile's walk as the height.
func TestStoreBlockNotesOnlyChainWritesThatMoveMembership(t *testing.T) {
	s := newOnMainChainTestStore(t)
	ctx := context.Background()

	storeBlocks(t, s, block1, block2, block3)

	watch := s.watchOnMainChainWrites()

	storeBlocks(t, s, blockAlternative2)

	invalid := createBlock3OnFork(block1)
	_, _, err := s.StoreBlock(ctx, invalid, "peer", options.WithInvalid(true))
	require.NoError(t, err)

	moved, floor := s.closeOnMainChainWatch(watch)
	require.False(t, moved, "a fork block below the tip and an invalid record move nothing")
	require.Equal(t, noRepairFloor, floor)

	watch = s.watchOnMainChainWrites()

	alt3 := createBlock3OnFork(blockAlternative2)
	alt4 := createBlock3OnFork(alt3)
	storeBlocks(t, s, alt3, alt4)
	require.True(t, getOnMainChain(t, s, alt4.Hash().CloneBytes()))

	moved, floor = s.closeOnMainChainWatch(watch)
	require.True(t, moved, "the reorg to alt4 is noted")
	require.Equal(t, s.reconcileFloor(alt4.Height), floor, "with the bottom of its reconcile's walk")
}

// TestChainWritesThatFailBeforeCommitAreNotNoted: a write that failed before it committed
// moved nothing. Noting it would make an unlocked walk run again for nothing. Each case
// here fails before its commit: a duplicate fork INSERT, a failed reconcile, and an
// InvalidateBlock and a RevalidateBlock whose UPDATE fails.
func TestChainWritesThatFailBeforeCommitAreNotNoted(t *testing.T) {
	s := newOnMainChainTestStore(t)
	ctx := context.Background()

	alt3, alt4 := reorgFixture(t, s)

	watch := s.watchOnMainChainWrites()

	// blockAlternative2 is a fork block, so it takes the transaction path, and its INSERT
	// fails on the duplicate hash.
	_, _, err := s.StoreBlock(ctx, blockAlternative2, "peer")
	require.Error(t, err, "a duplicate fork block fails")

	// alt3 would leave the tip alone and alt4 would take it, but the reconcile fails.
	storeBlocks(t, s, alt3)

	s.reconcileHook = func() error { return errors.NewStorageError("injected reconcile failure") }
	_, _, err = s.StoreBlock(ctx, alt4, "peer")
	require.Error(t, err, "the reconcile fails, so the transaction rolls back")
	s.reconcileHook = nil

	// Both operator UPDATEs abort inside the database.
	_, err = s.db.Exec(`CREATE TRIGGER fail_invalidate BEFORE UPDATE OF invalid ON blocks WHEN NEW.invalid = 1 AND OLD.invalid = 0 BEGIN SELECT RAISE(ABORT, 'injected'); END`)
	require.NoError(t, err)
	_, err = s.db.Exec(`CREATE TRIGGER fail_revalidate BEFORE UPDATE OF invalid ON blocks WHEN NEW.invalid = 0 AND OLD.invalid = 1 BEGIN SELECT RAISE(ABORT, 'injected'); END`)
	require.NoError(t, err)

	_, err = s.InvalidateBlock(ctx, block3.Hash())
	require.Error(t, err, "the invalidation's UPDATE fails")

	invalid := createBlock3OnFork(block2)
	_, _, err = s.StoreBlock(ctx, invalid, "peer", options.WithInvalid(true))
	require.NoError(t, err)

	require.Error(t, s.RevalidateBlock(ctx, invalid.Hash()), "the revalidation's UPDATE fails")

	moved, floor := s.closeOnMainChainWatch(watch)
	require.False(t, moved, "no write that failed before its commit is noted")
	require.Equal(t, noRepairFloor, floor)

	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "nothing moved the best chain")
	require.Zero(t, s.mainChainRebuilding.Load(), "no guard is left held")
}

// TestFailedChainWriteDuringAnUnlockedWalkDoesNotRerunIt: a fork StoreBlock that fails
// before its commit while an unlocked full repair runs must not make the repair walk again.
func TestFailedChainWriteDuringAnUnlockedWalkDoesNotRerunIt(t *testing.T) {
	s := newOnMainChainTestStore(t)
	s.unlockedFullRepair = true
	setRepairBackoff(t, s, time.Hour)

	reorgFixture(t, s)

	fault := installRebuildFault(s)
	fault.failing.Store(false)

	recorded := s.rebuildTxHook
	s.rebuildTxHook = func(ctx context.Context, full bool, floor int64) error {
		if fault.calls.Load() == 0 {
			// A duplicate fork block: the transaction fails on its INSERT.
			if _, _, err := s.StoreBlock(ctx, blockAlternative2, "peer"); err == nil {
				return errors.NewProcessingError("the duplicate fork block was stored")
			}
		}

		return recorded(ctx, full, floor)
	}

	gaugeBefore := repairPending(t)
	s.markOnMainChainDirty(true, noRepairFloor, "test", errors.NewStorageError("injected"))
	waitForRepair(t, s)

	require.Equal(t, []bool{true}, fault.walks(), "one walk: the failed write is not an overlap")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	requireRepairSettled(t, s, gaugeBefore)
}

// TestStartupFullRebuildOverlappedByAReorgOwesABoundedWalk: an overlap on the startup
// rebuild is handled like one on the loop's walk. A StoreBlock reorg names the lowest
// height it can have moved, the startup walk left every row below that right, and the
// owed repair is a bounded walk from there under slowPathMu rather than a second walk to
// genesis.
func TestStartupFullRebuildOverlappedByAReorgOwesABoundedWalk(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, time.Hour)

	alt3, alt4 := reorgFixture(t, s)

	fault := installRebuildFault(s)
	fault.failing.Store(false)
	s.rebuildTxHook = reorgDuringWalk(s, fault, alt3, alt4)

	gaugeBefore := repairPending(t)
	s.startupRebuildOnMainChain(context.Background(), true, noRepairFloor)
	waitForRepair(t, s)

	require.Equal(t, []bool{true, false}, fault.walks(), "the overlapped startup walk, then one bounded repair")
	require.Equal(t, []bool{false, true}, fault.lockHeld())
	require.Equal(t, s.reconcileFloor(alt4.Height), fault.floorsSeen()[1])

	requireForkIsTheMainChain(t, s, alt3, alt4)
	requireRepairSettled(t, s, gaugeBefore)
}

// TestStartupRebuildDoesNotWaitOutAHeldSlowPathMu: the startup goroutine holds New's guard
// count while it checks for an overlap under slowPathMu, and an operator rebuild or a
// locked repair can hold slowPathMu for up to 30 min. Startup waits only a bounded time,
// then records the overlap it cannot rule out as an owed repair, which keeps readers on
// the flag-free walk without the goroutine waiting.
func TestStartupRebuildDoesNotWaitOutAHeldSlowPathMu(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	setRepairBackoff(t, s, time.Millisecond)

	s.onMainChainRepair.mu.Lock()
	s.onMainChainRepair.startupLockWait = 20 * time.Millisecond
	s.onMainChainRepair.mu.Unlock()

	storeBlocks(t, s, block1, block2)

	fault := installRebuildFault(s)
	fault.failing.Store(false)

	floor := s.startupRepairFloor(context.Background())
	gaugeBefore := repairPending(t)

	s.slowPathMu.Lock()

	done := make(chan struct{})

	go func() {
		defer close(done)

		s.startupRebuildOnMainChain(context.Background(), false, floor)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.slowPathMu.Unlock()
		t.Fatal("the startup rebuild waited for slowPathMu")
	}

	pending, full, owedFloor := repairRecord(s)
	require.True(t, pending, "the overlap it could not rule out is owed")
	require.False(t, full, "as the bounded walk the startup rebuild ran")
	require.Equal(t, floor, owedFloor)

	s.slowPathMu.Unlock()
	waitForRepair(t, s)

	require.Equal(t, []bool{false, false}, fault.walks(), "the startup rebuild and one bounded repair")
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	requireRepairSettled(t, s, gaugeBefore)
}

// TestLockedRepairAttemptIsNotOverlappedByAWriterItQueuedBehind: a bounded attempt holds
// slowPathMu, so no chain write can overlap it. A chain write the attempt waited behind
// committed before the attempt read anything, and must not count as an overlap that
// forces a second walk; on SQLite, behind an InvalidateBlock, that walk is a full one.
func TestLockedRepairAttemptIsNotOverlappedByAWriterItQueuedBehind(t *testing.T) {
	s := newOnMainChainTestStore(t)
	setRepairBackoff(t, s, time.Millisecond)

	storeBlocks(t, s, block1, block2)

	fault := installRebuildFault(s)
	fault.failing.Store(false)

	// A slow-path writer holds the lock when the repair becomes owed.
	s.slowPathMu.Lock()
	s.markOnMainChainDirty(false, int64(block1.Height), "test", errors.NewStorageError("injected"))

	// Let the loop reach the lock and queue behind the writer.
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, fault.calls.Load(), "the attempt waits for the writer")

	s.noteOnMainChainWrite(int64(block1.Height))
	s.slowPathMu.Unlock()

	waitForRepair(t, s)

	require.EqualValues(t, 1, fault.calls.Load(), "one walk: the writer finished before the attempt began")
}
