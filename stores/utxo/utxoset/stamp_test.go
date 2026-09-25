package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/bsv-blockchain/teranode/util/chainancestry/chainancestrytest"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// drainAll drives one drain the way the pruner service will: open, then every stampable window
// from the completion floor up, lowest first, each begun, paged and completed, until a window is
// not deep enough. It returns how many windows completed. liveTip is what the service would
// read uncached before each completion; the tests pass the ancestry's anchor height.
func drainAll(t *testing.T, s *Store, ctx context.Context, anc *chainancestry.Ancestry, liveTip uint32) int {
	t.Helper()

	drain, floors, err := s.OpenDrain(ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, drain.Close()) }()

	completed := 0

	for wLo := floors.StampCompleteFloor; ; wLo += TxMinedPartitionBlocks {
		state, err := drain.BeginWindow(ctx, wLo, anc)
		require.NoError(t, err)

		switch state {
		case pruner.StampWindowNotDeep:
			return completed
		case pruner.StampWindowSkipped:
			continue
		case pruner.StampWindowReady, pruner.StampWindowResumed:
		}

		for page := 0; page < drain.PagesPerWindow(); page++ {
			_, err := drain.StampPage(ctx, wLo, anc, page)
			require.NoError(t, err)
		}

		require.NoError(t, drain.CompleteWindow(ctx, wLo, anc, liveTip))

		completed++
	}
}

// stampThrough stamps every window up to and including window `through`, with the ancestry
// naming the given block ids at the given heights and a filler id everywhere else. It sets the
// store's height to the tip that makes `through` stampable and leaves it there. It returns the
// tip.
func stampThrough(t *testing.T, s *Store, ctx context.Context, through uint32, ids map[uint32]uint32) uint32 {
	t.Helper()

	tip := (through+1)*TxMinedPartitionBlocks - 1 + s.stampDepth
	require.NoError(t, s.SetBlockHeight(tip))

	floors, err := s.Floors(ctx)
	require.NoError(t, err)

	anc := chainancestrytest.Chain(t, floors.StampCompleteFloor, tip, ids)
	drainAll(t, s, ctx, anc, tip)

	return tip
}

// retireWindows stamps every window through `through` and then drops them the way the pruner
// pass will: the tip moved to the earliest height the drop rule allows, undo partitions due at
// that tip dropped first, then the containment windows. It restores the store's height
// afterwards, so a test that set one keeps it. Returns the number of windows dropped.
func retireWindows(t *testing.T, s *Store, ctx context.Context, through uint32, ids map[uint32]uint32) int {
	t.Helper()

	prev := s.GetBlockHeight()

	tip := stampThrough(t, s, ctx, through, ids)
	dropped := dropStamped(t, s, ctx, tip)

	if prev > 0 {
		require.NoError(t, s.SetBlockHeight(prev))
	}

	return dropped
}

// dropStamped drops the windows stamped at tip at the earliest height the rule allows: the tip
// moved to stamped_at + 1,728, undo partitions due at that height dropped first, then the
// containment windows. The store's height is left at the drop height.
func dropStamped(t *testing.T, s *Store, ctx context.Context, tip uint32) int {
	t.Helper()

	dropAt := tip + stampMarginBlocks + undoMaxLifeBlocks
	require.NoError(t, s.SetBlockHeight(dropAt))

	if dropAt > s.journalRetention {
		_, err := s.dropSpendJournalPartitionsBelow(ctx, dropAt-s.journalRetention)
		require.NoError(t, err)
	}

	dropped, err := s.dropStampedTxMinedWindows(ctx, dropAt)
	require.NoError(t, err)

	return dropped
}

// stampedAtOf reads a window's completion record; ok is false when it has none.
func stampedAtOf(t *testing.T, s *Store, ctx context.Context, wLo uint32) (uint32, bool) {
	t.Helper()

	var at int32

	err := s.pool.QueryRow(ctx, `SELECT stamped_at FROM tx_mined_stamped WHERE window_start = $1`, int32(wLo)).Scan(&at) //nolint:gosec // a height fits int32
	if err != nil {
		return 0, false
	}

	return uint32(at), true //nolint:gosec // a height is never negative
}

// windowAttached reports whether a window's partition is attached.
func windowAttached(t *testing.T, s *Store, ctx context.Context, window uint32) bool {
	t.Helper()

	st, err := s.txMinedWindowState(ctx, window)
	require.NoError(t, err)

	return st != nil && st.attached
}

// TestStampDepthIsDerivedFromCoinbaseMaturity pins the arithmetic the rest of the design rests
// on, so a change to any one constant is caught beside the others it must move with.
func TestStampDepthIsDerivedFromCoinbaseMaturity(t *testing.T) {
	require.Equal(t, uint32(288), StampDepthFor(100), "twice maturity, rounded up to a whole window")
	require.Equal(t, uint32(288), StampDepthFor(144))
	require.Equal(t, uint32(576), StampDepthFor(145))

	s, _ := newTestStore(t)
	require.Equal(t, uint32(288), s.stampDepth, "mainnet's maturity of 100")

	require.Equal(t, uint32(1728), uint32(undoMaxLifeBlocks), "retention plus one undo partition")
	require.Equal(t, uint32(SpendJournalPartitionBlocks), uint32(stampMarginBlocks), "the margin is one undo partition")
	require.Greater(t, uint32(claimReach), uint32(undoMaxLifeBlocks), "the create claim reaches past every undo copy")
}

// TestStampWritesTheWinnerAndDeletesTheIdentityRow is the ordinary case at the tip: a
// transaction seen before its block, mined in one block, stamped 288 deep. The window then
// drops on schedule and the UTXO alone still answers.
func TestStampWritesTheWinnerAndDeletesTheIdentityRow(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "unstamped until the window is 288 deep")
	require.Equal(t, int32(0), b)

	tip := stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.Equal(t, uint32(575), tip)

	require.Equal(t, [][2]int32{{100, 7}, {100, 7}}, pairsOf(t, s, ctx, tx), "every live UTXO carries the winner")
	require.False(t, identExists(t, s, ctx, tx), "and the identity row is gone")

	at, ok := stampedAtOf(t, s, ctx, 0)
	require.True(t, ok)
	require.Equal(t, uint32(575+288), at, "the tip plus one undo partition of margin")

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(288), floors.StampCompleteFloor)
	require.Equal(t, uint32(288), floors.StampFence)
	require.Equal(t, uint32(0), floors.DroppedFloor)

	// Not droppable until 1,728 past stamped_at.
	require.NoError(t, s.SetBlockHeight(863+1727))
	dropped, err := s.dropStampedTxMinedWindows(ctx, 863+1727)
	require.NoError(t, err)
	require.Equal(t, 0, dropped)

	require.NoError(t, s.SetBlockHeight(863+1728))
	dropped, err = s.dropStampedTxMinedWindows(ctx, 863+1728)
	require.NoError(t, err)
	require.Equal(t, 1, dropped)

	_, ok = stampedAtOf(t, s, ctx, 0)
	require.False(t, ok, "the drop deletes the completion record")

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs, "the UTXO answers once the window is gone")
}

// TestStampDeletesLosersAndLeavesForkOnlyTransactionsWaiting: a window with a fork block. The
// transaction in both blocks is stamped with the winner and the loser's row goes. The
// transaction only the fork block contained keeps its identity row, its marker and its UTXO at
// (0,0), and is counted as normal.
func TestStampDeletesLosersAndLeavesForkOnlyTransactionsWaiting(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	both := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, both, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(both), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(both), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100})
	require.NoError(t, err)

	forkOnly := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, forkOnly, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(forkOnly), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100})
	require.NoError(t, err)
	require.NotNil(t, markerOf(t, s, ctx, forkOnly), "a fork record leaves it waiting")

	losersBefore := testutil.ToFloat64(stampLosersDeleted)
	waitingBefore := testutil.ToFloat64(stampNoWinner.WithLabelValues("set"))

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

	require.Equal(t, [][2]int32{{100, 7}}, pairsOf(t, s, ctx, both), "the winner, never the loser")
	require.False(t, identExists(t, s, ctx, both))
	require.Equal(t, 1, minedRows(t, s, ctx, both), "block 8's row is gone")

	require.Equal(t, [][2]int32{{0, 0}}, pairsOf(t, s, ctx, forkOnly), "untouched")
	require.True(t, identExists(t, s, ctx, forkOnly))
	require.NotNil(t, markerOf(t, s, ctx, forkOnly))
	require.Equal(t, 0, minedRows(t, s, ctx, forkOnly), "its only row was a loser")

	require.Equal(t, losersBefore+2, testutil.ToFloat64(stampLosersDeleted))
	require.Equal(t, waitingBefore+1, testutil.ToFloat64(stampNoWinner.WithLabelValues("set")))
}

// TestStampNeverTouchesAnUnminedTransaction: an identity row with no containment at all is
// neither stamped nor deleted, however many windows pass it.
func TestStampNeverTouchesAnUnminedTransaction(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	unmined := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, unmined, 100)
	require.NoError(t, err)

	filler := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, filler, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	stampThrough(t, s, ctx, 1, map[uint32]uint32{100: 7})

	require.True(t, identExists(t, s, ctx, unmined))
	require.NotNil(t, markerOf(t, s, ctx, unmined))
	require.Equal(t, [][2]int32{{0, 0}}, pairsOf(t, s, ctx, unmined))
}

// TestStampFindsAStragglerByWhereItWasMined is the design's ST-1 and ST-2: window membership
// comes from containment and never from the height a transaction was first seen at.
func TestStampFindsAStragglerByWhereItWasMined(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	// Seen in window 0, mined in window 1.
	late := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, late, 286)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(late), utxo.MinedBlockInfo{BlockID: 20, BlockHeight: 289, OnLongestChain: true})
	require.NoError(t, err)

	// Seen in window 1, mined in window 0.
	early := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, early, 289)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(early), utxo.MinedBlockInfo{BlockID: 19, BlockHeight: 286, OnLongestChain: true})
	require.NoError(t, err)

	ids := map[uint32]uint32{286: 19, 289: 20}

	// Window 0 alone: early is stamped, late is left for window 1.
	stampThrough(t, s, ctx, 0, ids)

	require.Equal(t, [][2]int32{{286, 19}}, pairsOf(t, s, ctx, early))
	require.False(t, identExists(t, s, ctx, early))
	require.Equal(t, [][2]int32{{0, 0}}, pairsOf(t, s, ctx, late))
	require.True(t, identExists(t, s, ctx, late))

	stampThrough(t, s, ctx, 1, ids)

	require.Equal(t, [][2]int32{{289, 20}}, pairsOf(t, s, ctx, late))
	require.False(t, identExists(t, s, ctx, late))

	dropped := retireWindows(t, s, ctx, 1, ids)
	require.Equal(t, 2, dropped)

	got, err := s.Get(ctx, late.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{20}, got.BlockIDs)
}

// TestStampBelowTheCheckpointHasNoWorkAndStillCompletes is the design's ST-3: with every create
// carrying its block, tx_ident is empty, the pass stamps nothing, the window still gets its
// completion record and drops on schedule. A window that never ran the pass never drops.
func TestStampBelowTheCheckpointHasNoWorkAndStillCompletes(t *testing.T) {
	s, ctx := newTestStore(t)

	for i, h := range []uint32{100, 200, 400} {
		tx := mkTx(t, 1, 5_000+uint64(i)) //nolint:gosec // small
		_, err := s.Create(ctx, tx, h, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: 7 + uint32(i), BlockHeight: h, OnLongestChain: true})) //nolint:gosec // small
		require.NoError(t, err)
	}

	utxosBefore := testutil.ToFloat64(stampUTXOs)
	identBefore := testutil.ToFloat64(stampIdentityDeleted)
	losersBefore := testutil.ToFloat64(stampLosersDeleted)

	tip := stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7, 200: 8, 400: 9})

	require.Equal(t, losersBefore, testutil.ToFloat64(stampLosersDeleted), "no forks below the checkpoint")

	require.Equal(t, utxosBefore, testutil.ToFloat64(stampUTXOs), "nothing to stamp")
	require.Equal(t, identBefore, testutil.ToFloat64(stampIdentityDeleted))

	_, ok := stampedAtOf(t, s, ctx, 0)
	require.True(t, ok, "window 0 completed")
	_, ok = stampedAtOf(t, s, ctx, 288)
	require.False(t, ok, "window 1 was not deep enough at tip %d", tip)

	dropAt := tip + stampMarginBlocks + undoMaxLifeBlocks
	require.NoError(t, s.SetBlockHeight(dropAt))

	dropped, err := s.dropStampedTxMinedWindows(ctx, dropAt)
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "window 0 drops; window 1 has no completion record and stays")
	require.True(t, windowAttached(t, s, ctx, 1))
}

// TestStampResumesAfterACrashMidWindow is the design's ST-15: a pass abandoned after some pages
// leaves committed pages standing and no completion record; the next drain skips step 1,
// rescans, and ends in a state identical to an uninterrupted run.
func TestStampResumesAfterACrashMidWindow(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	var txs []*bt.Tx

	for i := 0; i < 6; i++ {
		tx := mkTx(t, 1, 5_000+uint64(i)) //nolint:gosec // small
		_, err := s.Create(ctx, tx, 99)
		require.NoError(t, err)
		_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
		require.NoError(t, err)

		txs = append(txs, tx)
	}

	require.NoError(t, s.SetBlockHeight(575))
	anc := chainancestrytest.Chain(t, 0, 575, map[uint32]uint32{100: 7})

	crash := errors.NewProcessingError("injected crash")

	s.stampPageHook = func(_ uint32, page int) error {
		if page == 3 {
			return crash
		}

		return nil
	}

	drain, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)

	state, err := drain.BeginWindow(ctx, 0, anc)
	require.NoError(t, err)
	require.Equal(t, pruner.StampWindowReady, state)

	for page := 0; page < 4; page++ {
		_, err := drain.StampPage(ctx, 0, anc, page)
		if page == 3 {
			require.ErrorIs(t, err, crash)
		} else {
			require.NoError(t, err)
		}
	}

	require.NoError(t, drain.Close())

	_, ok := stampedAtOf(t, s, ctx, 0)
	require.False(t, ok, "no completion record after the crash")

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), floors.StampCompleteFloor, "the completion floor did not move")
	require.Equal(t, uint32(288), floors.StampFence, "but the fence did")

	s.stampPageHook = nil

	drain, _, err = s.OpenDrain(ctx)
	require.NoError(t, err)

	state, err = drain.BeginWindow(ctx, 0, anc)
	require.NoError(t, err)
	require.Equal(t, pruner.StampWindowResumed, state, "step 1 is not run again")

	for page := 0; page < drain.PagesPerWindow(); page++ {
		_, err := drain.StampPage(ctx, 0, anc, page)
		require.NoError(t, err)
	}

	require.NoError(t, drain.CompleteWindow(ctx, 0, anc, 575))
	require.NoError(t, drain.Close())

	for _, tx := range txs {
		require.Equal(t, [][2]int32{{100, 7}}, pairsOf(t, s, ctx, tx))
		require.False(t, identExists(t, s, ctx, tx))
	}

	at, ok := stampedAtOf(t, s, ctx, 0)
	require.True(t, ok)
	require.Equal(t, uint32(863), at)
	require.Zero(t, testutil.ToFloat64(stampCompletionMissing))
}

// TestStampTreatsAWindowWithNoTableAsEmpty is decision 8: a missing window is counted, both
// stamp floors move past it, and the pass carries on to the next window.
func TestStampTreatsAWindowWithNoTableAsEmpty(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	// Only window 3 exists.
	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 999)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 30, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	before := testutil.ToFloat64(stampMissingWindows)

	stampThrough(t, s, ctx, 3, map[uint32]uint32{1000: 30})

	require.Equal(t, before+3, testutil.ToFloat64(stampMissingWindows), "windows 0, 1 and 2")
	require.Equal(t, [][2]int32{{1000, 30}}, pairsOf(t, s, ctx, tx))

	for _, w := range []uint32{0, 288, 576} {
		_, ok := stampedAtOf(t, s, ctx, w)
		require.False(t, ok, "a missing window gets no completion record; there is nothing to drop")
	}

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(4*288), floors.StampCompleteFloor)
	require.Equal(t, uint32(4*288), floors.StampFence)
}

// TestWindowDropWaitsForTheUndoPartitionsBelowItsStampedAt is the design's ST-26, the third
// drop condition in isolation: a window due by the tip is still held while an undo partition
// covering a height below its stamped_at is attached, and drops in the same pass as that
// partition once the undo drop runs first.
func TestWindowDropWaitsForTheUndoPartitionsBelowItsStampedAt(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	// A spend at 101 puts an undo copy in undo partition 0, which covers heights below 863.
	spendOneOutput(t, s, ctx, parent, 0, 101)

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

	at, ok := stampedAtOf(t, s, ctx, 0)
	require.True(t, ok)
	require.Equal(t, uint32(863), at)

	held := testutil.ToFloat64(dropHeldByUndo)

	require.NoError(t, s.SetBlockHeight(2591))

	dropped, err := s.dropStampedTxMinedWindows(ctx, 2591)
	require.NoError(t, err)
	require.Equal(t, 0, dropped, "held by undo partition 0")
	require.Equal(t, held+1, testutil.ToFloat64(dropHeldByUndo))
	require.True(t, windowAttached(t, s, ctx, 0))

	// The pruner pass drops undo first, then containment, in one call.
	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 2591, "test")
	require.NoError(t, err)

	require.False(t, windowAttached(t, s, ctx, 0))
	require.Equal(t, 0, journalLeaves(t, s, ctx), "undo partition 0 went first")
}

// TestWindowDropIsRefusedWhileAnIdentityRowIsJoinedToIt is the design's ST-4: the pre-drop
// check. An identity row planted back onto a stamped transaction refuses the drop, counts it
// and leaves the partition attached.
func TestWindowDropIsRefusedWhileAnIdentityRowIsJoinedToIt(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.False(t, identExists(t, s, ctx, tx))

	// The state the check detects: raw SQL only, because no store path can produce it.
	_, err = s.pool.Exec(ctx, `INSERT INTO tx_ident (leaf, txid, created_height) VALUES ($1, $2, 99)`,
		LeafFor(hashBytes(tx)), hashBytes(tx))
	require.NoError(t, err)

	before := testutil.ToFloat64(dropRefused)

	require.NoError(t, s.SetBlockHeight(2591))

	dropped, err := s.dropStampedTxMinedWindows(ctx, 2591)
	require.NoError(t, err)
	require.Equal(t, 0, dropped)
	require.Equal(t, before+1, testutil.ToFloat64(dropRefused))
	require.True(t, windowAttached(t, s, ctx, 0))

	dropIdentityRow(t, s, ctx, tx)

	dropped, err = s.dropStampedTxMinedWindows(ctx, 2591)
	require.NoError(t, err)
	require.Equal(t, 1, dropped)
}

// TestRetainIndefinitelySkipsTheDropsAndNotTheStamp is decision 12: the setting is an archive
// switch for the drops only.
func TestRetainIndefinitelySkipsTheDropsAndNotTheStamp(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
		withCheckpoints(ts, nil)
		ts.UtxoStore.RetainWindowsIndefinitely = true
	})

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, tx, 0, 101)

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.False(t, identExists(t, s, ctx, tx), "the stamp ran")

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(10_000))
	_, err = svc.Prune(ctx, 10_000, "test")
	require.NoError(t, err)

	require.True(t, windowAttached(t, s, ctx, 0), "the window is kept")
	require.Equal(t, 1, journalLeaves(t, s, ctx), "and so is the undo partition")
	require.Equal(t, float64(1), testutil.ToFloat64(retainIndefinitelyGauge))
}

// TestSecondDrainIsSkippedWhileOneIsOpen is the design's ST-30: the session lock.
func TestSecondDrainIsSkippedWhileOneIsOpen(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	first, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)

	before := testutil.ToFloat64(stampDrainsSkipped.WithLabelValues("lock_held"))

	_, _, err = s.OpenDrain(ctx)
	require.ErrorIs(t, err, ErrStampDrainBusy)
	require.Equal(t, before+1, testutil.ToFloat64(stampDrainsSkipped.WithLabelValues("lock_held")))

	require.NoError(t, first.Close())

	second, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)
	require.NoError(t, second.Close())
}

// TestStampRefusesAWindowWhoseBlockIsNotMined is the design's ST-22: a main-chain block of the
// window with mined_set false aborts the window with zero writes and counts it.
func TestStampRefusesAWindowWhoseBlockIsNotMined(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(575))
	anc := chainancestrytest.ChainNotMined(t, 0, 575, map[uint32]uint32{100: 7}, []uint32{150})

	before := testutil.ToFloat64(stampNotMinedAborts)

	drain, _, err := s.OpenDrain(ctx)
	require.NoError(t, err)

	_, err = drain.BeginWindow(ctx, 0, anc)
	require.Error(t, err)
	require.NoError(t, drain.Close())

	require.Equal(t, before+1, testutil.ToFloat64(stampNotMinedAborts))
	require.True(t, identExists(t, s, ctx, tx), "zero writes")
	require.Equal(t, [][2]int32{{0, 0}}, pairsOf(t, s, ctx, tx))

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), floors.StampFence)
}

// TestNoIndexOnUTXONamesMinedHeight pins the planner-trap guard: the page statement's
// mined_height = 0 test is safe only while no partial index on utxo names the column.
func TestNoIndexOnUTXONamesMinedHeight(t *testing.T) {
	s, ctx := newTestStore(t)

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'utxo%' AND indexdef LIKE '%mined_height%'`).Scan(&n))
	require.Zero(t, n)
}
