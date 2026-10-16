package peer

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// A reply queued behind blocks the peer is still sending is not a stall. A getdata can ask for
// several blocks, and when the first arrives the whole block-reply group is cleared, so the
// headers deadline counts down again while the peer sends the next one. At 1 GB and 5 to
// 10 MB/s that is longer than the 90 s headers budget: on 2026-09-24 mainnet disconnected peers
// for "headers timeout" while they were sending blocks, and lost the block one of them had just
// finished, which stopped the chain at 708,114.
func TestAReplyQueuedBehindAHealthyDownloadIsNotAStall(t *testing.T) {
	const budget = 20 * time.Minute

	require.True(t, shouldDeferQueuedResponse(wire.CmdHeaders, true, 0, budget), "bytes are arriving at a healthy rate")
	require.False(t, shouldDeferQueuedResponse(wire.CmdHeaders, false, 0, budget), "nothing arriving is a stall")
	require.False(t, shouldDeferQueuedResponse(wire.CmdBlock, true, 0, budget), "block replies keep their own extension rule")
}

// The healthy rate is the whole association's, so transaction relay alone kept it up with no
// block in flight, and a getheaders the peer never answered was deferred for good (review of
// 2026-10-12). A reply deferred for the block download budget is a stall.
func TestAQueuedReplyIsDeferredOnlyForTheBlockBudget(t *testing.T) {
	const budget = 20 * time.Minute

	require.True(t, shouldDeferQueuedResponse(wire.CmdHeaders, true, budget-time.Second, budget))
	require.False(t, shouldDeferQueuedResponse(wire.CmdHeaders, true, budget, budget), "deferred for the whole budget")

	now := time.Now()
	require.Zero(t, deferredDuration(time.Time{}, false, now))
	require.Equal(t, time.Minute, deferredDuration(now.Add(-time.Minute), true, now))
}

// The stall handler's own wiring of the deferral bound (review of 76be891d6): a late getheaders
// deferred tick after tick while bytes keep arriving is stamped once, when first deferred, and the
// peer is judged stalled on the first tick at or past the budget from that stamp. Re-stamping on
// each tick, or not stamping at all, would defer it for good.
func TestJudgeStallTick_AReplyDeferredEveryTickIsCutOffAtTheBudget(t *testing.T) {
	const budget = 2 * time.Minute

	budgetFor := func() time.Duration { return budget }

	start := time.Now()
	pending := map[string]time.Time{wire.CmdHeaders: start.Add(-time.Second)}
	deferredSince := make(map[string]time.Time)

	now := start
	ticks := 0

	for {
		command, verdict, _ := judgeStallTick(pending, deferredSince, now, 0, true, time.Time{}, budgetFor)
		require.Equal(t, wire.CmdHeaders, command)

		if verdict == stallTickDisconnect {
			break
		}

		require.Equal(t, stallTickDefer, verdict, "tick %d: bytes are arriving, so the late reply is deferred", ticks)
		require.Equal(t, start, deferredSince[wire.CmdHeaders], "the first deferral's stamp never moves")
		require.Equal(t, now.Add(stallResponseTimeout), pending[wire.CmdHeaders], "and its deadline is pushed out")

		ticks++
		require.Less(t, ticks, 100, "the deferral must end")

		now = pending[wire.CmdHeaders].Add(time.Second)
	}

	require.GreaterOrEqual(t, now.Sub(start), budget, "not cut off before the budget")
	require.Less(t, now.Sub(start), budget+stallResponseTimeout+time.Second, "and cut off on the first tick past it")
}

// Nothing late, nothing done; and a block still arriving has its group extended, not deferred.
func TestJudgeStallTick_TheOtherVerdicts(t *testing.T) {
	now := time.Now()
	budgetFor := func() time.Duration { return 20 * time.Minute }

	_, verdict, _ := judgeStallTick(map[string]time.Time{wire.CmdHeaders: now.Add(time.Minute)}, map[string]time.Time{}, now, 0, true, time.Time{}, budgetFor)
	require.Equal(t, stallTickNone, verdict)

	pending := map[string]time.Time{wire.CmdBlock: now.Add(-time.Second)}
	_, verdict, _ = judgeStallTick(pending, map[string]time.Time{}, now, 0, true, now.Add(-time.Minute), budgetFor)
	require.Equal(t, stallTickExtendBlock, verdict)
	require.Equal(t, now.Add(stallResponseTimeoutBlocks), pending[wire.CmdBlock])

	_, verdict, _ = judgeStallTick(map[string]time.Time{wire.CmdHeaders: now.Add(-time.Second)}, map[string]time.Time{}, now, 0, false, time.Time{}, budgetFor)
	require.Equal(t, stallTickDisconnect, verdict, "nothing arriving is a stall")
}
