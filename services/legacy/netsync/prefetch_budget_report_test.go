package netsync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// TestConsumerWait_Describe_NamesTheDownloadBudget is the regression test for a
// blind spot that has now cost two investigations.
//
// The watchdog exists because of the wedge at height 754,895 on 2026-09-08, and
// the file's own header describes that wedge as thirteen goroutines holding block
// weight against the download budget with two peer read loops blocked inside its
// acquire. A read loop blocked there reads nothing more from its socket, so the
// peer goes silent whatever it owes.
//
// The report never printed that budget. It printed the window's byte budget, a
// different one, which is empty during exactly this fault — so every stall report
// to date has read "0 bytes charged" while the budget that causes the stall was
// full and unmentioned. Measured 2026-09-10: two stall episodes with zero block
// arrivals from any of eight peers, one lasting seventeen minutes.
func TestConsumerWait_Describe_NamesTheDownloadBudget(t *testing.T) {
	now := time.Now()

	t.Run("a full budget with blocked read loops is named", func(t *testing.T) {
		w := &consumerWait{
			at:              now,
			parked:          113,
			downloadBudget:  64,
			downloadHeld:    60,
			downloadWaiters: 6,
		}

		line := w.describe(now)

		require.Contains(t, line, "60 of 64 download budget slots reserved",
			"the budget that can silence every peer must appear in the report that explains the silence")
		require.Contains(t, line, "6 peer read loops blocked on it",
			"a blocked read loop reads nothing from its socket, which is the whole mechanism")
	})

	t.Run("a quiet budget still reports, so a healthy reading is distinguishable from no reading", func(t *testing.T) {
		w := &consumerWait{at: now, downloadBudget: 64}

		line := w.describe(now)

		require.Contains(t, line, "0 of 64 download budget slots reserved",
			"absent and zero must not look the same, or the next investigation cannot tell them apart")
		require.False(t, strings.Contains(line, "read loops blocked"),
			"no clause about blocked read loops when none is blocked")
	})

	t.Run("a manager built without New prints nothing about a budget that does not exist", func(t *testing.T) {
		w := &consumerWait{at: now}

		require.False(t, strings.Contains(w.describe(now), "download budget"),
			"a zero budget means no manager built it (New always does), and reporting 0 of 0 would invent a constraint")
	})
}

// TestBlockPrefetchReserved_TracksAcquireAndRelease covers the counter the report
// reads. golang.org/x/sync/semaphore does not expose its own occupancy, which is
// why the figure was never reportable; this counter is the only way to see it.
func TestBlockPrefetchReserved_TracksAcquireAndRelease(t *testing.T) {
	const budget = 4

	newSM := func() *SyncManager {
		return &SyncManager{
			blockPrefetchBudget:      semaphore.NewWeighted(budget),
			blockPrefetchBudgetSlots: budget,
			inFlightBlocks:           make(map[chainhash.Hash]*inFlightBlock),
		}
	}

	t.Run("the fast path counts what it reserved", func(t *testing.T) {
		sm := newSM()
		h := chainhash.Hash{0x01}

		err := sm.AcquireBlockPrefetch(context.Background(), h)
		require.NoError(t, err)
		require.Equal(t, int64(1), sm.blockPrefetchReserved.Load())

		sm.ReleaseBlockPrefetch(h)
		require.Zero(t, sm.blockPrefetchReserved.Load(), "a released reservation must not linger in the figure")
	})

	t.Run("the release is once", func(t *testing.T) {
		sm := newSM()
		h := chainhash.Hash{0x02}

		err := sm.AcquireBlockPrefetch(context.Background(), h)
		require.NoError(t, err)

		sm.ReleaseBlockPrefetch(h)
		sm.ReleaseBlockPrefetch(h)

		require.Zero(t, sm.blockPrefetchReserved.Load(),
			"the second release is already a no-op for the semaphore and must be one for the counter too, or the figure drifts negative")
	})

	t.Run("a cancelled wait reserves nothing and counts nothing", func(t *testing.T) {
		sm := newSM()

		for i := 0; i < budget; i++ {
			require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{byte(i + 1)}))
		}
		require.Equal(t, int64(budget), sm.blockPrefetchReserved.Load())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := sm.AcquireBlockPrefetch(ctx, chainhash.Hash{0x99})
		require.Error(t, err)
		require.Equal(t, int64(budget), sm.blockPrefetchReserved.Load(),
			"an acquire that failed must leave the figure exactly where it was")
	})
}

// TestPublishConsumerWait_CarriesTheDownloadBudget pins the WIRING, not the
// rendering. A test that builds a consumerWait by hand proves the sentence is
// printable and nothing about whether the manager ever fills the fields in — a
// mutation that stopped reading the live counter and reported a constant zero
// passed the rendering tests untouched. This is the same trap as a settings field
// that is declared but never loaded: the struct says it exists and only the loader
// makes it real.
func TestPublishConsumerWait_CarriesTheDownloadBudget(t *testing.T) {
	const budget = 4

	t.Run("a live reservation reaches the snapshot", func(t *testing.T) {
		sm := &SyncManager{
			blockPrefetchBudget:      semaphore.NewWeighted(budget),
			blockPrefetchBudgetSlots: budget,
			inFlightBlocks:           make(map[chainhash.Hash]*inFlightBlock),
		}

		err := sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{0x11})
		require.NoError(t, err)

		sm.blockPrefetchWaiters.Add(2)

		sm.publishConsumerWait(time.Now())

		w, _ := sm.consumerWaitState.Load().(*consumerWait)
		require.NotNil(t, w)
		require.Equal(t, int64(budget), w.downloadBudget, "the ceiling must be carried, not recomputed by the reader")
		require.Equal(t, int64(1), w.downloadHeld, "the snapshot must read the live counter, not a constant")
		require.Equal(t, int64(2), w.downloadWaiters, "blocked read loops are the whole reason this figure is worth printing")
	})

	t.Run("a manager built without New leaves the fields alone", func(t *testing.T) {
		sm := &SyncManager{}

		sm.publishConsumerWait(time.Now())

		w, _ := sm.consumerWaitState.Load().(*consumerWait)
		require.NotNil(t, w)
		require.Zero(t, w.downloadBudget,
			"a nil budget is a manager built without New, and a zero ceiling is what tells describe to say nothing")
	})
}
