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
			queueArmOpen:    true,
			parked:          113,
			downloadBudget:  268435456,
			downloadHeld:    251658240,
			downloadWaiters: 6,
		}

		line := w.describe(now)

		require.Contains(t, line, "251658240 of 268435456 download budget bytes reserved",
			"the budget that can silence every peer must appear in the report that explains the silence")
		require.Contains(t, line, "6 peer read loops blocked on it",
			"a blocked read loop reads nothing from its socket, which is the whole mechanism")
	})

	t.Run("a quiet budget still reports, so a healthy reading is distinguishable from no reading", func(t *testing.T) {
		w := &consumerWait{at: now, queueArmOpen: true, downloadBudget: 268435456}

		line := w.describe(now)

		require.Contains(t, line, "0 of 268435456 download budget bytes reserved",
			"absent and zero must not look the same, or the next investigation cannot tell them apart")
		require.False(t, strings.Contains(line, "read loops blocked"),
			"no clause about blocked read loops when none is blocked")
	})

	t.Run("prefetch disabled prints nothing about a budget that does not exist", func(t *testing.T) {
		w := &consumerWait{at: now, queueArmOpen: true}

		require.False(t, strings.Contains(w.describe(now), "download budget"),
			"a nil budget means synchronous ingestion, and reporting 0 of 0 would invent a constraint")
	})
}

// TestBlockPrefetchReserved_TracksAcquireAndRelease covers the counter the report
// reads. golang.org/x/sync/semaphore does not expose its own occupancy, which is
// why the figure was never reportable; this counter is the only way to see it.
func TestBlockPrefetchReserved_TracksAcquireAndRelease(t *testing.T) {
	// Realistic sizes: AcquireBlockPrefetch floors every weight at
	// minInFlightBlockWeight (64 KiB) so a flood of tiny blocks cannot admit an
	// unbounded number of goroutines inside the byte budget, then clamps to the
	// budget. Sizes below that floor would all be charged the same and the test
	// would be measuring the floor rather than the counter.
	const budget = 4 << 20

	newSM := func() *SyncManager {
		sm := &SyncManager{
			blockPrefetchBudget:      semaphore.NewWeighted(budget),
			blockPrefetchBudgetBytes: budget,
			inFlightBlocks:           make(map[chainhash.Hash]*inFlightBlock),
		}

		return sm
	}

	t.Run("the fast path counts what it reserved", func(t *testing.T) {
		sm := newSM()
		h := chainhash.Hash{0x01}

		got, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, 1<<20)
		require.NoError(t, err)
		require.Equal(t, int64(1<<20), got)
		require.Equal(t, int64(1<<20), sm.blockPrefetchReserved.Load())

		sm.ReleaseBlockPrefetchBytes(h, got)
		require.Zero(t, sm.blockPrefetchReserved.Load(), "a released reservation must not linger in the figure")
	})

	t.Run("the release is once, matching the byte half of the gate", func(t *testing.T) {
		sm := newSM()
		h := chainhash.Hash{0x02}

		got, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, 2<<20)
		require.NoError(t, err)

		sm.ReleaseBlockPrefetchBytes(h, got)
		sm.ReleaseBlockPrefetchBytes(h, got)

		require.Zero(t, sm.blockPrefetchReserved.Load(),
			"the second release is already a no-op for the semaphore and must be one for the counter too, or the figure drifts negative")
	})

	t.Run("a cancelled wait reserves nothing and counts nothing", func(t *testing.T) {
		sm := newSM()

		first, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x03}, budget)
		require.NoError(t, err)
		require.Equal(t, int64(budget), sm.blockPrefetchReserved.Load())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = sm.AcquireBlockPrefetch(ctx, nil, chainhash.Hash{0x04}, 1<<20)
		require.Error(t, err)
		require.Equal(t, first, sm.blockPrefetchReserved.Load(),
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
	const budget = 4 << 20

	t.Run("a live reservation reaches the snapshot", func(t *testing.T) {
		sm := &SyncManager{
			blockPrefetchBudget:      semaphore.NewWeighted(budget),
			blockPrefetchBudgetBytes: budget,
			inFlightBlocks:           make(map[chainhash.Hash]*inFlightBlock),
		}

		_, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x11}, 1<<20)
		require.NoError(t, err)

		sm.blockPrefetchWaiters.Add(2)

		sm.publishConsumerWait(time.Now(), true, nil)

		w, _ := sm.consumerWaitState.Load().(*consumerWait)
		require.NotNil(t, w)
		require.Equal(t, int64(budget), w.downloadBudget, "the ceiling must be carried, not recomputed by the reader")
		require.Equal(t, int64(1<<20), w.downloadHeld, "the snapshot must read the live counter, not a constant")
		require.Equal(t, int64(2), w.downloadWaiters, "blocked read loops are the whole reason this figure is worth printing")
	})

	t.Run("prefetch disabled leaves the fields alone", func(t *testing.T) {
		sm := &SyncManager{}

		sm.publishConsumerWait(time.Now(), true, nil)

		w, _ := sm.consumerWaitState.Load().(*consumerWait)
		require.NotNil(t, w)
		require.Zero(t, w.downloadBudget,
			"synchronous ingestion has no budget, and a zero ceiling is what tells describe to say nothing")
	})
}
