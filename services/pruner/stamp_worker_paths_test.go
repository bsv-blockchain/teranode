package pruner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	blockassembly_api "github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// faultClient is the real local client over the real blockchain store with one answer bent at a
// time. Everything it does not bend is the store answering, so the chain the worker reads is
// real and only the failure being tested is injected.
type faultClient struct {
	blockchain.ClientI

	tipCalls     atomic.Int32
	failTipFrom  int32 // the nth and later uncached tip reads fail; 0 for never
	nilTip       bool
	ancestorErr  error
	fsmState     *blockchain.FSMStateType
	fsmErr       error
	emptyLinks   bool
	linksErr     error
	headerErrFor *chainhash.Hash
}

func (c *faultClient) GetBestBlockHeaderUncached(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	n := c.tipCalls.Add(1)

	if c.failTipFrom > 0 && n >= c.failTipFrom {
		return nil, nil, errors.NewServiceError("tip read failed")
	}

	if c.nilTip {
		return nil, nil, nil
	}

	return c.ClientI.GetBestBlockHeaderUncached(ctx)
}

func (c *faultClient) CheckBlockIsAncestorOfBlock(ctx context.Context, ids []uint32, hash *chainhash.Hash) (bool, error) {
	if c.ancestorErr != nil {
		return false, c.ancestorErr
	}

	return c.ClientI.CheckBlockIsAncestorOfBlock(ctx, ids, hash)
}

func (c *faultClient) GetFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	return c.fsmState, c.fsmErr
}

func (c *faultClient) GetBlockHeadersByParentLinks(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if c.linksErr != nil {
		return nil, nil, c.linksErr
	}

	if c.emptyLinks {
		return nil, nil, nil
	}

	return c.ClientI.GetBlockHeadersByParentLinks(ctx, hash, n)
}

func (c *faultClient) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if c.headerErrFor != nil && *c.headerErrFor == *hash {
		return nil, nil, errors.NewBlockNotFoundError("unknown block")
	}

	return c.ClientI.GetBlockHeader(ctx, hash)
}

// faultStamper is the fake stamper with one store answer bent at a time. It is a fake of the
// store the worker drives, as fakeStamper is, not of the chain.
type faultStamper struct {
	*fakeStamper

	openErr     error
	beginErr    error
	pageErr     error
	completeErr error
	skipFirst   bool
	alwaysDeep  bool // BeginWindow answers not deep for the first partition it is asked about
}

func (f *faultStamper) OpenDrain(ctx context.Context) (pruner.StampDrain, pruner.StampFloors, error) {
	if f.openErr != nil {
		return nil, pruner.StampFloors{}, f.openErr
	}

	_, floors, err := f.fakeStamper.OpenDrain(ctx)
	if err != nil {
		return nil, floors, err
	}

	return f, floors, nil
}

func (f *faultStamper) BeginWindow(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry) (pruner.StampWindowState, error) {
	if f.beginErr != nil {
		return 0, f.beginErr
	}

	if f.alwaysDeep {
		return pruner.StampWindowNotDeep, nil
	}

	if f.skipFirst && wLo == 0 {
		f.mu.Lock()
		f.floors.StampCompleteFloor = 288
		f.floors.StampFence = 288
		f.mu.Unlock()

		return pruner.StampWindowSkipped, nil
	}

	return f.fakeStamper.BeginWindow(ctx, wLo, anc)
}

func (f *faultStamper) StampPage(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry, page int) (pruner.StampPageResult, error) {
	if f.pageErr != nil {
		return pruner.StampPageResult{}, f.pageErr
	}

	return f.fakeStamper.StampPage(ctx, wLo, anc, page)
}

func (f *faultStamper) CompleteWindow(ctx context.Context, wLo uint32, anc *chainancestry.Ancestry, liveTip uint32) error {
	if f.completeErr != nil {
		return f.completeErr
	}

	return f.fakeStamper.CompleteWindow(ctx, wLo, anc, liveTip)
}

func newFaultStamper(pages int) *faultStamper {
	return &faultStamper{fakeStamper: newFakeStamper(pages)}
}

// A drain whose store answers with a real error, as opposed to "the lock is held", is counted as
// an error drain and retried on the timer; it is never mistaken for a skipped wake.
func TestStampDrainOpenErrorIsCountedAndRetried(t *testing.T) {
	chain := newTestChain(t, 575)
	fake := newFaultStamper(2)
	fake.openErr = errors.NewStorageError("open drain failed")
	s := newStampTestServer(t, chain.client, fake)

	before := getCounterValue(t, stampDrains, "error")
	skippedBefore := counterVecSum(t, stampWakesSkipped)

	report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

	require.Equal(t, "error", report.outcome)
	require.True(t, report.rearm())
	require.Equal(t, before+1, getCounterValue(t, stampDrains, "error"))
	require.Equal(t, skippedBefore, counterVecSum(t, stampWakesSkipped))
}

// Each store call of the drain can fail. The drain stops where it is, reports an error with what
// it had done, closes the store's drain and writes no completion record that was not asked for.
func TestStampDrainStopsOnAStoreErrorAtEachStep(t *testing.T) {
	boom := errors.NewStorageError("store said no")

	tests := []struct {
		name         string
		bend         func(*faultStamper)
		wantPages    int
		wantWindows  int
		wantComplete int
	}{
		{"begin", func(f *faultStamper) { f.beginErr = boom }, 0, 0, 0},
		{"page", func(f *faultStamper) { f.pageErr = boom }, 0, 0, 0},
		{"complete", func(f *faultStamper) { f.completeErr = boom }, 3, 0, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chain := newTestChain(t, 575)
			fake := newFaultStamper(3)
			tc.bend(fake)
			s := newStampTestServer(t, chain.client, fake)

			before := getCounterValue(t, stampDrains, "error")

			report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

			require.Equal(t, "error", report.outcome)
			require.Equal(t, tc.wantWindows, report.windows)
			require.Equal(t, tc.wantPages, report.pages)
			require.True(t, report.rearm(), "an error is retried")
			require.Equal(t, uint32(1), report.residualLag, "partition 0 is still stampable")
			require.Equal(t, before+1, getCounterValue(t, stampDrains, "error"))

			_, _, completes, _, closed := fake.snapshot()
			require.Empty(t, completes)
			require.Equal(t, 1, closed, "the drain is closed on every exit")
		})
	}
}

// A partition the store skips, because it has no table, is stepped over: the next partition is begun
// at the next start height and the drain goes on.
func TestStampDrainStepsOverASkippedWindow(t *testing.T) {
	chain := newTestChain(t, 863)
	fake := newFaultStamper(2)
	fake.skipFirst = true
	s := newStampTestServer(t, chain.client, fake)

	report := s.runStampDrain(s.ctx, notificationAt(chain, 863), false)

	require.Equal(t, "completed", report.outcome)
	require.Equal(t, 1, report.windows, "partition 1 only; partition 0 was skipped")

	_, _, completes, _, _ := fake.snapshot()
	require.Equal(t, []fakeComplete{{288, 863}}, completes)
}

// The worker computes stampability from its own floors and the tip; if the store judges the first
// partition not deep enough, which can happen only if the two disagree, the drain ends cleanly
// with nothing done.
func TestStampDrainEndsCompletedWhenTheStoreSaysNotDeep(t *testing.T) {
	chain := newTestChain(t, 575)
	fake := newFaultStamper(2)
	fake.alwaysDeep = true
	s := newStampTestServer(t, chain.client, fake)

	report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

	require.Equal(t, "completed", report.outcome)
	require.Zero(t, report.windows)
	require.Equal(t, uint32(1), report.residualLag)
	require.True(t, report.rearm(), "a partition is still stampable, so the timer is owed")
}

// A tip shallower than the first partition plus the depth has nothing to stamp. The drain says so
// without a chain walk and does not arm the retry timer: a notification will wake it.
func TestStampDrainWithNothingStampableWalksNoChain(t *testing.T) {
	chain := newTestChain(t, 300)
	counting := &countingClient{ClientI: chain.client}
	fake := newFaultStamper(2)
	s := newStampTestServer(t, counting, fake)

	before := getCounterValue(t, stampDrains, "nothing_stampable")

	report := s.runStampDrain(s.ctx, notificationAt(chain, 300), false)

	require.Equal(t, "nothing_stampable", report.outcome)
	require.False(t, report.rearm())
	require.Zero(t, counting.parentLinkFetches.Load())
	require.Equal(t, before+1, getCounterValue(t, stampDrains, "nothing_stampable"))

	begins, _, _, _, closed := fake.snapshot()
	require.Empty(t, begins)
	require.Equal(t, 1, closed)
}

// An error reading the tip ends the drain as an error, whether the client fails or answers with
// nothing, and the store's drain is still closed.
func TestStampDrainWithAnUnreadableTipIsAnError(t *testing.T) {
	for name, client := range map[string]*faultClient{
		"read fails":      {failTipFrom: 1},
		"answers nothing": {nilTip: true},
	} {
		t.Run(name, func(t *testing.T) {
			chain := newTestChain(t, 575)
			client.ClientI = chain.client
			fake := newFaultStamper(2)
			s := newStampTestServer(t, client, fake)

			report := s.runStampDrain(s.ctx, nil, false)

			require.Equal(t, "error", report.outcome)
			require.True(t, report.rearm())

			begins, _, _, opened, closed := fake.snapshot()
			require.Empty(t, begins)
			require.Equal(t, 1, opened)
			require.Equal(t, 1, closed)
		})
	}
}

// A walk of parent links that cannot be proved is counted by the check that failed and ends the
// drain; the store is never handed an ancestry nobody proved. Any other failure of the walk is a
// plain error. Both are retried.
func TestStampDrainRefusesAnAncestryItCannotProve(t *testing.T) {
	t.Run("rejected", func(t *testing.T) {
		chain := newTestChain(t, 575)
		client := &faultClient{ClientI: chain.client, emptyLinks: true}
		fake := newFaultStamper(2)
		s := newStampTestServer(t, client, fake)

		before := getCounterValue(t, stampAncestryRejected, chainancestry.CheckLength)
		drainsBefore := getCounterValue(t, stampDrains, "ancestry_rejected")

		report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

		require.Equal(t, "ancestry_rejected", report.outcome)
		require.True(t, report.rearm())
		require.Equal(t, before+1, getCounterValue(t, stampAncestryRejected, chainancestry.CheckLength))
		require.Equal(t, drainsBefore+1, getCounterValue(t, stampDrains, "ancestry_rejected"))

		begins, _, _, _, closed := fake.snapshot()
		require.Empty(t, begins)
		require.Equal(t, 1, closed)
	})

	t.Run("fetch fails", func(t *testing.T) {
		chain := newTestChain(t, 575)
		client := &faultClient{ClientI: chain.client, linksErr: errors.NewServiceError("walk failed")}
		fake := newFaultStamper(2)
		s := newStampTestServer(t, client, fake)

		before := getCounterValue(t, stampDrains, "error")

		report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

		require.Equal(t, "error", report.outcome)
		require.Equal(t, before+1, getCounterValue(t, stampDrains, "error"))

		begins, _, _, _, _ := fake.snapshot()
		require.Empty(t, begins)
	})
}

// The freshness check is fail-safe: if the worker cannot read the tip, or cannot tell whether
// its anchor is still an ancestor of a tip that moved, it abandons the drain rather than write
// under an ancestry it can no longer vouch for.
func TestStampDrainAbandonsWhenFreshnessCannotBeChecked(t *testing.T) {
	t.Run("tip unreadable between steps", func(t *testing.T) {
		chain := newTestChain(t, 575)
		// The first read is the anchor; every read after it fails.
		client := &faultClient{ClientI: chain.client, failTipFrom: 2}
		fake := newFaultStamper(2)
		s := newStampTestServer(t, client, fake)

		before := testutil.ToFloat64(stampDrainsAbandoned)

		report := s.runStampDrain(s.ctx, nil, false)

		require.Equal(t, "abandoned", report.outcome)
		require.Zero(t, report.pages, "nothing was written")
		require.Equal(t, before+1, testutil.ToFloat64(stampDrainsAbandoned))
	})

	t.Run("ancestor check fails after the chain moved", func(t *testing.T) {
		chain := newTestChain(t, 575)
		client := &faultClient{ClientI: chain.client, ancestorErr: errors.NewServiceError("ancestor query failed")}
		fake := newFaultStamper(3)
		fake.onPage = func(_ uint32, page int) {
			if page == 0 {
				chain.storeBlock(chain.hashes[575], 576, 0)
			}
		}
		s := newStampTestServer(t, client, fake)

		report := s.runStampDrain(s.ctx, nil, false)

		require.Equal(t, "abandoned", report.outcome)
		require.Equal(t, 1, report.pages, "page 0 committed before the move was seen")
		require.True(t, report.rearm())

		_, _, completes, _, closed := fake.snapshot()
		require.Empty(t, completes)
		require.Equal(t, 1, closed)
	})
}

// A notification for a block that is an ancestor of the tip is an ordinary late notification and
// is not counted stale. One naming a block the store has never heard of is.
func TestStampAnchorHintCountsOnlyWhatIsNotOnTheBestChain(t *testing.T) {
	chain := newTestChain(t, 575)
	unknown := chainhash.HashH([]byte("never stored"))
	client := &faultClient{ClientI: chain.client, headerErrFor: &unknown}
	fake := newFaultStamper(2)
	s := newStampTestServer(t, client, fake)

	before := testutil.ToFloat64(stampStaleAnchorHints)

	report := s.runStampDrain(s.ctx, &pruneSignal{blockHeight: 500, blockHash: chain.hashes[500]}, false)
	require.Equal(t, "completed", report.outcome)
	require.Equal(t, before, testutil.ToFloat64(stampStaleAnchorHints), "an ancestor is a late notification, not a stale one")

	s.stamper = newFaultStamper(2)
	report = s.runStampDrain(s.ctx, &pruneSignal{blockHeight: 500, blockHash: unknown}, false)
	require.Equal(t, "completed", report.outcome, "a stale hint changes nothing about the drain")
	require.Equal(t, before+1, testutil.ToFloat64(stampStaleAnchorHints))
}

// A timer-driven drain has no pruner processor in front of it, so the worker re-checks the start
// conditions itself, and each refusal names its reason and stamps nothing.
func TestStampTimerDrainRechecksTheStartConditions(t *testing.T) {
	running := blockchain.FSMStateRUNNING
	catching := blockchain.FSMStateCATCHINGBLOCKS

	tests := []struct {
		name       string
		tune       func(s *Server, c *faultClient)
		wantReason string
		wantRearm  bool
	}{
		{"below the minimum height", func(s *Server, _ *faultClient) { s.settings.Pruner.MinBlockHeight = 575 }, "below_min_height", false},
		{"catching up", func(s *Server, c *faultClient) {
			s.settings.Pruner.SkipDuringCatchup = true
			c.fsmState = &catching
		}, "catchup_mode", true},
		{"fsm unreadable", func(s *Server, c *faultClient) {
			s.settings.Pruner.SkipDuringCatchup = true
			c.fsmErr = errors.NewServiceError("fsm down")
		}, "fsm_error", true},
		{"running proceeds", func(s *Server, c *faultClient) {
			s.settings.Pruner.SkipDuringCatchup = true
			c.fsmState = &running
		}, "completed", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chain := newTestChain(t, 575)
			client := &faultClient{ClientI: chain.client}
			fake := newFaultStamper(2)
			s := newStampTestServer(t, client, fake)
			tc.tune(s, client)

			timerBefore := testutil.ToFloat64(stampTimerDrains)
			skipBefore := counterVecSum(t, stampWakesSkipped)

			report := s.runStampDrain(s.ctx, nil, true)

			require.Equal(t, tc.wantReason, report.outcome)
			require.Equal(t, tc.wantRearm, report.rearm())
			require.Equal(t, timerBefore+1, testutil.ToFloat64(stampTimerDrains))

			begins, _, _, _, closed := fake.snapshot()
			require.Equal(t, 1, closed)

			if tc.wantReason == "completed" {
				require.Equal(t, []uint32{0}, begins)
				require.Equal(t, skipBefore, counterVecSum(t, stampWakesSkipped))

				return
			}

			require.Empty(t, begins)
			require.Equal(t, skipBefore+1, counterVecSum(t, stampWakesSkipped), "counted as a skipped wake, not a drain")
		})
	}
}

// A timer-driven drain also waits for block assembly to have caught up to the tip, as the
// processor does for a notification, and gives up with its own reason when it has not.
func TestStampTimerDrainSkipsWhenBlockAssemblyIsNotReady(t *testing.T) {
	chain := newTestChain(t, 575)
	fake := newFaultStamper(2)
	s := newStampTestServer(t, chain.client, fake)
	s.settings.Pruner.BlockAssemblyWaitTimeout = 200 * time.Millisecond

	ba := &blockassembly.Mock{}
	ba.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{BlockAssemblyState: "resetting", CurrentHeight: 575}, nil)
	s.blockAssemblyClient = ba

	before := getCounterValue(t, stampWakesSkipped, "block_assembly_timeout")

	report := s.runStampDrain(s.ctx, nil, true)

	require.Equal(t, "block_assembly_timeout", report.outcome)
	require.True(t, report.rearm())
	require.Equal(t, before+1, getCounterValue(t, stampWakesSkipped, "block_assembly_timeout"))

	begins, _, _, _, _ := fake.snapshot()
	require.Empty(t, begins)
}

// A node that has stopped accepting blocks gets no notifications, yet must still work off its
// backlog: the retry timer armed at start-up runs a drain by itself. Cancelling the context then
// stops the goroutine.
func TestStampWorkerDrainsOnItsTimerWithNoNotificationAndStopsOnShutdown(t *testing.T) {
	chain := newTestChain(t, 575)
	fake := newFaultStamper(2)
	s := newStampTestServer(t, chain.client, fake)

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	stopped := make(chan struct{})

	go func() {
		s.stampWorker(ctx)
		close(stopped)
	}()

	require.Eventually(t, func() bool {
		_, _, completes, _, closed := fake.snapshot()

		return len(completes) == 1 && closed == 1
	}, 5*time.Second, 10*time.Millisecond, "no notification was sent, so only the timer can have run this")

	cancel()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the stamp worker did not stop on cancellation")
	}
}

func TestStampRearmByOutcome(t *testing.T) {
	tests := []struct {
		outcome  string
		lag      uint32
		wantArm  bool
		describe string
	}{
		{"no_chain_client", 0, false, "a missing client does not change on a timer"},
		{"nothing_stampable", 0, false, "a notification will wake it"},
		{"below_min_height", 0, false, "a notification will wake it"},
		{"completed", 0, false, "caught up"},
		{"completed", 2, true, "partitions still stampable"},
		{"abandoned", 0, true, "the chain moved; start over"},
		{"ancestry_rejected", 0, true, "retry"},
		{"error", 0, true, "retry"},
		{"lock_held", 0, true, "retry"},
		{"catchup_mode", 0, true, "retry"},
	}

	for _, tc := range tests {
		t.Run(tc.outcome+"/"+tc.describe, func(t *testing.T) {
			require.Equal(t, tc.wantArm, stampDrainReport{outcome: tc.outcome, residualLag: tc.lag}.rearm())
		})
	}
}

// stampableWindows is the worker's own arithmetic for which partitions a tip makes stampable.
func TestStampableWindows(t *testing.T) {
	tests := []struct {
		name                  string
		floor, tip, width, dp uint32
		want                  uint32
	}{
		{"tip below the first partition plus depth", 0, 574, 288, 288, 0},
		{"first partition just deep", 0, 575, 288, 288, 1},
		{"three deep", 0, 1151, 288, 288, 3},
		{"floor above the highest", 864, 1151, 288, 288, 0},
		{"floor midway", 288, 1151, 288, 288, 2},
		{"width plus depth overflows a short chain", 0, 10, 288, 288, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, stampableWindows(tc.floor, tc.tip, tc.width, tc.dp))
		})
	}
}

// A server with no store, or one whose stamp has a zero width, has no stamp worker.
func TestFindStamperIsNilWithoutAStoreOrWithAZeroWidthStamp(t *testing.T) {
	require.Nil(t, (&Server{}).findStamper())
	require.Nil(t, (&Server{utxoStore: stampingStore{stamp: nil}}).findStamper())
	require.Nil(t, (&Server{utxoStore: stampingStore{stamp: pruner.NoStamp}}).findStamper())
}
