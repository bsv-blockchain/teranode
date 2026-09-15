package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// assignPassDepth is both the read-ahead depth and the per-peer cap the harness
// runs with, and the two are deliberately equal. A pass then hands the single
// peer exactly its whole cap, which is the state the re-ask rule has to work in:
// a peer that has gone quiet holding a full slice has no budget left until
// somebody forgives it.
const assignPassDepth = 8

// reset forgets everything recorded so far, so a test can measure one pass
// without the previous pass's getdata in the total. Callers must first wait for
// the previous pass's message to have landed: the send is asynchronous, and a
// reset that races it drops the wrong pass's hashes.
func (r *getDataRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.hashes = nil
	r.msgs = 0
}

// assignHarness builds a manager the assignment pass can run against: headers
// for every height in [from, to], one connected peer that records every getdata
// it is sent, and a live download ledger.
//
// Settings come from the real loader and are then narrowed to assignPassDepth,
// so the numbers under test are small enough to reason about while the code
// path is the one an unconfigured node takes.
func assignHarness(t *testing.T, from, to int32) (*SyncManager, *getDataRecorder) {
	t.Helper()

	sm := assignManager(t, from, to)

	_, rec := schedulerPeer(t, sm, 1, to+1000)

	return sm, rec
}

// assignManager is assignHarness without the peer, for a test that needs to
// connect more than one.
//
// The wanted range is named by the header cache now, not the header list, so
// the run [from, to] is seeded there with chainOfHeaders — real linked headers,
// the same fixture header_cache_test.go uses, rather than the header list's old
// fixed byte-pattern hashes. Nothing downstream of assignWantedBlocks reads
// headerList, so it is left unseeded.
//
// BlockDownloadLowerWindow alone is what bounds these tests at assignPassDepth:
// lookaheadCeilingLocked anchors on the committed height now, which each test
// mocks the chain to report after calling this, so the ceiling engages on that alone and
// BlockDownloadWindow is left at its real default. Setting the node-wide window
// to the same value as the depth used to be how this fixture routed around a
// ceiling that could not engage at all; doing that here now would only mask
// whether the depth or the window was the thing actually binding.
func assignManager(t *testing.T, from, to int32) *SyncManager {
	t.Helper()

	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)

	sm.settings.Legacy.BlockDownloadLowerWindow = assignPassDepth
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = assignPassDepth

	parent := chainhash.Hash{0xaa}
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, from, chainOfHeaders(parent, int(to-from+1))))

	return sm
}

// waitForPass blocks until the pass's getdata has reached the peer's remote end,
// so a following reset cannot swallow it. The send is asynchronous, so sampling
// the recorder straight after the call would be sampling a race.
func waitForPass(t *testing.T, rec *getDataRecorder) {
	t.Helper()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"the pass's getdata should have reached the peer")
}

// TestAssignWantedBlocks_AsksForWhatIsWantedAndNotOwed is the ordinary pass.
func TestAssignWantedBlocks_AsksForWhatIsWantedAndNotOwed(t *testing.T) {
	sm, rec := assignHarness(t, 1, 20)
	mockCommittedTip(t, sm, 10, 0)

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"a node with headers above its best block must ask somebody for them")
	require.LessOrEqual(t, rec.count(), assignPassDepth,
		"and it must not ask beyond the read-ahead depth")
}

// TestAssignWantedBlocks_FollowsAChainAdvanceMadeOutsideLegacySync pins the
// other bug the stored, self-updated tip had, alongside the same-height reorg
// wedge covered in header_cache_wiring_test.go: a chain advance made by any
// route OTHER than this package's own HandleBlockDirect/HandleConvertedBlock
// commits — a different node process, a different code path, anything — used
// to leave the stored copy behind for good, pinning the wanted range below the
// real tip for the rest of the process's life. Reading the chain directly, as
// committedTip now does, has no such copy to go stale: the very next read sees
// whatever the chain reports, whoever advanced it.
//
// Asserted on wantedBlocks directly, by height, rather than through a getdata
// recorder: a second pass's heights 16-18 would already be owed to the peer
// from the first pass, and filtering that out is unownedBlocks' job, not
// evidence about what the wanted range itself starts at.
func TestAssignWantedBlocks_FollowsAChainAdvanceMadeOutsideLegacySync(t *testing.T) {
	sm := assignManager(t, 1, 30)
	mockCommittedTip(t, sm, 10, 0)

	best, _, ok := sm.committedTip()
	require.True(t, ok)
	require.Equal(t, int32(10), best, "sanity: the mock answers what this test just told it to")

	wanted := sm.wantedBlocks(best)
	require.NotEmpty(t, wanted)
	require.Equal(t, int32(11), wanted[0].height, "the range starts just above the old tip")

	// The chain advances by some route this package never touches — no
	// HandleBlockDirect, no HandleConvertedBlock, nothing on this SyncManager
	// at all. Only the mocked answer changes, standing in for that other route.
	mockCommittedTip(t, sm, 15, 1)

	best, _, ok = sm.committedTip()
	require.True(t, ok)
	require.Equal(t, int32(15), best, "committedTip must follow the chain's advance on the very next read")

	wanted = sm.wantedBlocks(best)
	require.NotEmpty(t, wanted)
	require.Equal(t, int32(16), wanted[0].height,
		"the wanted range must start above the NEW tip, not the old one a locally cached copy would still be reporting")
}

// TestAssignWantedBlocks_DoesNotReAskForABlockAlreadyOwed pins the duplicate
// guard. Asking again inside the retry window wastes a slot the pass could have
// spent on a block nobody owes.
func TestAssignWantedBlocks_DoesNotReAskForABlockAlreadyOwed(t *testing.T) {
	sm, rec := assignHarness(t, 1, 20)
	mockCommittedTip(t, sm, 10, 0)

	sm.assignWantedBlocks()
	waitForPass(t, rec)

	first := rec.all()
	require.NotEmpty(t, first, "the first pass must ask for something for the second to have anything to skip")

	rec.reset()
	sm.assignWantedBlocks()

	require.False(t, WaitUntil(func() bool { return rec.count() > 0 }, 2*time.Second),
		"a second pass with nothing delivered and nothing expired must ask for nothing")
}

// TestAssignWantedBlocks_ReAsksWhenTheOwnerHasGoneQuiet is the help-a-struggling-peer
// rule, and it needs two peers because the help has to come from somewhere. One
// peer takes the whole run and goes quiet; once the retry window expires the
// block must be asked of the OTHER peer, and never a second time of the peer
// that already owes it — a peer that answered both requests would have its
// second copy arrive unowned and lose its association for it.
func TestAssignWantedBlocks_ReAsksWhenTheOwnerHasGoneQuiet(t *testing.T) {
	sm := assignManager(t, 1, 20)
	mockCommittedTip(t, sm, 10, 0)

	_, first := schedulerPeer(t, sm, 1, 1020)
	_, second := schedulerPeer(t, sm, 2, 1020)

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return first.count()+second.count() >= assignPassDepth }, 5*time.Second),
		"the first pass must place the whole wanted range before anybody can go quiet on it")

	// Runs are handed out contiguously, so one peer takes the lot. Which one
	// depends on peer-id ordering, so read it off the recorders rather than
	// assuming.
	quiet, helper := first, second
	if quiet.count() == 0 {
		quiet, helper = second, first
	}

	require.Equal(t, assignPassDepth, quiet.count(),
		"the run goes to one peer in one piece")
	require.Zero(t, helper.count(),
		"which leaves the other peer owing nothing, and free to help")

	quiet.reset()
	helper.reset()

	// The tracker's clock is already injectable; setting the field is how the
	// package ages an assignment without sleeping a minute. Safe unsynchronised
	// here because the manager is not running and this goroutine is the only one
	// touching the ledger.
	sm.blockDownloads.now = func() time.Time {
		return time.Now().Add(blockRequestRetryInterval + time.Second)
	}

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return helper.count() > 0 }, 5*time.Second),
		"a block whose owner has gone quiet past the retry window must be asked of the other peer")
	require.False(t, WaitUntil(func() bool { return quiet.count() > 0 }, 2*time.Second),
		"and never a second time of the peer that already owes it, whose duplicate copy would look unrequested")
}

// TestAssignWantedBlocks_TerminatesWhenEverythingIsOwed is the spin found in the
// shipped binary on 2026-09-12: a pass whose every candidate was owed by a peer
// requested nothing, reported more work to do, and repeated for ever at one
// blockchain round trip per candidate.
func TestAssignWantedBlocks_TerminatesWhenEverythingIsOwed(t *testing.T) {
	sm, rec := assignHarness(t, 1, 20)
	mockCommittedTip(t, sm, 10, 0)

	sm.assignWantedBlocks()
	waitForPass(t, rec)

	done := make(chan struct{})

	go func() {
		sm.assignWantedBlocks()
		close(done)
	}()

	// A failure here reads oddly and it is worth knowing why before blaming
	// flakiness. t.Fatal below only stops THIS goroutine, so a spinning
	// assignWantedBlocks keeps the process alive and the run ends as a
	// package-level "test timed out" panic whose stack points at the call in the
	// goroutine above. That panic is this guard firing.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the assignment pass did not finish: it has no loop to spin in, so this means one was added")
	}
}
