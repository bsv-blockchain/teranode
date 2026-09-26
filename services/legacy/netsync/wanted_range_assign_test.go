package netsync

import (
	"fmt"
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

	peer, rec := schedulerPeer(t, sm, 1, to+1000)
	wireStreamingPath(sm, peer)

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
// BlockDownloadWindow is what bounds these tests at assignPassDepth: it is
// both the node-wide request budget and wantedBlocks' own read-ahead depth
// now, anchored on the committed height each test mocks the chain to report
// after calling this.
func assignManager(t *testing.T, from, to int32) *SyncManager {
	t.Helper()

	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)

	sm.settings.Legacy.BlockDownloadWindow = assignPassDepth
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

	// newDownloadAssigner's remaining budget — both the node-wide window and
	// each peer's own share — is read BEFORE unownedBlocksUpTo forgives
	// anything, so the harness defaults (window and per-peer cap both equal
	// to assignPassDepth, exactly what the first pass consumes) compute zero
	// room on the second pass and never place the re-ask this test is about.
	// Both widened well past what the ten candidates in [1,20] above the
	// committed tip of 10 could ever need, so the harness default's role —
	// bounding the FIRST pass to assignPassDepth's worth of read-ahead — is
	// carried by the header cache alone here; the assertions below already
	// use >= rather than == for that reason.
	sm.settings.Legacy.BlockDownloadWindow = 1024
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 20

	firstPeer, first := schedulerPeer(t, sm, 1, 1020)
	secondPeer, second := schedulerPeer(t, sm, 2, 1020)
	wireStreamingPath(sm, firstPeer, secondPeer)

	sm.assignWantedBlocks()

	require.True(t, WaitUntil(func() bool { return first.count()+second.count() >= assignPassDepth }, 5*time.Second),
		"the first pass must place the whole wanted range before anybody can go quiet on it")

	// Blocks are dealt out in turn, so each peer owes half. Neither has sent a
	// block byte, so once the retry window passes both are quiet, and each of
	// their blocks must go to the other peer.
	firstOwed := first.all()
	secondOwed := second.all()

	require.NotEmpty(t, firstOwed)
	require.NotEmpty(t, secondOwed)

	first.reset()
	second.reset()

	// The tracker's clock is already injectable; setting the field is how the
	// package ages an assignment without sleeping a minute. Safe unsynchronised
	// here because the manager is not running and this goroutine is the only one
	// touching the ledger.
	sm.blockDownloads.now = func() time.Time {
		return time.Now().Add(blockRequestRetryInterval + time.Second)
	}

	sm.assignWantedBlocks()

	time.Sleep(300 * time.Millisecond)
	fmt.Printf("DEBUG second-pass first=%d second=%d want=%d ledgerLen=%d\n", first.count(), second.count(), len(firstOwed)+len(secondOwed), sm.blockDownloads.Len())

	require.True(t, WaitUntil(func() bool { return first.count()+second.count() == len(firstOwed)+len(secondOwed) }, 5*time.Second),
		"every block whose owner has gone quiet past the retry window must be asked of the other peer")

	for _, h := range first.all() {
		require.Contains(t, secondOwed, h, "the first peer is only asked for the second peer's blocks")
	}

	for _, h := range second.all() {
		require.Contains(t, firstOwed, h, "and never a second time for its own, whose duplicate copy would look unrequested")
	}
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

// A block re-asked because its owner went quiet goes to the fastest peer, even one whose queue is
// full. Only a peer with room used to be offered it, and fast peers are the ones whose queues are
// full: on 2026-09-25 the 4 GB block 760,331 was re-asked of a peer at 2.7 MB/s and took 22 minutes,
// while peers at 40 to 50 MB/s were busy. The chain was waiting on it the whole time.
func TestAReAskedBlockGoesToTheFastestPeerEvenWithAFullQueue(t *testing.T) {
	sm := schedulerManager(t)

	slowPeer, _ := schedulerPeer(t, sm, 140, 1000)
	fastPeer, _ := schedulerPeer(t, sm, 141, 1000)

	slow := &assignerPeer{peer: slowPeer, budget: 1, rate: float64(5 << 20)}
	fast := &assignerPeer{peer: fastPeer, budget: 0, rate: float64(50 << 20)}
	assigner := &downloadAssigner{peers: []*assignerPeer{slow}, full: []*assignerPeer{fast}, remaining: 1}

	reAsked := wantedBlock{height: 5, hash: chainhash.Hash{0x61}, reAsked: true}
	ordinary := wantedBlock{height: 6, hash: chainhash.Hash{0x62}}

	sm.requestBlocks(assigner, []wantedBlock{reAsked, ordinary}, 0)

	require.NotNil(t, fast.getData, "the re-asked block goes to the fastest peer")
	require.Len(t, fast.getData.InvList, 1)
	require.Equal(t, reAsked.hash, fast.getData.InvList[0].Hash)

	require.NotNil(t, slow.getData, "an ordinary block still needs a peer with room")
	require.Len(t, slow.getData.InvList, 1)
	require.Equal(t, ordinary.hash, slow.getData.InvList[0].Hash)
}
