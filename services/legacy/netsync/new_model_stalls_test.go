package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// cacheManager builds a manager in the new model: the setting on, a header
// cache naming the run above the committed tip, and one peer to ask.
//
// It returns the peer alongside the manager and the recorder, unlike the
// brief this was drafted from: TestNewModel_AnArrivalAloneDoesNotMoveTheCeiling
// needs a registered peer to drive a real handleBlockMsg call through, and
// building a second one there would leave two peers competing for the same
// scheduler pass in a test that wants exactly one.
func cacheManager(t *testing.T, best int32, depth int32) (*SyncManager, *peerpkg.Peer, *getDataRecorder) {
	t.Helper()

	sm := newRaceManager(t)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.settings.Legacy.BlockDownloadLowerWindow = int(depth)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = int(depth)

	mockCommittedTip(t, sm, uint32(best), 0) //nolint:gosec // a small test height

	parent := chainhash.Hash{0xaa}
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, best+1, chainOfHeaders(parent, int(depth)+4)))

	peer, rec := schedulerPeer(t, sm, 1, best+1000)

	return sm, peer, rec
}

// TestNewModel_ACheckpointAnchorDoesNotGateAnything is the stall that had
// Hetzner mainnet wedged for ten hours and forty minutes on 2026-09-13, and for
// eight hours and thirty-three minutes the night before, written as a test.
//
// The old path marked the header at a committed checkpoint as an anchor and
// refused to walk while that anchor was at the front. The only code that
// cleared the mark ran when an incoming headers batch appended a header
// matching the next checkpoint, and once the list already held every remaining
// checkpoint no batch could ever append one again. The gate then never opened
// for the life of the process, and only losing the memory it lived in ever
// cleared it.
//
// That whole mechanism — the list, the anchor flag, and the check that read it
// — is deleted along with the header list itself, so there is no longer a way
// to even construct the wedged state: cacheManager's ordinary header-cache
// setup is already the only state fetchHeaderBlocks knows how to read, and it
// requests blocks from it unconditionally.
func TestNewModel_ACheckpointAnchorDoesNotGateAnything(t *testing.T) {
	sm, _, rec := cacheManager(t, 33333, 8)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"fetchHeaderBlocks must request blocks from the header cache with nothing standing in the way; this is the property whose absence wedged mainnet on 2026-09-13")
}

// TestNewModel_LeavingHeadersFirstModeLosesNothing pins that there is no
// state transition left that can silently change the wanted range.
//
// This used to pin resetHeaderState, the function that threw away the entire
// header list and that on the multi-peer path was never even reached. That
// function is deleted along with the list it rebuilt: the checkpoint is
// recomputed on demand from the chain's own committed height wherever it is
// needed, and there is no longer a stored copy for anything to reset.
// leaveHeadersFirstMode is what is left of any headers-first state
// transition, and it touches exactly one field — the atomic bool itself —
// which wantedBlocks never reads at all.
//
// This asserts the wanted range directly, by height and by hash, rather than
// by watching a peer get asked, for the same reason the original version of
// this test did: a whole fetchHeaderBlocks pass run around the transition
// cannot tell "the transition is harmless" apart from "the transition was
// never on the path being measured." Asserting sm.wantedBlocks() directly,
// before and after, is what makes the transition the only thing that could
// break the assertion.
func TestNewModel_LeavingHeadersFirstModeLosesNothing(t *testing.T) {
	sm, _, _ := cacheManager(t, 500, 8)

	best, _, _ := sm.committedTip()

	before := sm.wantedBlocks(best)
	require.NotEmpty(t, before, "sanity: there must be a wanted range for the transition to have a chance of losing")

	sm.leaveHeadersFirstMode()

	after := sm.wantedBlocks(best)

	require.Equal(t, before, after,
		"leaving headers-first mode must not change the wanted range, by height or by hash; the new model reads it from the header cache and the committed height, neither of which that transition touches")
}

// TestNewModel_AnArrivalAloneDoesNotMoveTheCeiling is the negative half of the
// bound, and it is the regression guard for the defect that shipped to mainnet.
//
// The read-ahead ceiling used to be anchored to the front of the header list,
// which advances when a block ARRIVES rather than when it commits. So every
// arrival raised the front, which raised the ceiling, which licensed another
// depth's worth of requests, with no coupling to the committer at all. Measured
// during a genesis resync: the front stood at height 4,877 with the chain
// settled at 868, and the park held its full 4,096 entries.
//
// A block delivered with no body is exactly that arrival without a commit: the
// current lookaheadCeilingLocked already anchors on the committed height read
// from the chain itself, and nothing about an arrival that never commits
// changes what the chain reports as its tip. This pins that property directly
// rather than trusting it did not regress.
//
// handleBlockMsg here returns a single error, not the three values the
// original brief called it with — that draft predates the head/tail split this
// package's handleBlockMsgHead/handleBlockMsgTail division introduced.
func TestNewModel_AnArrivalAloneDoesNotMoveTheCeiling(t *testing.T) {
	const depth = int32(4)

	sm, peer, rec := cacheManager(t, 500, depth)

	sm.fetchHeaderBlocks()
	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"sanity: the first pass must place work")

	before := rec.count()
	rec.reset()

	bestBeforeArrival, _, _ := sm.committedTip()

	sm.headerMu.Lock()
	ceilingBefore, limitedBefore := sm.lookaheadCeilingLocked(bestBeforeArrival)
	sm.headerMu.Unlock()

	require.True(t, limitedBefore, "the ceiling must be engaged, or this test proves nothing")

	// An arrival with no body: handleBlockMsg's own pre-checks (FSM state, then
	// ownership) have to be satisfied for the call to reach the "no block"
	// return this test wants to drive, rather than bailing out earlier at
	// "unknown peer" — which would exercise nothing.
	//
	// Added to the mock cacheManager already installed via mockCommittedTip,
	// not a fresh replacement for it: committedTip's own GetBestBlockHeader
	// stub has to stay in place for the "after" read below to answer at all.
	running := blockchain2.FSMStateRUNNING
	blockchainClient, ok := sm.blockchainClient.(*blockchain2.Mock)
	require.True(t, ok, "harness check: cacheManager must install a *blockchain2.Mock")
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	// maybeRequestMoreHeaders reaches this once the cache runs past what
	// cacheManager seeded, which this test's own arrival does not commit past —
	// harmless if never called, needed if the ceiling ever moves.
	blockchainClient.Mock.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{}}, nil)
	sm.ctx = context.Background()

	hash, ok := sm.headerCache.At(501)
	require.True(t, ok)

	sm.blockDownloads.Add(peer, hash)

	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: hash, peer: peer})
	require.Error(t, err, "a queue message carrying no block is a programming fault, not a sync one, and handleBlockMsg says so")

	// Read fresh again, the same way assignWantedBlocks would on the next pass:
	// the point under test is that this SECOND read of the real chain still
	// comes back unchanged, because the arrival above never committed anything.
	bestAfterArrival, _, _ := sm.committedTip()
	require.Equal(t, bestBeforeArrival, bestAfterArrival,
		"an arrival that never committed must not move what the chain reports as its tip")

	sm.headerMu.Lock()
	ceilingAfter, limitedAfter := sm.lookaheadCeilingLocked(bestAfterArrival)
	sm.headerMu.Unlock()

	require.True(t, limitedAfter)
	require.Equal(t, ceilingBefore, ceilingAfter,
		"a block that arrived but did not commit must not raise the ceiling; that ratchet filled the park and cost 1.8 blocks a minute on mainnet")
	require.Positive(t, before)
}
