package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// headerListBackHeight reads the height of the header the next round will be
// asked to continue from, which is the height the stop hash used to be measured
// against.
func headerListBackHeight(t *testing.T, sm *SyncManager) int32 {
	t.Helper()

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	back := sm.headerList.Back()
	require.NotNil(t, back)

	node, ok := back.Value.(*headerNode)
	require.True(t, ok)

	return node.height
}

// TestHeadersRound_TheRequestIsNotBoundedByTheNextCheckpoint pins the question
// the node asks, not the answer it hopes for.
//
// A peer serves fork+1 through and including the stop block, so a stop hash at
// the next checkpoint makes the width of the answer checkpointHeight minus the
// height of locator[0] — and locator[0] is the back of the header list, which
// climbs towards that checkpoint all round. The question therefore narrows as
// the round progresses and reaches zero width at the top of the span, where an
// empty reply is the correct answer and handleHeadersMsg returns from it without
// touching any state.
//
// Asking to the end of the peer's chain cannot be answered with nothing by a
// peer that has anything, which is the whole point. The second half of this test
// is the guard on the fix: the locator itself must not have moved, because the
// round still has to continue from the back.
func TestHeadersRound_TheRequestIsNotBoundedByTheNextCheckpoint(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd1}
	msg, hashes := linkedHeaders(anchor, 40, &nonce)

	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 150, 1000)
	successor, _, successorHeaders := demotionPeer(t, sm, 151, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()
	require.Equal(t, successor, sm.loadSyncPeer(), "the stalled peer must have been demoted, or no round goes out at all")

	require.True(t, WaitUntil(func() bool { return successorHeaders.count() > 0 }, 5*time.Second),
		"the new sync peer should have been asked to continue the headers round")

	got := successorHeaders.last()
	require.NotNil(t, got)

	cp := sm.nextCheckpointSnapshot()
	require.NotNil(t, cp, "sanity: the round is still aimed at a checkpoint")

	require.Equal(t, zeroHash, got.HashStop,
		"the round must ask to the end of the peer's chain: a stop hash is a ceiling, and a ceiling the back is climbing towards is a question that runs out of answers")
	require.NotEqual(t, *cp.Hash, got.HashStop,
		"the checkpoint is enforced by the height compare in the splice loop, never by the wire stop hash")

	require.NotEmpty(t, got.BlockLocatorHashes)
	require.Equal(t, &hashes[len(hashes)-1], got.BlockLocatorHashes[0],
		"only the stop hash changes: the locator still starts at the back of the header list we kept")
	require.Greater(t, len(got.BlockLocatorHashes), 1,
		"and still steps back through the list, so a peer that cannot reach the back can find a fork point")
}

// TestHeadersRound_AnAnchorOneBelowTheCheckpointStillAsksAnAnswerableQuestion is
// the regression test for the seven-hour Hetzner mainnet stall of 2026-09-11.
//
// The node held headers up to 849,999 and had committed only up to 800,128. Its
// locator was anchored on the back of the header list, at 849,999, and its stop
// hash named the 850,000 checkpoint. A stop hash one above locator[0] is a
// one-block question. The peer answered it correctly, in 7.5 ms, with zero
// headers, and an empty headers message is the one shape handleHeadersMsg
// returns from before touching any state — no splice, no fetch, no retry. The
// node re-asked the same one-block question every three and a half minutes for
// seven hours while the blocks it actually needed sat 50,000 below.
func TestHeadersRound_AnAnchorOneBelowTheCheckpointStillAsksAnAnswerableQuestion(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xd2}
	msg, hashes := linkedHeaders(anchor, 95, &nonce)

	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 152, 1000)
	successor, _, successorHeaders := demotionPeer(t, sm, 153, 1000)

	// seedFetchHeaders anchors at height 10, so 95 linked headers put the back
	// at 105 — above this manager's committed height of 100, exactly as the
	// header list outruns the chain on a real node.
	seedFetchHeaders(t, sm, stalled, anchor, msg)

	backHeight := headerListBackHeight(t, sm)
	require.Equal(t, int32(105), backHeight)

	// The checkpoint is set after the seeding because resetHeaderState re-derives
	// it from the chain params, and it is set to exactly one above the back:
	// the 849,999 against 850,000 of the stall, in miniature.
	checkpointHash := chainhash.Hash{0xd3}
	sm.headerMu.Lock()
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: backHeight + 1, Hash: &checkpointHash}
	sm.headerMu.Unlock()

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()
	require.Equal(t, successor, sm.loadSyncPeer())

	require.True(t, WaitUntil(func() bool { return successorHeaders.count() > 0 }, 5*time.Second),
		"the new sync peer should have been asked to continue the headers round")

	got := successorHeaders.last()
	require.NotNil(t, got)
	require.NotEmpty(t, got.BlockLocatorHashes)
	require.Equal(t, &hashes[len(hashes)-1], got.BlockLocatorHashes[0],
		"sanity: the locator is anchored one below the checkpoint, which is the state under test")
	require.Equal(t, zeroHash, got.HashStop,
		"with locator[0] one below the checkpoint a checkpoint stop hash is a one-block question, and zero headers is its correct answer")
	require.NotEqual(t, checkpointHash, got.HashStop,
		"the checkpoint must not be the ceiling on the wire")
}

// TestHandleHeadersMsg_ABatchThatOvershootsTheCheckpointStillVerifiesIt is the
// safety argument for removing the stop hash.
//
// Without a ceiling on the wire the last batch of a checkpoint span comes back
// wider than the span, up to a full 2000 headers past the checkpoint. That
// overshoot must change nothing: the checkpoint is still verified, the headers
// above it are decoded and dropped rather than spliced, and the blocks below it
// are still fetched. The bound has always lived in the
// node.height == sm.nextCheckpoint.Height compare inside the splice loop, which
// breaks out of the batch the moment it matches.
func TestHandleHeadersMsg_ABatchThatOvershootsTheCheckpointStillVerifiesIt(t *testing.T) {
	const (
		anchorHeight = 10
		toCheckpoint = 41
		overshoot    = 50
	)

	var nonce uint32

	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, data := connectRacePeer(t, 154, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	anchor := chainhash.Hash{0xd4}
	msg, hashes := linkedHeaders(anchor, toCheckpoint+overshoot, &nonce)

	// The checkpoint is the last header of the span; everything after it in the
	// same message is the overshoot a zero stop hash now brings back.
	checkpoint := hashes[toCheckpoint-1]

	sm.resetHeaderState(&anchor, anchorHeight)
	sm.headerMu.Lock()
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: anchorHeight + toCheckpoint, Hash: &checkpoint}
	sm.headerMu.Unlock()
	sm.headersFirstMode.Store(true)

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	require.True(t, syncPeer.Connected(), "an overshooting batch is an honest answer to the question we asked")

	require.False(t, sm.anchorIsStillTheFront(),
		"the checkpoint was verified, so the round's anchor must have been trimmed out of the list")

	require.True(t, sm.headerIsInTheList(checkpoint), "the checkpoint header itself is spliced")
	require.False(t, sm.headerIsInTheList(hashes[toCheckpoint]),
		"the first header above the checkpoint must be decoded and dropped, not spliced")
	require.False(t, sm.headerIsInTheList(hashes[len(hashes)-1]),
		"and so must the last of the overshoot")

	// The anchor is gone and the checkpoint header stays, so the list holds
	// exactly the span.
	require.Equal(t, toCheckpoint, sm.headerListLen(),
		"the list must stop at the checkpoint: the stop hash was never what kept it there")

	require.True(t, WaitUntil(func() bool { return data.count() > 0 }, 5*time.Second),
		"verifying the checkpoint is what releases the download walk, so getdata must have gone out")
}
