package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// unconnectedBatch builds a headers batch hanging off a parent this node has
// never heard of, which is the fault the disconnect under test punishes. The
// nonce is bumped per header, so every batch is a fresh set of hashes and none
// of them can be recognised by the late-reply carve-out's header-index test.
func unconnectedBatch(nonce *uint32) *wire.MsgHeaders {
	msg, _ := linkedHeaders(chainhash.Hash{0x9e, byte(*nonce)}, 3, nonce)

	return msg
}

// unconnectedRunLength asks the manager what it has charged this peer, rather
// than inferring it from the disconnect. Nothing outside handleHeadersMsg writes
// the counter, so this is the wiring itself.
func unconnectedRunLength(t *testing.T, sm *SyncManager, p *peerpkg.Peer) int32 {
	t.Helper()

	state, ok := sm.peerStates.Get(p)
	require.True(t, ok)

	return state.unconnectingHeaders.Load()
}

// TestHandleHeadersMsg_AnUnconnectedBatchCostsNothingFirstTime is the fix for
// the 800128 stall. At 01:28:30 on 2026-09-11 Hetzner mainnet disconnected
// 51.75.213.175 for one headers batch that did not connect — the only occurrence
// of that message in the whole run — and that peer was carrying the sync alone.
// Three minutes later the node committed its last block and then did nothing for
// seven hours. SV Node charges this fault nothing at all the first time.
//
// The peer here is neither the sync peer nor inside a demotion cooldown, so the
// late-reply carve-out cannot fire and the disconnect path is what is under
// test.
func TestHandleHeadersMsg_AnUnconnectedBatchCostsNothingFirstTime(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe1}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm := newDemotionManager(t)

	peer, _, _ := demotionPeer(t, sm, 140, 1000)
	seedFetchHeaders(t, sm, peer, anchor, msg)

	require.Nil(t, sm.loadSyncPeer(), "no sync peer, so the carve-out for the elected peer cannot apply")

	state, ok := sm.peerStates.Get(peer)
	require.True(t, ok)
	require.False(t, state.inDemotionCooldown(), "and no cooldown, so the carve-out for a demoted peer cannot apply either")

	lenBefore := sm.headerListLen()

	sm.handleHeadersMsg(&headersMsg{headers: unconnectedBatch(&nonce), peer: peer})

	require.True(t, peer.Connected(),
		"one headers batch that connects to nothing must not cost the peer its connection")
	require.Equal(t, lenBefore, sm.headerListLen(),
		"and none of its headers may enter the list")
	require.Equal(t, int32(1), unconnectedRunLength(t, sm, peer),
		"the batch is charged rather than ignored, or the run can never reach the threshold")
}

// TestHandleHeadersMsg_TenUnconnectedBatchesCostTheConnection pins the boundary
// rather than a round number, so the threshold cannot drift without a test
// noticing. Ten is SV Node's MAX_UNCONNECTING_HEADERS (validation.h:220), the
// point at which the reference first charges anything for this fault.
func TestHandleHeadersMsg_TenUnconnectedBatchesCostTheConnection(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe2}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm := newDemotionManager(t)

	peer, _, _ := demotionPeer(t, sm, 141, 1000)
	seedFetchHeaders(t, sm, peer, anchor, msg)

	for i := 1; i < maxUnconnectingHeaderBatches; i++ {
		sm.handleHeadersMsg(&headersMsg{headers: unconnectedBatch(&nonce), peer: peer})

		require.True(t, peer.Connected(),
			"batch %d of %d must still be forgiven", i, maxUnconnectingHeaderBatches)
	}

	require.Equal(t, int32(maxUnconnectingHeaderBatches-1), unconnectedRunLength(t, sm, peer))

	sm.handleHeadersMsg(&headersMsg{headers: unconnectedBatch(&nonce), peer: peer})

	require.False(t, peer.Connected(),
		"batch %d is where the connection goes", maxUnconnectingHeaderBatches)
}

// TestHandleHeadersMsg_AConnectingBatchResetsTheUnconnectedCount pins the one
// rule that stops an intermittent fault accumulating to a disconnect over hours:
// a batch that connects puts the run back to zero, exactly as
// net_processing.cpp:3450-3456 does. Without it the counter is just a slower
// version of the first-offence disconnect.
func TestHandleHeadersMsg_AConnectingBatchResetsTheUnconnectedCount(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe3}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm := newDemotionManager(t)

	peer, _, _ := demotionPeer(t, sm, 142, 1000)
	seedFetchHeaders(t, sm, peer, anchor, msg)

	for i := 1; i < maxUnconnectingHeaderBatches; i++ {
		sm.handleHeadersMsg(&headersMsg{headers: unconnectedBatch(&nonce), peer: peer})
	}

	require.True(t, peer.Connected())

	// One honest continuation onto the back of the list.
	good, goodHashes := linkedHeaders(hashes[len(hashes)-1], 2, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: good, peer: peer})

	require.Equal(t, len(hashes)+len(goodHashes)+1, sm.headerListLen(),
		"the connecting batch has to actually connect, or the reset is not what this test measures")
	require.Zero(t, unconnectedRunLength(t, sm, peer), "a batch that connects ends the run")

	// Nine more. Eighteen unconnected batches in total, and the peer survives.
	for i := 1; i < maxUnconnectingHeaderBatches; i++ {
		sm.handleHeadersMsg(&headersMsg{headers: unconnectedBatch(&nonce), peer: peer})

		require.True(t, peer.Connected(),
			"batch %d after the reset must be forgiven, the run restarted at zero", i)
	}

	// And the run that restarted still bites at the same boundary.
	sm.handleHeadersMsg(&headersMsg{headers: unconnectedBatch(&nonce), peer: peer})

	require.False(t, peer.Connected(), "the reset restarts the run, it does not disarm it")
}

// TestHandleHeadersMsg_ACheckpointMismatchStillCostsTheConnectionAtOnce pins the
// one header fault that must stay immediate. A header at a checkpoint height
// whose hash is not the checkpoint's is a peer feeding us a chain we know to be
// wrong; SV Node answers that with DoS(100), an instant ban. Softening the
// non-connecting case must not soften this one.
func TestHandleHeadersMsg_ACheckpointMismatchStillCostsTheConnectionAtOnce(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe4}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm := newDemotionManager(t)

	peer, _, _ := demotionPeer(t, sm, 143, 1000)
	seedFetchHeaders(t, sm, peer, anchor, msg)

	// seedFetchHeaders anchors at height 10, so the seeded headers are 11..15 and
	// the next one along is 16. Put the checkpoint there, with a hash no
	// generated header can match.
	checkpointHash := chainhash.Hash{0xee}

	sm.headerMu.Lock()
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 16, Hash: &checkpointHash}
	sm.headerMu.Unlock()

	bad, _ := linkedHeaders(hashes[len(hashes)-1], 1, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: bad, peer: peer})

	require.False(t, peer.Connected(),
		"a checkpoint hash mismatch is charged on the first offence, not counted")
	require.Zero(t, unconnectedRunLength(t, sm, peer),
		"and it is not the unconnected-batch fault, so it must not touch that run")
}

// TestHandleHeadersMsg_AForgivenBatchAsksTheSamePeerNothing pins the return on
// the forgiven path, which nothing pinned when the softening first landed.
//
// The send at the bottom of handleHeadersMsg anchors its locator on the last
// header the loop looked at. On this path that is the first header of the batch
// that did not connect, a header this node does not hold, so it is the one
// anchor certain to bring back another batch that cannot be spliced. Without the
// return the peer answers its own bad answer nine more times, 1.6 MB and ten
// header-list lock acquisitions, and is disconnected at the threshold anyway.
// Forgiving the fault would then buy latency instead of survival, which is the
// opposite of why it was forgiven.
//
// HONEST LIMIT, so nobody reads more into this than it proves. Removing the
// return does NOT make this test fail: with the return deleted, and the
// mutation confirmed applied and compiling, no getheaders reaches this peer
// anyway. So the test pins the observable contract and does not discriminate
// the line that enforces it. Something between the switch and the send is
// already returning in this harness and I did not establish what, because the
// package's test logger discards output and probe logging proved nothing. The
// live-node review that found the fall-through reported a concrete locator
// hash, so the two observations disagree and the disagreement is unresolved.
// The return is kept because it makes the comment above it true and cannot
// make the behaviour worse, not because this test proves it necessary.
func TestHandleHeadersMsg_AForgivenBatchAsksTheSamePeerNothing(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe7}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm := newDemotionManager(t)

	peer, _, sent := demotionPeer(t, sm, 142, 1000)
	seedFetchHeaders(t, sm, peer, anchor, msg)

	require.Nil(t, sm.loadSyncPeer(), "no sync peer, so the late-reply carve-out cannot fire")

	before := sent.count()

	sm.handleHeadersMsg(&headersMsg{headers: unconnectedBatch(&nonce), peer: peer})

	require.True(t, peer.Connected(), "sanity: the first unconnected batch is forgiven, not punished")
	require.Equal(t, int32(1), unconnectedRunLength(t, sm, peer), "sanity: and it is charged")

	require.Equal(t, before, sent.count(),
		"a forgiven batch must leave the peer alone: the only locator this path could build is anchored on the header we just failed to place, so the reply could only be another batch we cannot splice")
}
