package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// honestHeadersFor models what a peer does with a locator we send it: it answers
// from the newest hash in the locator that is on its own chain, and from the
// start of the chain when none of them is. That second case is the one that costs
// a peer its connection — the reply is anchored somewhere we have never heard of,
// so nothing in it can connect to anything we hold.
func honestHeadersFor(t *testing.T, locator []*chainhash.Hash, known map[chainhash.Hash]struct{}, n int, nonce *uint32) *wire.MsgHeaders {
	t.Helper()

	for _, h := range locator {
		if _, ok := known[*h]; ok {
			msg, _ := linkedHeaders(*h, n, nonce)

			return msg
		}
	}

	// SV Node's FindForkInGlobalIndex falls back to the genesis block when it
	// recognises nothing in the locator, and LocateHeaders then answers from
	// height 1.
	msg, _ := linkedHeaders(chainhash.Hash{0x77}, n, nonce)

	return msg
}

// TestHeadersRoundLocator_APeerThatCannotReachTheBackKeepsItsConnection is the
// hazard the branch's own one-hash locator introduced. Keeping the header list
// across a sync-peer change means the next getheaders has to continue from the
// back of that list rather than from our database best block, because
// handleHeadersMsg requires incoming headers to connect to the back. A locator
// holding only the back asks a question that a peer which has not got that far
// cannot answer: it recognises nothing, so it replies from the start of the
// chain, and we disconnect it with a misbehaviour warning for answering us
// honestly.
//
// startSync elects any connected candidate above our own height, which mid-round
// can be hundreds of headers below the back of the list. This is that peer.
func TestHeadersRoundLocator_APeerThatCannotReachTheBackKeepsItsConnection(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf8}
	msg, hashes := linkedHeaders(anchor, 40, &nonce)

	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 130, 1000)
	short, _, shortHeaders := demotionPeer(t, sm, 131, 210)

	// The list runs 201 to 240. The short peer claims 210: comfortably above our
	// database best height of 100, so it is electable, and comfortably below the
	// back of the list, so it cannot answer a question asked only about the back.
	sm.resetHeaderState(&anchor, 200)
	sm.headersFirstMode.Store(true)
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: stalled})
	require.Equal(t, len(hashes)+1, sm.headerListLen(), "the seeded headers should all have linked")

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()
	require.Equal(t, short, sm.loadSyncPeer(), "the short peer is the only candidate outside a demotion cooldown")

	require.True(t, WaitUntil(func() bool { return shortHeaders.count() > 0 }, 5*time.Second),
		"the new sync peer should have been asked to continue the headers round")

	locator := shortHeaders.last().BlockLocatorHashes
	require.NotEmpty(t, locator)
	require.Equal(t, &hashes[len(hashes)-1], locator[0],
		"the round still has to continue from the back of the list we kept")

	// Everything this peer has: our chain up to 210, which is the anchor and the
	// first ten headers of the list. Nothing above. Our own database best block
	// is deliberately left out, so a locator that reaches nothing but that is
	// still treated as unanswerable.
	known := map[chainhash.Hash]struct{}{anchor: {}}
	for i := 0; i <= 9; i++ {
		known[hashes[i]] = struct{}{}
	}

	listLenBefore := sm.headerListLen()

	sm.handleHeadersMsg(&headersMsg{headers: honestHeadersFor(t, locator, known, 5, &nonce), peer: short})

	require.True(t, short.Connected(),
		"a peer that answers our locator from the newest block it has must keep its connection")
	require.Equal(t, listLenBefore, sm.headerListLen(),
		"and its reply, which does not continue the round, must be ignored rather than spliced in")
}
