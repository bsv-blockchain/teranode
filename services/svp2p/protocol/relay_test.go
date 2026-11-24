package protocol

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// selectRelayTargets: pure, table-driven.
// ---------------------------------------------------------------------------

func TestSelectRelayTargets(t *testing.T) {
	hash := chainhash.Hash{0x01}
	header := &wire.BlockHeader{}

	t.Run("a sendheaders peer with the parent gets a headers message, not an inv", func(t *testing.T) {
		peer := &Peer{}

		out := selectRelayTargets([]relayCandidate{{peer: peer, wantsHeaders: true, hasParent: true}}, hash, header)

		require.Len(t, out, 1)
		require.Same(t, peer, out[0].peer)

		got, ok := out[0].msg.(*wire.MsgHeaders)
		require.True(t, ok, "expected *wire.MsgHeaders, got %T", out[0].msg)
		require.Len(t, got.Headers, 1)
		require.Equal(t, header, got.Headers[0])
	})

	t.Run("a non-sendheaders peer gets a plain block inv, not headers", func(t *testing.T) {
		peer := &Peer{}

		out := selectRelayTargets([]relayCandidate{{peer: peer, wantsHeaders: false, hasParent: true}}, hash, header)

		require.Len(t, out, 1)
		require.Same(t, peer, out[0].peer)

		got, ok := out[0].msg.(*wire.MsgInv)
		require.True(t, ok, "expected *wire.MsgInv, got %T", out[0].msg)
		require.Len(t, got.InvList, 1)
		require.Equal(t, wire.InvTypeBlock, got.InvList[0].Type)
		require.Equal(t, hash, got.InvList[0].Hash)
	})

	// Fix round 1, review finding I2: a sendheaders peer that cannot connect
	// the header (hasParent false) must fall back to inv, not get headers it
	// cannot place.
	t.Run("a sendheaders peer missing the parent gets inv, not headers", func(t *testing.T) {
		peer := &Peer{}

		out := selectRelayTargets([]relayCandidate{{peer: peer, wantsHeaders: true, hasParent: false}}, hash, header)

		require.Len(t, out, 1)

		got, ok := out[0].msg.(*wire.MsgInv)
		require.True(t, ok, "expected *wire.MsgInv (SendBlockHeaders' fRevertToInv, net_processing.cpp:5308-5312), got %T", out[0].msg)
		require.Equal(t, hash, got.InvList[0].Hash)
	})

	// Fix round 1, review finding I1 + Minor 2: hasBlock gates BOTH branches
	// (net_processing.cpp:5453-5455 re-tests PeerHasHeader before the inv
	// fallback too), not only the headers branch.
	t.Run("a peer that already has the block gets nothing, whether it wants headers or not", func(t *testing.T) {
		headersPeer := &Peer{}
		invPeer := &Peer{}

		out := selectRelayTargets([]relayCandidate{
			{peer: headersPeer, wantsHeaders: true, hasParent: true, hasBlock: true},
			{peer: invPeer, wantsHeaders: false, hasBlock: true},
		}, hash, header)

		require.Empty(t, out)
	})

	t.Run("a mixed batch routes each candidate independently", func(t *testing.T) {
		// Each identified by connectedAt rather than left as an identical
		// zero-value literal: testify's map-key lookups below compare by
		// reflect.DeepEqual, which would treat every &Peer{} as equal to
		// every other one and defeat the "which peer got what" assertions.
		headersPeer := &Peer{connectedAt: time.Unix(1, 0)}
		invPeer := &Peer{connectedAt: time.Unix(2, 0)}
		hasBlockPeer := &Peer{connectedAt: time.Unix(3, 0)}
		missingParentPeer := &Peer{connectedAt: time.Unix(4, 0)}

		out := selectRelayTargets([]relayCandidate{
			{peer: headersPeer, wantsHeaders: true, hasParent: true},
			{peer: invPeer, wantsHeaders: false, hasParent: true},
			{peer: hasBlockPeer, wantsHeaders: false, hasBlock: true},
			{peer: missingParentPeer, wantsHeaders: true, hasParent: false},
		}, hash, header)

		require.Len(t, out, 3)

		byPeer := map[*Peer]wire.Message{}
		for _, d := range out {
			byPeer[d.peer] = d.msg
		}

		_, gotHeaders := byPeer[headersPeer].(*wire.MsgHeaders)
		require.True(t, gotHeaders)

		_, gotInv := byPeer[invPeer].(*wire.MsgInv)
		require.True(t, gotInv)

		_, gotFallbackInv := byPeer[missingParentPeer].(*wire.MsgInv)
		require.True(t, gotFallbackInv, "the missing-parent peer must fall back to inv, not get nothing")

		require.NotContains(t, byPeer, hasBlockPeer)
	})
}

// ---------------------------------------------------------------------------
// peerHasHeader: pure, against a real HeaderIndex.
// ---------------------------------------------------------------------------

func TestPeerHasHeader(t *testing.T) {
	genesis := syncGenesis()
	chain := minedRun(genesis, 3, 1)

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	for _, h := range chain {
		_, err := idx.AddHeader(h)
		require.NoError(t, err)
	}

	tip, ok := idx.Lookup(chain[2].BlockHash())
	require.True(t, ok)

	mid, ok := idx.Lookup(chain[1].BlockHash())
	require.True(t, ok)

	t.Run("nil index answers false", func(t *testing.T) {
		state := &peerSyncState{pindexBestKnownBlock: &tip}
		require.False(t, peerHasHeader(nil, state, chain[0].BlockHash()))
	})

	t.Run("nil state answers false", func(t *testing.T) {
		require.False(t, peerHasHeader(idx, nil, chain[0].BlockHash()))
	})

	t.Run("a hash not in the index answers false", func(t *testing.T) {
		state := &peerSyncState{pindexBestKnownBlock: &tip, pindexBestHeaderSent: &tip}
		require.False(t, peerHasHeader(idx, state, chainhash.Hash{0xEE}))
	})

	t.Run("a hash the peer announced to us (pindexBestKnownBlock) answers true", func(t *testing.T) {
		state := &peerSyncState{pindexBestKnownBlock: &tip}
		require.True(t, peerHasHeader(idx, state, chain[0].BlockHash()), "chain[0] is an ancestor of tip")
		require.True(t, peerHasHeader(idx, state, tip.Hash), "a header is its own ancestor at its own height")
	})

	t.Run("a hash WE already told the peer about (pindexBestHeaderSent) answers true", func(t *testing.T) {
		state := &peerSyncState{pindexBestHeaderSent: &tip}
		require.True(t, peerHasHeader(idx, state, chain[0].BlockHash()))
	})

	t.Run("a hash above both trackers' height answers false", func(t *testing.T) {
		state := &peerSyncState{pindexBestKnownBlock: &mid, pindexBestHeaderSent: &mid}
		require.False(t, peerHasHeader(idx, state, tip.Hash), "tip is above mid, which is all either tracker covers")
	})

	t.Run("neither tracker set answers false", func(t *testing.T) {
		state := &peerSyncState{}
		require.False(t, peerHasHeader(idx, state, chain[0].BlockHash()))
	})
}

// ---------------------------------------------------------------------------
// PeerManager.RelayBlock: real peers over real sockets.
// ---------------------------------------------------------------------------

// relayTestFixture wires a real, running PeerManager with a two-header chain
// indexed, so getheaders/pindexBestHeaderSent tests have something to serve.
type relayTestFixture struct {
	m       *PeerManager
	genesis *wire.BlockHeader
	chain   []*wire.BlockHeader
}

func newRelayTestFixture(t *testing.T) *relayTestFixture {
	t.Helper()

	genesis := syncGenesis()
	chain := minedRun(genesis, 2, 1)

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	for _, h := range chain {
		_, err := idx.AddHeader(h)
		require.NoError(t, err)
	}

	m := syncTestManager(t, idx, &recordingIngestor{})

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))

	t.Cleanup(func() {
		cancel()
		_ = m.Stop()
	})

	return &relayTestFixture{m: m, genesis: genesis, chain: chain}
}

// peersSnapshot copies the connection registry so a caller can diff it after
// dialing a new connection.
func (f *relayTestFixture) peersSnapshot() map[*Peer]*SyncPeer {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()

	out := make(map[*Peer]*SyncPeer, len(f.m.peers))
	for p, s := range f.m.peers {
		out[p] = s
	}

	return out
}

// connect dials the manager, completes the handshake, optionally negotiates
// sendheaders, and returns the wire-level scripted peer plus the manager's own
// *Peer/*SyncPeer pair for it — found by diffing the registry across the dial,
// since an inbound connection's ephemeral port is not known in advance.
//
// The initial getheaders that Established() sends is drained and answered
// with an empty headers message: it is the establishment barrier (a
// non-sync-enabled manager would never send it, so its arrival proves the
// handshake finished from the manager's side, not just the scripted peer's),
// and answering empty ends the headers-first round cleanly rather than
// leaving it stuck mid-sync for the rest of the test.
func (f *relayTestFixture) connect(t *testing.T, sendHeaders bool) (*scriptedPeer, *Peer, *SyncPeer) {
	t.Helper()

	before := f.peersSnapshot()

	far := dialScripted(t, f.m.ListenAddrs()[0])

	version := remoteVersion(uint64(time.Now().UnixNano())) //nolint:gosec // test-only nonce
	version.Services = wire.SFNodeNetwork
	far.completeOutboundHandshakeAs(t, version)

	_, ok := far.readUntil(t, wire.CmdGetHeaders).(*wire.MsgGetHeaders)
	require.True(t, ok)
	far.write(t, wire.NewMsgHeaders())

	if sendHeaders {
		far.write(t, wire.NewMsgSendHeaders())
	}

	// A ping/pong round trip barriers the sendheaders write above: each
	// peer's inbound messages are processed in order on its own goroutine, so
	// the pong only comes back after sendheaders has already been applied to
	// PeerInfo.WantsHeaders.
	nonce := uint64(time.Now().UnixNano()) //nolint:gosec // test-only nonce
	far.write(t, wire.NewMsgPing(nonce))

	pong, ok := far.readUntil(t, wire.CmdPong).(*wire.MsgPong)
	require.True(t, ok)
	require.Equal(t, nonce, pong.Nonce)

	var (
		peer     *Peer
		syncPeer *SyncPeer
	)

	require.Eventually(t, func() bool {
		f.m.mu.Lock()
		defer f.m.mu.Unlock()

		for p, s := range f.m.peers {
			if _, existed := before[p]; !existed {
				peer, syncPeer = p, s
				return true
			}
		}

		return false
	}, 2*time.Second, 10*time.Millisecond, "the manager never registered the new connection")

	return far, peer, syncPeer
}

// TestRelayBlockSelectsHeadersOrInv is the wire-level pin on the sendheaders
// split: a peer that negotiated sendheaders AND can connect the new header
// receives a headers message for a relayed block, and a peer that did not
// negotiate sendheaders receives a plain inv — read from two real sockets in
// the same RelayBlock call.
func TestRelayBlockSelectsHeadersOrInv(t *testing.T) {
	f := newRelayTestFixture(t)

	headersPeer, _, headersSync := f.connect(t, true)
	invPeer, _, _ := f.connect(t, false)

	// The headers peer must already have the block's PARENT for the headers
	// branch to apply at all (review finding I2) — driven the same way
	// TestRelayBlockExcludesPeerThatAnnouncedByHeaders establishes it,
	// announcing our own tip back to us as a peer normally would.
	_, _, err := f.m.Headers(headersSync, &wire.MsgHeaders{Headers: []*wire.BlockHeader{f.chain[len(f.chain)-1]}})
	require.NoError(t, err)

	block := minedChild(f.chain[len(f.chain)-1], testEasyBits, 99)
	hash := block.BlockHash()

	f.m.RelayBlock(hash, block)

	headersMsg, ok := headersPeer.readUntil(t, wire.CmdHeaders).(*wire.MsgHeaders)
	require.True(t, ok)
	require.Len(t, headersMsg.Headers, 1)
	require.Equal(t, hash, headersMsg.Headers[0].BlockHash())

	invMsg, ok := invPeer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Len(t, invMsg.InvList, 1)
	require.Equal(t, wire.InvTypeBlock, invMsg.InvList[0].Type)
	require.Equal(t, hash, invMsg.InvList[0].Hash)
}

// TestRelayBlockExcludesOriginatorAndDuplicates pins the known-block set's
// two jobs at once: the peer that already told us about a hash (the
// originator) never gets it announced back, and a second RelayBlock call for
// the same hash announces to no one, including a peer that was never told the
// first time.
//
// Neither negative is asserted by silence (the D5 trap: "received nothing"
// cannot tell correctly-suppressed apart from sent-but-slow). Both are
// asserted by requiring a SECOND, always-announced block to be the very next
// relay-related message the suppressed peer receives: sends for one peer
// leave PeerManager.RelayBlock through a single ordered channel
// (transport.Conn.Send -> its one writer goroutine), so if the suppressed
// block had actually been sent, it would arrive before the second one, not
// after.
func TestRelayBlockExcludesOriginator(t *testing.T) {
	f := newRelayTestFixture(t)

	originator, _, originatorSync := f.connect(t, false)
	bystander, _, _ := f.connect(t, false)

	block := minedChild(f.chain[len(f.chain)-1], testEasyBits, 100)
	hash := block.BlockHash()

	// The originator told us about this hash first (an inv), which is
	// legacy's own AddKnownInventory moment (peer_server.go processInvMsg) —
	// driven directly through the dispatch method rather than over the wire,
	// since WHERE the mark comes from is Task 9/11 territory; this test pins
	// only that RelayBlock reads it.
	_, err := f.m.Inv(originatorSync, &wire.MsgInv{InvList: []*wire.InvVect{wire.NewInvVect(wire.InvTypeBlock, &hash)}})
	require.NoError(t, err)

	f.m.RelayBlock(hash, block)

	// The barrier: the bystander, who never announced this hash, must get it
	// — proving the relay actually ran for this call.
	invMsg, ok := bystander.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, hash, invMsg.InvList[0].Hash)

	// A second, distinct block the originator has genuinely not seen.
	block2 := minedChild(block, testEasyBits, 101)
	hash2 := block2.BlockHash()

	f.m.RelayBlock(hash2, block2)

	// If the first RelayBlock call had (incorrectly) sent hash's inv to the
	// originator, it would be sitting ahead of hash2's inv on this same
	// connection (transport.Conn.Send -> one writer goroutine keeps sends to
	// one peer in call order). It is not: the first inv the originator ever
	// receives is hash2's, which proves hash's was never sent.
	second, ok := originator.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, hash2, second.InvList[0].Hash, "the originator must never receive the block it announced to us")
}

// TestRelayBlockSuppressesDuplicateAnnouncement pins the other half of the
// known-block set: a second blocks-final event for a hash already relayed
// (a Kafka replay, or a fast reorg back onto a block already announced) is
// not announced again to a peer that already received it. Proven the same
// way as the originator test: a second, distinct block that peer has not yet
// seen must be the very next inv it receives.
func TestRelayBlockSuppressesDuplicateAnnouncement(t *testing.T) {
	f := newRelayTestFixture(t)

	peer, _, _ := f.connect(t, false)

	block := minedChild(f.chain[len(f.chain)-1], testEasyBits, 110)
	hash := block.BlockHash()

	f.m.RelayBlock(hash, block)

	first, ok := peer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, hash, first.InvList[0].Hash)

	// The replay: same hash, same header, second blocks-final event.
	f.m.RelayBlock(hash, block)

	block2 := minedChild(block, testEasyBits, 111)
	hash2 := block2.BlockHash()

	f.m.RelayBlock(hash2, block2)

	second, ok := peer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, hash2, second.InvList[0].Hash, "a repeated blocks-final event for an already-relayed hash must not be announced again")
}

// TestRelayBlockSkipsRedundantHeadersAfterGetHeaders pins pindexBestHeaderSent
// end to end: a sendheaders peer who already received this exact block via a
// getheaders reply gets no second, redundant headers announcement for it from
// the relay. Proven the same way as the suppression test above: a second
// block the peer has NOT already been sent must be the next headers message
// it receives.
func TestRelayBlockSkipsRedundantHeadersAfterGetHeaders(t *testing.T) {
	f := newRelayTestFixture(t)

	far, _, _ := f.connect(t, true)

	tip := f.chain[len(f.chain)-1]
	block := minedChild(tip, testEasyBits, 200)
	hash := block.BlockHash()

	_, err := f.m.headerIndex.AddHeader(block)
	require.NoError(t, err)
	require.True(t, f.m.SetActiveTip(hash), "block must become the active tip for Serving to reach it")

	// Serve a getheaders that reaches `block`, over the real wire so the reply
	// actually goes out through the same dispatch/send path RelayBlock's
	// headers announcement will later share — which is exactly what sets
	// pindexBestHeaderSent to it (Serving.OnGetHeaders).
	locator := wire.NewMsgGetHeaders()
	locator.BlockLocatorHashes = []*chainhash.Hash{ptr(f.genesis.BlockHash())}
	far.write(t, locator)

	served, ok := far.readUntil(t, wire.CmdHeaders).(*wire.MsgHeaders)
	require.True(t, ok)
	require.NotEmpty(t, served.Headers)
	require.Equal(t, hash, served.Headers[len(served.Headers)-1].BlockHash(), "the getheaders reply must reach block for this test to mean anything")

	f.m.RelayBlock(hash, block)

	block2 := minedChild(block, testEasyBits, 201)
	hash2 := block2.BlockHash()

	f.m.RelayBlock(hash2, block2)

	next, ok := far.readUntil(t, wire.CmdHeaders).(*wire.MsgHeaders)
	require.True(t, ok)
	require.Len(t, next.Headers, 1)
	require.Equal(t, hash2, next.Headers[0].BlockHash(), "a block already covered by a getheaders reply must not be re-announced by the relay")
}

// TestRelayBlockIgnoresNilHeader documents that a nil header is a caller
// error the relay refuses rather than panics on: bridge/kafka.go never
// passes one, but the wiring boundary between a decoded Kafka message and
// this call is worth defending explicitly.
func TestRelayBlockIgnoresNilHeader(t *testing.T) {
	f := newRelayTestFixture(t)

	far, _, _ := f.connect(t, false)

	f.m.RelayBlock(chainhash.Hash{0x01}, nil)

	// Prove nothing arrived by requiring a second, real relay's inv to be the
	// first thing this connection ever receives.
	block := minedChild(f.chain[len(f.chain)-1], testEasyBits, 300)
	hash := block.BlockHash()

	f.m.RelayBlock(hash, block)

	invMsg, ok := far.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, hash, invMsg.InvList[0].Hash)
}

func ptr(h chainhash.Hash) *chainhash.Hash { return &h }

// TestRelayBlockExcludesPeerThatAnnouncedByHeaders is the fix round 1 pin for
// review finding I1: net_processing.cpp PeerHasHeader (net_processing.cpp:
// 311-327, bitcoin-sv@879fc8b42) tests BOTH pindexBestKnownBlock (what a peer
// told US) and pindexBestHeaderSent (what WE told the peer) before
// SendBlockHeaders announces a block, and SendBlockHeaders calls it for
// exactly this reason (net_processing.cpp:5298-5300: a peer already known to
// have the block is skipped, not re-announced). A peer that announces a
// block to us via HEADERS — the modern, and this relay's own, announcement
// form — updates pindexBestKnownBlock (headersync.go OnHeaders ->
// updateBlockAvailability) exactly as an inv announcement does. The relay
// must read that signal, not only its own knownBlocks set (which Inv/
// BlockDone populate but Headers does not).
func TestRelayBlockExcludesPeerThatAnnouncedByHeaders(t *testing.T) {
	f := newRelayTestFixture(t)

	originator, _, originatorSync := f.connect(t, false)
	bystander, _, _ := f.connect(t, false)

	block := minedChild(f.chain[len(f.chain)-1], testEasyBits, 400)
	hash := block.BlockHash()

	// The originator announced this block to us by HEADERS, not inv — driven
	// directly through the dispatch method, as the existing inv-originator
	// test drives m.Inv, since WHERE the announcement comes from is Task 5's
	// territory; this test pins only that RelayBlock reads the signal it
	// leaves behind.
	_, _, err := f.m.Headers(originatorSync, &wire.MsgHeaders{Headers: []*wire.BlockHeader{block}})
	require.NoError(t, err)

	f.m.RelayBlock(hash, block)

	// The barrier: the bystander, who was told nothing, must get it.
	invMsg, ok := bystander.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, hash, invMsg.InvList[0].Hash)

	// A second, distinct block the originator has genuinely not seen.
	block2 := minedChild(block, testEasyBits, 401)
	hash2 := block2.BlockHash()

	f.m.RelayBlock(hash2, block2)

	second, ok := originator.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, hash2, second.InvList[0].Hash,
		"a peer that announced this block to us by headers must never receive it announced back")
}

// TestRelayBlockFallsBackToInvWhenPeerMissesParent is the fix round 1 pin for
// review finding I2: SendBlockHeaders only takes the headers branch for a
// peer that can connect the new header — PeerHasHeader(prev) true, or the
// block is genesis (net_processing.cpp:5301-5307) — and falls back to inv
// otherwise (:5308-5312 sets fRevertToInv; the actual inv send, itself gated
// by ANOTHER PeerHasHeader(pindex) check, is at :5449-5455). A sendheaders
// peer that has never been told anything about our chain cannot connect a
// header extending our tip, so it must get inv, not a header it cannot place.
func TestRelayBlockFallsBackToInvWhenPeerMissesParent(t *testing.T) {
	f := newRelayTestFixture(t)

	peer, _, _ := f.connect(t, true) // sendheaders, but told nothing about our chain

	block := minedChild(f.chain[len(f.chain)-1], testEasyBits, 410)
	hash := block.BlockHash()

	f.m.RelayBlock(hash, block)

	invMsg, ok := peer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Equal(t, hash, invMsg.InvList[0].Hash,
		"a sendheaders peer missing the block's parent must get inv, not a header it cannot connect")
}

// TestRelayBlockKeepsSendingHeadersAcrossConsecutiveBlocks is the fix round
// 2 pin for the IMPORTANT finding: the pindexBestHeaderSent write RelayBlock
// makes after a headers send (net_processing.cpp:5372-5373) is what lets a
// sendheaders peer keep RECEIVING headers for consecutive new blocks,
// because peerHasHeader's hasParent test for block N+1 reads exactly what
// the write for block N left behind. Without the write, block N+1's parent
// (block N) would never register as known to the peer via
// pindexBestHeaderSent, and the peer would silently drop to inv from the
// second block onward.
//
// This is deliberately NOT the same shape as
// TestRelayBlockSelectsHeadersOrInv or the other fix round 1 tests: those
// establish hasParent by simulating the peer ANNOUNCING our tip to us
// (pindexBestKnownBlock). This test establishes it purely through
// RelayBlock's own writes, across two consecutive calls, which is the one
// path only the write under test can satisfy.
func TestRelayBlockKeepsSendingHeadersAcrossConsecutiveBlocks(t *testing.T) {
	f := newRelayTestFixture(t)

	peer, _, syncPeer := f.connect(t, true)

	tip := f.chain[len(f.chain)-1]

	// The peer must already have TIP for the first block's hasParent to
	// hold — established the same way TestRelayBlockSelectsHeadersOrInv
	// does, via a direct Headers() announcement, so this test isolates the
	// write under test rather than depending on it for the FIRST block too.
	_, _, err := f.m.Headers(syncPeer, &wire.MsgHeaders{Headers: []*wire.BlockHeader{tip}})
	require.NoError(t, err)

	blockN := minedChild(tip, testEasyBits, 500)
	hashN := blockN.BlockHash()

	_, err = f.m.headerIndex.AddHeader(blockN)
	require.NoError(t, err)

	f.m.RelayBlock(hashN, blockN)

	firstHeaders, ok := peer.readUntil(t, wire.CmdHeaders).(*wire.MsgHeaders)
	require.True(t, ok)
	require.Len(t, firstHeaders.Headers, 1)
	require.Equal(t, hashN, firstHeaders.Headers[0].BlockHash(), "sanity: the first block must actually be announced by headers")

	blockN1 := minedChild(blockN, testEasyBits, 501)
	hashN1 := blockN1.BlockHash()

	_, err = f.m.headerIndex.AddHeader(blockN1)
	require.NoError(t, err)

	f.m.RelayBlock(hashN1, blockN1)

	secondHeaders, ok := peer.readUntil(t, wire.CmdHeaders).(*wire.MsgHeaders)
	require.True(t, ok)
	require.Len(t, secondHeaders.Headers, 1)
	require.Equal(t, hashN1, secondHeaders.Headers[0].BlockHash(),
		"the second consecutive block must also be announced by headers, not fall back to inv")
}

// ---------------------------------------------------------------------------
// selectTxRelayTargets: pure, table-driven (Task 13).
// ---------------------------------------------------------------------------

func TestSelectTxRelayTargets(t *testing.T) {
	tx := TxHashAndFee{TxHash: chainhash.Hash{0x01}, Fee: 1000, Size: 1000} // 1000 sat/kB

	t.Run("a plain peer with no filter gets the tx", func(t *testing.T) {
		peer := &Peer{}

		out := selectTxRelayTargets([]txRelayCandidate{{peer: peer}}, tx)

		require.Len(t, out, 1)
		require.Same(t, peer, out[0])
	})

	t.Run("a peer with fRelayTxes=false gets nothing, filter or no filter", func(t *testing.T) {
		peer := &Peer{}

		out := selectTxRelayTargets([]txRelayCandidate{{peer: peer, relayDisabled: true}}, tx)

		require.Empty(t, out)
	})

	t.Run("a peer that already has this exact tx gets nothing again", func(t *testing.T) {
		peer := &Peer{}

		out := selectTxRelayTargets([]txRelayCandidate{{peer: peer, known: true}}, tx)

		require.Empty(t, out)
	})

	// E5: pair every suppression with a positive arrival on another peer in
	// the same call, so a nil/empty result can only mean "correctly
	// suppressed", never "the selection function itself is broken".
	t.Run("feefilter suppresses a peer below the rate but not one at or above it", func(t *testing.T) {
		belowFilter := &Peer{connectedAt: time.Unix(1, 0)} // filter 1001 > 1000 sat/kB fee rate
		atFilter := &Peer{connectedAt: time.Unix(2, 0)}    // filter 1000 == fee rate: not suppressed (legacy: feePerKB < feeFilter, not <=)
		noFilter := &Peer{connectedAt: time.Unix(3, 0)}

		out := selectTxRelayTargets([]txRelayCandidate{
			{peer: belowFilter, feeFilter: 1001},
			{peer: atFilter, feeFilter: 1000},
			{peer: noFilter, feeFilter: 0},
		}, tx)

		require.Len(t, out, 2, "exactly the two peers whose filter does not exceed the fee rate")
		require.NotContains(t, out, belowFilter)
		require.Contains(t, out, atFilter)
		require.Contains(t, out, noFilter)
	})

	t.Run("a zero-size tx cannot be suppressed by any filter (MaxInt64 sentinel)", func(t *testing.T) {
		zeroSize := TxHashAndFee{TxHash: chainhash.Hash{0x02}, Fee: 0, Size: 0}
		peer := &Peer{}

		out := selectTxRelayTargets([]txRelayCandidate{{peer: peer, feeFilter: math.MaxInt64}}, zeroSize)

		require.Len(t, out, 1, "a tx this port cannot rate must never be suppressed (legacy's own fail-open default)")
	})

	t.Run("a mixed batch routes each candidate independently", func(t *testing.T) {
		disabledPeer := &Peer{connectedAt: time.Unix(1, 0)}
		knownPeer := &Peer{connectedAt: time.Unix(2, 0)}
		filteredPeer := &Peer{connectedAt: time.Unix(3, 0)}
		okPeer := &Peer{connectedAt: time.Unix(4, 0)}

		out := selectTxRelayTargets([]txRelayCandidate{
			{peer: disabledPeer, relayDisabled: true},
			{peer: knownPeer, known: true},
			{peer: filteredPeer, feeFilter: 1001},
			{peer: okPeer},
		}, tx)

		require.Len(t, out, 1)
		require.Same(t, okPeer, out[0])
	})
}

func TestFeePerKB(t *testing.T) {
	t.Run("rounds down like legacy's integer division", func(t *testing.T) {
		// 999 sat over 1000 bytes = 0.999 sat/byte = 999 sat/kB exactly;
		// 1 sat over 3 bytes = 333.33... sat/kB, truncated to 333.
		require.Equal(t, int64(999), feePerKB(TxHashAndFee{Fee: 999, Size: 1000}))
		require.Equal(t, int64(333), feePerKB(TxHashAndFee{Fee: 1, Size: 3}))
	})

	t.Run("zero size is the MaxInt64 sentinel, not a division by zero", func(t *testing.T) {
		require.Equal(t, int64(math.MaxInt64), feePerKB(TxHashAndFee{Fee: 500, Size: 0}))
	})
}
