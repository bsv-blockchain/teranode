package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// invRepairPeer connects a peer whose remote end records both the getdata and
// the getheaders messages it receives, and registers it on the inv path's terms
// — with the request queue and tx map handleInvMsg dereferences.
//
// demotionPeer records the same two message types but registers a state with no
// request queue, which drainRequestQueue at the bottom of handleInvMsg walks
// unconditionally. connectRacePeer registers the right state but installs no
// getheaders listener, so the repair would go out unseen.
func invRepairPeer(t *testing.T, sm *SyncManager, idx uint8, lastBlock int32) (*peerpkg.Peer, *getDataRecorder, *getHeadersRecorder, *peerSyncState) {
	t.Helper()

	data := &getDataRecorder{}
	headers := &getHeadersRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetData:    func(_ *peerpkg.Peer, msg *wire.MsgGetData) { data.record(msg) },
			OnGetHeaders: func(_ *peerpkg.Peer, msg *wire.MsgGetHeaders) { headers.record(msg) },
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}
	localCfg := peerpkg.Config{
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}

	remote, local, err := MakeConnectedPeers(t, remoteCfg, localCfg, idx)
	require.NoError(t, err)

	local.UpdateLastBlockHeight(lastBlock)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	state := registerInvPeer(sm, local, lastBlock)
	t.Cleanup(state.requestedTxns.Stop)

	return local, data, headers, state
}

// headerListBackHash reads the hash the next headers round will be asked to
// continue from. Read off the list itself rather than off headerListLocator, so
// the assertion is not built out of the function it is checking.
func headerListBackHash(t *testing.T, sm *SyncManager) chainhash.Hash {
	t.Helper()

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	back := sm.headerList.Back()
	require.NotNil(t, back)

	node, ok := back.Value.(*headerNode)
	require.True(t, ok)
	require.NotNil(t, node.hash)

	return *node.hash
}

// invRepairHarness is a manager in headers-first mode with a header list that a
// round has already filled, an announcer that is the sync peer, and a separate
// peer that supplied those headers.
//
// The two peers are separate on purpose. handleHeadersMsg credits the sender of
// a batch it splices with a PROVEN claim, and a pending claim never displaces a
// proven one, so an announcer that had also seeded the headers could not show
// the pending claim the repair records.
type invRepairHarness struct {
	sm         *SyncManager
	announcer  *peerpkg.Peer
	state      *peerSyncState
	data       *getDataRecorder
	headers    *getHeadersRecorder
	baseline   int
	headerBack chainhash.Hash
}

func newInvRepairHarness(t *testing.T, idx uint8, known map[chainhash.Hash]uint32) *invRepairHarness {
	t.Helper()

	sm := newInvManager(t, known)
	sm.blockSizeTracker = newBlockSizeTracker(10)

	// The peer that fills the header list. It is not the sync peer and never
	// announces anything in these tests.
	seeder, _, _, _ := invRepairPeer(t, sm, idx, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0xe1, idx}
	msg, _ := linkedHeaders(anchor, 25, &nonce)

	seedFetchHeaders(t, sm, seeder, anchor, msg)

	announcer, data, headers, state := invRepairPeer(t, sm, idx+1, 1000)
	sm.storeSyncPeer(announcer, &syncPeerState{})

	require.True(t, sm.headersFirstMode.Load(), "harness check: the repair only runs in headers-first mode")
	require.False(t, sm.current(), "harness check: this is the IBD state the stall happened in")

	return &invRepairHarness{
		sm:        sm,
		announcer: announcer,
		state:     state,
		data:      data,
		headers:   headers,
		// Seeding the header list ends with a getheaders for the next round, so
		// what this test is about is the message AFTER that one.
		baseline:   headers.count(),
		headerBack: headerListBackHash(t, sm),
	}
}

func (h *invRepairHarness) announce(hashes ...chainhash.Hash) {
	h.sm.handleInvMsg(&invMsg{inv: blockInv(hashes...), peer: h.announcer})
}

// TestHandleInvMsg_AnAnnouncedBlockWeCannotPlaceAsksForItsHeaders is the
// recovery route that does not run through the headers round.
//
// On Hetzner mainnet on 2026-09-11 the node held headers to 849,999 with a
// committed tip of 800,128. Its headers round asked a question whose only honest
// answer was zero headers, and it sat idle for seven hours while peers went on
// announcing blocks. A getheaders anchored on the back of the header list, sent
// on one of those announcements, is answered from the checkpoint span forward.
func TestHandleInvMsg_AnAnnouncedBlockWeCannotPlaceAsksForItsHeaders(t *testing.T) {
	// Not in the known map, so the chain answers not-found for it: a block we
	// cannot place, which is the whole trigger.
	announced := chainhash.Hash{0xe9}

	h := newInvRepairHarness(t, 160, nil)

	h.announce(announced)

	require.True(t, WaitUntil(func() bool { return h.headers.count() > h.baseline }, invQuietPeriod),
		"an announcement of a block we cannot place must produce a getheaders")
	require.Equal(t, h.baseline+1, h.headers.count(), "exactly one repair request, not one per inv vector")

	sent := h.headers.last()
	require.NotNil(t, sent)

	require.Equal(t, announced, sent.HashStop,
		"the stop hash is the announced block, so the peer serves from our back up to it")

	require.NotEmpty(t, sent.BlockLocatorHashes)
	require.Equal(t, h.headerBack, *sent.BlockLocatorHashes[0],
		"the locator still opens on the back of the header list, so the reply splices onto it")
}

// TestHandleInvMsg_AnAnnouncedBlockAsksForNoBlockData is the negative that
// separates this from the getdata it replaces. SV Node used to request the full
// block here and deliberately stopped: falling back to an inv usually means a
// reorg, whose headers are needed before any block body is worth asking for
// (net_processing.cpp:2429-2435).
func TestHandleInvMsg_AnAnnouncedBlockAsksForNoBlockData(t *testing.T) {
	announced := chainhash.Hash{0xea}

	h := newInvRepairHarness(t, 162, nil)

	// Filling the header list hands out block work of its own, so the assertion
	// is that the announcement adds none, not that none has ever been sent.
	before := h.data.count()

	h.announce(announced)

	require.True(t, WaitUntil(func() bool { return h.headers.count() > h.baseline }, invQuietPeriod),
		"harness check: the repair must have run for the negative below to mean anything")

	// QueueMessage hands off to the peer's writer goroutine, so a getdata on its
	// way would pass an immediate read.
	require.False(t, WaitUntil(func() bool { return h.data.count() > before }, invQuietPeriod),
		"the repair asks for headers and never for the block itself")
}

// TestHandleInvMsg_TwoDifferentAnnouncementsBothGetRepairRequests pins the
// property that keeps this route working where the round's own request stops.
//
// PushGetHeadersMsg filters a repeat of the same (locator[0], stopHash) pair for
// the peer's whole lifetime, with no expiry (peer.go:1132-1142). The round asks
// with a constant key and can be swallowed by that filter; using the announced
// hash as the stop hash makes every announcement a different question.
func TestHandleInvMsg_TwoDifferentAnnouncementsBothGetRepairRequests(t *testing.T) {
	first := chainhash.Hash{0xeb}
	second := chainhash.Hash{0xec}

	h := newInvRepairHarness(t, 164, nil)

	h.announce(first)
	require.True(t, WaitUntil(func() bool { return h.headers.count() == h.baseline+1 }, invQuietPeriod),
		"the first announcement must produce a repair request")
	require.Equal(t, first, h.headers.last().HashStop)

	h.announce(second)
	require.True(t, WaitUntil(func() bool { return h.headers.count() == h.baseline+2 }, invQuietPeriod),
		"the second announcement must not be swallowed by the peer's duplicate filter")
	require.Equal(t, second, h.headers.last().HashStop)
}

// TestHandleInvMsg_AnAnnouncedBlockWeAlreadyHaveAsksForNothing scopes the repair
// to what it is for. A peer announcing a block we hold tells us its height and
// nothing else needs doing.
//
// newInvManager registers the per-hash answer before the catch-all not-found,
// which is the order testify needs: it matches the first registered expectation
// whose arguments fit.
func TestHandleInvMsg_AnAnnouncedBlockWeAlreadyHaveAsksForNothing(t *testing.T) {
	announced := chainhash.Hash{0xed}

	// Above the 1000 the announcer claimed at handshake, because
	// noteBestKnownHeight is monotone and a lower credit would be invisible.
	h := newInvRepairHarness(t, 166, map[chainhash.Hash]uint32{announced: 1500})

	h.announce(announced)

	require.False(t, WaitUntil(func() bool { return h.headers.count() > h.baseline }, invQuietPeriod),
		"a block we already hold needs no header repair")

	require.Equal(t, int32(1500), h.state.BestKnownHeight(),
		"the height credit on the known-block path must survive the new else arm")
	require.Equal(t, proofProven, h.state.Claim().proof,
		"a block we can place is proof, not a pending hash")
}

// TestHandleInvMsg_AnAnnouncedBlockSomebodyAlreadyOwesAsksForNothing copies SV
// Node's IsInFlight gate (net_processing.cpp:2427). A block already assigned to
// a peer is a block whose headers we have placed, so there is nothing to repair
// and the announcement is just noise.
func TestHandleInvMsg_AnAnnouncedBlockSomebodyAlreadyOwesAsksForNothing(t *testing.T) {
	announced := chainhash.Hash{0xee}

	h := newInvRepairHarness(t, 168, nil)

	require.True(t, h.sm.blockDownloads.Add(h.announcer, announced))

	h.announce(announced)

	require.False(t, WaitUntil(func() bool { return h.headers.count() > h.baseline }, invQuietPeriod),
		"a block already owed by a peer must not trigger a header repair")
}

// TestHandleInvMsg_TheAnnouncerIsRememberedAsASourceForThatBlock is the other
// half of the repair. SV Node calls it UpdateBlockAvailability and does it on
// this path unconditionally (net_processing.cpp:2426). Without it the peer that
// told us about the block is not usable as a download source when the headers
// for it finally arrive.
func TestHandleInvMsg_TheAnnouncerIsRememberedAsASourceForThatBlock(t *testing.T) {
	announced := chainhash.Hash{0xef}

	h := newInvRepairHarness(t, 170, nil)

	require.Equal(t, proofNone, h.state.Claim().proof,
		"harness check: the announcer has demonstrated nothing yet")

	h.announce(announced)

	claim := h.state.Claim()
	require.Equal(t, proofPending, claim.proof,
		"a hash we cannot place is worth remembering and worth nothing as permission")
	require.Equal(t, announced, claim.hash)
}
