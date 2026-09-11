package netsync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// getHeadersRecorder collects the getheaders messages a peer's remote end is
// sent, so a test can read the locator the node actually asked with.
type getHeadersRecorder struct {
	mu   sync.Mutex
	msgs []*wire.MsgGetHeaders
}

func (r *getHeadersRecorder) record(msg *wire.MsgGetHeaders) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.msgs = append(r.msgs, msg)
}

func (r *getHeadersRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.msgs)
}

// last returns the most recent getheaders locator, or nil if none arrived.
func (r *getHeadersRecorder) last() *wire.MsgGetHeaders {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.msgs) == 0 {
		return nil
	}

	return r.msgs[len(r.msgs)-1]
}

// demotionPeer connects a live peer whose remote end records both the getdata
// and the getheaders messages it receives, and registers it with the manager as
// a sync candidate claiming the given height.
func demotionPeer(t *testing.T, sm *SyncManager, idx uint8, lastBlock int32) (*peerpkg.Peer, *getDataRecorder, *getHeadersRecorder) {
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

	state := &peerSyncState{
		syncCandidate: true,
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	t.Cleanup(state.requestedTxns.Stop)
	state.noteBestKnownHeight(lastBlock)

	sm.peerStates.Set(local, state)

	return local, data, headers
}

// newDemotionManager builds the smallest manager that can run the real
// handleCheckSyncPeer, the real startSync and the real fetchHeaderBlocks back to
// back. We are at height 100, every block asked about is unknown, and the next
// checkpoint is far above anything the tests generate so the headers-first
// branches are the ones that run.
func newDemotionManager(t *testing.T) *SyncManager {
	t.Helper()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	blockchainClient.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.Mock.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{bestHeader.Hash()}, nil)
	blockchainClient.Mock.On("CatchUpBlocks", mock.Anything).Return(nil)
	blockchainClient.Mock.On("Run", mock.Anything, mock.Anything).Return(nil)
	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient
	sm.blockSizeTracker = newBlockSizeTracker(10)

	checkpointHash := chainhash.Hash{0xcc}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 1_000_000, Hash: &checkpointHash}

	return sm
}

// stalledSyncPeerState is the state of a sync peer that has delivered no block
// for longer than the stall window, with no throughput sample to excuse it.
func stalledSyncPeerState() *syncPeerState {
	return &syncPeerState{lastBlockTime: time.Now().Add(-maxLastBlockTime - time.Minute)}
}

// headerListEpochNow reads the header list's generation counter, which
// resetHeaderStateLocked bumps. An unchanged value is what proves the list is
// the same list rather than a rebuilt one that happens to be the same length.
func headerListEpochNow(sm *SyncManager) uint64 {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	return sm.headerListEpoch
}

// TestStalledSyncPeer_IsDemotedAndStaysConnected is the anchor test. A sync peer
// that stops delivering blocks loses the headers role and nothing else: it keeps
// its connection, its registration, the blocks it still owes us, and the
// transactions we are still waiting on from it. Under multi-peer downloads it is
// still a perfectly good block source, and every one of those four things is
// something today's rotation destroys.
func TestStalledSyncPeer_IsDemotedAndStaysConnected(t *testing.T) {
	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 100, 1000)
	successor, _, _ := demotionPeer(t, sm, 101, 1000)

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	owed := []chainhash.Hash{{0xa0}, {0xa1}, {0xa2}}
	for _, h := range owed {
		require.True(t, sm.blockDownloads.Add(stalled, h))
	}

	state, exists := sm.peerStates.Get(stalled)
	require.True(t, exists)

	awaitedTx := chainhash.Hash{0xb0}
	state.requestedTxns.Set(awaitedTx, struct{}{})

	sm.handleCheckSyncPeer()

	require.True(t, stalled.Connected(), "a peer that is merely slow at headers must keep its connection")

	_, stillRegistered := sm.peerStates.Get(stalled)
	require.True(t, stillRegistered, "the demoted peer must stay a registered, schedulable peer")

	for _, h := range owed {
		require.True(t, sm.blockDownloads.HasOwner(stalled, h),
			"revoking a connected peer's block ownership makes its late copies look unrequested")
	}

	// Ownership is kept so a late copy is admitted; the budget is not, or the
	// peer the demotion deliberately kept in order to keep using it is handed no
	// block work at all until the hour-long ownership ceiling expires. Those are
	// two different questions about the same record, and counting them as one is
	// what made the demoted peer useless.
	require.Zero(t, sm.blockDownloads.CountForPeer(stalled),
		"a peer let off its slice must get that budget back straight away")

	_, txStillWanted := state.requestedTxns.Get(awaitedTx)
	require.True(t, txStillWanted, "the demoted peer's outstanding transaction requests must survive")

	require.Equal(t, successor, sm.loadSyncPeer(), "the headers role must move to the other candidate")
	require.False(t, stalled.SyncPeer(), "the demoted peer must no longer think it is the sync peer")
}

// TestDemotedSyncPeer_IsNotReElectedStraightAway pins the replacement exclusion.
// The disconnect used to be what kept the outgoing peer out of the election that
// runs immediately afterwards; with the peer kept, something else has to, or the
// node hands the role straight back to the peer it just judged stalled and buys
// another stall window of no progress.
//
// The election is deterministic because the only other peer is at our own
// height, which puts it in the last-resort pool — and a peer at our height is
// never made sync peer at all. So while the cooldown holds, the stalled peer is
// the ONLY peer that could be elected and the node deliberately ends up with
// none; once the cooldown passes, that same peer is elected. One peer, two
// outcomes, no random choice either way.
func TestDemotedSyncPeer_IsNotReElectedStraightAway(t *testing.T) {
	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 102, 1000)
	demotionPeer(t, sm, 103, 100)

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()
	require.NotEqual(t, stalled, sm.loadSyncPeer(),
		"a peer inside its demotion cooldown must not be re-elected")
	require.Nil(t, sm.loadSyncPeer(),
		"and the only other peer is at our own height, so there is nobody to promote")

	// The cooldown expires, and the same election runs again.
	state, exists := sm.peerStates.Get(stalled)
	require.True(t, exists)
	state.clearDemotionCooldown()

	sm.startSync()
	require.Equal(t, stalled, sm.loadSyncPeer(),
		"once the cooldown has passed the peer must be electable again")
}

// TestDemotion_WithNoOtherCandidateStillElectsTheDemotedPeer pins the escape
// hatch. A node with one peer must not stop syncing for three minutes because
// that peer is the one it just demoted.
func TestDemotion_WithNoOtherCandidateStillElectsTheDemotedPeer(t *testing.T) {
	sm := newDemotionManager(t)

	only, _, _ := demotionPeer(t, sm, 104, 1000)

	sm.storeSyncPeer(only, stalledSyncPeerState())
	only.SetSyncPeer(true)

	sm.handleCheckSyncPeer()

	require.Equal(t, only, sm.loadSyncPeer(), "the only peer we have must be elected regardless of its cooldown")
	require.True(t, only.Connected())
}

// TestDemotion_KeepsTheHeaderList pins the headers themselves. Today's rotation
// re-anchors on our local best block and throws away every header downloaded
// since the last checkpoint, which costs the whole node a fresh getheaders round
// and every peer its slice because one peer was slow.
//
// The generation counter is the assertion that cannot be faked: the reset bumps
// it, so an unchanged counter proves this is the same list and not a rebuilt one
// that happens to be the same length.
func TestDemotion_KeepsTheHeaderList(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf1}
	msg, hashes := linkedHeaders(anchor, 40, &nonce)

	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 105, 1000)
	_, _, _ = demotionPeer(t, sm, 106, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)

	epochBefore := headerListEpochNow(sm)
	cursorBefore, ok := startHeaderHash(t, sm)
	require.True(t, ok)
	require.Equal(t, hashes[0], cursorBefore)

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()

	require.Equal(t, len(hashes)+1, sm.headerListLen(), "the downloaded headers must survive a demotion")
	require.True(t, sm.headersFirstMode.Load(), "headers-first mode must stay on")
	require.Equal(t, epochBefore, headerListEpochNow(sm), "the header list must be the same list, not a rebuilt one")

	cursorAfter, ok := startHeaderHash(t, sm)
	require.True(t, ok, "the download cursor must stay in the list")
	require.Equal(t, hashes[0], cursorAfter)
}

// TestDemotion_TheNewSyncPeerContinuesTheHeadersRoundFromTheBackOfTheList is the
// trap that comes with keeping the list. handleHeadersMsg requires every
// incoming header to connect to the back of the list, and a locator built from
// our own database best block is hundreds of headers below that — so the new
// sync peer would answer honestly and be disconnected for it.
func TestDemotion_TheNewSyncPeerContinuesTheHeadersRoundFromTheBackOfTheList(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf2}
	msg, hashes := linkedHeaders(anchor, 40, &nonce)

	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 107, 1000)
	successor, _, successorHeaders := demotionPeer(t, sm, 108, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)

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
		"the locator must start at the back of the header list we kept")
	require.Greater(t, len(got.BlockLocatorHashes), 1,
		"and step back through the list, so a peer that cannot reach the back can still find a fork point")

	// And the honest answer to that locator has to be accepted.
	more, moreHashes := linkedHeaders(hashes[len(hashes)-1], 5, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: more, peer: successor})

	require.True(t, successor.Connected(), "an honest continuation must not cost the new sync peer its connection")
	require.Equal(t, len(hashes)+len(moreHashes)+1, sm.headerListLen(),
		"the continuation headers must have linked onto the list we kept")
	require.NotEmpty(t, moreHashes)
}

// TestDemotion_LateHeadersFromTheDemotedPeerDoNotCostItItsConnection is the
// second half of that trap. The demoted peer still has a getheaders outstanding,
// and by the time it answers the new sync peer has already extended the list, so
// its reply no longer connects to the back. It is an honest answer to a question
// we asked, and disconnecting it — with a misbehaviour warning, no less — throws
// away the very peer we kept so it could carry block bodies.
//
// Genuinely unconnected headers, whose parent we have never heard of, still cost
// the sender its connection.
func TestDemotion_LateHeadersFromTheDemotedPeerDoNotCostItItsConnection(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf3}
	msg, hashes := linkedHeaders(anchor, 20, &nonce)

	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 109, 1000)
	successor, _, _ := demotionPeer(t, sm, 110, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()
	require.Equal(t, successor, sm.loadSyncPeer())

	// The new sync peer extends the list, so the back moves on.
	continuation, continuationHashes := linkedHeaders(hashes[len(hashes)-1], 5, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: continuation, peer: successor})

	lenBefore := sm.headerListLen()
	require.Equal(t, len(hashes)+len(continuationHashes)+1, lenBefore)

	// The demoted peer's answer to the locator we gave it before the swap: the
	// same headers, arriving too late to connect to the back.
	sm.handleHeadersMsg(&headersMsg{headers: continuation, peer: stalled})

	require.True(t, stalled.Connected(), "a late answer to our own getheaders is not misbehaviour")
	require.Equal(t, lenBefore, sm.headerListLen(), "a late duplicate must not be re-linked into the list")

	// Headers whose parent we have never seen are a different matter.
	junkParent := chainhash.Hash{0x9e}
	junk, _ := linkedHeaders(junkParent, 3, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: junk, peer: stalled})

	require.False(t, stalled.Connected(), "headers that connect to nothing we know must still be punished")
}

// TestDemotion_ReopensOnlyTheDemotedPeersSliceAndRewindsToItsLowestBlock is the
// replacement for the recovery the header-state reset used to provide, and the
// place the historical duplicate-commit storm has to stay dead.
//
// The demoted peer's own outstanding blocks are reopened for re-request and the
// download cursor is moved back onto the lowest of them, so somebody else can
// take them on the next pass. Every other peer's outstanding blocks keep
// vouching for themselves, which is what stops the re-walk asking a second peer
// for a block that is still in flight — the exact mechanism behind the 40P01
// deadlock and duplicate-commit storm the whole-ledger back-date caused.
func TestDemotion_ReopensOnlyTheDemotedPeersSliceAndRewindsToItsLowestBlock(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf4}
	msg, hashes := linkedHeaders(anchor, 12, &nonce)

	sm := newDemotionManager(t)

	stalled, stalledData, _ := demotionPeer(t, sm, 111, 1000)
	successor, successorData, _ := demotionPeer(t, sm, 112, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)

	// The state a pass leaves behind: the stalled peer owes the first four
	// blocks, the successor the next four, and the cursor has moved past both.
	stalledSlice := hashes[0:4]
	successorSlice := hashes[4:8]

	for _, h := range stalledSlice {
		require.True(t, sm.blockDownloads.Add(stalled, h))
	}

	for _, h := range successorSlice {
		require.True(t, sm.blockDownloads.Add(successor, h))
	}

	sm.headerMu.Lock()
	sm.startHeader = sm.headerIndex[hashes[8]]
	sm.headerMu.Unlock()

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()

	for _, h := range stalledSlice {
		require.False(t, sm.blockDownloads.RequestedWithin(h, blockRequestRetryInterval),
			"the demoted peer's blocks must be askable of somebody else")
		require.True(t, sm.blockDownloads.HasOwner(stalled, h),
			"reopening must not revoke the demoted peer's permission to deliver")
	}

	for _, h := range successorSlice {
		require.True(t, sm.blockDownloads.RequestedWithin(h, blockRequestRetryInterval),
			"another peer's in-flight block must still vouch for itself, or the re-walk asks a second peer for it")
	}

	cursor, ok := startHeaderHash(t, sm)
	require.True(t, ok)
	require.Equal(t, stalledSlice[0], cursor, "the cursor must be back on the lowest block the demoted peer owed")

	// The next pass has to recover exactly that slice and nothing else.
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool {
		return stalledData.count()+successorData.count() >= len(stalledSlice)
	}, 5*time.Second), "the reopened slice should have been asked for again")

	asked := append(stalledData.all(), successorData.all()...)

	for _, h := range stalledSlice {
		require.Contains(t, asked, h, "the demoted peer's slice must be re-requested")
	}

	for _, h := range successorSlice {
		require.NotContains(t, asked, h, "a block already in flight must not be asked of a second peer")
	}
}

// TestDemotion_OffPathDisconnectsAndResetsExactlyAsBefore is the rollback lever.
// With multi-peer block download off, the sync peer is the only source of block
// bodies, so keeping a stalled one buys nothing and today's behaviour is the
// right behaviour: disconnect it, release everything it owed, and start the
// header round again from our own best block.
func TestDemotion_OffPathDisconnectsAndResetsExactlyAsBefore(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf5}
	msg, hashes := linkedHeaders(anchor, 20, &nonce)

	sm := newDemotionManager(t)
	sm.settings.Legacy.MultiPeerBlockDownload = false

	stalled, _, _ := demotionPeer(t, sm, 113, 1000)
	successor, _, _ := demotionPeer(t, sm, 114, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)
	epochBefore := headerListEpochNow(sm)

	require.True(t, sm.blockDownloads.Add(stalled, hashes[0]))

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()

	require.False(t, stalled.Connected(), "with the fan-out off a stalled sync peer is still disconnected")
	require.Zero(t, sm.blockDownloads.CountForPeer(stalled), "a disconnected peer's blocks must be released")
	require.Equal(t, 1, sm.headerListLen(), "the header list is still thrown away and re-anchored")
	require.NotEqual(t, epochBefore, headerListEpochNow(sm), "the reset must bump the list generation")
	require.Equal(t, successor, sm.loadSyncPeer())
}

// TestDemotion_AHeadersBatchThatStartsConnectingAndThenStopsIsStillPunished pins
// the narrow shape of the late-reply leniency. Forgiving a batch whose FIRST
// header connects to a header we hold is recognising our own question coming
// back late. Forgiving one that links two headers on and then jumps sideways
// would forgive a peer feeding us a doctored chain, and the check that separates
// them is that nothing in the batch has linked yet.
func TestDemotion_AHeadersBatchThatStartsConnectingAndThenStopsIsStillPunished(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf6}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm := newDemotionManager(t)

	peer, _, _ := demotionPeer(t, sm, 116, 1000)
	seedFetchHeaders(t, sm, peer, anchor, msg)

	// Two headers that link onto the back, then one that hangs off a header from
	// the middle of the list instead.
	good, goodHashes := linkedHeaders(hashes[len(hashes)-1], 2, &nonce)
	sideways, _ := linkedHeaders(hashes[2], 1, &nonce)

	mixed := wire.NewMsgHeaders()
	for _, h := range good.Headers {
		require.NoError(t, mixed.AddBlockHeader(h))
	}

	require.NoError(t, mixed.AddBlockHeader(sideways.Headers[0]))

	sm.handleHeadersMsg(&headersMsg{headers: mixed, peer: peer})

	require.False(t, peer.Connected(),
		"a batch that links onto the list and then jumps sideways is not a late reply")
	require.Equal(t, len(hashes)+len(goodHashes)+1, sm.headerListLen(),
		"the headers that did link stay linked")
}

// TestDemotion_TheLateHeadersCarveOutIsScopedAndExpires is ChiR5.
//
// The carve-out above turns a non-connecting headers batch into a silent ignore
// instead of a disconnect. Its test was "does this batch's first header, or its
// parent, sit in the header index?", which any header we currently hold
// satisfies, from anybody, for as long as we hold it. So a peer that once
// contributed a batch could re-send it, or any prefix of it, indefinitely: 2000
// headers of bandwidth and decode plus a headerMu acquisition each time, the
// same lock the block-queue consumer takes first in headers-first mode, and
// nothing at all for the sender.
//
// Three end states are pinned. A peer that is neither the sync peer nor inside a
// demotion cooldown is disconnected for a batch that never connects, however
// familiar its headers are. The demoted peer is covered only while its cooldown
// is running, which is the expiry the carve-out did not have. And the current
// sync peer stays covered, because startSync elects on height alone and a peer
// hundreds of headers below the back of the list answers our locator from the
// newest block it has: see
// TestHeadersRoundLocator_APeerThatCannotReachTheBackKeepsItsConnection, which
// is why scoping this to the demotion cooldown alone is too narrow.
func TestDemotion_TheLateHeadersCarveOutIsScopedAndExpires(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf7}
	msg, hashes := linkedHeaders(anchor, 20, &nonce)

	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 121, 1000)
	successor, _, _ := demotionPeer(t, sm, 122, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()
	require.Equal(t, successor, sm.loadSyncPeer())

	continuation, _ := linkedHeaders(hashes[len(hashes)-1], 5, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: continuation, peer: successor})

	// Registered after the election, so it cannot have been the peer startSync
	// picked.
	bystander, _, _ := demotionPeer(t, sm, 123, 1000)

	sm.handleHeadersMsg(&headersMsg{headers: continuation, peer: bystander})

	require.False(t, bystander.Connected(),
		"the carve-out is for a peer whose late reply we caused, not for anybody re-sending headers we happen to hold")

	// The sync peer re-sending a batch it already contributed is the elected
	// peer answering our locator from where its own chain ends, so it keeps the
	// carve-out.
	sm.handleHeadersMsg(&headersMsg{headers: continuation, peer: successor})

	require.True(t, successor.Connected(),
		"the current sync peer must keep the carve-out, it is the peer we are asking")

	state, ok := sm.peerStates.Get(stalled)
	require.True(t, ok)
	require.True(t, state.inDemotionCooldown(), "demotion should have stamped a cooldown to scope the carve-out to")

	// The demotion window closes. From here the demoted peer is just a peer.
	state.clearDemotionCooldown()

	sm.handleHeadersMsg(&headersMsg{headers: continuation, peer: stalled})

	require.False(t, stalled.Connected(),
		"once the cooldown has expired the carve-out no longer covers the demoted peer either")
}
