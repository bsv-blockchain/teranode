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
// back. We are at height 100, every block asked about is unknown. chainParams is
// MainNet's real checkpoint table (via newRaceManager), and none of these tests
// generate a hash that happens to coincide with one, which is what keeps the
// headers-first branches the ones that run without needing a synthetic
// checkpoint of their own.
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

	return sm
}

// stalledSyncPeerState is the state of a sync peer that has delivered no block
// for longer than the stall window, with no throughput sample to excuse it.
func stalledSyncPeerState() *syncPeerState {
	return &syncPeerState{lastBlockTime: time.Now().Add(-maxLastBlockTime - time.Minute)}
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
// Pointer identity is the assertion that cannot be faked: nothing that merely
// reads the cache can produce a new *headerCache, so an unchanged pointer
// proves this is the same cache and not a rebuilt one that happens to hold the
// same headers.
func TestDemotion_KeepsTheHeaderList(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf1}
	msg, hashes := linkedHeaders(anchor, 40, &nonce)

	sm := newDemotionManager(t)

	stalled, _, _ := demotionPeer(t, sm, 105, 1000)
	_, _, _ = demotionPeer(t, sm, 106, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)

	cacheBefore := sm.headerCache

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()

	require.Equal(t, len(hashes), sm.headerCache.Len(), "the downloaded headers must survive a demotion")
	require.True(t, sm.headersFirstMode.Load(), "headers-first mode must stay on")
	require.Same(t, cacheBefore, sm.headerCache, "the header cache must be the same cache, not a rebuilt one")
}

// TestDemotion_ReopensOnlyTheDemotedPeersSliceAndAsksForItAgain is the
// replacement for the recovery the header-state reset used to provide, and the
// place the historical duplicate-commit storm has to stay dead.
//
// The demoted peer's own outstanding blocks are reopened for re-request, so
// somebody else can take them on the next pass. Every other peer's outstanding
// blocks keep vouching for themselves, which is what stops the next pass asking
// a second peer for a block that is still in flight — the exact mechanism
// behind the 40P01 deadlock and duplicate-commit storm the whole-ledger
// back-date caused.
func TestDemotion_ReopensOnlyTheDemotedPeersSliceAndAsksForItAgain(t *testing.T) {
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

// TestDemotion_OffPathDisconnectsButKeepsTheHeaderCache is the rollback lever.
// With multi-peer block download off, the sync peer is the only source of block
// bodies, so keeping a stalled one buys nothing: disconnect it and release
// everything it owed.
//
// This used to also pin that the off path threw the header list away and
// re-anchored it, in contrast with the demotion path's keeping it — that was
// the very distinction the whole-ledger back-date and header-list rebuild used
// to draw between the two routes. There is no rebuild left to draw it with:
// nothing on either path writes sm.headerCache except a fresh getheaders
// reply, so the off path leaves the cache exactly as untouched as the
// demotion path does. What is still real and still worth pinning is that the
// disconnect itself, and the release of what the stalled peer owed, still
// happen.
func TestDemotion_OffPathDisconnectsButKeepsTheHeaderCache(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf5}
	msg, hashes := linkedHeaders(anchor, 20, &nonce)

	sm := newDemotionManager(t)
	sm.settings.Legacy.MultiPeerBlockDownload = false

	stalled, _, _ := demotionPeer(t, sm, 113, 1000)
	successor, _, _ := demotionPeer(t, sm, 114, 1000)

	seedFetchHeaders(t, sm, stalled, anchor, msg)
	cacheBefore := sm.headerCache

	require.True(t, sm.blockDownloads.Add(stalled, hashes[0]))

	sm.storeSyncPeer(stalled, stalledSyncPeerState())
	stalled.SetSyncPeer(true)

	sm.handleCheckSyncPeer()

	require.False(t, stalled.Connected(), "with the fan-out off a stalled sync peer is still disconnected")
	require.Zero(t, sm.blockDownloads.CountForPeer(stalled), "a disconnected peer's blocks must be released")
	require.Same(t, cacheBefore, sm.headerCache, "even the off-path disconnect must not rebuild the header cache")
	require.Equal(t, successor, sm.loadSyncPeer())
}
