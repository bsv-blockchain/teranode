package netsync

import (
	"container/list"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// headerRequestRecorder records what a peer was asked to fetch next: the stop
// hash of every getheaders, and how many getblocks went out.
type headerRequestRecorder struct {
	mu        sync.Mutex
	stopHash  []chainhash.Hash
	getBlocks int
}

func (r *headerRequestRecorder) noteGetHeaders(msg *wire.MsgGetHeaders) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stopHash = append(r.stopHash, msg.HashStop)
}

func (r *headerRequestRecorder) noteGetBlocks() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.getBlocks++
}

func (r *headerRequestRecorder) askedForHeadersUpTo(hash chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, got := range r.stopHash {
		if got.IsEqual(&hash) {
			return true
		}
	}

	return false
}

func (r *headerRequestRecorder) getBlocksCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getBlocks
}

// connectHeaderRequestPeer returns a live peer whose remote end records the two
// things a checkpoint transition can send.
func connectHeaderRequestPeer(t *testing.T, idx uint8) (*peerpkg.Peer, *headerRequestRecorder) {
	t.Helper()

	rec := &headerRequestRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetHeaders: func(_ *peerpkg.Peer, msg *wire.MsgGetHeaders) { rec.noteGetHeaders(msg) },
			OnGetBlocks:  func(_ *peerpkg.Peer, _ *wire.MsgGetBlocks) { rec.noteGetBlocks() },
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}
	localCfg := peerpkg.Config{
		Listeners:        peerpkg.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}

	remote, local, err := MakeConnectedPeers(t, remoteCfg, localCfg, idx)
	require.NoError(t, err)

	local.UpdateLastBlockHeight(1000)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	return local, rec
}

// twoCheckpoints is a chain with a checkpoint at 200 and a final one at 300.
func twoCheckpoints() (*chaincfg.Params, chaincfg.Checkpoint, chaincfg.Checkpoint) {
	first := chaincfg.Checkpoint{Height: 200, Hash: &chainhash.Hash{0xc1}}
	final := chaincfg.Checkpoint{Height: 300, Hash: &chainhash.Hash{0xc2}}

	params := chaincfg.MainNetParams
	params.Checkpoints = []chaincfg.Checkpoint{first, final}

	return &params, first, final
}

// newCheckpointManager is a headers-first manager sitting on the first of two
// checkpoints.
func newCheckpointManager(t *testing.T) (*SyncManager, chaincfg.Checkpoint, chaincfg.Checkpoint) {
	t.Helper()

	params, first, final := twoCheckpoints()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	client.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 200}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{}}, nil)

	client.On("CatchUpBlocks", mock.Anything).Return(nil)
	client.On("Run", mock.Anything, mock.Anything).Return(nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client
	sm.chainParams = params
	sm.nextCheckpoint = &first

	return sm, first, final
}

// connectCheckpointCandidate registers a live peer with the manager as an
// electable sync candidate well above our height, so the real startSync inside
// handleCheckSyncPeer has somebody to pick.
func connectCheckpointCandidate(t *testing.T, sm *SyncManager, idx uint8) (*peerpkg.Peer, *headerRequestRecorder) {
	t.Helper()

	peer, rec := connectHeaderRequestPeer(t, idx)

	state := &peerSyncState{
		syncCandidate: true,
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	t.Cleanup(state.requestedTxns.Stop)
	state.noteBestKnownHeight(peer.LastBlock())

	sm.peerStates.Set(peer, state)

	return peer, rec
}

// TestSyncManager_ACheckpointReachedWithNoPeerStillAsksForThoseHeadersLater is
// the path out of the park drain that had no way back.
//
// A parked block can be the checkpoint block, and it can be drained after the
// peer that delivered it has gone and while there is no sync peer at all. The
// nil-peer arm correctly refused to advance the checkpoint, so the node kept its
// record of the round it still owed — but nothing ever asked for it.
// checkpointBlockCommitted is reached only by a block committing, and a
// checkpoint block commits exactly once: it is in the chain afterwards,
// haveInventory answers true for it and the download walk never asks again. So
// headers-first sync stopped at that checkpoint for the life of the process.
//
// Nothing in this test invokes the transition a second time. The one call it
// makes is the one production makes, with nil, and the getheaders that follows
// has to be produced by the sync-peer check on its own. That is the difference
// from the version of this test that shipped with the nil-peer arm: it called
// checkpointBlockCommitted again by hand, which is a step production never
// performs, so it pinned idempotency rather than recovery and passed against the
// broken code.
func TestSyncManager_ACheckpointReachedWithNoPeerStillAsksForThoseHeadersLater(t *testing.T) {
	sm, first, final := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	// The checkpoint block commits from the park with nobody to ask.
	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))

	require.Equal(t, first.Height, sm.nextCheckpointSnapshot().Height,
		"nothing was asked for, so the round the node still owes must not have been advanced past")

	// A candidate turns up, and the ticker that elects sync peers runs. From
	// here the test does nothing but wait.
	_, rec := connectCheckpointCandidate(t, sm, 66)

	sm.handleCheckSyncPeer()

	require.True(t, WaitUntil(func() bool { return rec.askedForHeadersUpTo(*final.Hash) }, 5*time.Second),
		"the election must replay the deferred round on its own: the node still has to ask for the headers it has not got")

	require.True(t, sm.headersFirstMode.Load(),
		"there is another checkpoint to reach, so headers-first sync must still be on")
	require.Equal(t, final.Height, sm.nextCheckpointSnapshot().Height,
		"the replay is what advances the checkpoint, so it must have advanced")
	require.Nil(t, sm.pendingCheckpoint.Load(), "the deferred round has been asked for and must not be replayed again")
}

// TestSyncManager_ACheckpointReachedWithNoPeerIsStillMarkedAsTheAnchor covers
// the other half of the nil-peer arm. The checkpoint node stays in the header
// list so the next round's first header can prove it links to it, which makes it
// a block now in this node's chain that no peer will ever send again. If it is
// not marked, removeHeaderAnchorLocked — which removes by identity — cannot trim
// it, and a headers round arriving by any other route wedges on a front that
// will never be delivered.
func TestSyncManager_ACheckpointReachedWithNoPeerIsStillMarkedAsTheAnchor(t *testing.T) {
	sm, first, _ := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	sm.headerMu.Lock()
	sm.headerList = list.New()
	sm.headerIndex = make(map[chainhash.Hash]*list.Element)
	sm.indexHeaderLocked(sm.headerList.PushBack(&headerNode{height: first.Height, hash: first.Hash}), *first.Hash)
	sm.headerMu.Unlock()

	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))

	sm.headerMu.Lock()
	node, ok := sm.headerIndex[*first.Hash].Value.(*headerNode)
	sm.headerMu.Unlock()

	require.True(t, ok)
	require.True(t, node.isAnchor, "a committed checkpoint left in the list is the next round's anchor, peer or no peer")
}

// TestSyncManager_TheFinalCheckpointLeavesHeadersFirstMode is the other half of
// the same function, and the reason the nil-peer guard cannot simply return
// early without thinking: past the last checkpoint the node has to stop driving
// sync from the header list and go back to asking for blocks by inventory.
func TestSyncManager_TheFinalCheckpointLeavesHeadersFirstMode(t *testing.T) {
	sm, _, final := newCheckpointManager(t)
	sm.nextCheckpoint = &final
	sm.headersFirstMode.Store(true)

	peer, rec := connectHeaderRequestPeer(t, 67)
	sm.storeSyncPeer(peer, &syncPeerState{})

	require.NoError(t, sm.checkpointBlockCommitted(peer, *final.Hash))

	require.True(t, WaitUntil(func() bool { return rec.getBlocksCount() > 0 }, 5*time.Second),
		"past the final checkpoint the node asks for blocks by inventory")

	require.False(t, sm.headersFirstMode.Load(), "headers-first sync is over")
	require.Nil(t, sm.nextCheckpointSnapshot(), "there is no checkpoint left to head for")
}

// TestSyncManager_ACheckpointCommittedWithNothingLeftToReachIsANoOp covers the
// third way in: a block that was the checkpoint of a list built before the last
// checkpoint was passed. There is nothing to advance and nobody to ask.
func TestSyncManager_ACheckpointCommittedWithNothingLeftToReachIsANoOp(t *testing.T) {
	sm, first, _ := newCheckpointManager(t)
	sm.nextCheckpoint = nil

	peer, rec := connectHeaderRequestPeer(t, 68)
	sm.storeSyncPeer(peer, &syncPeerState{})

	require.NoError(t, sm.checkpointBlockCommitted(peer, *first.Hash))

	require.Zero(t, rec.getBlocksCount(), "nothing to ask for")
	require.False(t, rec.askedForHeadersUpTo(*first.Hash))
}

// TestSyncManager_ADeferredRoundIsDroppedOnceItHasBeenAskedFor pins the guard on
// the replay. A deferred round is only still owed while the checkpoint it
// belongs to has not moved; replaying one whose round has already been asked for
// would advance the checkpoint a second time and skip a whole round of headers,
// and nothing recovers a skipped round.
//
// Nothing in the tree advances the checkpoint except the transition itself, so
// this branch cannot fire today. It is pinned because the failure it prevents is
// silent and unrecoverable, while the failure it risks, repeating a transition,
// is free.
func TestSyncManager_ADeferredRoundIsDroppedOnceItHasBeenAskedFor(t *testing.T) {
	sm, first, final := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))
	require.NotNil(t, sm.pendingCheckpoint.Load(), "sanity: the round is owed")

	// Something else asks for the round and moves the checkpoint on.
	sm.headerMu.Lock()
	sm.nextCheckpoint = &final
	sm.headerMu.Unlock()

	peer, _ := connectHeaderRequestPeer(t, 118)
	sm.storeSyncPeer(peer, &syncPeerState{})

	sm.drainPendingCheckpoint()

	require.Nil(t, sm.pendingCheckpoint.Load(), "a round already asked for must be dropped, not held")

	cp := sm.nextCheckpointSnapshot()
	require.NotNil(t, cp, "the checkpoint must not be advanced a second time, which past the last one leaves headers-first mode")
	require.Equal(t, final.Height, cp.Height, "the checkpoint must not be advanced a second time")
}

// TestSyncManager_ACheckpointWithNothingLeftToReachDefersNothing is the peerless
// half of TestSyncManager_ACheckpointCommittedWithNothingLeftToReachIsANoOp.
// Past the last checkpoint there is no round to ask for, so there is nothing to
// defer either and the ticker must not be left carrying one.
func TestSyncManager_ACheckpointWithNothingLeftToReachDefersNothing(t *testing.T) {
	sm, first, _ := newCheckpointManager(t)
	sm.nextCheckpoint = nil
	sm.headersFirstMode.Store(true)

	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))

	require.Nil(t, sm.pendingCheckpoint.Load(), "there is no round left to ask for")
}

// TestSyncManager_ARebuiltHeaderStateAimsAtTheCheckpointAboveTheTip is the
// recovery ChiR13 asks for, end to end.
//
// A checkpoint block committing with nobody to ask leaves the checkpoint where
// it is on purpose, because the round of headers it owes has not been asked for.
// The sync-peer rotation that follows rebuilds the header list from the tip, and
// the tip is now at or past that checkpoint. Left as it was, startSync's
// headers-first gate reads bestHeight < nextCheckpoint.Height as false for good:
// the node takes the getblocks branch, headers-first mode stays off, and the
// replay then sends a getheaders into a mode whose reply handler disconnects the
// peer that answers.
//
// Rebuilding the list rebuilds the checkpoint it is aimed at, so the election
// asks for the round itself, with the mode on, and the replay's own guard drops
// the round because it really has been asked for.
func TestSyncManager_ARebuiltHeaderStateAimsAtTheCheckpointAboveTheTip(t *testing.T) {
	sm, first, final := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	// The checkpoint block commits from the park with nobody to ask.
	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))
	require.NotNil(t, sm.pendingCheckpoint.Load(), "sanity: the round is owed")

	// The sync peer departs and updateSyncPeer rebuilds the header list around
	// the tip, which is the checkpoint block that has just committed.
	tip := chainhash.Hash{0xb0}
	sm.resetHeaderState(&tip, first.Height)

	require.False(t, sm.headersFirstMode.Load(), "sanity: rebuilding the list turns headers-first mode off")
	require.Equal(t, final.Height, sm.nextCheckpointSnapshot().Height,
		"the rebuilt list is aimed at the checkpoint above the tip, or startSync's headers-first gate is false for good")

	// A candidate turns up and the ticker that elects sync peers runs.
	_, rec := connectCheckpointCandidate(t, sm, 119)

	sm.handleCheckSyncPeer()

	require.True(t, WaitUntil(func() bool { return rec.askedForHeadersUpTo(*final.Hash) }, 5*time.Second),
		"the election asks for the round itself, from its own headers-first branch")

	require.True(t, sm.headersFirstMode.Load(),
		"the election turned headers-first mode back on, so the peer is not disconnected for answering")
	require.Zero(t, rec.getBlocksCount(), "the node is still below a checkpoint, so it must not fall back to inventory")
	require.Nil(t, sm.pendingCheckpoint.Load(), "the round has been asked for, so the replay must have dropped it")
}

// TestSyncManager_ADeferredRoundIsNotAskedForWithHeadersFirstModeOff pins the
// second half of ChiR13, the guard on the send itself.
//
// The replay is the only caller of checkpointBlockCommitted that does not arrive
// through advanceHeaderListFor, which returns isCheckpointBlock false outright
// when the mode is off. So it is the only one that has to establish that
// precondition for itself, and a getheaders sent with the mode off costs the
// peer its whole association: handleHeadersMsg disconnects a peer that sends
// headers while the mode is off, ahead of even the empty-message check.
//
// The re-derive above is what stops this state arising, so this branch should
// not be reachable today. It is pinned because the cost of getting it wrong is
// the self-inflicted disconnect this PR exists to remove.
func TestSyncManager_ADeferredRoundIsNotAskedForWithHeadersFirstModeOff(t *testing.T) {
	sm, first, final := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))
	require.NotNil(t, sm.pendingCheckpoint.Load(), "sanity: the round is owed")

	// The mode goes off with the checkpoint left where it was, which is the
	// state the replay must refuse to send into.
	sm.headersFirstMode.Store(false)

	peer, rec := connectHeaderRequestPeer(t, 120)
	sm.storeSyncPeer(peer, &syncPeerState{})

	sm.drainPendingCheckpoint()

	require.Equal(t, first.Height, sm.nextCheckpointSnapshot().Height,
		"the round was not asked for, so the checkpoint must not have been advanced past it")

	// The send is over a real connection, so give it a window to arrive rather
	// than reading the recorder the instant the call returns.
	require.False(t, WaitUntil(func() bool { return rec.askedForHeadersUpTo(*final.Hash) }, 500*time.Millisecond),
		"no getheaders may go out with the mode off: the peer would be disconnected for answering it")
}

// TestSyncManager_ADeferredRoundWithStillNobodyToAskIsPutBack covers the
// restore. The drain takes the round out with a Swap, so a tick that finds no
// sync peer has to put it back or the round is lost for the life of the
// process, which is the wedge the deferral exists to prevent.
//
// The restore is a CompareAndSwap, not a Store: the drain runs on the sync-peer
// ticker while checkpointBlockCommitted stores from the block-queue consumer, so
// the round in the ticker's hand can in principle be the stale one. No reachable
// path stores a second round while one is pending, so this test pins the
// restore, not the race.
func TestSyncManager_ADeferredRoundWithStillNobodyToAskIsPutBack(t *testing.T) {
	sm, first, _ := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))

	pending := sm.pendingCheckpoint.Load()
	require.NotNil(t, pending, "sanity: the round is owed")

	// The tick runs and the election still produced nobody.
	sm.drainPendingCheckpoint()

	require.Same(t, pending, sm.pendingCheckpoint.Load(),
		"with nobody to ask the round is still owed and must be waiting for the next tick")
	require.Equal(t, first.Height, sm.nextCheckpointSnapshot().Height,
		"nothing was asked for, so the checkpoint must not have moved")
}

// TestSyncManager_AStaleRebuildHeightStillLetsTheElectionReAim is the regression
// test for ChiR15.
//
// Both callers of the header-state rebuild read their height outside headerMu
// and then wait to acquire it, and on that path the lock is contended by every
// arriving block. So a checkpoint transition can commit in the gap and the
// height handed to the rebuild can already be behind it. Re-deriving from that
// stale height leaves the checkpoint naming a block that is now in our chain,
// and startSync's gate is bestHeight < nextCheckpoint.Height, which such a
// checkpoint can never satisfy. Headers-first mode would then stay off for the
// life of the process, silently: the rebuild cleared startHeader, no headers can
// arrive to repopulate it, and the walk the scheduler assigns slices from has
// nothing to walk.
//
// The stale read is expressed here as an argument rather than raced for.
func TestSyncManager_AStaleRebuildHeightStillLetsTheElectionReAim(t *testing.T) {
	sm, first, final := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	// The checkpoint block commits from the park with nobody to ask, so the
	// nil-peer arm deliberately leaves the checkpoint where it is.
	require.NoError(t, sm.checkpointBlockCommitted(nil, *first.Hash))

	// The rebuild runs with a height read BEFORE that commit, one below the
	// checkpoint whose block is now in the chain.
	tip := chainhash.Hash{0xb0}
	sm.resetHeaderState(&tip, first.Height-1)

	require.Equal(t, first.Height, sm.nextCheckpointSnapshot().Height,
		"sanity: the stale height cannot advance the checkpoint, which is the state under test")

	// The election is the one place a current height is in hand, so it must
	// repair the aim rather than trust the last rebuild.
	_, rec := connectCheckpointCandidate(t, sm, 121)

	sm.handleCheckSyncPeer()

	require.True(t, WaitUntil(func() bool { return rec.askedForHeadersUpTo(*final.Hash) }, 5*time.Second),
		"the election must re-aim above the committed checkpoint and ask for the round")
	require.True(t, sm.headersFirstMode.Load(),
		"headers-first mode must come back on, or this PR's own feature is off until a restart")
	require.Zero(t, rec.getBlocksCount(),
		"the node is still below a checkpoint, so it must not fall back to the inventory path")
}

// TestSyncManager_AStaleRebuildHeightCannotRewindTheCheckpoint pins the other
// interleaving of ChiR15, where the checkpoint block arrived from a connected
// peer so the transition DID advance the checkpoint. An unconditional re-derive
// from a stale height would put it back onto the checkpoint that just committed.
func TestSyncManager_AStaleRebuildHeightCannotRewindTheCheckpoint(t *testing.T) {
	sm, first, final := newCheckpointManager(t)
	sm.headersFirstMode.Store(true)

	// The transition has already advanced the checkpoint to the next one.
	sm.nextCheckpoint = &final

	// A rebuild then runs holding a height read before that transition.
	tip := chainhash.Hash{0xb0}
	sm.resetHeaderState(&tip, first.Height-1)

	require.Equal(t, final.Height, sm.nextCheckpointSnapshot().Height,
		"a stale height must never walk the checkpoint back onto a block already in the chain")
}
