package netsync

import (
	"container/list"
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// getDataRecorder collects the block hashes a peer's remote end is asked for.
type getDataRecorder struct {
	mu     sync.Mutex
	hashes []chainhash.Hash
	msgs   int
}

func (r *getDataRecorder) record(msg *wire.MsgGetData) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.msgs++

	for _, iv := range msg.InvList {
		if iv.Type == wire.InvTypeBlock {
			r.hashes = append(r.hashes, iv.Hash)
		}
	}
}

// messages reports how many getdata messages arrived, as distinct from how many
// hashes they carried between them.
func (r *getDataRecorder) messages() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.msgs
}

func (r *getDataRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.hashes)
}

func (r *getDataRecorder) all() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]chainhash.Hash, len(r.hashes))
	copy(out, r.hashes)

	return out
}

// connectRacePeer returns a live peer whose remote end records every getdata it
// is sent. The remote peer is returned too so the caller keeps it alive for the
// duration of the test.
func connectRacePeer(t *testing.T, idx uint8, lastBlock int32) (*peerpkg.Peer, *peerpkg.Peer, *getDataRecorder) {
	t.Helper()

	rec := &getDataRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnGetData: func(_ *peerpkg.Peer, msg *wire.MsgGetData) {
				rec.record(msg)
			},
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

	local.UpdateLastBlockHeight(lastBlock)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	return local, remote, rec
}

// newRaceManager builds the smallest SyncManager the frontier-race path needs.
// Settings come from the real loader, so the defaults under test are the ones an
// unconfigured node actually gets.
func newRaceManager(t *testing.T) *SyncManager {
	t.Helper()

	sm := &SyncManager{
		logger:         ulogger.TestLogger{},
		settings:       test.CreateBaseTestSettings(t),
		chainParams:    &chaincfg.MainNetParams,
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		headerList:     list.New(),
		blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
		racedBlocks: expiringmap.New[chainhash.Hash, map[*peerpkg.Peer]struct{}](racedBlockGraceTTL).
			WithMaxSize(racedBlockGraceMaxTracked),
	}
	sm.headersFirstMode.Store(true)

	// Mirror New, which only rebuilds the header state when a checkpoint exists
	// (see the DisableCheckpoints branch there). resetHeaderStateLocked treats a
	// nil checkpoint as terminal and will not derive one, so a manager built
	// without this would push no anchor and the walk would have nothing to do.
	sm.nextCheckpoint = sm.findNextHeaderCheckpoint(0)

	t.Cleanup(func() { sm.racedBlocks.Stop() })

	return sm
}

// registerRacePeer adds a peer to the manager as a sync candidate and returns
// its state.
func registerRacePeer(sm *SyncManager, p *peerpkg.Peer) *peerSyncState {
	state := &peerSyncState{
		syncCandidate: true,
	}
	sm.peerStates.Set(p, state)

	return state
}

// TestFrontierRace_AsksASecondPeerForTheStuckBlock is the anchor test. The sync
// peer has gone quiet on the one block everything else is queued behind, so a
// second connected peer must be asked for that same block — and only that block
// — while the original request is left alone.
func TestFrontierRace_AsksASecondPeerForTheStuckBlock(t *testing.T) {
	sm := newRaceManager(t)

	syncPeer, _, syncRec := connectRacePeer(t, 1, 1000)
	other, _, otherRec := connectRacePeer(t, 2, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// The sync peer was asked for this block and has not answered for 30
	// seconds, comfortably past the 20 second default.
	frontier := chainhash.Hash{0xaa}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	sm.raceFrontierBlock(time.Now())

	require.True(t, WaitUntil(func() bool { return otherRec.count() > 0 }, 5*time.Second),
		"the second peer should have been asked for the stuck block")
	require.Equal(t, []chainhash.Hash{frontier}, otherRec.all(),
		"the second peer should be asked for the stuck block and nothing else")
	require.Zero(t, syncRec.count(), "the original peer must not be asked again")

	// The reply has to be authorised, or the delivering peer would be
	// disconnected for sending a block we never asked for.
	_, exists := sm.peerStates.Get(other)
	require.True(t, exists)
	authorised := sm.blockDownloads.HasOwner(other, frontier)
	require.True(t, authorised, "the second peer's reply must be authorised in advance")

	// The original request stands: we added a copy, we did not move the block.
	stillAsked := sm.blockDownloads.HasOwner(syncPeer, frontier)
	require.True(t, stillAsked, "the original request must be left in place")
}

// TestFrontierRace_RunsFromTheBlockHandler proves the feature is actually
// reachable in a running node rather than only when a test calls it directly:
// the block handler's own timer must fire the race with no help.
func TestFrontierRace_RunsFromTheBlockHandler(t *testing.T) {
	sm := newRaceManager(t)
	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})
	sm.msgChan = make(chan interface{}, 1)

	syncPeer, _, _ := connectRacePeer(t, 3, 1000)
	other, _, otherRec := connectRacePeer(t, 4, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0xbb}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	go sm.blockHandler()

	t.Cleanup(func() {
		close(sm.quit)
		<-sm.handlerDone
	})

	require.True(t, WaitUntil(func() bool { return otherRec.count() > 0 }, 3*frontierCheckInterval),
		"the block handler's timer should have raced the stuck block without being prompted")
	require.Equal(t, []chainhash.Hash{frontier}, otherRec.all())
}

// TestFrontierRaceTarget covers every reason not to race. Each case must return
// false; the last one is the case that must return true, so a change that made
// the gates unconditionally strict would not slip through.
func TestFrontierRaceTarget(t *testing.T) {
	syncPeer, _, _ := connectRacePeer(t, 5, 1000)
	other, _, _ := connectRacePeer(t, 6, 1000)
	shortPeer, _, _ := connectRacePeer(t, 7, 100)

	stale := func() time.Time { return time.Now().Add(-30 * time.Second) }

	// pulling makes a peer look like it is actively bringing bytes in, in the
	// same terms samplePeerThroughput records: two samples a tick apart with a
	// healthy delta between them.
	pulling := func(t *testing.T, sm *SyncManager, p *peerpkg.Peer) {
		t.Helper()

		state, exists := sm.peerStates.Get(p)
		require.True(t, exists)

		state.assocReadBytesLastTick.Store(0)
		state.assocReadBytes.Store(64 << 20)
		state.throughputTicks.Store(2)
	}

	// setup builds a manager in the state where a race SHOULD happen, so each
	// case only has to break the one thing it is about.
	setup := func(t *testing.T) *SyncManager {
		sm := newRaceManager(t)
		registerRacePeer(sm, syncPeer)
		registerRacePeer(sm, other)
		sm.storeSyncPeer(syncPeer, &syncPeerState{})
		sm.setFrontier(chainhash.Hash{0xcc}, 500, stale())

		// A frontier is by definition a block we have already asked somebody for
		// — publishFrontier will not publish one we have not — and unless a case
		// says otherwise that somebody is the sync peer, which is the only shape
		// this path had before the scheduler existed.
		sm.blockDownloads.Add(syncPeer, chainhash.Hash{0xcc})

		return sm
	}

	t.Run("races when the frontier is stuck and nothing else is wrong", func(t *testing.T) {
		sm := setup(t)
		hash, height, target, ok := sm.frontierRaceTarget(time.Now())
		require.True(t, ok)
		require.Equal(t, chainhash.Hash{0xcc}, hash)
		require.Equal(t, int32(500), height)
		require.Equal(t, other, target)
	})

	t.Run("no frontier", func(t *testing.T) {
		sm := setup(t)
		sm.clearFrontier()
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("frontier not stuck for long enough yet", func(t *testing.T) {
		sm := setup(t)
		sm.setFrontier(chainhash.Hash{0xdd}, 500, time.Now().Add(-2*time.Second))
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("racing switched off by legacy_maxBlockParallelFetch", func(t *testing.T) {
		sm := setup(t)
		sm.settings.Legacy.MaxBlockParallelFetch = 1
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("racing switched off by legacy_blockSlowFetchTimeout", func(t *testing.T) {
		sm := setup(t)
		sm.settings.Legacy.BlockSlowFetchTimeout = 0
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("not in headers-first mode", func(t *testing.T) {
		sm := setup(t)
		sm.headersFirstMode.Store(false)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("already at the configured number of peers", func(t *testing.T) {
		sm := setup(t)
		require.True(t, sm.registerFrontierRacer(chainhash.Hash{0xcc}, other))
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok, "default of 2 is the holder plus one racer, so no third peer")
	})

	t.Run("we are throttling our own reads", func(t *testing.T) {
		sm := setup(t)
		sm.blockBacklog.Store(1)
		require.True(t, sm.localReadBackpressured())
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("the peer that owes the frontier is still pulling bytes", func(t *testing.T) {
		sm := setup(t)
		pulling(t, sm, syncPeer)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok, "a peer mid-transfer of a large block is slow, not stalled")
	})

	// The same question, of a peer that is not the sync peer. This is the
	// ordinary case once bodies come from everybody, and it used to get no
	// throughput test at all: the check consulted the sync peer's sample and only
	// when the sync peer owed the block, so a non-sync owner streaming a
	// multi-gigabyte block was raced regardless of how fast it was going.
	t.Run("a non-sync peer that owes the frontier is still pulling bytes", func(t *testing.T) {
		sm := setup(t)
		sm.blockDownloads.RemoveOwner(syncPeer, chainhash.Hash{0xcc})
		require.True(t, sm.blockDownloads.Add(other, chainhash.Hash{0xcc}))

		pulling(t, sm, other)

		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok,
			"the owner is mid-transfer, so racing it wastes the bandwidth already spent")
	})

	// And the flip side, so the case above is not passing for the wrong reason:
	// a silent owner must still be raced.
	t.Run("a non-sync peer that owes the frontier and is silent is raced", func(t *testing.T) {
		sm := setup(t)
		sm.blockDownloads.RemoveOwner(syncPeer, chainhash.Hash{0xcc})
		require.True(t, sm.blockDownloads.Add(other, chainhash.Hash{0xcc}))

		_, _, target, ok := sm.frontierRaceTarget(time.Now())
		require.True(t, ok, "an owner bringing nothing in is what racing is for")
		require.Equal(t, syncPeer, target,
			"and the owner must not be asked again, which leaves the sync peer")
	})

	// The throughput sample this node keeps belongs to the sync peer, and under
	// the fan-out the frontier is routinely owed by somebody else — so the
	// sample can suppress the race for a block it knows nothing about. That is
	// the state the race exists for: everything behind the frontier is piling up
	// in the park while the sync peer's own later run comes in at a healthy rate,
	// and nothing else fires until the sync peer's 180-second stall window ends.
	t.Run("a healthy sync peer does not speak for a frontier another peer owes", func(t *testing.T) {
		sm := setup(t)
		sm.blockDownloads.RemoveOwner(syncPeer, chainhash.Hash{0xcc})
		sm.blockDownloads.Add(other, chainhash.Hash{0xcc})
		sm.storeSyncPeer(syncPeer, &syncPeerState{
			ticks:                  1,
			assocReadBytes:         64 << 20,
			assocReadBytesLastTick: 0,
		})

		_, _, target, ok := sm.frontierRaceTarget(time.Now())
		require.True(t, ok, "the sync peer's throughput says nothing about a block another peer owes")
		require.Equal(t, syncPeer, target,
			"and the peer already sitting on the block must not be asked for it twice, which leaves the sync peer")
	})

	t.Run("no sync peer", func(t *testing.T) {
		sm := setup(t)
		sm.storeSyncPeer(nil, nil)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("the only other peer is not a sync candidate", func(t *testing.T) {
		sm := setup(t)
		state, exists := sm.peerStates.Get(other)
		require.True(t, exists)
		state.syncCandidate = false
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("the only other peer is not connected", func(t *testing.T) {
		sm := setup(t)
		sm.peerStates.Delete(other)
		registerRacePeer(sm, &peerpkg.Peer{})
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("the only other peer does not have the block yet", func(t *testing.T) {
		sm := setup(t)
		sm.peerStates.Delete(other)
		registerRacePeer(sm, shortPeer)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)
	})

	t.Run("there is no other peer at all", func(t *testing.T) {
		sm := setup(t)
		sm.peerStates.Delete(other)
		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok, "with only the peer that already owes us the block, do nothing")
	})
}

// TestNoteRaceWinner_CancelsTheRequestWithEveryoneElse is the guard against
// leaving requests behind that will never be answered. One stale entry counts
// against the in-flight limit fetchHeaderBlocks uses, and that limit drops to a
// single block once blocks get large, so a leftover would stop fetching outright.
func TestNoteRaceWinner_ReleasesEveryoneElseWithoutRevokingThem(t *testing.T) {
	sm := newRaceManager(t)

	syncPeer, _, _ := connectRacePeer(t, 8, 1000)
	other, _, _ := connectRacePeer(t, 9, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0xee}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	sm.raceFrontierBlock(time.Now())

	asked := sm.blockDownloads.HasOwner(other, frontier)
	require.True(t, asked, "precondition: the second peer was asked")

	sm.noteRaceWinner(frontier)

	// Released, not revoked. The budget goes, which is the whole reason the
	// release exists — a request that will never be answered must not go on
	// counting against what we ask that peer for next. Permission stays, because
	// either peer may still be part-way through sending its copy, and a block
	// arriving from a peer that no longer owns it costs that peer its whole
	// association. Cancelling did both at once and so had to be papered over by a
	// separate grace map with its own expiry.
	require.Zero(t, sm.blockDownloads.CountForPeer(syncPeer),
		"the original peer's obligation must be released once the block arrives")
	require.Zero(t, sm.blockDownloads.CountForPeer(other),
		"the second peer's obligation must be released too")

	require.True(t, sm.blockDownloads.HasOwner(syncPeer, frontier),
		"but its permission to deliver must survive, or its copy arrives unowned")
	require.True(t, sm.blockDownloads.HasOwner(other, frontier),
		"and so must the second peer's")

	sm.frontierMu.Lock()
	racers := len(sm.frontierRacers)
	sm.frontierMu.Unlock()
	require.Zero(t, racers, "the race is over")
}

// TestBlockRacedTo_OnlyForPeersWeAsked checks the one place this change relaxes
// an existing defence. A peer that sends a block nobody asked for is still
// disconnected; only the specific peers we asked for the specific block we raced
// get a pass.
func TestBlockRacedTo_OnlyForPeersWeAsked(t *testing.T) {
	sm := newRaceManager(t)

	syncPeer, _, _ := connectRacePeer(t, 10, 1000)
	other, _, _ := connectRacePeer(t, 11, 1000)
	stranger, _, _ := connectRacePeer(t, 12, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0x0f}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	// Only one other peer is registered at this point, so it is the one raced to.
	sm.raceFrontierBlock(time.Now())

	// Nothing is forgiven until the race is actually decided.
	require.False(t, sm.BlockRacedTo(other, &frontier))

	sm.noteRaceWinner(frontier)

	registerRacePeer(sm, stranger)

	require.True(t, sm.BlockRacedTo(other, &frontier), "we asked this peer for this block")
	require.True(t, sm.BlockRacedTo(syncPeer, &frontier), "we asked this peer for this block first")
	require.False(t, sm.BlockRacedTo(stranger, &frontier), "we never asked this peer")

	unrelated := chainhash.Hash{0x99}
	require.False(t, sm.BlockRacedTo(other, &unrelated), "we never asked for this block")
}

// TestHandleBlockMsg_LateCopyOfARacedBlockIsNotPunished checks the exception is
// wired into the block handler itself, not just available as a helper. A peer
// that answers a request we cancelled keeps its connection; a peer that sends a
// block nobody asked for still loses it.
func TestHandleBlockMsg_LateCopyOfARacedBlockIsNotPunished(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	syncPeer, _, _ := connectRacePeer(t, 14, 1000)
	loser, _, _ := connectRacePeer(t, 15, 1000)
	stranger, _, _ := connectRacePeer(t, 16, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, loser)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0x77}
	sm.blockDownloads.Add(syncPeer, frontier)
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	// Race it, then let it be delivered, which cancels the request with the
	// loser and leaves it with a copy still on the way. Only one other peer is
	// registered at this point, so it is the one raced to.
	sm.raceFrontierBlock(time.Now())
	sm.noteRaceWinner(frontier)

	registerRacePeer(sm, stranger)

	// The node will still accept the loser's copy, which is the property that
	// keeps it connected. It is no longer a special case bolted on beside the
	// ownership question: the loser still owns the block, so it answers the same
	// question every honest delivery answers.
	require.True(t, sm.BlockRequested(loser, &frontier),
		"a peer we asked must still be allowed to deliver, however late")

	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: frontier, peer: loser})
	require.True(t, loser.Connected(), "a peer that answered our own request must not be disconnected")

	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: frontier, peer: stranger})
	require.Error(t, err)
	require.True(t, WaitUntil(func() bool { return !stranger.Connected() }, 2*time.Second),
		"a peer we never asked must still be disconnected for an unrequested block")
}

// TestFetchHeaderBlocks_PublishesTheFrontier proves the frontier is actually
// recorded by the code that sends the block requests. Nothing else sets it, so
// without this call the race could never find a block to run on.
func TestFetchHeaderBlocks_PublishesTheFrontier(t *testing.T) {
	blockchainClient := &blockchain2.Mock{}
	// No block we ask about is already in our chain, so all of them get requested.
	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient
	sm.blockSizeTracker = newBlockSizeTracker(10)

	syncPeer, _, syncRec := connectRacePeer(t, 17, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	first := chainhash.Hash{0xf1}
	second := chainhash.Hash{0xf2}

	// Seeded through the index, as every production push is: fetchHeaderBlocks
	// asks the index whether startHeader is still the live holder of its hash
	// before acting on lookups made with the header lock released.
	sm.headerMu.Lock()
	sm.startHeader = sm.headerList.PushBack(&headerNode{height: 10, hash: &first})
	sm.indexHeaderLocked(sm.startHeader, first)
	sm.indexHeaderLocked(sm.headerList.PushBack(&headerNode{height: 11, hash: &second}), second)
	sm.headerMu.Unlock()

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return syncRec.count() == 2 }, 5*time.Second),
		"both blocks should have been requested from the sync peer")

	sm.frontierMu.Lock()
	defer sm.frontierMu.Unlock()
	require.Equal(t, first, sm.frontierHash, "the oldest requested block is what everything else waits on")
	require.Equal(t, int32(10), sm.frontierHeight)
}

// TestHandleBlockMsg_AdvancesTheFrontier proves the other half of the wiring:
// when the block everything was queued behind arrives, the block handler moves
// the frontier on. If it did not, the race would keep firing at a block that has
// already been delivered.
func TestHandleBlockMsg_AdvancesTheFrontier(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	checkpointHash := chainhash.Hash{0xcf}
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 999, Hash: &checkpointHash}

	syncPeer, _, _ := connectRacePeer(t, 18, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	arriving := chainhash.Hash{0xa1}
	next := chainhash.Hash{0xa2}
	sm.headerList.PushBack(&headerNode{height: 10, hash: &arriving})
	sm.headerList.PushBack(&headerNode{height: 11, hash: &next})
	sm.startHeader = nil // everything in the list has been requested

	sm.blockDownloads.Add(syncPeer, arriving)
	sm.setFrontier(arriving, 10, time.Now().Add(-30*time.Second))

	// Carrying no block makes handleBlockMsg bail immediately after the
	// header-list bookkeeping, which is the part under test.
	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: arriving, peer: syncPeer})
	require.Error(t, err)

	sm.frontierMu.Lock()
	defer sm.frontierMu.Unlock()
	require.Equal(t, next, sm.frontierHash, "the next block in the list is now what everything waits on")
	require.Equal(t, int32(11), sm.frontierHeight)
}

// TestSetFrontier_AgeIsMeasuredFromTheBlockNotTheUpdate pins the behaviour the
// age gate depends on. fetchHeaderBlocks republishes the same frontier every
// time it tops up the request pipeline; if each republish reset the clock, the
// block would never look stuck and the race could never start.
func TestSetFrontier_AgeIsMeasuredFromTheBlockNotTheUpdate(t *testing.T) {
	sm := newRaceManager(t)

	first := time.Now().Add(-30 * time.Second)
	sm.setFrontier(chainhash.Hash{0x01}, 10, first)
	sm.setFrontier(chainhash.Hash{0x01}, 10, time.Now())

	require.WithinDuration(t, first, sm.frontierStartedAt(), time.Millisecond,
		"republishing the same frontier must not restart its clock")

	// A genuinely new frontier does restart it, and forgets the old racers.
	other, _, _ := connectRacePeer(t, 13, 1000)
	require.True(t, sm.registerFrontierRacer(chainhash.Hash{0x01}, other))

	moved := time.Now()
	sm.setFrontier(chainhash.Hash{0x02}, 11, moved)

	require.WithinDuration(t, moved, sm.frontierStartedAt(), time.Millisecond)

	sm.frontierMu.Lock()
	racers := len(sm.frontierRacers)
	sm.frontierMu.Unlock()
	require.Zero(t, racers, "peers racing the previous block are no longer racing anything")
}

// TestPublishFrontier covers what counts as a block worth racing.
func TestPublishFrontier(t *testing.T) {
	hashA := chainhash.Hash{0xa1}
	hashB := chainhash.Hash{0xb2}

	t.Run("publishes the oldest block already asked for", func(t *testing.T) {
		sm := newRaceManager(t)
		sm.headerList.PushBack(&headerNode{height: 10, hash: &hashA})
		last := sm.headerList.PushBack(&headerNode{height: 11, hash: &hashB})
		sm.startHeader = last // everything before it has been requested

		sm.publishFrontier(time.Now())

		sm.frontierMu.Lock()
		defer sm.frontierMu.Unlock()
		require.Equal(t, hashA, sm.frontierHash)
		require.Equal(t, int32(10), sm.frontierHeight)
	})

	t.Run("nothing is stuck when the front block has not been asked for", func(t *testing.T) {
		sm := newRaceManager(t)
		front := sm.headerList.PushBack(&headerNode{height: 10, hash: &hashA})
		sm.startHeader = front

		sm.publishFrontier(time.Now())

		sm.frontierMu.Lock()
		defer sm.frontierMu.Unlock()
		require.Equal(t, chainhash.Hash{}, sm.frontierHash)
	})

	t.Run("nothing is stuck outside headers-first mode", func(t *testing.T) {
		sm := newRaceManager(t)
		sm.headerList.PushBack(&headerNode{height: 10, hash: &hashA})
		sm.headerList.PushBack(&headerNode{height: 11, hash: &hashB})
		sm.headersFirstMode.Store(false)

		sm.publishFrontier(time.Now())

		sm.frontierMu.Lock()
		defer sm.frontierMu.Unlock()
		require.Equal(t, chainhash.Hash{}, sm.frontierHash)
	})

	t.Run("resetting the header state clears the frontier", func(t *testing.T) {
		sm := newRaceManager(t)
		sm.setFrontier(hashA, 10, time.Now())
		sm.resetHeaderState(&hashB, 11)

		_, _, _, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok)

		sm.frontierMu.Lock()
		defer sm.frontierMu.Unlock()
		require.Equal(t, chainhash.Hash{}, sm.frontierHash)
	})
}

// TestFrontierRace_AFullLedgerDoesNotSuppressTheRaceForever proves that a race
// abandoned because the download ledger was full leaves nothing behind. The
// registration is taken back out, so once there is room again the same frontier
// block can still be raced.
//
// Getting this wrong is permanent, not transient: the abandoned registration
// counts towards legacy_maxBlockParallelFetch, and the frontier only moves when
// the block arrives — so the one block holding up sync would never be raced by
// anybody again.
func TestFrontierRace_AFullLedgerDoesNotSuppressTheRaceForever(t *testing.T) {
	sm := newRaceManager(t)

	// The sync peer is put below the frontier height so it is ruled out as a
	// racer by the "has it told us it has the block" test, leaving exactly one
	// eligible peer. Otherwise either peer could be picked, since a frontier
	// nobody owes rules nobody out.
	syncPeer, _, syncRec := connectRacePeer(t, 1, 100)
	other, _, otherRec := connectRacePeer(t, 2, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, other)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// Fill the ledger with blocks owed by the sync peer, deliberately leaving
	// the frontier itself out of it: that is the one case the size cap can turn
	// a race away, a frontier whose own record aged out while the ledger stayed
	// full.
	filler := make([]chainhash.Hash, 0, maxTrackedBlockDownloads)

	for i := 0; i < maxTrackedBlockDownloads; i++ {
		var h chainhash.Hash

		binary.LittleEndian.PutUint32(h[:4], uint32(i))
		h[31] = 0xfe // keep every filler clear of the frontier hash below

		require.True(t, sm.blockDownloads.Add(syncPeer, h), "filling the ledger should succeed")

		filler = append(filler, h)
	}

	frontier := chainhash.Hash{0xaa}
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	sm.raceFrontierBlock(time.Now())

	require.Zero(t, otherRec.count(), "a full ledger cannot authorise a reply, so nothing may go out on the wire")
	require.Zero(t, syncRec.count(), "the peer that cannot have the block must never be asked")
	require.False(t, sm.blockDownloads.HasOwner(other, frontier), "the racer must not be recorded as an owner")

	// The end state that matters: the race is still available. Make one slot of
	// room and the very same frontier block must be raced to the very same peer.
	sm.blockDownloads.RemoveOwner(syncPeer, filler[0])

	sm.raceFrontierBlock(time.Now())

	require.True(t, WaitUntil(func() bool { return otherRec.count() > 0 }, 5*time.Second),
		"once there is room the stuck frontier block must still be raceable")
	require.Equal(t, []chainhash.Hash{frontier}, otherRec.all(),
		"the second peer should be asked for the stuck block and nothing else")
	require.True(t, sm.blockDownloads.HasOwner(other, frontier),
		"the second peer's reply must be authorised in advance")
}

// TestNoteRaceWinner_CancelsTheRealOwnerNotTheSyncPeer covers the shape its
// sibling above does not: the frontier owed by a peer that is not the sync peer.
//
// That is the ordinary case once bodies come from every eligible peer. The
// frontier belongs to whichever peer the scheduler gave it to, and a demotion
// moves the headers role to somebody else while leaving the original owner's
// assignment exactly where it is. Assuming the owner was the sync peer had two
// costs: the real owner kept a live in-flight slot for the whole hour-long
// ownership ceiling — the stall this helper exists to prevent — and it was left
// out of the grace set, so its copy turning up later cost an honest peer its
// whole association.
func TestNoteRaceWinner_ReleasesTheRealOwnerNotTheSyncPeer(t *testing.T) {
	sm := newRaceManager(t)

	// The sync peer is below the frontier height, so it is neither the owner nor
	// an eligible racer, and the racer choice is deterministic.
	syncPeer, _, syncRec := connectRacePeer(t, 20, 100)
	owner, _, _ := connectRacePeer(t, 21, 1000)
	racer, _, racerRec := connectRacePeer(t, 22, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, owner)
	registerRacePeer(sm, racer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0xef}

	// A non-sync peer owes the frontier, which is what the scheduler produces.
	require.True(t, sm.blockDownloads.Add(owner, frontier))
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	sm.raceFrontierBlock(time.Now())

	require.True(t, WaitUntil(func() bool { return racerRec.count() > 0 }, 5*time.Second),
		"precondition: the eligible peer was raced the frontier block")
	require.Zero(t, syncRec.count(), "the peer below the frontier height must not be asked")

	sm.noteRaceWinner(frontier)

	require.Zero(t, sm.blockDownloads.CountForPeer(owner),
		"the peer that actually owed the block must be released, or it holds an in-flight slot for the hour")
	require.Zero(t, sm.blockDownloads.CountForPeer(racer),
		"the racer must be released too")

	// And neither may be punished for a copy still on the wire. The owner is the
	// likelier of the two, having been asked first.
	require.True(t, sm.blockDownloads.HasOwner(owner, frontier),
		"the real owner must keep permission to deliver the block we asked it for")
	require.True(t, sm.blockDownloads.HasOwner(racer, frontier),
		"and so must the racer")
}

// TestFrontierRace_ADepartedRacerDoesNotDisableTheRace pins the other way a race
// could be switched off permanently.
//
// frontierRacers is only cleared wholesale — when the frontier moves, or when
// the raced block arrives. clearRequestedState releases a departed peer's ledger
// ownership and never touches this map. So a racer that disconnects stayed in the
// set, and because the set is what maxRacing is measured against, at the default
// of two a single departed racer meant "already racing as hard as we can" for as
// long as the frontier sat on that block. The frontier only moves when the block
// arrives, so that is until the 180-second backstop fires — the exact stall this
// file exists to avoid, reintroduced by the thing meant to fix it.
func TestFrontierRace_ADepartedRacerDoesNotDisableTheRace(t *testing.T) {
	sm := newRaceManager(t)

	// Below the frontier height, so it is neither owner nor eligible racer and
	// the choice of target is deterministic.
	syncPeer, _, syncRec := connectRacePeer(t, 30, 100)
	owner, _, _ := connectRacePeer(t, 31, 1000)
	departed, _, _ := connectRacePeer(t, 32, 1000)
	live, _, liveRec := connectRacePeer(t, 33, 1000)

	registerRacePeer(sm, syncPeer)
	registerRacePeer(sm, owner)
	registerRacePeer(sm, departed)
	registerRacePeer(sm, live)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	frontier := chainhash.Hash{0xfa}

	require.True(t, sm.blockDownloads.Add(owner, frontier))
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	// One racer was already asked, and has since gone.
	require.True(t, sm.registerFrontierRacer(frontier, departed))
	departed.DisconnectWithInfo("test: the racer goes away")

	require.True(t, WaitUntil(func() bool { return !departed.Connected() }, 5*time.Second),
		"precondition: the racer has actually gone")

	sm.raceFrontierBlock(time.Now())

	require.True(t, WaitUntil(func() bool { return liveRec.count() > 0 }, 5*time.Second),
		"a departed racer must not count towards the parallel-fetch limit; the stuck block must still be raced")
	require.Equal(t, []chainhash.Hash{frontier}, liveRec.all(),
		"and it must be raced the frontier block, nothing else")
	require.Zero(t, syncRec.count(), "the peer below the frontier height must not be asked")

	require.True(t, sm.blockDownloads.HasOwner(live, frontier),
		"the racer's reply has to be authorised in advance")
}

// TestFrontierRace_NilPeerStatesDoesNotPanic proves the nil-peerStates guard in
// frontierRaceTarget actually guards. It used to sit below the owner-throughput
// loop, which calls sm.peerStates.Get; SyncedMap.Get takes the receiver's lock,
// so a nil map panics on the way in rather than returning "not found", and the
// one state the guard claimed to defend against crashed before reaching it.
func TestFrontierRace_NilPeerStatesDoesNotPanic(t *testing.T) {
	sm := newRaceManager(t)

	owner, _, _ := connectRacePeer(t, 60, 1000)
	registerRacePeer(sm, owner)
	sm.storeSyncPeer(owner, &syncPeerState{})

	frontier := chainhash.Hash{0xcd}

	require.True(t, sm.blockDownloads.Add(owner, frontier))
	sm.setFrontier(frontier, 500, time.Now().Add(-30*time.Second))

	// Precondition: every earlier bail-out is cleared, so the loop over the
	// ledger's owners is genuinely reached. The sole owner is also the only
	// registered peer, so there is nobody left to race and ok is false.
	_, _, target, ok := sm.frontierRaceTarget(time.Now())
	require.False(t, ok)
	require.Nil(t, target)

	sm.peerStates = nil

	require.NotPanics(t, func() {
		_, _, target, ok := sm.frontierRaceTarget(time.Now())
		require.False(t, ok, "with no peer states there is nobody to race")
		require.Nil(t, target)
	})
}

// TestHandleBlockMsg_TheOwnersThatDidNotDeliverGetTheirBudgetBack is the release
// the frontier race had and nothing else did.
//
// An arrival discharged only the peer that delivered. The other owners were let
// off solely by noteRaceWinner, which returns before it reaches ForgiveOwners
// unless the block is the current frontier and the racer set is non-empty. The
// walk creates a second owner with no race involved: once a rewind puts the
// cursor in front of a block whose owner was asked more than
// blockRequestRetryInterval ago and is still transferring, the retry skip no
// longer covers it and the assigner is free to hand it to somebody else. That
// first owner then held a live in-flight slot in CountForPeer until its own copy
// landed, it disconnected, or the assignment aged out an hour later.
//
// The manager here has no frontier and no racers, so the race path is not the
// one under test. The end state asserted is the ledger's, not the call's: budget
// back, permission kept, hash immediately re-requestable.
func TestHandleBlockMsg_TheOwnersThatDidNotDeliverGetTheirBudgetBack(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	slow, _, _ := connectRacePeer(t, 76, 1000)
	registerRacePeer(sm, slow)

	deliverer, _, _ := connectRacePeer(t, 77, 1000)
	registerRacePeer(sm, deliverer)

	hash := chainhash.Hash{0x7a}
	require.True(t, sm.blockDownloads.Add(slow, hash))
	require.True(t, sm.blockDownloads.Add(deliverer, hash))
	require.Equal(t, 1, sm.blockDownloads.CountForPeer(slow))

	sm.frontierMu.Lock()
	noFrontier := sm.frontierHash != hash && len(sm.frontierRacers) == 0
	sm.frontierMu.Unlock()
	require.True(t, noFrontier, "sanity: the race path must not be what releases the slow peer here")

	// Carrying no block makes handleBlockMsg bail just past the discharge, so
	// the error only says it got that far.
	require.Error(t, sm.handleBlockMsg(&blockQueueMsg{blockHash: hash, peer: deliverer}))

	require.Zero(t, sm.blockDownloads.CountForPeer(deliverer),
		"the peer that answered no longer owes the block")
	require.Zero(t, sm.blockDownloads.CountForPeer(slow),
		"the peer that did not answer must stop spending an in-flight slot on a block already in hand")
	require.True(t, sm.blockDownloads.HasOwner(slow, hash),
		"it keeps permission to deliver, so a copy already on the wire is admitted rather than costing it its association")
	require.False(t, sm.blockDownloads.RequestedWithin(hash, blockRequestRetryInterval),
		"and the hash is re-requestable at once, which is what a rewind onto a rejected block needs")
}
