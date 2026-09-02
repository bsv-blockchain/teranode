package netsync

import (
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
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newInvManager builds the smallest SyncManager the inv path needs. known maps a
// block hash to the height the chain already has it at; anything not in the map
// comes back as not-found, which is how an inv for a block we do not have yet
// behaves. The best block is left at height 100, far below mainnet's last
// checkpoint, so sm.current() is false — the state a node is in during IBD.
func newInvManager(t *testing.T, known map[chainhash.Hash]uint32) *SyncManager {
	t.Helper()

	running := blockchain2.FSMStateRUNNING
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	blockchainClient.Mock.On("GetBestBlockHeader", mock.Anything).
		Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)

	for hash, height := range known {
		h := hash
		blockchainClient.Mock.On("GetBlockHeader", mock.Anything, &h).
			Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: height}, nil)
	}

	blockchainClient.Mock.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("not found"))
	blockchainClient.Mock.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	return sm
}

// registerInvPeer registers a peer with the request queue and tx map the inv
// path dereferences, seeded with the height the peer announced at handshake.
func registerInvPeer(sm *SyncManager, p *peerpkg.Peer, startingHeight int32) *peerSyncState {
	state := &peerSyncState{
		syncCandidate: true,
		requestQueue:  txmap.NewSyncedSlice[wire.InvVect](maxRequestedBlocks),
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	state.noteBestKnownHeight(startingHeight)
	sm.peerStates.Set(p, state)

	return state
}

// invQuietPeriod is how long a test waits before concluding that no getdata is
// coming. Sends are asynchronous, so a negative assertion has to give the peer's
// writer goroutine time to have delivered one.
const invQuietPeriod = 500 * time.Millisecond

// blockInv builds an inv message announcing the given blocks, in order. The
// last one is the "last block" the inv path uses to learn the peer's height.
func blockInv(hashes ...chainhash.Hash) *wire.MsgInv {
	msg := wire.NewMsgInv()

	for i := range hashes {
		_ = msg.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hashes[i]))
	}

	return msg
}

// TestBestKnownHeight_IsMonotone pins that a peer's best known height only ever
// rises. A scheduler asking "which peers claim to have block N" must never see a
// peer's answer go backwards because a late, lower report overwrote a higher one.
func TestBestKnownHeight_IsMonotone(t *testing.T) {
	state := &peerSyncState{}

	require.Zero(t, state.BestKnownHeight(), "a fresh peer state knows of no blocks")

	state.noteBestKnownHeight(100)
	require.Equal(t, int32(100), state.BestKnownHeight())

	state.noteBestKnownHeight(50)
	require.Equal(t, int32(100), state.BestKnownHeight(),
		"a lower report must not lower what we already know the peer has")

	state.noteBestKnownHeight(150)
	require.Equal(t, int32(150), state.BestKnownHeight(), "a higher report must raise it")
}

// TestBestKnownHeight_ConcurrentWritersLeaveTheMaximum drives the three
// goroutines that report a height in a running node — the block-queue consumer,
// the per-message inv handler and the per-message headers handler all share the
// same *peerSyncState by pointer. Concurrency is the subject, so goroutines are
// the point and t.Parallel() is still not used.
func TestBestKnownHeight_ConcurrentWritersLeaveTheMaximum(t *testing.T) {
	const (
		climbers  = 16
		ladder    = 40_000
		topHeight = 1 << 20
	)

	state := &peerSyncState{}

	var wg sync.WaitGroup

	wg.Add(climbers)

	for i := 0; i < climbers; i++ {
		go func() {
			defer wg.Done()

			// Climbers only ever report heights below the top, so a climber that
			// clobbers the top height can never repair it by reporting the top
			// itself later.
			for h := 1; h <= ladder; h++ {
				state.noteBestKnownHeight(int32(h))
			}
		}()
	}

	// Report the top height while the climbers are still going. Any of them
	// part-way through a read-then-write at that instant would store its own
	// lower height on top of it.
	time.Sleep(5 * time.Millisecond)
	state.noteBestKnownHeight(topHeight)

	wg.Wait()

	require.Equal(t, int32(topHeight), state.BestKnownHeight(),
		"concurrent reports must leave exactly the maximum")
}

// TestBestKnownHeight_SeededFromStartingHeight pins that a peer arrives with the
// height it announced at handshake already recorded. Without the seed a
// scheduler would read zero and conclude a freshly connected peer has nothing.
func TestBestKnownHeight_SeededFromStartingHeight(t *testing.T) {
	chainParams := &chaincfg.MainNetParams
	running := blockchain2.FSMStateRUNNING

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := &SyncManager{
		ctx:              context.Background(),
		settings:         test.CreateBaseTestSettings(t),
		logger:           ulogger.TestLogger{},
		chainParams:      chainParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
	}

	// The remote end advertises height 800000 in its version message, which is
	// what the local Peer records as its starting height. Services are left at
	// zero so the peer is not a sync candidate and handleNewPeerMsg does not go
	// on to start a sync.
	remoteCfg := peerpkg.Config{
		Listeners:        peerpkg.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
		NewestBlock: func() (*chainhash.Hash, int32, error) {
			return &chainhash.Hash{}, 800000, nil
		},
	}
	localCfg := peerpkg.Config{
		Listeners:        peerpkg.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}

	remote, local, err := MakeConnectedPeers(t, remoteCfg, localCfg, 60)
	require.NoError(t, err)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	require.Equal(t, int32(800000), local.StartingHeight(), "harness check: the handshake carried the height")

	sm.handleNewPeerMsg(local)

	state, exists := sm.peerStates.Get(local)
	require.True(t, exists, "the peer must be registered")
	require.Equal(t, int32(800000), state.BestKnownHeight(),
		"a newly registered peer must start at the height it announced, not zero")
}

// TestBestKnownHeight_RaisedFromHeaders pins that a batch of headers raises the
// sending peer's best known height to the top of the batch. A peer that hands us
// headers up to 1200 demonstrably has the chain that far.
func TestBestKnownHeight_RaisedFromHeaders(t *testing.T) {
	sm := newHeaderLockManager(t, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 61, 1000)
	state := registerInvPeer(sm, syncPeer, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// Anchor the list at 1190 so a batch of ten headers ends at 1200.
	// resetHeaderState turns headers-first mode off, so turn it back on.
	anchor := chainhash.Hash{0xb0}
	sm.resetHeaderState(&anchor, 1190)
	sm.headersFirstMode.Store(true)

	var nonce uint32

	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	require.Equal(t, int32(1200), state.BestKnownHeight(),
		"a headers batch must raise the sending peer's best known height to the top of the batch")
}

// TestBestKnownHeight_RaisedFromInv pins that an inv from the sync peer for a
// block we already have raises that peer's best known height to the block's
// height, reusing the lookup handleInvMsg already does.
func TestBestKnownHeight_RaisedFromInv(t *testing.T) {
	announced := chainhash.Hash{0xc1}

	sm := newInvManager(t, map[chainhash.Hash]uint32{announced: 900})

	syncPeer, _, _ := connectRacePeer(t, 62, 900)
	state := registerInvPeer(sm, syncPeer, 100)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	sm.handleInvMsg(&invMsg{inv: blockInv(announced), peer: syncPeer})

	require.Equal(t, int32(900), state.BestKnownHeight(),
		"an inv for a block we already have must raise the announcing peer's best known height")
}

// TestHandleInvMsg_NonSyncPeerInvIsStillIgnoredWhileNotCurrent is the anti-hoist
// pin. During IBD we deliberately drop block invs from anyone but the sync peer
// (manager.go, "Ignore invs from peers that aren't the sync if we are not
// current") so we do not go fetching a mass of orphans. Recording peer heights
// must not become a reason to start processing that traffic.
//
// Three assertions, all about that one early return: nothing is asked of the
// peer, the chain is never consulted about the announced hash, and the peer's
// recorded height is untouched.
func TestHandleInvMsg_NonSyncPeerInvIsStillIgnoredWhileNotCurrent(t *testing.T) {
	// The batch announces a block we do not have followed by the peer's tip,
	// which we do. Without the early return the first would be fetched and the
	// second would be looked up for its height, so all three assertions below
	// have something to catch.
	wanted := chainhash.Hash{0xc2}
	tip := chainhash.Hash{0xc4}

	sm := newInvManager(t, map[chainhash.Hash]uint32{tip: 900})

	// Headers-first has its own guard, tested separately. Turning it off here
	// leaves the not-current early return as the only thing standing between
	// this inv and a fetch.
	sm.headersFirstMode.Store(false)

	syncPeer, _, _ := connectRacePeer(t, 63, 900)
	registerInvPeer(sm, syncPeer, 100)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	other, _, otherRec := connectRacePeer(t, 64, 900)
	otherState := registerInvPeer(sm, other, 100)

	require.False(t, sm.current(), "harness check: this test is about the not-current path")

	sm.handleInvMsg(&invMsg{inv: blockInv(wanted, tip), peer: other})

	// A negative assertion on an asynchronous send has to wait: QueueMessage
	// hands the message to the peer's writer goroutine, so an immediate read of
	// the recorder would pass even when a getdata is on its way.
	require.False(t, WaitUntil(func() bool { return otherRec.count() > 0 }, invQuietPeriod),
		"a non-sync peer must not be asked for anything while we are not current")

	blockchainMock, ok := sm.blockchainClient.(*blockchain2.Mock)
	require.True(t, ok)
	blockchainMock.AssertNotCalled(t, "GetBlockHeader", mock.Anything, &tip)

	require.Equal(t, int32(100), otherState.BestKnownHeight(),
		"learning heights must not be hoisted above the early return that protects IBD")
}

// TestHandleInvMsg_HeadersFirstInvIsIgnored pins the second early return: while
// headers-first sync is driving the download, a block inv is noted as known
// inventory and nothing else. Requesting it here would cut across the header
// list's own ordering.
func TestHandleInvMsg_HeadersFirstInvIsIgnored(t *testing.T) {
	// Not in the known map, so we do not have it — without the headers-first
	// guard this inv would be queued and a getdata sent.
	announced := chainhash.Hash{0xc3}

	sm := newInvManager(t, nil)

	syncPeer, _, syncRec := connectRacePeer(t, 65, 900)
	registerInvPeer(sm, syncPeer, 100)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	require.True(t, sm.headersFirstMode.Load(), "harness check: this test is about headers-first mode")

	sm.handleInvMsg(&invMsg{inv: blockInv(announced), peer: syncPeer})

	require.False(t, WaitUntil(func() bool { return syncRec.count() > 0 }, invQuietPeriod),
		"no getdata must go out for an inv received during headers-first sync")
}

// TestHandleInvMsg_AFullLedgerHoldsTheInvInsteadOfDroppingIt pins the work item.
//
// A block the ledger will not take is a block we must not ask for, or the reply
// arrives with nothing vouching for it and costs an honest peer its connection.
// That part was right. What was wrong was where the block went: the inv had
// already been shifted off the request queue before the ledger was consulted, so
// breaking out of the loop discarded it. The comment relied on the peer
// announcing it again, and a one-shot inv — the only kind there is past the final
// checkpoint, where there are no more headers rounds — never comes again.
//
// The header walk gets this right by leaving its cursor on the block it could
// not place. The inv queue has to do the same.
func TestHandleInvMsg_AFullLedgerHoldsTheInvInsteadOfDroppingIt(t *testing.T) {
	announced := chainhash.Hash{0xc9}

	// Not in the chain, so it is a block the node would want to fetch.
	sm := newInvManager(t, nil)

	syncPeer, _, rec := connectRacePeer(t, 66, 900)
	registerInvPeer(sm, syncPeer, 100)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// Past the final checkpoint: headers-first mode is off, so an inv is the
	// only thing that will ever tell us about this block. That is precisely the
	// case where discarding the work item cannot be recovered from.
	sm.headersFirstMode.Store(false)

	// Fill the ledger so no new hash can be taken.
	filler := newTestPeer(t, "localhost:18444")

	for i := 0; i < maxTrackedBlockDownloads; i++ {
		var h chainhash.Hash

		binary.LittleEndian.PutUint32(h[:4], uint32(i))
		h[31] = 0xfd

		require.True(t, sm.blockDownloads.Add(filler, h))
	}

	state, exists := sm.peerStates.Get(syncPeer)
	require.True(t, exists)

	sm.handleInvMsg(&invMsg{inv: blockInv(announced), peer: syncPeer})

	require.Zero(t, rec.count(),
		"a block the ledger cannot vouch for must not be asked of anybody")

	require.Equal(t, 1, state.requestQueue.Length(),
		"the announcement is the only record that this block exists, so it must stay queued until there is room")

	held, ok := state.requestQueue.Get(0)
	require.True(t, ok)
	require.Equal(t, announced, held.Hash, "the held item must be the block that could not be placed")
}
