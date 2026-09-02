package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// fillLedger records `n` distinct made-up blocks against p, so the ledger can be
// driven up to its size cap without a live network. The hashes only ever set
// bytes 0-2, so a caller can keep its own hashes clear of them by marking a
// later byte.
func fillLedger(tr *blockDownloadTracker, p *peerpkg.Peer, n int) {
	for i := 0; i < n; i++ {
		h := chainhash.Hash{}
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		h[2] = byte(i >> 16)

		tr.Add(p, h)
	}
}

// TestBlockDownloadTracker_FloodMustNotDisplaceTheBlockWeAreWaitingOn pins which
// side of the size cap gives way. The block we have waited longest for is the
// frontier — the one every other header is queued behind — and it is by
// definition the oldest assignment in the ledger. Dropping its record while it
// is still on the way turns its arrival into an unrequested block, which costs
// the peer that answered us its connection and leaves sync waiting on a peer we
// just threw away.
//
// So a burst of announcements has to be absorbed by refusing the burst, never by
// forgetting work already in progress.
func TestBlockDownloadTracker_FloodMustNotDisplaceTheBlockWeAreWaitingOn(t *testing.T) {
	tr, advance := newTestTracker(blockRequestAssignmentTTL)

	p := newTestPeer(t, "localhost:18431")

	// The filler hashes only ever set bytes 0-2, so marking the last byte keeps
	// the frontier distinct from all of them.
	frontier := chainhash.Hash{}
	frontier[31] = 0xfe
	tr.Add(p, frontier)

	advance(time.Second)

	// Enough to take the ledger past its cap, with the last one arriving when
	// there is certainly no room left.
	fillLedger(tr, p, maxTrackedBlockDownloads)

	overflow := chainhash.Hash{}
	overflow[31] = 0xfd
	tr.Add(p, overflow)

	require.True(t, tr.HasOwner(p, frontier),
		"the block we have waited longest for must survive a burst of newer announcements")
	require.False(t, tr.HasOwner(p, overflow),
		"an announcement arriving at a full ledger must be refused, not admitted at the frontier's expense")
	require.LessOrEqual(t, tr.Len(), maxTrackedBlockDownloads,
		"the ledger must stay within its cap")
}

// TestHandleBlockMsg_FloodMustNotCostTheFrontierPeerItsConnection is the same
// defect at the end of the path it damages. A peer we asked for the frontier
// block delivers it, exactly as asked, after a burst of announcements has filled
// the ledger. Whether its record survived the burst is the whole question: the
// disconnect decision reads that record, and a peer whose record is gone is
// treated as having sent a block nobody wanted.
func TestHandleBlockMsg_FloodMustNotCostTheFrontierPeerItsConnection(t *testing.T) {
	running := blockchain2.FSMStateRUNNING
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.Mock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = blockchainClient

	asked, _, _ := connectRacePeer(t, 60, 1000)
	registerRacePeer(sm, asked)

	frontier := chainhash.Hash{}
	frontier[31] = 0xfe
	sm.blockDownloads.Add(asked, frontier)

	// A second peer announces enough blocks to take the ledger past its cap.
	flooder, _, _ := connectRacePeer(t, 61, 1000)
	registerRacePeer(sm, flooder)
	fillLedger(sm.blockDownloads, flooder, maxTrackedBlockDownloads+1)

	require.True(t, sm.blockDownloads.HasOwner(asked, frontier),
		"the ledger must still vouch for the block we asked this peer for")

	// Carrying no block makes handleBlockMsg bail straight after the check under
	// test, so the error only tells us whether it got past the disconnect.
	err := sm.handleBlockMsg(&blockQueueMsg{blockHash: frontier, peer: asked})
	require.Error(t, err)

	require.False(t, WaitUntil(func() bool { return !asked.Connected() }, time.Second),
		"a peer delivering the block we asked it for must not lose its connection to somebody else's announcement burst")
}

// TestBlockDownloadTracker_FullLedgerStillTakesAnotherOwnerForABlockItHas pins
// the one thing a full ledger must never turn away. The frontier race asks a
// second peer for the block sync is already stuck on, which adds an owner to a
// block the ledger is holding rather than a block to the ledger — so the size
// cap has nothing to protect against and the race must go ahead. Refusing it
// would leave the recovery for a stall unavailable exactly when the ledger is
// under pressure, which is when stalls happen.
func TestBlockDownloadTracker_FullLedgerStillTakesAnotherOwnerForABlockItHas(t *testing.T) {
	tr, _ := newTestTracker(blockRequestAssignmentTTL)

	first := newTestPeer(t, "localhost:18432")
	racer := newTestPeer(t, "localhost:18433")

	frontier := chainhash.Hash{}
	frontier[31] = 0xfe
	require.True(t, tr.Add(first, frontier))

	fillLedger(tr, first, maxTrackedBlockDownloads)

	newBlock := chainhash.Hash{}
	newBlock[31] = 0xfd
	require.False(t, tr.Add(first, newBlock), "sanity: the ledger is full and refuses a block it does not know")

	require.True(t, tr.Add(racer, frontier), "a second owner for a block already tracked adds no block, so the cap cannot refuse it")
	require.True(t, tr.HasOwner(racer, frontier))
	require.True(t, tr.HasOwner(first, frontier), "and the peer we asked first keeps its record")
}

// TestBlockDownloadTracker_ExpiryMakesRoomBeforeARequestIsTurnedAway pins the
// order the cap is applied in. Aged-out assignments are records of blocks whose
// hour is up and whose copies are no longer welcome, so throwing them away costs
// nothing — and it must be tried before a live request is refused, or a ledger
// full of dead entries would stop the node fetching blocks it has every right to
// ask for.
func TestBlockDownloadTracker_ExpiryMakesRoomBeforeARequestIsTurnedAway(t *testing.T) {
	tr, advance := newTestTracker(blockRequestAssignmentTTL)

	p := newTestPeer(t, "localhost:18434")

	fillLedger(tr, p, maxTrackedBlockDownloads)

	blocked := chainhash.Hash{}
	blocked[31] = 0xfe
	require.False(t, tr.Add(p, blocked), "a full ledger of live assignments refuses a new block")

	// Every filler assignment is now past the ownership ceiling, so none of them
	// is owed any more.
	advance(blockRequestAssignmentTTL)

	admitted := chainhash.Hash{}
	admitted[31] = 0xfd
	require.True(t, tr.Add(p, admitted), "expiry must make room before a request is turned away")
	require.True(t, tr.HasOwner(p, admitted))
}

// TestFetchHeaderBlocks_NeverAsksForABlockTheLedgerWillNotTrack follows the
// refusal out to the wire, which is the only place it means anything. The ledger
// saying "no room" is not a fix on its own: if the getdata goes out regardless,
// the block comes back with nothing vouching for it and the peer that answered
// is disconnected — the exact harm the cap was supposed to avoid. So the request
// must not be sent, and the header must stay queued so it is asked for again
// once there is room.
func TestFetchHeaderBlocks_NeverAsksForABlockTheLedgerWillNotTrack(t *testing.T) {
	sm := newHeaderLockManager(t, nil, nil)

	syncPeer, _, rec := connectRacePeer(t, 62, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	anchor := chainhash.Hash{}
	anchor[31] = 0xa0
	sm.resetHeaderState(&anchor, 10)
	sm.headersFirstMode.Store(true)

	var nonce uint32

	headers, hashes := linkedHeaders(anchor, 4, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: headers, peer: syncPeer})

	// Somebody else's announcements have taken every slot the ledger has.
	flooder, _, _ := connectRacePeer(t, 63, 1000)
	registerRacePeer(sm, flooder)
	fillLedger(sm.blockDownloads, flooder, maxTrackedBlockDownloads)

	sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return rec.count() > 0 }, 500*time.Millisecond),
		"a full ledger must hold the request back rather than ask for a block it cannot vouch for")

	for _, h := range hashes {
		require.False(t, sm.blockDownloads.HasOwner(syncPeer, h), "sanity: nothing was recorded either")
	}

	// The flooder goes away, which releases everything it was owed, and the
	// headers we held off on are asked for on the next pass — they were never
	// skipped, only postponed.
	sm.blockDownloads.ClearPeer(flooder)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() == len(hashes) }, 5*time.Second),
		"once there is room the held-back headers must be requested, not lost")

	for _, h := range rec.all() {
		require.True(t, sm.blockDownloads.HasOwner(syncPeer, h),
			"every block that goes out on the wire must be one the ledger vouches for")
	}
}
