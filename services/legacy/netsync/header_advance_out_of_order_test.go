package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// buildHeaderRound puts a round of n+1 linked headers into the list and returns
// the n that are eligible for removal.
//
// The checkpoint is the LAST header of the round, which matters: reaching the
// checkpoint is what makes handleHeadersMsg trim the round's anchor out of the
// list. Without that the anchor stays at the front, nothing ever matches the
// front, and the frontier is never published at all — publishFrontierLocked
// clears it whenever the front is an anchor. A harness built the other way
// looks plausible and reproduces none of the behaviour under test, which is how
// the first version of this file went wrong.
//
// The returned slice excludes the checkpoint header, which is deliberately
// never removed because the next round links to it.
func buildHeaderRound(t *testing.T, n int) (*SyncManager, []chainhash.Hash) {
	t.Helper()

	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 91, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xb1}
	msg, round := linkedHeaders(anchor, n+1, &nonce)
	checkpoint := round[len(round)-1]

	sm.resetHeaderState(&anchor, 10)
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: int32(10 + n + 1), Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	require.False(t, sm.anchorIsStillTheFront(), "the round reaches the checkpoint, so its anchor is trimmed")

	return sm, round[:n]
}

// anchorIsStillTheFront is anchorIsStillTheFrontLocked with the lock taken, for
// tests that check the harness is in the shape they assume.
func (sm *SyncManager) anchorIsStillTheFront() bool {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	return sm.anchorIsStillTheFrontLocked()
}

func (sm *SyncManager) headerIsInTheList(h chainhash.Hash) bool {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	return sm.headerIndex[h] != nil
}

func (sm *SyncManager) frontierHashForTest() chainhash.Hash {
	sm.frontierMu.Lock()
	defer sm.frontierMu.Unlock()

	return sm.frontierHash
}

func (sm *SyncManager) frontHash(t *testing.T) chainhash.Hash {
	t.Helper()

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	front := sm.headerList.Front()
	require.NotNil(t, front, "the list should not be empty")

	node, ok := front.Value.(*headerNode)
	require.True(t, ok)

	return *node.hash
}

// TestSyncManager_ACommittedBlockLeavesTheHeaderListWhateverTheOrder is the
// race that jammed mainnet for twenty-eight minutes on 2026-09-09.
//
// Two paths commit a parked block, and they advance the header list in opposite
// order. The sweep commits the block and advances afterwards
// (block_park_drain.go, in commitParkedEntry). The dispatcher advances first and
// commits afterwards (block_park_drain.go, in the dispatch turn). They are
// racing claimants for the same park, which that file says in as many words.
//
// Advancing used to remove a header only when the arriving hash matched the
// FRONT of the list. So when the dispatcher claimed block N+1 while block N was
// still mid-commit and therefore still the front, N+1's advance matched nothing
// and did nothing. N's commit then returned, its advance removed it, and N+1 was
// left at the front of the list as a block already in the chain, with nobody
// left who would ever advance for it again.
//
// What that cost on mainnet: the frontier is published from the front, so it
// named a block already committed; the frontier race asked seven peers for it
// over twenty-eight minutes; and every time one of those peers was lost the
// cursor rewind found the hash still in the header index and wound the whole
// download back to it. The node ran dry for eight minutes and forty-four
// seconds with an empty pipeline and nothing on disk. It recovered only when a
// duplicate copy of the block was finally delivered and matched the front.
func TestSyncManager_ACommittedBlockLeavesTheHeaderListWhateverTheOrder(t *testing.T) {
	sm, round := buildHeaderRound(t, 4)

	first, second := round[0], round[1]

	require.True(t, sm.headerIsInTheList(first), "the round should be in the list")
	require.True(t, sm.headerIsInTheList(second))
	require.Equal(t, first, sm.frontHash(t), "the list runs in ascending height")

	// The dispatcher claims the SECOND block and advances for it while the
	// first is still mid-commit, so the first is still the front.
	sm.advanceHeaderListFor(second)

	// The first block's commit now returns and its own advance fires.
	sm.advanceHeaderListFor(first)

	require.False(t, sm.headerIsInTheList(first), "the first block committed, so its header must be gone")
	require.False(t, sm.headerIsInTheList(second),
		"the second block committed too, so its header must be gone; leaving it makes the frontier name a block already in the chain and lets the cursor rewind onto it")

	require.Equal(t, round[2], sm.frontHash(t),
		"the front should be the oldest block still actually wanted")
}

// TestSyncManager_RemovingAMiddleHeaderDoesNotMoveTheFrontier checks the other
// half. Taking a header out of the middle changes nothing the front-facing
// machinery depends on: the block at the front is still the one holding sync
// up, so the frontier must not move.
func TestSyncManager_RemovingAMiddleHeaderDoesNotMoveTheFrontier(t *testing.T) {
	sm, round := buildHeaderRound(t, 4)

	before := sm.frontierHashForTest()
	require.Equal(t, round[0], before, "the frontier starts on the front block")

	sm.advanceHeaderListFor(round[2])

	require.False(t, sm.headerIsInTheList(round[2]))
	require.Equal(t, before, sm.frontierHashForTest(),
		"removing a header from the middle must leave the frontier on the block still missing")
	require.Equal(t, round[0], sm.frontHash(t))
}

// TestSyncManager_RemovingTheCursorsHeaderDoesNotOrphanTheWalk covers the
// hazard that only appears once a header can be removed from the middle.
//
// startHeader is a pointer to a list element, and the walk asks for blocks from
// it forwards. Removing the element it points at would leave it detached: its
// Next() is nil on a removed element, so the walk would stop asking for
// anything and sync would wedge silently. This cannot happen while only the
// front is ever removed, because the front is always behind the cursor.
func TestSyncManager_RemovingTheCursorsHeaderDoesNotOrphanTheWalk(t *testing.T) {
	sm, round := buildHeaderRound(t, 4)

	// Put the cursor on the third block, then commit that block out of order.
	sm.headerMu.Lock()
	e := sm.headerIndex[round[2]]
	require.NotNil(t, e)
	sm.startHeader = e
	sm.headerMu.Unlock()

	sm.advanceHeaderListFor(round[2])

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	require.NotNil(t, sm.startHeader, "the walk must still have a cursor")

	node, ok := sm.startHeader.Value.(*headerNode)
	require.True(t, ok)
	require.Equal(t, round[3], *node.hash,
		"the cursor must move to the next header, not dangle on a removed element")

	require.NotNil(t, sm.headerList.Front(), "and the list must still be walkable")
}

// TestSyncManager_TheCheckpointNodeStaysWhereverItSits pins the one header that
// must NOT be removed. The next round of headers links to it, so taking it out
// loses the anchor.
func TestSyncManager_TheCheckpointNodeStaysWhereverItSits(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 92, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xc1}
	msg, round := linkedHeaders(anchor, 4, &nonce)
	checkpoint := round[len(round)-1]

	sm.resetHeaderState(&anchor, 10)
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 14, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	for _, h := range round[:len(round)-1] {
		sm.advanceHeaderListFor(h)
	}

	isCheckpoint, _ := sm.advanceHeaderListFor(checkpoint)
	require.True(t, isCheckpoint, "the checkpoint block must report itself")
	require.True(t, sm.headerIsInTheList(checkpoint),
		"the checkpoint node anchors the next round and must stay in the list")
}
