package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// placeCursorOn puts the download cursor on the header carrying hash, which is
// how a test reaches a state a live node walks itself into over many passes.
//
// headerMu is taken and released here, never held across a call into the walk:
// snapshotHeaderCandidates takes the same lock itself, so a test still holding
// it deadlocks instead of failing.
func placeCursorOn(t *testing.T, sm *SyncManager, hash chainhash.Hash) {
	t.Helper()

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	element, indexed := sm.headerIndex[hash]
	require.True(t, indexed, "the header the cursor is being placed on must be in the list")

	sm.startHeader = element
}

// runWalkWithin runs one pass of the header walk and fails the test if it does
// not return in time, rather than hanging the whole package. A walk that cannot
// terminate is worse than the stall these changes are here to cure: it holds a
// CPU and keeps re-taking headerMu in front of the block-queue consumer, which
// is the narrowest goroutine in the service.
func runWalkWithin(t *testing.T, sm *SyncManager, limit time.Duration) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)

		sm.fetchHeaderBlocks()
	}()

	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("the header walk did not finish within %s, which means the round loop is not terminating", limit)
	}
}

// TestHeaderWalk_RecoversACursorStrandedAboveTheLookaheadCeiling is the stall
// this change exists to cure, and it is a stall with no way out of its own.
//
// The walk is forward-only. commitHeaderCandidates advances startHeader past
// every header it considers, and the only things that move it back are the drop
// paths, which fire when a block is delivered and then discarded. A block that
// is requested and simply never arrives fires none of them, and the download
// ledger holds it for an hour before expiring it.
//
// So the cursor can come to rest above the read-ahead ceiling, and once it has,
// nothing recovers it: the ceiling is anchored to the last COMMITTED block, the
// cursor is above it, so the walk breaks on its first header and asks for
// nothing; nothing is asked for, so nothing arrives; nothing arrives, so nothing
// commits; nothing commits, so the anchor never rises to reach the cursor.
//
// Measured on mainnet on 2026-09-12 at height 11238: park empty, download window
// empty, nothing in flight, the block loop idle for over three minutes, and
// 955,208 headers queued running to height 966,445. A node with work in front of
// it, requesting none of it.
func TestHeaderWalk_RecoversACursorStrandedAboveTheLookaheadCeiling(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe1}
	msg, hashes := linkedHeaders(anchor, 12, &nonce)

	// The anchor block is the one the header list is hung off, so this node
	// holds it already and the walk must not ask any peer for it. Everything
	// else comes back not-found, and is therefore wanted.
	sm := newFetchLockManager(t, []chainhash.Hash{anchor}, nil, nil)

	// Budgets are left wide so the read-ahead ceiling is the only bound that can
	// bind, which is what makes the stranding reproducible at all.
	sm.settings.Legacy.BlockDownloadLowerWindow = 4

	syncPeer, syncRec := schedulerPeer(t, sm, 91, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	// seedFetchHeaders anchors the list at height 10, so the seeded headers run
	// from height 11 and a lower window of 4 puts the ceiling at height 14.
	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// Height 17, three clear of the ceiling: the position an earlier pass walked
	// the cursor to while the ceiling was anchored higher up.
	placeCursorOn(t, sm, hashes[6])

	require.Zero(t, sm.blockDownloads.Len(),
		"the stall had an empty download window, so nothing may be in flight here either")

	runWalkWithin(t, sm, 30*time.Second)

	require.True(t, WaitUntil(func() bool { return syncRec.count() >= 4 }, 5*time.Second),
		"a node with headers in front of it and nothing in flight has to ask for something")

	require.Equal(t, hashes[0:4], syncRec.all(),
		"the walk must restart at the front, which is the lowest block still wanted, and stop at the ceiling")

	cursor, onHeader := startHeaderHash(t, sm)
	require.True(t, onHeader, "the walk must still have somewhere to resume from")
	require.Equal(t, hashes[4], cursor,
		"and must come to rest on the first header the ceiling refused, not back where it was stranded")
}

// TestHeaderWalk_TerminatesWhenEveryCandidateIsAlreadyHeld is the no-loop proof
// for the ordinary shape of a wasted pass: every header the walk reaches names a
// block this node already has.
//
// The assertion that carries it is the lookup count. Each candidate costs one
// "do we already have this?" question, which is a gRPC round trip to the
// blockchain service, so one question per header is proof that the round loop
// walked the list once and stopped — a loop that re-walked would show as a count
// that keeps climbing, and one that never terminated would not reach the
// assertion at all.
func TestHeaderWalk_TerminatesWhenEveryCandidateIsAlreadyHeld(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe2}
	msg, hashes := linkedHeaders(anchor, 12, &nonce)

	// Every seeded block, and the anchor the list hangs off, answered as one
	// this node holds. There is nothing for this pass to ask anybody for.
	held := append([]chainhash.Hash{anchor}, hashes...)

	sm := newFetchLockManager(t, held, nil, nil)

	syncPeer, syncRec := schedulerPeer(t, sm, 92, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	runWalkWithin(t, sm, 30*time.Second)

	require.Zero(t, syncRec.count(), "a block this node already holds must not be asked for")
	require.Zero(t, sm.blockDownloads.Len(), "and must not be recorded as owed by anybody")

	// len(hashes), not len(held): the cursor starts on the first SEEDED header,
	// so the anchor the list hangs off is behind the walk and never asked about.
	require.Equal(t, len(hashes), blockHeaderLookups(t, sm),
		"every header must be asked about exactly once: a second question about the same header is a round that re-walked ground it had already covered")

	_, onHeader := startHeaderHash(t, sm)
	require.False(t, onHeader,
		"the walk ran off the end of the list, which is what re-enables the getblocks fallback")
}

// TestHeaderWalk_TerminatesWhenEveryCandidateIsAlreadyInFlight is the no-loop
// proof for the shape that actually spun, and it is the one the round loop could
// not survive before this change.
//
// A block another peer already owes is stepped over rather than asked for twice,
// and the cursor PINS in front of it so the next pass comes back to it. When the
// first candidate is such a block the pass ends with the cursor exactly where it
// started — and the next round of the same pass would then snapshot the same
// hashes, ask the same questions, and refuse the same headers. It could place no
// request either, so assigner.remaining never fell and nothing was left to stop
// the loop.
//
// The four blocks are owed by a peer that takes no part in the pass, so the only
// budget they can touch is the node-wide window, which is far above four.
func TestHeaderWalk_TerminatesWhenEveryCandidateIsAlreadyInFlight(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe3}
	msg, hashes := linkedHeaders(anchor, 12, &nonce)

	sm := newFetchLockManager(t, []chainhash.Hash{anchor}, nil, nil)

	// The ceiling at height 14 makes the four blocks below it the whole of what
	// this pass may consider, and all four are already in flight.
	sm.settings.Legacy.BlockDownloadLowerWindow = 4

	syncPeer, syncRec := schedulerPeer(t, sm, 93, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	elsewhere, _, _ := connectRacePeer(t, 94, 1000)
	for i := 0; i < 4; i++ {
		require.True(t, sm.blockDownloads.Add(elsewhere, hashes[i]),
			"the ledger has to take the record, or the block is not in flight for the walk's purposes")
	}

	runWalkWithin(t, sm, 30*time.Second)

	require.Zero(t, syncRec.count(),
		"a block a live peer already owes must not be asked for a second time: the second copy arrives unowned and costs an honest peer its connection")

	require.Equal(t, 4, blockHeaderLookups(t, sm),
		"the four candidates must be asked about once between them: a fifth question means a second round re-walked the same headers")

	cursor, onHeader := startHeaderHash(t, sm)
	require.True(t, onHeader, "the cursor must stay in the list")
	require.Equal(t, hashes[0], cursor,
		"and must stay pinned in front of the lowest block nobody has delivered, which is what brings the walk back to it")
}
