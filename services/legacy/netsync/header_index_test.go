package netsync

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// headerIndexSnapshot copies the index under headerMu so a test can assert on
// it without racing the manager's own goroutines.
func headerIndexSnapshot(sm *SyncManager) map[chainhash.Hash]*list.Element {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	out := make(map[chainhash.Hash]*list.Element, len(sm.headerIndex))
	for k, v := range sm.headerIndex {
		out[k] = v
	}

	return out
}

// requireIndexMatchesList is the invariant the index exists to hold: exactly one
// live entry per distinct hash in the list, and every entry resolving to the
// element that actually holds that hash. Walking the list and comparing element
// pointers is what catches a detached entry left behind by a missed maintenance
// point — a length check alone would not.
func requireIndexMatchesList(t *testing.T, sm *SyncManager, where string) {
	t.Helper()

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	live := make(map[chainhash.Hash]*list.Element)

	for e := sm.headerList.Front(); e != nil; e = e.Next() {
		node, ok := e.Value.(*headerNode)
		require.True(t, ok, "%s: list holds something that is not a headerNode", where)
		require.NotNil(t, node.hash, "%s: list holds a headerNode with no hash", where)

		// Last write wins, matching indexHeaderLocked.
		live[*node.hash] = e
	}

	require.Len(t, sm.headerIndex, len(live),
		"%s: the index must hold exactly one entry per distinct hash in the list", where)

	for hash, want := range live {
		got, ok := sm.headerIndex[hash]
		require.True(t, ok, "%s: %s is in the list but not in the index", where, hash)
		require.True(t, got == want, "%s: the index entry for %s points at a different element", where, hash)
	}
}

// seedHeaderIndexManager builds a manager in headers-first mode with an anchor
// in the header list, and returns it.
func seedHeaderIndexManager(t *testing.T, peerID uint8, anchor chainhash.Hash) *SyncManager {
	t.Helper()

	sm := newHeaderLockManager(t, nil, nil)

	p, _, _ := connectRacePeer(t, peerID, 1000)
	registerRacePeer(sm, p)
	sm.storeSyncPeer(p, &syncPeerState{})

	sm.resetHeaderState(&anchor, 10)
	// resetHeaderState turns headers-first mode off; these tests are about the
	// headers-first paths, so turn it back on.
	sm.headersFirstMode.Store(true)

	return sm
}

// TestHeaderIndex_ClearedOnFinalCheckpointInit is the site the design first
// missed. When the last checkpoint is reached, handleBlockMsg leaves
// headers-first mode and wipes the header list. An index that is not wiped with
// it keeps resolving every hash from the last checkpoint interval — up to about
// 43,000 entries on mainnet — to elements that are no longer in any list. That
// is both a permanent leak and a correctness bug, because a later lookup hands
// back a detached element.
func TestHeaderIndex_ClearedOnFinalCheckpointInit(t *testing.T) {
	anchor := chainhash.Hash{0xa1}
	sm := seedHeaderIndexManager(t, 40, anchor)

	peer := sm.loadSyncPeer()

	var nonce uint32

	msg, hashes := linkedHeaders(anchor, 5, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: peer})

	require.Equal(t, 6, sm.headerListLen(), "the anchor plus five headers")
	require.Len(t, headerIndexSnapshot(sm), 6, "every header in the list must be indexed")
	requireIndexMatchesList(t, sm, "after the headers arrived")

	// The final-checkpoint transition: leave headers-first mode and wipe the
	// list. This is the one-line body of manager.go's "reached the final
	// checkpoint" branch.
	sm.leaveHeadersFirstMode()

	require.Zero(t, sm.headerListLen(), "the header list must be empty after leaving headers-first mode")
	require.Empty(t, headerIndexSnapshot(sm),
		"the index must be emptied with the list, or it strands a whole checkpoint interval of entries")

	for _, h := range hashes {
		require.Nil(t, sm.headerElement(h), "%s must not resolve to a detached element", h)
	}

	require.Nil(t, sm.headerElement(anchor), "the anchor must not resolve to a detached element")
}

// TestHeaderIndex_MatchesTheListAfterEveryOperation walks the invariant through
// each of the ways the list changes in a running node: headers pushed, the front
// removed as a block arrives, the front removed on the checkpoint branch, a
// reset, and the final-checkpoint wipe.
func TestHeaderIndex_MatchesTheListAfterEveryOperation(t *testing.T) {
	anchor := chainhash.Hash{0xa2}
	sm := seedHeaderIndexManager(t, 41, anchor)
	peer := sm.loadSyncPeer()

	requireIndexMatchesList(t, sm, "after the reset that seeded the anchor")

	// Push: a batch of well-linked headers.
	var nonce uint32

	msg, hashes := linkedHeaders(anchor, 5, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: peer})
	requireIndexMatchesList(t, sm, "after a headers batch was pushed")

	// Front removal: the block at the front of the list arrives. The message
	// carries no block, so handleBlockMsg returns straight after the header-list
	// bookkeeping that is under test.
	sm.blockDownloads.Add(peer, anchor)
	_ = sm.handleBlockMsg(&blockQueueMsg{blockHash: anchor, peer: peer})
	requireIndexMatchesList(t, sm, "after the front block arrived")

	require.Nil(t, sm.headerElement(anchor), "the removed front must be gone from the index")
	require.NotNil(t, sm.headerElement(hashes[0]), "the rest of the list must still be indexed")

	// Checkpoint branch: a batch whose last header is the next checkpoint makes
	// handleHeadersMsg drop the front anchor before fetching blocks.
	tip := hashes[len(hashes)-1]

	msg2, hashes2 := linkedHeaders(tip, 3, &nonce)

	sm.headerMu.Lock()
	// The anchor sat at height 10, so the five headers ran 11..15 and this batch
	// runs 16..18. Aim the checkpoint at the last of them.
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 18, Hash: &hashes2[2]}
	sm.headerMu.Unlock()

	sm.handleHeadersMsg(&headersMsg{headers: msg2, peer: peer})
	requireIndexMatchesList(t, sm, "after the checkpoint batch dropped the front")

	// Reset: only the new anchor survives.
	newAnchor := chainhash.Hash{0xa3}
	sm.resetHeaderState(&newAnchor, 20)
	requireIndexMatchesList(t, sm, "after a reset")

	// Final-checkpoint wipe.
	sm.leaveHeadersFirstMode()
	requireIndexMatchesList(t, sm, "after leaving headers-first mode")
}

// TestHeaderIndex_ResetLeavesOnlyTheAnchor pins that a reset drops every stale
// entry rather than adding the new anchor on top of the old index.
func TestHeaderIndex_ResetLeavesOnlyTheAnchor(t *testing.T) {
	anchor := chainhash.Hash{0xa4}
	sm := seedHeaderIndexManager(t, 42, anchor)
	peer := sm.loadSyncPeer()

	var nonce uint32

	msg, hashes := linkedHeaders(anchor, 4, &nonce)
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: peer})
	require.Equal(t, 5, sm.headerListLen())

	newAnchor := chainhash.Hash{0xa5}
	sm.resetHeaderState(&newAnchor, 50)

	require.Equal(t, 1, sm.headerListLen(), "only the new anchor should remain in the list")
	require.Len(t, headerIndexSnapshot(sm), 1, "only the new anchor should remain in the index")
	require.NotNil(t, sm.headerElement(newAnchor), "the new anchor must be indexed")

	for _, h := range hashes {
		require.Nil(t, sm.headerElement(h), "%s must have been dropped by the reset", h)
	}
}

// TestHeaderIndex_DuplicateHashDoesNotEvictTheLiveEntry pins the removal guard.
// A list tolerates the same hash twice; a map cannot. Indexing is last-write-wins,
// so removing the older of two elements with the same hash must not delete the
// entry that points at the newer one still in the list.
func TestHeaderIndex_DuplicateHashDoesNotEvictTheLiveEntry(t *testing.T) {
	anchor := chainhash.Hash{0xa6}
	sm := seedHeaderIndexManager(t, 43, anchor)

	dup := chainhash.Hash{0xdd}

	sm.headerMu.Lock()
	first := sm.headerList.PushBack(&headerNode{height: 11, hash: &dup})
	sm.indexHeaderLocked(first, dup)
	second := sm.headerList.PushBack(&headerNode{height: 12, hash: &dup})
	sm.indexHeaderLocked(second, dup)
	sm.headerMu.Unlock()

	require.True(t, sm.headerElement(dup) == second, "the later push must own the index entry")

	// Remove the older duplicate, exactly as the front-removal path does.
	sm.headerMu.Lock()
	sm.unindexHeaderLocked(first, dup)
	sm.headerList.Remove(first)
	sm.headerMu.Unlock()

	require.True(t, sm.headerElement(dup) == second,
		"removing a stale duplicate must not evict the entry for the element still in the list")
}
