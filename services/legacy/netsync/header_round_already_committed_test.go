package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_AHeadersRoundDoesNotReaddBlocksTheChainAlreadyHas is the
// twenty-eight-minute stall on Hetzner mainnet, 2026-09-09.
//
// The chain runs AHEAD of the header list, routinely. Blocks arrive out of order,
// park on disk, and commit from there when their parent lands, so the committed
// height can be dozens of blocks past the last header the list ever held. Nothing
// is wrong with that; it is what the park is for.
//
// What goes wrong is the next round of headers. A peer answers with headers from
// wherever the locator pointed, which is behind the chain, and every one that
// links onto the back of the list is pushed in and indexed with no check against
// what the node already has. The front of the list then becomes a block already
// in the chain.
//
// Everything downstream reads the list as "blocks we still need". The frontier is
// published from the front, so it names a committed block; the frontier race asks
// peer after peer for it; and rewindToLowestHeader finds it in the header index,
// so losing any of those peers winds the whole download back to a height the
// chain passed long ago.
//
// Observed: the list drained to "no frontier is published" at 17:30:19, a peer
// delivered 38,602 headers at 17:30:58, and at 17:30:59 the frontier named block
// 761392, which had committed at 17:23:28. It was then asked for five times over
// the following eight minutes while the tip ran on to 761531.
func TestSyncManager_AHeadersRoundDoesNotReaddBlocksTheChainAlreadyHas(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 93, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	// The list holds only its anchor, the block the node had when the round was
	// asked for. That is the state the log shows: no frontier published, nothing
	// holding up commits.
	anchor := chainhash.Hash{0xd1}
	sm.resetHeaderState(&anchor, 10)

	msg, round := linkedHeaders(anchor, 8, &nonce)
	checkpoint := round[len(round)-1]

	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 18, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	// The park has committed five blocks past the anchor while the round was in
	// flight, so heights 11 to 15 are in the chain already. Only 16 onwards is
	// still wanted.
	const committed = 15

	sm.noteCommittedHeight(committed)

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	// round[0] is height 11, so round[committed-11] is the last committed one and
	// round[committed-10] is the first block still needed.
	firstStillWanted := round[committed-10]

	front := sm.frontHash(t)
	require.Equal(t, firstStillWanted, front,
		"the front of the header list must be the oldest block still WANTED; a block the chain already has makes the frontier name it and lets the cursor rewind onto it")

	require.Equal(t, firstStillWanted, sm.frontierHashForTest(),
		"and the frontier, which is published from that front, must name it too")

	for i := 0; i <= committed-11; i++ {
		require.False(t, sm.headerIsInTheList(round[i]),
			"header %d is for a block already in the chain and must not be in the list, or rewindToLowestHeader can wind the download back onto it", 11+i)
	}
}

// TestSyncManager_AHeadersRoundKeepsEverythingStillWanted is the other half. The
// filter must drop only what the chain has; dropping a header the node still
// needs would lose the block for the rest of the round, because a fresh
// getheaders is built from the list's back and cannot refill a hole below it.
func TestSyncManager_AHeadersRoundKeepsEverythingStillWanted(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 94, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xd2}
	sm.resetHeaderState(&anchor, 10)

	msg, round := linkedHeaders(anchor, 8, &nonce)
	checkpoint := round[len(round)-1]

	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 18, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	// Nothing committed beyond the anchor, so the whole round is still wanted.
	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	for i, h := range round {
		require.True(t, sm.headerIsInTheList(h),
			"header %d is still wanted and must be kept", 11+i)
	}

	require.Equal(t, round[0], sm.frontHash(t), "the front is the oldest block wanted")
}

// TestSyncManager_AboveTheLastCheckpointTheChainIsAsked covers the other half of
// the decision.
//
// Below the last checkpoint the chain is checkpoint-verified and there is one of
// it, so a header at or below the highest committed height is one we have and
// height alone settles it for free. Above the last checkpoint that stops being
// true: a header at a height the chain has reached may still be on a fork we do
// not hold, so the only honest answer comes from the blockchain store.
//
// The lookups must happen with headerMu released. Every other reader of the
// header list takes that lock, and a blocking client call underneath it would
// serialise the whole sync path; the code that removes a header on commit says
// so in as many words and unlocks by hand rather than with defer for exactly
// this reason.
func TestSyncManager_AboveTheLastCheckpointTheChainIsAsked(t *testing.T) {
	sm := newFetchLockManager(t, nil, nil, nil)

	syncPeer, _, _ := connectRacePeer(t, 95, 1000)
	registerRacePeer(sm, syncPeer)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	var nonce uint32

	anchor := chainhash.Hash{0xd3}
	sm.resetHeaderState(&anchor, 10)

	msg, round := linkedHeaders(anchor, 8, &nonce)
	checkpoint := round[len(round)-1]

	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 18, Hash: &checkpoint}
	sm.headersFirstMode.Store(true)

	// No checkpoints at all, so every height in this round is above the last one
	// and height can settle nothing.
	sm.chainParams = &chaincfg.Params{Checkpoints: nil}

	// A committed height that WOULD have trimmed the first five by height alone.
	sm.noteCommittedHeight(15)

	// The chain says it holds the first three and nothing else. Specific
	// expectations go in before the catch-all, because testify takes the first
	// one that matches.
	client, ok := sm.blockchainClient.(*blockchain2.Mock)
	require.True(t, ok)

	for i := 0; i < 3; i++ {
		hash := round[i]
		client.Mock.On("GetBlockExists", mock.Anything, &hash).Return(true, nil)
	}

	client.Mock.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: syncPeer})

	for i := 0; i < 3; i++ {
		require.False(t, sm.headerIsInTheList(round[i]),
			"header %d: the chain says it has this block, so its header must go", 11+i)
	}

	for i := 3; i < len(round); i++ {
		require.True(t, sm.headerIsInTheList(round[i]),
			"header %d: the chain does not have this block, so its header must stay even though height alone would have dropped it", 11+i)
	}

	require.Equal(t, round[3], sm.frontHash(t),
		"the front is the oldest block the chain actually lacks")
}
