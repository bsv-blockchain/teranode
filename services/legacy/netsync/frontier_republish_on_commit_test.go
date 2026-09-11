package netsync

import (
	"container/list"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestAdvanceHeaderListFor_RepublishesWhenTheHeaderHasAlreadyGone is the
// regression test for the last known case of a committed block being asked for
// again.
//
// The frontier is not stored, it is derived from the front of the header list,
// and advanceHeaderListFor used to republish it only when the header it removed
// had been at that front. A block whose header had already left the list found
// nothing to remove, so nothing republished, and the frontier went on naming a
// block this node had just accepted. The racer then asked peers for it, since
// its whole job is to chase whatever the frontier names.
//
// Measured on mainnet: block 762018 committed at 00:53:46 and was raced four
// more times over the following five and a half minutes. Its frontier had been
// stamped eight seconds before the commit and never moved.
func TestAdvanceHeaderListFor_RepublishesWhenTheHeaderHasAlreadyGone(t *testing.T) {
	sm := newRaceManager(t)

	// The block that is about to commit. Its header has already left the list,
	// so nothing in the list or the index names it.
	committed := chainhash.Hash{0xc0}

	// What the list actually holds: the next block the chain wants, and one
	// behind it so a cursor can sit past the front.
	nextHash := chainhash.Hash{0xf1}
	afterHash := chainhash.Hash{0xf2}

	nextElem := sm.headerList.PushBack(&headerNode{height: 762019, hash: &nextHash})
	afterElem := sm.headerList.PushBack(&headerNode{height: 762020, hash: &afterHash})

	sm.headerIndex = map[chainhash.Hash]*list.Element{
		nextHash:  nextElem,
		afterHash: afterElem,
	}
	sm.startHeader = afterElem

	// The stale frontier: it still names the block that is committing now.
	sm.setFrontier(committed, 762018, time.Now())
	require.Equal(t, committed, sm.frontierHash, "precondition: the frontier names the committing block")

	isCheckpoint, removed := sm.advanceHeaderListFor(committed)

	require.False(t, isCheckpoint, "the committing block is not a checkpoint")
	require.Nil(t, removed, "precondition: its header had already left the list, so nothing was removed")

	require.Equal(t, nextHash, sm.frontierHash,
		"a commit must leave the frontier naming the list's front, or the racer keeps asking peers for a block this node already has")
	require.Equal(t, int32(762019), sm.frontierHeight,
		"the republished frontier must carry the front's height too")
}

// TestAdvanceHeaderListFor_RepublishesWhenTheHeaderWasNotAtTheFront covers the
// sibling case: the header was in the list but behind the front, so the removal
// happened and still nothing republished. The front does not move here, so the
// republish must be a no-op on the hash while still correcting a frontier that
// was already stale.
func TestAdvanceHeaderListFor_RepublishesWhenTheHeaderWasNotAtTheFront(t *testing.T) {
	sm := newRaceManager(t)

	frontHash := chainhash.Hash{0xf1}
	midHash := chainhash.Hash{0xf2}
	backHash := chainhash.Hash{0xf3}

	frontElem := sm.headerList.PushBack(&headerNode{height: 762019, hash: &frontHash})
	midElem := sm.headerList.PushBack(&headerNode{height: 762020, hash: &midHash})
	backElem := sm.headerList.PushBack(&headerNode{height: 762021, hash: &backHash})

	sm.headerIndex = map[chainhash.Hash]*list.Element{
		frontHash: frontElem,
		midHash:   midElem,
		backHash:  backElem,
	}
	sm.startHeader = backElem

	// Stale from an earlier miss: the frontier names a block no longer in the list.
	stale := chainhash.Hash{0xc0}
	sm.setFrontier(stale, 762018, time.Now())

	_, removed := sm.advanceHeaderListFor(midHash)
	require.NotNil(t, removed, "precondition: the header was in the list and was removed")

	require.Equal(t, frontHash, sm.frontierHash,
		"a commit behind the front must still correct a stale frontier")
}
