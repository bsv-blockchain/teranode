package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestDispatcher_AFrontierTailWithNoKnownHeightIsNotAParent covers the two arms of
// resolveParent that dispatch a block without resolving its height.
//
// A block already in the chain, and a block whose parent is in the frontier but is
// not its tail, are both dispatched with d.height still zero (manager.go, the exists
// arm and the inFlight arm). bd.dispatch stamps that zero into the frontier entry, and
// a non-windowed dispatch is admitted only into an empty frontier, so the zero-height
// entry is the tail. parentFor then hands it to the next arriving child as a resolved
// parent, the child is given height 0+1, and HandleBlockDirect's own mismatch guard
// cannot catch it because a legacy wire block reports its height as unknown.
//
// What the child then costs is not a wrong-height commit: block validation's
// deriveBlockHeight compares the claimed height against the real parent's and returns
// BlockInvalidError. That error is not a transient local fault, so the block is
// rejected to the peer, the delivering peer's whole association is evicted as
// misbehaving, and every descendant is suppressed for the cascade TTL. A valid block
// and an honest peer, both judged, for a height this node made up.
//
// So a tail whose height is unknown must not be a parent. The child then falls through
// to the inFlight check and is dispatched with a nil parent, which means the worker
// looks the parent up in the chain, which is the answer that was always correct.
func TestDispatcher_AFrontierTailWithNoKnownHeightIsNotAParent(t *testing.T) {
	bd, _ := testDispatcher(t, 2)

	parentHash := chainhash.HashH([]byte("parent with an unresolved height"))

	// Exactly what bd.dispatch builds for a dispatch whose head returned before it
	// resolved a height: the two early-return arms of resolveParent.
	bd.frontier = append(bd.frontier, &frontierEntry{
		hash:       parentHash,
		height:     0,
		rpcStarted: make(chan struct{}),
		settled:    make(chan struct{}),
	})

	require.True(t, bd.inFlight(parentHash), "precondition: the block is in flight")

	require.Nil(t, bd.parentFor(&parentHash),
		"a frontier tail whose height is unknown must not be resolved as a parent, or its child is validated at height 1")

	// A tail with a real height is still a parent: the guard must not disarm the window.
	childHash := chainhash.HashH([]byte("child at a known height"))

	bd.frontier = append(bd.frontier, &frontierEntry{
		hash:       childHash,
		height:     750_700,
		rpcStarted: make(chan struct{}),
		settled:    make(chan struct{}),
	})

	p := bd.parentFor(&childHash)
	require.NotNil(t, p, "a tail with a known height is still the in-flight parent of the next block")
	require.Equal(t, uint32(750_700), p.height)
	require.Equal(t, uint32(750_700), bd.tailHeight(), "and it is what the block-assembly lag arm measures from")
}
