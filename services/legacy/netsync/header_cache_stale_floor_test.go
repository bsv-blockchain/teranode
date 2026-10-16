package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// A fill reads the committed tip before it waits for fillMu, and PruneTo can
// record a higher tip in between. These tests pin what such a stale read gets:
// the part of the batch at or below the recorded floor is committed history, so
// it is never judged as new headers, and an honest peer is never disconnected
// for having been asked about a tip the node has since passed.

// An honest lagging peer's short reply that ends at or below the recorded floor
// is dropped without blame, even though a held checkpoint sits above it.
func TestHeaderCacheStaleFloor_AnHonestShortReplyBelowTheFloorIsNotAFork(t *testing.T) {
	tip := chainhash.Hash{0xc0}
	honest, hashes := linkedRun(tip, 25)

	cache := newHeaderCache().WithCheckpoints(forkCheckpoints(hashes))
	require.True(t, cache.FillFrom("A", tip, 1, honest).accepted)
	require.Equal(t, int32(20), cache.ProvenTo())

	// The committed tip moves to 8 (hashes[7]) while B's fill waited.
	cache.PruneTo(8, hashes[7])

	// B read the tip at 3 (hashes[2]) and replies with honest 4..6.
	result := cache.FillFrom("B", hashes[2], 4, honest[3:6])
	require.False(t, result.rejection.disconnects(), "an honest peer was disconnected: %s %s", result.rejection, result.detail)
	require.Equal(t, headerAccepted, result.rejection)
	require.False(t, result.accepted)

	_, _, ok := cache.PeerTop("B")
	require.False(t, ok, "nothing from a reply wholly at or below the floor is held for B")

	for h := int32(9); h <= 25; h++ {
		got, ok := cache.At(h)
		require.True(t, ok)
		require.Equal(t, hashes[h-1], got, "height %d still names A's branch", h)
	}
}

// A stale-tip reply that reaches above the floor is clipped there: the headers
// at or below the floor are not judged or held, and the rest is held on the
// floor as a branch that connects to it.
func TestHeaderCacheStaleFloor_AReplyCrossingTheFloorIsClippedToIt(t *testing.T) {
	tip := chainhash.Hash{0xc0}
	honest, hashes := linkedRun(tip, 25)

	cache := newHeaderCache().WithCheckpoints(forkCheckpoints(hashes))
	cache.PruneTo(8, hashes[7])

	// B read the tip at 3 and replies with honest 4..25, across the checkpoint
	// at 20.
	result := cache.FillFrom("B", hashes[2], 4, honest[3:])
	require.Equal(t, headerAccepted, result.rejection)
	require.True(t, result.accepted)
	require.Equal(t, int32(9), result.low, "the first header held is one above the floor")
	require.Equal(t, 17, result.added)
	require.Equal(t, 17, cache.heldHeaders(), "nothing at or below the floor is held")
	require.Equal(t, int32(20), cache.ProvenTo())

	top, topHash, ok := cache.PeerTop("B")
	require.True(t, ok)
	require.Equal(t, int32(25), top)
	require.Equal(t, hashes[24], topHash)
}

// A stale-tip reply whose header at the floor height is not the recorded floor
// is not on the committed chain there, and the cache cannot judge it without
// the store; it is dropped without blame and nothing is held.
func TestHeaderCacheStaleFloor_AReplyThatMissesTheFloorIsDropped(t *testing.T) {
	tip := chainhash.Hash{0xc0}
	honest, hashes := linkedRun(tip, 25)

	cache := newHeaderCache().WithCheckpoints(forkCheckpoints(hashes))
	cache.PruneTo(8, chainhash.Hash{0x99})

	result := cache.FillFrom("B", hashes[2], 4, honest[3:])
	require.False(t, result.rejection.disconnects())
	require.False(t, result.accepted)
	require.Zero(t, cache.heldHeaders())
}
