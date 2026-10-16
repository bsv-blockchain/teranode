package subtreevalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// A recycled duplicate-check set must come back empty, or a later subtree
// would report a transaction it never saw as a duplicate.
func TestSeenPool_RecycledSetIsEmpty(t *testing.T) {
	s := getSeenSet(1000)
	for i := 0; i < 1000; i++ {
		s.m[chainhash.HashH([]byte{byte(i), byte(i >> 8)})] = struct{}{}
	}

	putSeenSet(s)
	require.Empty(t, s.m, "putSeenSet must clear the map before pooling it")

	require.Empty(t, getSeenSet(10).m)
}

// A set made for fewer hashes than the subtree is replaced by one made for the
// subtree's size, so the map is presized as the unpooled code made it.
func TestSeenPool_GrowsToRequestedSize(t *testing.T) {
	s := &seenSet{m: make(map[chainhash.Hash]struct{}, 8), size: 8}
	seenPool.Put(s)

	got := getSeenSet(4096)
	require.GreaterOrEqual(t, got.size, 4096)
	require.Empty(t, got.m)
}

// clear keeps a map's capacity, so a set made for an unusually large subtree
// is dropped rather than pinned in the pool.
func TestSeenPool_OversizedSetIsNotPooled(t *testing.T) {
	s := &seenSet{m: map[chainhash.Hash]struct{}{{1}: {}}, size: seenPoolMaxEntries + 1}

	putSeenSet(s)
	require.Len(t, s.m, 1, "an oversized set is left alone, not cleared and pooled")
}

// A small subtree after a full-size one gets a map sized for itself: inserting
// a few thousand hashes into a table sized for 1M costs ~15x more than into a
// fresh one. The big set stays pooled for the next big subtree.
func TestSeenPool_SmallSubtreeGetsAFreshMap(t *testing.T) {
	big := getSeenSet(seenPoolMaxEntries)
	putSeenSet(big)

	small := getSeenSet(4096)
	require.LessOrEqual(t, small.size, 4*4096, "a small subtree must not get a full-size map")
	require.Empty(t, small.m)
}
