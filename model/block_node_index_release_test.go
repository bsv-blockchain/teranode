package model

import (
	"bytes"
	"context"
	"os"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// nodeIndexBlockWithMmap is a 3-subtree block whose middle subtree is
// mmap-backed, with the in-memory node index built over it the way
// checkDuplicateTransactions builds it during Valid.
func nodeIndexBlockWithMmap(t *testing.T) (b *Block, mmapDir string) {
	t.Helper()

	b = shapeBlock(t, 4, []int{4, 4, 3}, seqHashAt)

	serialized, err := b.SubtreeSlices[1].Serialize()
	require.NoError(t, err)

	mmapDir = t.TempDir()
	mmapSt, err := subtreepkg.NewSubtreeFromReaderMmap(bytes.NewReader(serialized), mmapDir)
	require.NoError(t, err)
	require.True(t, mmapSt.IsMmapBacked())

	b.SubtreeSlices[1] = mmapSt

	require.NoError(t, b.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 4, nil))

	_, ok := b.txMap.(*blockNodeIndex)
	require.True(t, ok, "got %T", b.txMap)

	return b, mmapDir
}

// requireIndexIntact checks the index still answers every tx of the block from
// the node arrays it was built over.
func requireIndexIntact(t *testing.T, m blockTxMap) {
	t.Helper()

	for g := 1; g < 11; g++ {
		idx, ok := m.Get(seqHashAt(g))
		require.True(t, ok, "g=%d", g)
		require.Equal(t, uint64(g), idx)
	}
}

func mmapFiles(t *testing.T, dir string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	return len(entries)
}

// The node index stores no hashes: it reads them from the subtrees' node arrays.
// A release while it is live (cache eviction, the TTL cleaner) must not pool a
// heap array, which the next subtree load would overwrite, nor unmap an mmap
// one. Both are let go once the index is released.
func TestReleaseSubtreeNodes_WhileNodeIndexLive(t *testing.T) {
	b, mmapDir := nodeIndexBlockWithMmap(t)
	m := b.txMap

	var pooled [][]subtreepkg.Node

	require.NoError(t, b.ReleaseSubtreeNodes(func(nodes []subtreepkg.Node) { pooled = append(pooled, nodes) }))

	require.Empty(t, pooled, "no node array may be pooled while the index reads it")
	require.Equal(t, 1, mmapFiles(t, mmapDir), "the mmap subtree must stay mapped while the index reads it")

	for _, st := range b.SubtreeSlices {
		require.Nil(t, st, "the release still detaches every subtree from the block")
	}

	requireIndexIntact(t, m)

	b.releaseTxMap()

	require.Zero(t, mmapFiles(t, mmapDir), "releasing the index closes the subtrees whose close was deferred")
	requireUnpinned(t, b)

	// with no live index the release pools again
	fresh := shapeBlock(t, 4, []int{4, 4, 3}, seqHashAt)
	b.SubtreeSlices = fresh.SubtreeSlices

	require.NoError(t, b.ReleaseSubtreeNodes(func(nodes []subtreepkg.Node) { pooled = append(pooled, nodes) }))
	require.Len(t, pooled, 3)
}

// TryReleaseSubtreeNodes (the lastValidatedBlocks eviction) takes the same path.
func TestTryReleaseSubtreeNodes_WhileNodeIndexLive(t *testing.T) {
	b, mmapDir := nodeIndexBlockWithMmap(t)
	m := b.txMap

	var pooled int

	released, err := b.TryReleaseSubtreeNodes(func([]subtreepkg.Node) { pooled++ })
	require.NoError(t, err)
	require.True(t, released)
	require.Zero(t, pooled)
	require.Equal(t, 1, mmapFiles(t, mmapDir))

	requireIndexIntact(t, m)

	b.releaseTxMap()
	require.Zero(t, mmapFiles(t, mmapDir))
}

// A reload of the same block (getAndValidateSubtrees closes the mmap survivors)
// must not unmap a subtree a live index reads either.
func TestCloseMmapSubtrees_WhileNodeIndexLive(t *testing.T) {
	b, mmapDir := nodeIndexBlockWithMmap(t)
	m := b.txMap

	b.subtreeSlicesMu.Lock()
	require.NoError(t, b.closeMmapSubtreesLocked())
	b.subtreeSlicesMu.Unlock()

	require.Equal(t, 1, mmapFiles(t, mmapDir))
	requireIndexIntact(t, m)

	b.releaseTxMap()
	require.Zero(t, mmapFiles(t, mmapDir))
}

// The load-time dedup builds the index under the subtree mutex; it is held the same way.
func TestGetAndValidateSubtreesWithDedup_ReleaseWhileNodeIndexLive(t *testing.T) {
	b, _, _, store := dedupLoadFixture(t, nil)

	deduped, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, 4)
	require.NoError(t, err)
	require.True(t, deduped)

	_, ok := b.txMap.(*blockNodeIndex)
	require.True(t, ok, "got %T", b.txMap)

	var pooled int

	require.NoError(t, b.ReleaseSubtreeNodes(func([]subtreepkg.Node) { pooled++ }))
	require.Zero(t, pooled)

	b.subtreeSlicesMu.Lock()
	require.Len(t, b.deferredCloses, 2)
	b.subtreeSlicesMu.Unlock()

	b.releaseTxMap()
	requireUnpinned(t, b)
}

// requireUnpinned checks nothing pins the block's node arrays and nothing waits to be closed.
func requireUnpinned(t *testing.T, b *Block) {
	t.Helper()

	require.Zero(t, b.nodeIndexPins.Load())

	b.subtreeSlicesMu.Lock()
	defer b.subtreeSlicesMu.Unlock()

	require.Empty(t, b.deferredCloses)
}

// releaseTxMap is idempotent: a second call neither unpins twice nor closes twice.
func TestReleaseTxMap_NodeIndexTwice(t *testing.T) {
	b, mmapDir := nodeIndexBlockWithMmap(t)
	require.Equal(t, int32(1), b.nodeIndexPins.Load())

	require.NoError(t, b.ReleaseSubtreeNodes(nil))
	b.releaseTxMap()
	b.releaseTxMap()

	require.Zero(t, mmapFiles(t, mmapDir))
	requireUnpinned(t, b)
}

// An index replaced without releaseTxMap must not leave the block pinned for
// good, or every later release would skip pooling and never close an mmap.
func TestCheckDuplicateTransactions_ReplacedIndexDoesNotLeakPin(t *testing.T) {
	b, mmapDir := nodeIndexBlockWithMmap(t)

	require.NoError(t, b.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 4, nil))
	require.Equal(t, int32(1), b.nodeIndexPins.Load(), "the replaced index unpinned")

	require.NoError(t, b.ReleaseSubtreeNodes(nil))
	b.releaseTxMap()

	require.Zero(t, mmapFiles(t, mmapDir))
	requireUnpinned(t, b)
}

// A pinned release detaches the subtrees from the block but leaves their Nodes
// attached until releaseTxMap: the separate dedup pass reads subtree.Nodes from
// subtrees it captured, and a nil there would skip the subtree's duplicate check
// without an error.
func TestReleaseSubtreeNodes_PinnedKeepsNodesAttached(t *testing.T) {
	b, _ := nodeIndexBlockWithMmap(t)

	captured := append([]*subtreepkg.Subtree(nil), b.SubtreeSlices...)
	lens := make([]int, len(captured))

	for i, st := range captured {
		lens[i] = len(st.Nodes)
	}

	require.NoError(t, b.ReleaseSubtreeNodes(nil))

	for i, st := range captured {
		require.Nil(t, b.SubtreeSlices[i])
		require.Len(t, st.Nodes, lens[i], "subtree %d", i)
	}

	b.releaseTxMap()

	for i, st := range captured {
		require.Empty(t, st.Nodes, "subtree %d is detached once the index is gone", i)
	}

	requireUnpinned(t, b)
}
