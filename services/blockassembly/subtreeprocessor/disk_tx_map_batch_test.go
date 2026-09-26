package subtreeprocessor

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func batchTestHash(i int) chainhash.Hash {
	var h chainhash.Hash
	binary.LittleEndian.PutUint64(h[:], uint64(i)+1)
	binary.LittleEndian.PutUint64(h[8:], 0x9e3779b97f4a7c15*uint64(i+1))

	return h
}

// The batched index update must store exactly what one UpdateSubtreeIndex call
// per hash stores, including for entries still pending in the writer, and must
// leave the inpoints untouched.
func TestDiskTxMap_UpdateSubtreeIndexBatch(t *testing.T) {
	m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir(), t.TempDir()}})
	require.NoError(t, err)

	defer m.Close()

	const n = 5000

	nodes := make([]subtreepkg.Node, 0, n)

	for i := 0; i < n; i++ {
		h := batchTestHash(i)
		inp := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(i + n)}, []uint32{1, uint32(i % 5)})
		m.Set(h, &inp)
		nodes = append(nodes, subtreepkg.Node{Hash: h})
	}

	// Not yet written: a hash the batch must skip without failing.
	missing := subtreepkg.Node{Hash: batchTestHash(10 * n)}

	require.NoError(t, m.UpdateSubtreeIndexBatch(append(nodes[:3000:3000], missing), 7))

	for i, node := range nodes {
		got, ok := m.Get(node.Hash)
		require.True(t, ok, "hash %d missing", i)

		want := int16(0)
		if i < 3000 {
			want = 7
		}

		require.Equal(t, want, got.SubtreeIndex, "subtree index of %d", i)
		require.Equal(t, []chainhash.Hash{batchTestHash(i + n)}, got.GetParentTxHashes(), "inpoints of %d", i)
	}

	_, ok := m.Get(missing.Hash)
	require.False(t, ok)
}

func TestDiskTxMap_UpdateSubtreeIndexBatchMatchesSingle(t *testing.T) {
	single, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
	require.NoError(t, err)

	defer single.Close()

	batched, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir(), t.TempDir(), t.TempDir()}})
	require.NoError(t, err)

	defer batched.Close()

	nodes := make([]subtreepkg.Node, 0, 500)

	for i := 0; i < 500; i++ {
		h := batchTestHash(i)
		inp := subtreepkg.TxInpoints{}
		single.Set(h, &inp)
		batched.Set(h, &inp)
		nodes = append(nodes, subtreepkg.Node{Hash: h})
	}

	for _, node := range nodes {
		require.NoError(t, single.UpdateSubtreeIndex(node.Hash, 42))
	}

	require.NoError(t, batched.UpdateSubtreeIndexBatch(nodes, 42))

	for _, node := range nodes {
		a, okA := single.Get(node.Hash)
		b, okB := batched.Get(node.Hash)
		require.Equal(t, okA, okB)
		require.Equal(t, a.SubtreeIndex, b.SubtreeIndex)
	}
}

// A batch in which no node has an entry writes nothing and leaves the map fully
// usable. (The abandoned-batch leak this path once had is not observable here;
// it was measured separately.)
func TestDiskTxMap_UpdateSubtreeIndexBatchAllMissing(t *testing.T) {
	m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir(), t.TempDir()}})
	require.NoError(t, err)

	defer m.Close()

	missing := []subtreepkg.Node{{Hash: batchTestHash(1)}, {Hash: batchTestHash(2)}}
	require.NoError(t, m.UpdateSubtreeIndexBatch(missing, 3))

	for _, node := range missing {
		_, ok := m.Get(node.Hash)
		require.False(t, ok)
	}

	inp := subtreepkg.TxInpoints{}
	m.Set(batchTestHash(1), &inp)
	require.NoError(t, m.UpdateSubtreeIndexBatch(missing, 3))

	got, ok := m.Get(batchTestHash(1))
	require.True(t, ok)
	require.Equal(t, int16(3), got.SubtreeIndex)
}
