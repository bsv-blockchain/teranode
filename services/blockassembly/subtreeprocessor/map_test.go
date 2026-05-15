package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// testInpointPair is a (hash, inpoints) pair only used to keep the bulk-insert
// tests below readable; the production API now takes parallel slices.
type testInpointPair struct {
	Hash     chainhash.Hash
	Inpoints *subtreepkg.TxInpoints
}

// inpointSlicesFor builds the parallel (keys, values) slices accepted by
// PutMultiBucketTxInpoints from a list of (hash, inpoints) pairs.
func inpointSlicesFor(pairs ...testInpointPair) ([]chainhash.Hash, []*subtreepkg.TxInpoints) {
	keys := make([]chainhash.Hash, len(pairs))
	vals := make([]*subtreepkg.TxInpoints, len(pairs))

	for i, p := range pairs {
		keys[i] = p.Hash
		vals[i] = p.Inpoints
	}

	return keys, vals
}

func makeHash(b byte) chainhash.Hash {
	var h chainhash.Hash
	for i := range h {
		h[i] = b
	}

	return h
}

func TestSplitSwissMap_ClearEmptiesAndPreservesUsability(t *testing.T) {
	m := NewSplitSwissMap(16, 64)

	for i := 0; i < 32; i++ {
		require.NoError(t, m.Put(makeHash(byte(i))))
	}

	require.Equal(t, 32, m.Length(), "populated map length should match insert count")

	m.Clear()

	require.Equal(t, 0, m.Length(), "Clear must empty every bucket")

	// After Clear, the same buckets must still be usable for further inserts.
	for i := 0; i < 8; i++ {
		require.NoError(t, m.Put(makeHash(byte(100+i))))
	}

	require.Equal(t, 8, m.Length(), "map must accept new inserts after Clear")
	require.True(t, m.Exists(makeHash(100)), "post-clear inserts must be queryable")
	require.False(t, m.Exists(makeHash(0)), "pre-clear entries must not be present")
}

func TestSplitTxInpointsMap_PutMultiBucketTxInpointsRespectsExistingEntries(t *testing.T) {
	m := NewSplitTxInpointsMap(16)

	hashA := makeHash(1)
	hashB := makeHash(2)
	inpointsA := &subtreepkg.TxInpoints{}
	inpointsB := &subtreepkg.TxInpoints{}

	// Pre-existing entry for hashA to verify duplicate handling.
	_, ok := m.SetIfNotExists(hashA, inpointsA)
	require.True(t, ok, "first SetIfNotExists must succeed")

	bucketA := m.BucketFor(hashA)
	bucketB := m.BucketFor(hashB)

	// Drive both through the bulk path even if they land in different buckets.
	keysA, valsA := inpointSlicesFor(testInpointPair{hashA, inpointsA})
	resA := m.PutMultiBucketTxInpoints(bucketA, keysA, valsA)
	require.Equal(t, []bool{false}, resA, "duplicate insert via bulk must report wasInserted=false")

	if bucketA == bucketB {
		// Combined batch into one bucket: order must be preserved.
		keys, vals := inpointSlicesFor(
			testInpointPair{hashA, inpointsA},
			testInpointPair{hashB, inpointsB},
		)
		res := m.PutMultiBucketTxInpoints(bucketA, keys, vals)
		require.Equal(t, []bool{false, true}, res, "duplicate then new in one bucket")
	} else {
		keysB, valsB := inpointSlicesFor(testInpointPair{hashB, inpointsB})
		resB := m.PutMultiBucketTxInpoints(bucketB, keysB, valsB)
		require.Equal(t, []bool{true}, resB, "new insert via bulk must report wasInserted=true")
	}

	gotA, foundA := m.Get(hashA)
	require.True(t, foundA)
	require.Same(t, inpointsA, gotA, "bulk path must not overwrite an existing entry")

	gotB, foundB := m.Get(hashB)
	require.True(t, foundB)
	require.Same(t, inpointsB, gotB, "bulk path must insert new entries")
}

func TestSplitTxInpointsMap_ClearPreservesBucketStructure(t *testing.T) {
	m := NewSplitTxInpointsMap(8)

	for i := 0; i < 16; i++ {
		_, _ = m.SetIfNotExists(makeHash(byte(i)), &subtreepkg.TxInpoints{})
	}

	require.Equal(t, 16, m.Length())

	m.Clear()
	require.Equal(t, 0, m.Length())

	// Re-insert and confirm bucket routing still works (no nil-bucket panic).
	for i := 0; i < 4; i++ {
		_, ok := m.SetIfNotExists(makeHash(byte(200+i)), &subtreepkg.TxInpoints{})
		require.True(t, ok)
	}

	require.Equal(t, 4, m.Length())
}
