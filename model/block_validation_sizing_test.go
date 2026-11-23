package model

import (
	"context"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func testInpoint(i int) subtreepkg.Inpoint {
	var inp subtreepkg.Inpoint
	inp.Hash[0] = byte(i)
	inp.Hash[1] = byte(i >> 8)
	inp.Hash[2] = byte(i >> 16)
	inp.Index = uint32(i) //nolint:gosec // test index
	return inp
}

// withPreallocCaps shrinks the preallocation caps so the capped path is
// reachable without materialising a multi-GB map. No t.Parallel().
func withPreallocCaps(t *testing.T, txMap uint64) {
	t.Helper()

	origTx := maxTxMapPrealloc
	maxTxMapPrealloc = txMap

	t.Cleanup(func() { maxTxMapPrealloc = origTx })
}

// requireBucketsFit asserts every bucket holds its share of n plus the variance
// headroom, and no more than one swiss group above that.
func requireBucketsFit(t *testing.T, capacities []int, n uint64, buckets uint16) {
	t.Helper()

	need := int(bucketCapacity(n, buckets))

	for i, c := range capacities {
		require.GreaterOrEqualf(t, c, need, "bucket %d", i)
		require.LessOrEqualf(t, c, need+16, "bucket %d is sized above its need", i)
	}
}

func txMapCapacities(m *txmap.SplitSwissMapUint64) []int {
	inner := m.Map()
	out := make([]int, 0, txMapBuckets)

	for i := uint16(0); i < txMapBuckets; i++ {
		out = append(out, inner[i].Map().Capacity())
	}

	return out
}

func TestBucketCapacity(t *testing.T) {
	require.GreaterOrEqual(t, bucketCapacity(0, 8192), uint32(1))

	// 588M entries over 8192 buckets: mean ~71.8K, sd ~268. The max of 8192
	// buckets sits ~4.3 sd above the mean, so ~1.9% over the mean covers it.
	const n = 588_000_000
	mean := float64(n) / 8192
	got := float64(bucketCapacity(n, 8192))
	require.Greater(t, got, mean*1.015)
	require.Less(t, got, mean*1.025, "headroom must track the variance, not a flat 20%%")

}

func TestNewBlockTxMap_SizedToNeed(t *testing.T) {
	const n = 1 << 20

	m := newBlockTxMap(n)

	requireBucketsFit(t, txMapCapacities(m), n, txMapBuckets)
}

func TestNewBlockTxMap_CapsPreallocation(t *testing.T) {
	withPreallocCaps(t, 1<<14)

	m := newBlockTxMap(1 << 40)

	requireBucketsFit(t, txMapCapacities(m), 1<<14, txMapBuckets)

	// The map still holds entries beyond the preallocation.
	for i := 0; i < 1<<15; i++ {
		require.NoError(t, m.Put(testHash(i), uint64(i))) //nolint:gosec // test index
	}
}

func TestBucketCapacity_StaysBelowSwissWrap(t *testing.T) {
	got := bucketCapacity(math.MaxUint64, 1)
	require.Equal(t, uint32(maxBucketCapacity), got)

	// dolthub/swiss sizes groups as (n + groupLoad - 1) / groupLoad in uint32.
	require.Greater(t, got+16, got, "the swiss group arithmetic must not wrap at the cap")
}

// TestValidOrderAndBlessed_DuplicateInputRejected pins the verdict of a block
// whose last transaction spends the anchor output node 1 already spent: the
// in-memory batched check and the disk-backed per-input check both reject it as
// an invalid block with duplicate inputs, naming the later transaction.
func TestValidOrderAndBlessed_DuplicateInputRejected(t *testing.T) {
	const leaves = 64

	block, deps, concurrency := buildBlockForValidOrderBench(t, leaves, 2)

	// The last transaction spends the anchor output node 1 already spent, so
	// validation fails after every earlier input is in the map.
	store, ok := deps.subtreeStore.(*mockSubtreeStore)
	require.True(t, ok)

	subtree := block.SubtreeSlices[0]
	key := string(subtree.RootHash()[:])

	meta, err := subtreepkg.NewSubtreeMetaFromBytes(subtree, store.data[key])
	require.NoError(t, err)

	anchor, err := meta.GetTxInpoints(1)
	require.NoError(t, err)
	require.Len(t, anchor, 1)
	require.NoError(t, meta.SetTxInpoints(leaves-1, subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{anchor[0].Hash}, []uint32{1, anchor[0].Index})))

	store.data[key], err = meta.Serialize()
	require.NoError(t, err)

	lastTx := block.SubtreeSlices[0].Nodes[leaves-1].Hash.String()

	for name, dirs := range map[string][]string{"memory": nil, "disk": {t.TempDir()}} {
		err = block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, concurrency, dirs, 2)
		require.ErrorContains(t, err, "duplicate inputs", name)
		require.ErrorContains(t, err, lastTx, name)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), name)
	}
}

func testHash(i int) chainhash.Hash {
	return chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), 0x7e})
}
