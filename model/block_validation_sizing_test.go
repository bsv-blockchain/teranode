package model

import (
	"context"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
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
func withPreallocCaps(t *testing.T, txMap, parentSpends uint64) {
	t.Helper()

	origTx, origPS := maxTxMapPrealloc, maxParentSpendsPrealloc
	maxTxMapPrealloc, maxParentSpendsPrealloc = txMap, parentSpends

	t.Cleanup(func() { maxTxMapPrealloc, maxParentSpendsPrealloc = origTx, origPS })
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

func parentSpendsCapacities(m *SplitSyncedParentMap) []int {
	out := make([]int, 0, len(m.buckets))
	for i := range m.buckets {
		out = append(out, m.buckets[i].m.Capacity())
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
	withPreallocCaps(t, 1<<14, 1<<14)

	m := newBlockTxMap(1 << 40)

	requireBucketsFit(t, txMapCapacities(m), 1<<14, txMapBuckets)

	// The map still holds entries beyond the preallocation.
	for i := 0; i < 1<<15; i++ {
		require.NoError(t, m.Put(testHash(i), uint64(i))) //nolint:gosec // test index
	}
}

func TestNewBlockParentSpendsMap_SizedToNeed(t *testing.T) {
	const n = 1 << 20

	requireBucketsFit(t, parentSpendsCapacities(newBlockParentSpendsMap(n)), n, parentSpendsBuckets)
}

func TestNewBlockParentSpendsMap_CapsPreallocation(t *testing.T) {
	withPreallocCaps(t, 1<<14, 1<<14)

	requireBucketsFit(t, parentSpendsCapacities(newBlockParentSpendsMap(1<<40)), 1<<14, parentSpendsBuckets)
}

// TestInpointBucket_SpreadsOutputsOfOneParent pins that the spent outputs of one
// parent do not all land in one bucket, which the per-bucket headroom (sized for
// independent keys) cannot absorb.
func TestInpointBucket_SpreadsOutputsOfOneParent(t *testing.T) {
	const outputs = 200_000

	counts := make(map[uint16]int)

	var parent subtreepkg.Inpoint
	parent.Hash[0], parent.Hash[1] = 0xab, 0xcd

	for i := 0; i < outputs; i++ {
		parent.Index = uint32(i) //nolint:gosec // test index
		counts[inpointBucket(parent, parentSpendsBuckets)]++
	}

	mean := float64(outputs) / float64(parentSpendsBuckets)
	for b, c := range counts {
		require.LessOrEqualf(t, float64(c), mean+5*math.Sqrt(mean)+8, "bucket %d holds %d of one parent's outputs", b, c)
	}

	require.Equal(t, inpointBucket(parent, parentSpendsBuckets), inpointBucket(parent, parentSpendsBuckets), "deterministic")
}

func TestBucketCapacity_StaysBelowSwissWrap(t *testing.T) {
	got := bucketCapacity(math.MaxUint64, 1)
	require.Equal(t, uint32(maxBucketCapacity), got)

	// dolthub/swiss sizes groups as (n + groupLoad - 1) / groupLoad in uint32.
	require.Greater(t, got+16, got, "the swiss group arithmetic must not wrap at the cap")
}

// TestSplitSyncedParentMap_GrowsInSmallSteps pins that an undersized bucket
// grows by an eighth rather than doubling, so an underestimate costs at most
// ~12.5% over the need instead of up to 2x.
func TestSplitSyncedParentMap_GrowsInSmallSteps(t *testing.T) {
	m := NewSplitSyncedParentMap(1, 64)

	const n = 10_000

	for i := 0; i < n; i++ {
		inserted, err := m.SetIfNotExists(testInpoint(i))
		require.NoError(t, err)
		require.True(t, inserted)
	}

	capacity := m.buckets[0].m.Count() + m.buckets[0].m.Capacity()
	require.LessOrEqual(t, capacity, n+n/8+16, "a doubling swiss map would sit at up to 2x")

	for i := 0; i < n; i++ {
		inserted, err := m.SetIfNotExists(testInpoint(i))
		require.NoError(t, err)
		require.False(t, inserted, "inpoint %d survived the growth", i)
	}
}

func TestSplitSyncedParentMap_Length(t *testing.T) {
	m := NewSplitSyncedParentMap(16, 100)
	require.Equal(t, uint64(0), m.Length())

	for i := 0; i < 50; i++ {
		inserted, err := m.SetIfNotExists(testInpoint(i))
		require.NoError(t, err)
		require.True(t, inserted)
	}

	_, err := m.SetIfNotExists(testInpoint(0))
	require.NoError(t, err)

	require.Equal(t, uint64(50), m.Length(), "a duplicate is not counted twice")

	m.Clear()
	require.Equal(t, uint64(0), m.Length())
}

func TestInMemoryParentSpendsCapacity(t *testing.T) {
	t.Run("no measurement starts at one input per transaction", func(t *testing.T) {
		require.Equal(t, uint64(1_000_000), inMemoryParentSpendsCapacity(1_000_000, 0, 2))
	})

	t.Run("a measurement sizes with a small margin", func(t *testing.T) {
		require.Equal(t, uint64(1_030_000), inMemoryParentSpendsCapacity(1_000_000, 1_000, 3))
		require.Equal(t, uint64(2_575_000), inMemoryParentSpendsCapacity(1_000_000, 2_500, 3))
	})

	t.Run("the ratio is clamped to the multiplier before the margin is added", func(t *testing.T) {
		require.Equal(t, uint64(2_060_000), inMemoryParentSpendsCapacity(1_000_000, 2_500_000, 2))
		require.Equal(t, uint64(1_030_000), inMemoryParentSpendsCapacity(1_000_000, 2_500, 0), "a 0 multiplier means 1")
	})

	// A chain sustaining just above the multiplier must still get the margin;
	// clamping after the margin sized it at exactly the multiplier, so every
	// bucket regrew every block.
	t.Run("a ratio just above the multiplier keeps its margin", func(t *testing.T) {
		require.Equal(t, uint64(2_060_000), inMemoryParentSpendsCapacity(1_000_000, 2_050, 2))
	})

	t.Run("never below one input per transaction plus the margin", func(t *testing.T) {
		require.Equal(t, uint64(1_030_000), inMemoryParentSpendsCapacity(1_000_000, 500, 2))
	})

	t.Run("never zero", func(t *testing.T) {
		require.Equal(t, uint64(1), inMemoryParentSpendsCapacity(0, 1_000, 2))
		require.Equal(t, uint64(1), inMemoryParentSpendsCapacity(0, 0, 2))
	})

	t.Run("does not wrap", func(t *testing.T) {
		require.Equal(t, uint64(math.MaxUint64), inMemoryParentSpendsCapacity(math.MaxUint64/2, 3_000, 5))
	})
}

func resetObservedInpoints() {
	inpointsPerTx.mu.Lock()
	inpointsPerTx.last, inpointsPerTx.previous = 0, 0
	inpointsPerTx.mu.Unlock()
}

func lastInpointsPerTx() uint64 {
	inpointsPerTx.mu.Lock()
	defer inpointsPerTx.mu.Unlock()

	return inpointsPerTx.last
}

// withMinMeasuredTxs lowers the measurement threshold so small test blocks are
// recorded. No t.Parallel().
func withMinMeasuredTxs(t *testing.T, n uint64) {
	t.Helper()

	orig := minMeasuredTxs
	minMeasuredTxs = n

	t.Cleanup(func() { minMeasuredTxs = orig })
}

func TestRecordInpointsPerTx(t *testing.T) {
	t.Cleanup(resetObservedInpoints)
	resetObservedInpoints()
	withMinMeasuredTxs(t, 0)

	// entryCount includes the coinbase placeholder, which has no inputs.
	recordInpointsPerTx(1_000, 1_001)
	require.Equal(t, uint64(1_000), lastInpointsPerTx())

	recordInpointsPerTx(2_001, 1_001)
	require.Equal(t, uint64(2_001), lastInpointsPerTx())

	recordInpointsPerTx(1, 4)
	require.Equal(t, uint64(334), lastInpointsPerTx(), "rounds up, so sizing never undershoots the measurement")

	recordInpointsPerTx(5, 1)
	require.Equal(t, uint64(334), lastInpointsPerTx(), "a coinbase-only block records nothing")
}

// TestRecordInpointsPerTx_IgnoresSmallBlocks pins that blocks below
// minMeasuredTxs do not move the sizing: two cheap consolidation blocks would
// otherwise size the next large block's map for their ratio.
func TestRecordInpointsPerTx_IgnoresSmallBlocks(t *testing.T) {
	t.Cleanup(resetObservedInpoints)
	resetObservedInpoints()

	recordInpointsPerTx(5_000, 2)
	recordInpointsPerTx(5_000, 2)
	require.Equal(t, uint64(0), sizingInpointsPerTxMilli())

	recordInpointsPerTx(minMeasuredTxs, minMeasuredTxs+1)
	require.Equal(t, uint64(1_000), sizingInpointsPerTxMilli())
}

// TestSizingRatio_OutlierDoesNotInflateNextBlock pins that one input-heavy
// block does not size the next block's map: sizing uses the smaller of the last
// two measurements, so only a shift that persists for two blocks moves it up.
func TestSizingRatio_OutlierDoesNotInflateNextBlock(t *testing.T) {
	t.Cleanup(resetObservedInpoints)
	resetObservedInpoints()
	withMinMeasuredTxs(t, 0)

	require.Equal(t, uint64(0), sizingInpointsPerTxMilli(), "nothing measured yet")

	recordInpointsPerTx(1_000, 1_001)
	require.Equal(t, uint64(1_000), sizingInpointsPerTxMilli(), "a single measurement is used as is")

	recordInpointsPerTx(8_000, 1_001) // a consolidation-heavy outlier
	require.Equal(t, uint64(1_000), sizingInpointsPerTxMilli())

	recordInpointsPerTx(1_000, 1_001)
	require.Equal(t, uint64(1_000), sizingInpointsPerTxMilli())

	recordInpointsPerTx(3_000, 1_001)
	recordInpointsPerTx(3_000, 1_001)
	require.Equal(t, uint64(3_000), sizingInpointsPerTxMilli(), "a sustained shift is followed")

	recordInpointsPerTx(1_200, 1_001)
	require.Equal(t, uint64(1_200), sizingInpointsPerTxMilli(), "a drop is followed at once")
}

// TestValidOrderAndBlessed_RecordsMeasuredInpointsPerTx pins that a successful
// in-memory validation records the block's real inputs per transaction, which
// sizes the next block's parent-spends map.
func TestValidOrderAndBlessed_RecordsMeasuredInpointsPerTx(t *testing.T) {
	t.Cleanup(resetObservedInpoints)
	resetObservedInpoints()
	withMinMeasuredTxs(t, 0)

	const leaves = 64

	block, deps, concurrency := buildBlockForValidOrderBench(t, leaves, 2)

	// Node 0 is the coinbase placeholder, node 1 spends the external anchor,
	// node 2 has one earlier sibling, every later node spends two.
	inputs := uint64(1 + 1 + 2*(leaves-3))

	require.NoError(t, block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, concurrency, nil, 2))

	// The coinbase placeholder is not a spending transaction.
	want := (inputs*1000 + leaves - 2) / (leaves - 1)
	require.Equal(t, want, lastInpointsPerTx())
}

// TestValidOrderAndBlessed_FailedBlockDoesNotRecord pins that the ratio is only
// recorded once the block validated: a block rejected part way through has put
// a partial input count in the map, and a peer could otherwise steer the next
// block's sizing with invalid blocks.
func TestValidOrderAndBlessed_FailedBlockDoesNotRecord(t *testing.T) {
	t.Cleanup(resetObservedInpoints)
	resetObservedInpoints()
	withMinMeasuredTxs(t, 0)

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

	err = block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, concurrency, nil, 2)
	require.ErrorContains(t, err, "duplicate inputs")
	require.Equal(t, uint64(0), lastInpointsPerTx())
}

// TestBlock_InMemoryParentSpendsCapacity pins the sizing validOrderAndBlessed
// uses: the block's own entry count, the recorded ratio and the configured
// multiplier as its ceiling.
func TestBlock_InMemoryParentSpendsCapacity(t *testing.T) {
	t.Cleanup(resetObservedInpoints)
	resetObservedInpoints()
	withMinMeasuredTxs(t, 0)

	const leaves = 64

	block, _, _ := buildBlockForValidOrderBench(t, leaves, 2)

	// Sized from the loaded body, never the peer-supplied count (issue 1501).
	block.TransactionCount = 1 << 40

	require.Equal(t, uint64(leaves), block.inMemoryParentSpendsCapacity(2), "one input per tx before anything is measured")

	// Two input-heavy blocks: 3 inputs per tx.
	recordInpointsPerTx(3_000, 1_001)
	recordInpointsPerTx(3_000, 1_001)

	// Clamped to the multiplier of 2, plus the margin: ceil(64 * 2.06).
	require.Equal(t, uint64(132), block.inMemoryParentSpendsCapacity(2))
	// A larger multiplier lets the measured ratio through: ceil(64 * 3.09).
	require.Equal(t, uint64(198), block.inMemoryParentSpendsCapacity(5))
}

func TestValidOrderAndBlessed_DiskPathDoesNotRecord(t *testing.T) {
	t.Cleanup(resetObservedInpoints)
	resetObservedInpoints()
	withMinMeasuredTxs(t, 0)

	block, deps, concurrency := buildBlockForValidOrderBench(t, 64, 2)

	require.NoError(t, block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, concurrency, []string{t.TempDir()}, 2))
	require.Equal(t, uint64(0), lastInpointsPerTx())
}

func testHash(i int) chainhash.Hash {
	return chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), 0x7e})
}
