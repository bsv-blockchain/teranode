package model

import (
	"context"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/dolthub/swiss"
	"github.com/stretchr/testify/require"
)

// These tests pin issue 1428 — a block whose TransactionCount exceeds 2^32
// must be handled in 64-bit, not fail a uint32 narrowing with a retryable
// processing error that made block validation refetch the same
// consensus-valid block forever.
//
// Deliberately NOT tested by materialising a >2^32-entry map: GetTxMap
// preallocates eagerly from its hint, and a single such call costs tens of
// GB of RSS (the same trap TestGetTxMap_OversizedAllocatesFresh documents
// removing after it dominated CI memory, issue 1051). The narrowing seam
// itself no longer exists at the type level — GetTxMap/PutTxMap take uint64 —
// so the pure classification and clamp logic carries the behavioural pin.

// withTxMapSizeClasses and withParentSpendsSizeClasses swap a package-global
// size-class table for the duration of one test and restore it afterwards.
//
// The tables are global mutable state that the pools were built from at init, so
// a test that shrinks one must always restore it or it corrupts every later test
// in the package. Routing every swap through these helpers keeps that restore in
// one place rather than relying on each test remembering its own t.Cleanup.
// Shrinking is safe because the pools are indexed by position and every
// surviving index still resolves to a real pool. Callers must not use
// t.Parallel().
func withTxMapSizeClasses(t *testing.T, classes []uint32) {
	t.Helper()

	orig := txMapSizeClasses
	txMapSizeClasses = classes

	t.Cleanup(func() { txMapSizeClasses = orig })
}

func withParentSpendsSizeClasses(t *testing.T, classes []uint64) {
	t.Helper()

	orig := parentSpendsSizeClasses
	parentSpendsSizeClasses = classes

	t.Cleanup(func() { parentSpendsSizeClasses = orig })
}

// TestTxMapClassIdx64Bit pins the size-class classification on 64-bit counts:
// everything at or below the largest class resolves to a pool class, and
// every count above it — including counts beyond uint32, which previously
// could not even be expressed — resolves to the allocate-fresh path.
func TestTxMapClassIdx64Bit(t *testing.T) {
	largestClass := uint64(txMapSizeClasses[len(txMapSizeClasses)-1])

	require.Equal(t, 0, txMapClassIdxFor(0))
	require.Equal(t, 0, txMapClassIdxFor(1))
	require.Equal(t, len(txMapSizeClasses)-1, txMapClassIdxFor(largestClass))

	for _, n := range []uint64{largestClass + 1, math.MaxUint32, math.MaxUint32 + 1, 1 << 40, math.MaxUint64} {
		require.Equal(t, -1, txMapClassIdxFor(n), "count %d must take the allocate-fresh path", n)
	}
}

// TestTxMapAllocHintClamp pins the preallocation-hint bound: any count above
// the largest pooled size class caps the hint at that class (preallocation only
// — the map resizes on insert), everything else passes through unchanged.
func TestTxMapAllocHintClamp(t *testing.T) {
	largestClass := txMapSizeClasses[len(txMapSizeClasses)-1]

	require.Equal(t, uint32(0), txMapAllocHint(0))
	require.Equal(t, uint32(1<<20), txMapAllocHint(1<<20))
	require.Equal(t, largestClass, txMapAllocHint(uint64(largestClass)))

	for _, n := range []uint64{uint64(largestClass) + 1, math.MaxUint32, math.MaxUint32 + 1, 1 << 40, math.MaxUint64} {
		require.Equal(t, largestClass, txMapAllocHint(n), "count %d must be bounded to the largest class", n)
	}
}

// TestTxMapAllocHintAvoidsConstructorOverflow pins the bound below the point
// where the map constructor's own per-bucket arithmetic wraps. It computes
// per-bucket size as (hint + hint/5) in uint32, so any hint above
// txMapConstructorWrapThreshold overflows and preallocates an arbitrary amount
// unrelated to the count (math.MaxUint32 wraps to ~859M entries).
//
// Asserted three ways, because the first two alone would be self-referential:
// the 1/5 headroom below is transcribed from the dependency, so checking the
// constant against that transcription proves only that the transcription is
// self-consistent. The third check observes the real constructor, so a
// dependency bump that changes the factor fails here instead of leaving the
// constant quietly wrong.
func TestTxMapAllocHintAvoidsConstructorOverflow(t *testing.T) {
	// The threshold really is the last value the transcribed arithmetic holds
	// exactly, and one past it wraps.
	for _, tc := range []struct {
		hint  uint32
		exact bool
	}{
		{hint: uint32(txMapConstructorWrapThreshold), exact: true},
		{hint: uint32(txMapConstructorWrapThreshold) + 1, exact: false},
		{hint: math.MaxUint32, exact: false},
	} {
		got := uint64(tc.hint) + uint64(tc.hint)/5
		require.Equal(t, tc.exact, got == uint64(tc.hint+tc.hint/5),
			"hint %d: expected exact=%v for the constructor's (hint + hint/5)", tc.hint, tc.exact)
	}

	// Every hint this package can produce stays at or below that threshold.
	for _, n := range []uint64{0, 1 << 20, uint64(txMapSizeClasses[len(txMapSizeClasses)-1]), math.MaxUint32, 1 << 40, math.MaxUint64} {
		require.LessOrEqual(t, uint64(txMapAllocHint(n)), txMapConstructorWrapThreshold,
			"hint for count %d must stay below the constructor's wrap point", n)
	}

	// The headroom factor really is 1/5, observed rather than transcribed. The
	// threshold above cannot be probed directly (constructing at that size costs
	// tens of GB), but the factor that determines it can: with a single bucket the
	// constructor's per-bucket size is the whole (hint + hint/5), so a fresh map's
	// capacity exposes the factor for a few hundred KB.
	const (
		probeHint    uint32 = 1000
		probeBuckets uint16 = 1
	)

	transcribed := (probeHint + probeHint/5) / uint32(probeBuckets)
	want := swiss.NewMap[chainhash.Hash, uint64](transcribed).Capacity()
	got := txmap.NewSplitSwissMapUint64(probeHint, probeBuckets).Map()[0].Map().Capacity()

	require.Equal(t, want, got,
		"go-tx-map's per-bucket headroom factor no longer matches the 1/5 transcribed here, so txMapConstructorWrapThreshold is stale")
}

// TestParentSpendsAllocHint pins the parent-spends preallocation bound, which
// exists for the same reason as txMapAllocHint: NewSplitSyncedParentMap
// preallocates eagerly per bucket and computes uint32((n + n/5) / nrOfBuckets),
// so an unbounded hint asks for an unbounded allocation and can truncate.
func TestParentSpendsAllocHint(t *testing.T) {
	largestClass := parentSpendsSizeClasses[len(parentSpendsSizeClasses)-1]

	require.Equal(t, uint64(0), parentSpendsAllocHint(0))
	require.Equal(t, uint64(1<<20), parentSpendsAllocHint(1<<20))
	require.Equal(t, largestClass, parentSpendsAllocHint(largestClass))

	for _, n := range []uint64{largestClass + 1, 1 << 40, math.MaxUint64} {
		require.Equal(t, largestClass, parentSpendsAllocHint(n), "count %d must be bounded", n)
	}

	// Every bounded hint must survive the constructor's per-bucket arithmetic
	// without truncating to an arbitrary value.
	for _, n := range []uint64{0, 1 << 20, largestClass, largestClass + 1, math.MaxUint64} {
		h := parentSpendsAllocHint(n)
		perBucket := (h + h/5) / uint64(parentSpendsBuckets)
		require.Equal(t, perBucket, uint64(uint32(perBucket)),
			"hint %d (from count %d) truncates in the constructor", h, n)
	}
}

// TestTxMapPoolRoundTrip64BitKey pins Get/Put with the 64-bit key on pooled
// (cheap) size classes: the map must be usable and returnable with the same
// uint64 count a block carries.
func TestTxMapPoolRoundTrip64BitKey(t *testing.T) {
	for _, n := range []uint64{0, 1, 1 << 12, 1 << 20} {
		m := GetTxMap(n)
		require.NotNil(t, m, "count %d", n)

		hash := chainhash.HashH([]byte{byte(n), byte(n >> 8), 0xab})
		require.NoError(t, m.Put(hash, 1))
		require.True(t, m.Exists(hash))

		PutTxMap(m, n)
	}
}

// TestCheckDuplicateTransactionsUsesFullCount drives the real duplicate-check
// path with a pooled-size count and verifies the pooled map is released with
// the same 64-bit key. The >2^32 seam is covered by the classification and
// clamp tests above (materialising such a map costs tens of GB — see the
// header comment).
func TestCheckDuplicateTransactionsUsesFullCount(t *testing.T) {
	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	for i := byte(1); i <= 3; i++ {
		hash := chainhash.HashH([]byte{i, 0xdd})
		require.NoError(t, subtree.AddNode(hash, 1, 0))
	}

	block := &Block{
		Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
		TransactionCount: 4,
		SubtreeSlices:    []*subtreepkg.Subtree{subtree},
	}

	require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 4, nil))

	// The map is sized from the loaded body, not the claimed count.
	require.Equal(t, uint64(4), block.txMapCount)

	// Hold the map across release and assert PutTxMap cleared it, which only
	// happens on the pooled branch. This proves the release key still resolves
	// to a pool class — it does NOT pin the exact key, since at this size any
	// key at or below the largest class also pools and clears. The exact key is
	// pinned by the b.txMapCount assertion above, which is the value releaseTxMap
	// passes.
	//
	// Observing the map after handing it back to a package-global sync.Pool is
	// only sound because this test owns it until it returns: the assertion must
	// stay immediately after releaseTxMap, and this test must never be given
	// t.Parallel() (the package convention anyway), or a concurrent GetTxMap on
	// the 4K class could take the map first.
	pooled, ok := block.txMap.(*txmap.SplitSwissMapUint64)
	require.True(t, ok)
	require.Equal(t, 3, pooled.Length())

	block.releaseTxMap()
	require.Nil(t, block.txMap)
	require.Equal(t, 0, pooled.Length(), "released map must be cleared and pooled, not dropped")
}

// TestCheckDuplicateTransactionsAboveUint32 is the end-to-end pin issue 1428
// asked for: a block claiming more than math.MaxUint32 transactions must run the
// real duplicate-check path without returning the retryable processing error the
// old uint32 narrowing produced.
//
// It costs a four-node map. The claimed count no longer sizes anything — the
// hint comes from the loaded body (txMapEntryCount) — so nothing here
// materialises a large allocation, which is what previously made this scenario
// untestable (the CI-memory trap of issue 1051).
//
// The size classes are shrunk for the duration anyway, so that if the sizing
// ever regresses to the claimed count this test stays cheap and fails on the
// assertion instead of trying to preallocate for 2^32 entries. txMapPools is
// built from the original classes at init, so every surviving index still
// resolves to a real pool. The Cleanup restore is mandatory and this test must
// not run in parallel, since it mutates package state.
func TestCheckDuplicateTransactionsAboveUint32(t *testing.T) {
	withTxMapSizeClasses(t, []uint32{1 << 12})

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	for i := byte(1); i <= 3; i++ {
		hash := chainhash.HashH([]byte{i, 0x77})
		require.NoError(t, subtree.AddNode(hash, 1, 0))
	}

	block := &Block{
		Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
		TransactionCount: math.MaxUint32 + 1,
		SubtreeSlices:    []*subtreepkg.Subtree{subtree},
	}

	require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 4, nil),
		"a count above MaxUint32 must not fail the duplicate check")

	// Sized from the four loaded nodes, not from the claimed 2^32+1.
	require.Equal(t, uint64(4), block.txMapCount)

	pooled, ok := block.txMap.(*txmap.SplitSwissMapUint64)
	require.True(t, ok)
	require.Equal(t, 3, pooled.Length())

	block.releaseTxMap()
	require.Equal(t, 0, pooled.Length(), "released map must be cleared and pooled, not dropped")
}

// TestGetParentSpendsMapBoundsPreallocation pins the bound at its call site, not
// just in the helper: GetParentSpendsMap must preallocate from the bounded hint
// for a count above every size class. Observed through the underlying swiss map's
// Capacity(), which on a freshly built map is the preallocation limit.
//
// The size classes are shrunk so the over-max branch is reachable cheaply; the
// unbounded reference below is deliberately sized to cost tens of MB rather than
// the hundreds of GB a realistic over-max count would ask for. parentSpendsPools
// is built from the original classes at init, so the surviving index still
// resolves to a real pool. Cleanup restore is mandatory; no t.Parallel().
func TestGetParentSpendsMapBoundsPreallocation(t *testing.T) {
	withParentSpendsSizeClasses(t, []uint64{1 << 8})

	const overMax = 1 << 20 // above the shrunk largest class, so idx < 0

	require.Equal(t, -1, parentSpendsClassIdxFor(overMax), "count must take the allocate-fresh path")
	require.Equal(t, uint64(1<<8), parentSpendsAllocHint(overMax))

	got := GetParentSpendsMap(overMax)
	require.NotNil(t, got)

	bounded := NewSplitSyncedParentMap(parentSpendsBuckets, parentSpendsAllocHint(overMax))
	unbounded := NewSplitSyncedParentMap(parentSpendsBuckets, overMax)

	require.Equal(t, bounded.buckets[0].m.Capacity(), got.buckets[0].m.Capacity(),
		"GetParentSpendsMap must preallocate from the bounded hint")
	require.NotEqual(t, unbounded.buckets[0].m.Capacity(), got.buckets[0].m.Capacity(),
		"this assertion is only meaningful if the bounded and unbounded hints differ")

	// The map still accepts entries beyond the bounded preallocation.
	ok, err := got.SetIfNotExists(subtreepkg.Inpoint{Hash: chainhash.HashH([]byte{0x5a}), Index: 0})
	require.NoError(t, err)
	require.True(t, ok)
}

// TestParentSpendsCapacity pins the parent-spends sizing helper: it multiplies
// the body-derived node count by the assumed inputs per transaction, treats a 0
// multiplier as 1, never returns 0 (the mmap-backed table rejects a zero
// capacity), and on an overflowing operator-supplied multiplier degrades to the
// unmultiplied count rather than carrying a wrapped product into
// NewSplitSyncedParentMap, whose uint32((e + e/5) / buckets) would turn it into
// an arbitrary per-bucket size.
func TestParentSpendsCapacity(t *testing.T) {
	require.Equal(t, uint64(2_000), parentSpendsCapacity(1_000, 2))
	require.Equal(t, uint64(1_000), parentSpendsCapacity(1_000, 1))
	require.Equal(t, uint64(1_000), parentSpendsCapacity(1_000, 0), "a 0 multiplier means 1")

	require.Equal(t, uint64(1), parentSpendsCapacity(0, 2), "capacity must never be 0")
	require.Equal(t, uint64(1), parentSpendsCapacity(0, 0))

	// An overflowing multiplier degrades to the node count, not a wrapped value.
	const entryCount = 4
	require.Equal(t, uint64(entryCount), parentSpendsCapacity(entryCount, math.MaxUint64),
		"an overflowing multiplier must fall back to the unmultiplied count")
	require.Equal(t, uint64(entryCount), parentSpendsCapacity(entryCount, math.MaxUint64/2))

	// Every value the helper can return must survive the constructor's per-bucket
	// arithmetic without truncating, which is the property the fallback protects.
	for _, m := range []uint64{0, 1, 2, 16, math.MaxUint64 / 2, math.MaxUint64} {
		c := parentSpendsCapacity(entryCount, m)
		require.Equal(t, (c+c/5)/uint64(parentSpendsBuckets), uint64(uint32((c+c/5)/uint64(parentSpendsBuckets))),
			"capacity %d (multiplier %d) truncates in the constructor", c, m)
	}
}

// TestTxMapEntryCountIgnoresClaimedCount pins that the sizing hint is taken from
// the loaded body and never from the peer-supplied TransactionCount: a block
// claiming a huge count while carrying few (or no) subtree nodes must size for
// what it carries (issue 1501).
func TestTxMapEntryCountIgnoresClaimedCount(t *testing.T) {
	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(chainhash.HashH([]byte{0x01}), 1, 0))

	// A block that claims 2^40 transactions but carries two nodes.
	block := &Block{TransactionCount: 1 << 40, SubtreeSlices: []*subtreepkg.Subtree{subtree}}
	require.Equal(t, uint64(2), block.txMapEntryCount())

	// The zero-subtree shape from issue 1501: nothing loaded, so nothing sized.
	empty := &Block{TransactionCount: math.MaxUint64}
	require.Equal(t, uint64(0), empty.txMapEntryCount())

	// A nil slice entry must not panic or inflate the count.
	withNil := &Block{TransactionCount: 1 << 40, SubtreeSlices: []*subtreepkg.Subtree{nil, subtree}}
	require.Equal(t, uint64(2), withNil.txMapEntryCount())
}
