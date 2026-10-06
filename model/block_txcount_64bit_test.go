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

// These tests pin issue 1428 — a block whose TransactionCount exceeds 2^32
// must be handled in 64-bit, not fail a uint32 narrowing with a retryable
// processing error that made block validation refetch the same
// consensus-valid block forever.
//
// Deliberately NOT tested by materialising a >2^32-entry map: newBlockTxMap
// preallocates eagerly from its hint, and a single such call costs tens of
// GB of RSS (the trap that dominated CI memory in issue 1051). The narrowing seam
// itself no longer exists at the type level — newBlockTxMap takes a uint64 — so the
// pure preallocation-cap logic carries the behavioural pin.

// TestPreallocCaps64Bit pins the preallocation bound on 64-bit counts: any
// count above the cap, including counts beyond uint32, preallocates exactly as
// much as the cap and never wraps (the map grows on insert past it).
func TestPreallocCaps64Bit(t *testing.T) {
	for _, n := range []uint64{maxTxMapPrealloc + 1, math.MaxUint32 + 1, 1 << 40, math.MaxUint64} {
		require.Equal(t, bucketCapacity(maxTxMapPrealloc, txMapBuckets), bucketCapacity(min(n, maxTxMapPrealloc), txMapBuckets), "tx count %d", n)
	}

	for _, n := range []uint64{maxParentSpendsPrealloc + 1, 1 << 40, math.MaxUint64} {
		require.Equal(t, bucketCapacity(maxParentSpendsPrealloc, parentSpendsBuckets), bucketCapacity(min(n, maxParentSpendsPrealloc), parentSpendsBuckets), "inpoint count %d", n)
	}

	// The caps stay far below the per-bucket uint32 saturation point.
	require.Less(t, bucketCapacity(maxParentSpendsPrealloc, parentSpendsBuckets), uint32(math.MaxUint32/2))
}

// TestNewBlockTxMap64BitCount pins allocation with the uint64 count a block
// carries, at cheap sizes: the map must be usable.
func TestNewBlockTxMap64BitCount(t *testing.T) {
	for _, n := range []uint64{0, 1, 1 << 12, 1 << 20} {
		m := newBlockTxMap(n)
		require.NotNil(t, m, "count %d", n)

		hash := chainhash.HashH([]byte{byte(n), byte(n >> 8), 0xab})
		require.NoError(t, m.Put(hash, 1))
		require.True(t, m.Exists(hash))
	}
}

// TestCheckDuplicateTransactionsUsesFullCount drives the real duplicate-check
// path and verifies the map is sized from the loaded body and let go on
// release. The >2^32 seam is covered by the cap tests above (materialising such a
// map costs tens of GB — see the header comment).
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

	inMemory, ok := block.txMap.(*txmap.SplitSwissMapUint64)
	require.True(t, ok)
	require.Equal(t, 3, inMemory.Length())

	block.releaseTxMap()
	require.Nil(t, block.txMap)
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
// The preallocation caps are shrunk for the duration anyway, so that if the
// sizing ever regresses to the claimed count this test stays cheap and fails on
// the assertion instead of trying to preallocate for 2^32 entries. The Cleanup
// restore is mandatory and this test must not run in parallel, since it mutates
// package state.
func TestCheckDuplicateTransactionsAboveUint32(t *testing.T) {
	withPreallocCaps(t, 1<<12, 1<<12)

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

	inMemory, ok := block.txMap.(*txmap.SplitSwissMapUint64)
	require.True(t, ok)
	require.Equal(t, 3, inMemory.Length())

	block.releaseTxMap()
	require.Nil(t, block.txMap)
}

// TestParentSpendsCapacity pins the parent-spends sizing helper: it multiplies
// the body-derived node count by the assumed inputs per transaction, treats a 0
// multiplier as 1, never returns 0 (the mmap-backed table rejects a zero
// capacity), and on an overflowing operator-supplied multiplier degrades to the
// unmultiplied count rather than carrying a wrapped product into the
// disk-backed map's FilterCapacity.
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
