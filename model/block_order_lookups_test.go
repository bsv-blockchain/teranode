package model

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// countingTxMap counts the lookups made on a block's txMap.
type countingTxMap struct {
	blockTxMap
	gets atomic.Int64
}

func (c *countingTxMap) Get(hash chainhash.Hash) (uint64, bool) {
	c.gets.Add(1)

	return c.blockTxMap.Get(hash)
}

// validOrderAndBlessed looks up parents only: a tx's own index is its position in the block.
func TestValidOrderAndBlessed_LooksUpParentsOnly(t *testing.T) {
	const size = 4

	lens := []int{4, 4, 3}

	specs := map[int]parentSpec{
		2: {[]int{1}, []uint32{0}},
		5: {[]int{1, 2}, []uint32{1, 0}},
		9: {[]int{5, 8}, []uint32{0, 0}},
	}

	block, deps, _ := buildOrderFixture(t, size, lens, specs)
	require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, nil))
	block.txMap.Freeze()

	counter := &countingTxMap{blockTxMap: block.txMap}
	block.txMap = counter

	require.NoError(t, block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, 1, nil, 2))

	// 10 txs besides the coinbase: 7 with the anchor as their one parent, and the three above
	// with 1, 2 and 2 parents
	require.Equal(t, int64(7+1+2+2), counter.gets.Load())
}

// The order verdicts hold when the first subtree is not full, so a subtree's index space is
// wider than its length.
func TestValidOrderAndBlessed_OrderWithPartialSubtrees(t *testing.T) {
	const size = 4

	lens := []int{3, 3, 2}

	scenarios := map[string]struct {
		specs   map[int]parentSpec
		wantErr string
	}{
		"valid, parents across subtrees": {
			specs: map[int]parentSpec{
				2: {[]int{1}, []uint32{0}},
				5: {[]int{1, 2}, []uint32{1, 0}},
				9: {[]int{4, 6}, []uint32{0, 0}},
			},
		},
		"parent after child, other subtree": {
			specs:   map[int]parentSpec{2: {[]int{8}, []uint32{0}}},
			wantErr: "comes before parent transaction",
		},
		"parent after child, same subtree": {
			specs:   map[int]parentSpec{4: {[]int{6}, []uint32{0}}},
			wantErr: "comes before parent transaction",
		},
	}

	for name, sc := range scenarios {
		t.Run(name, func(t *testing.T) {
			block, deps, _ := buildOrderFixture(t, size, lens, sc.specs)
			require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, nil))
			block.txMap.Freeze()

			err := block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, 1, nil, 2)
			if sc.wantErr == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, sc.wantErr)
		})
	}
}

// orderTestModes runs a test against both txMap kinds a block can be validated with: the
// in-memory node index, and the disk map (block_diskMapDirs), which pins nothing.
func orderTestModes(t *testing.T, run func(t *testing.T, dirs []string)) {
	t.Run("node index", func(t *testing.T) { run(t, nil) })
	t.Run("disk map", func(t *testing.T) { run(t, []string{t.TempDir()}) })
}

// A release of the first subtree's nodes while validOrderAndBlessed runs (unpinned on the disk
// map path) must leave the verdict retryable: a tx's own index is computed from the subtree size
// that filled the txMap, not from the subtree as it is now, which would read 0 and put every
// tx at its position within its subtree.
func TestValidOrderAndBlessed_DetachedFirstSubtreeIsNeverInvalid(t *testing.T) {
	const size = 4

	lens := []int{4, 4, 3}

	specs := map[int]parentSpec{
		2: {[]int{1}, []uint32{0}},
		5: {[]int{1, 2}, []uint32{1, 0}},
		9: {[]int{5, 8}, []uint32{0, 0}},
	}

	orderTestModes(t, func(t *testing.T, dirs []string) {
		for run := 0; run < 50; run++ {
			block, deps, _ := buildOrderFixture(t, size, lens, specs)
			require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, dirs))
			block.txMap.Freeze()

			block.SubtreeSlices[0].ReleaseNodes()

			err := block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, 4, dirs, 2)
			require.Error(t, err, "run %d: a block whose subtree was released cannot be validated", run)
			require.False(t, errors.Is(err, errors.ErrBlockInvalid), "run %d: a released subtree must not make an honest block invalid: %v", run, err)
			require.True(t, errors.Is(err, errors.ErrProcessing), "run %d: the verdict must be retryable: %v", run, err)
		}
	})
}

// The index space is the one the txMap was filled with, so a later change of the first subtree's
// capacity (a reload can give it a different, allocator-dependent cap) must not change a verdict.
func TestValidOrderAndBlessed_UsesTheSubtreeSizeTheTxMapWasFilledWith(t *testing.T) {
	const size = 4

	lens := []int{4, 4, 3}

	// tx 5 (subtree 1) spends tx 8 (subtree 2): invalid. With subtree 0 read wider, tx 5's own
	// index would land past tx 8's and the block would pass.
	specs := map[int]parentSpec{5: {[]int{8}, []uint32{0}}}

	orderTestModes(t, func(t *testing.T, dirs []string) {
		block, deps, _ := buildOrderFixture(t, size, lens, specs)
		require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, dirs))
		block.txMap.Freeze()

		first := block.SubtreeSlices[0]
		wider := make([]subtreepkg.Node, len(first.Nodes), 4*cap(first.Nodes))
		copy(wider, first.Nodes)
		first.Nodes = wider

		err := block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, 1, dirs, 2)
		require.ErrorContains(t, err, "comes before parent transaction")
	})
}

// A txMap that a caller filled itself records no size; the check then reads subtree 0 as it is,
// and a released subtree 0 (size 0) with more subtrees behind it must give a retryable error
// rather than an index space in which every subtree starts at 0.
func TestValidOrderAndBlessed_UnrecordedSizeWithReleasedFirstSubtreeIsProcessingError(t *testing.T) {
	const size = 4

	block, deps, _ := buildOrderFixture(t, size, []int{4, 4, 3}, map[int]parentSpec{5: {[]int{1}, []uint32{0}}})
	require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, nil))
	block.txMap.Freeze()

	block.txMapSubtreeSize = 0 // as a direct caller that set b.txMap itself
	block.SubtreeSlices[0].ReleaseNodes()

	err := block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, 1, nil, 2)
	require.True(t, errors.Is(err, errors.ErrProcessing), "got %v", err)
	require.ErrorContains(t, err, "its size is unknown")
}

// The duplicate check refuses to fill a txMap whose index space it cannot define: a first subtree
// of size 0 with more subtrees behind it would put every subtree at index 0.
func TestCheckDuplicateTransactions_RefusesAFirstSubtreeOfSizeZero(t *testing.T) {
	block, _, _ := buildOrderFixture(t, 4, []int{4, 4, 3}, map[int]parentSpec{})
	block.SubtreeSlices[0].Nodes = nil

	err := block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, nil)
	require.True(t, errors.Is(err, errors.ErrProcessing), "got %v", err)
	require.Nil(t, block.txMap, "no txMap is built")
	require.Zero(t, block.txMapSubtreeSize)
}

// releaseTxMap forgets the recorded size even when there is no txMap left to close (a fill that
// failed after recording it).
func TestReleaseTxMap_ForgetsTheSizeWithoutATxMap(t *testing.T) {
	block := &Block{txMapSubtreeSize: 4}
	block.releaseTxMap()
	require.Zero(t, block.txMapSubtreeSize)
}
