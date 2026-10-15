package model

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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
