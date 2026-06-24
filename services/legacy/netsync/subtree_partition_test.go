package netsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionLegacyBlock(t *testing.T) {
	tests := []struct {
		name            string
		totalLeaves     int
		maxItems        int
		wantSubtreeSize int
		wantK           int
		wantFinalLeaves int
		wantErr         bool
	}{
		{name: "single subtree, 1 leaf", totalLeaves: 1, maxItems: 4, wantSubtreeSize: 1, wantK: 1, wantFinalLeaves: 1},
		{name: "single subtree, partial", totalLeaves: 3, maxItems: 4, wantSubtreeSize: 3, wantK: 1, wantFinalLeaves: 3},
		{name: "single subtree, exact match", totalLeaves: 4, maxItems: 4, wantSubtreeSize: 4, wantK: 1, wantFinalLeaves: 4},
		{name: "two subtrees, last 1 leaf", totalLeaves: 5, maxItems: 4, wantSubtreeSize: 4, wantK: 2, wantFinalLeaves: 1},
		{name: "two subtrees, last 2 leaves", totalLeaves: 6, maxItems: 4, wantSubtreeSize: 4, wantK: 2, wantFinalLeaves: 2},
		{name: "two subtrees, last 3 leaves (non-pow2)", totalLeaves: 7, maxItems: 4, wantSubtreeSize: 4, wantK: 2, wantFinalLeaves: 3},
		{name: "two subtrees, last full", totalLeaves: 8, maxItems: 4, wantSubtreeSize: 4, wantK: 2, wantFinalLeaves: 4},
		{name: "three subtrees, last 3 leaves (non-pow2)", totalLeaves: 11, maxItems: 4, wantSubtreeSize: 4, wantK: 3, wantFinalLeaves: 3},
		{name: "three subtrees, last full", totalLeaves: 12, maxItems: 4, wantSubtreeSize: 4, wantK: 3, wantFinalLeaves: 4},
		{name: "production-sized, last 1 leaf", totalLeaves: 1048577, maxItems: 1048576, wantSubtreeSize: 1048576, wantK: 2, wantFinalLeaves: 1},

		// Issue #901: the 900,679-tx production block. With maxItems = 32,768 this
		// previously bottomed out at subtreeSize = 2, K = 450,340. Now it must stay
		// at subtreeSize = maxItems with a single non-power-of-two final subtree.
		{name: "issue #901 production case (900679 txs, maxItems 32768)", totalLeaves: 900679, maxItems: 32768, wantSubtreeSize: 32768, wantK: 28, wantFinalLeaves: 15943},

		// Other adversarial counts that used to degenerate to small subtreeSize.
		{name: "adversarial N=21 (binary 10101), maxItems=8", totalLeaves: 21, maxItems: 8, wantSubtreeSize: 8, wantK: 3, wantFinalLeaves: 5},
		{name: "adversarial N=15 (binary 1111), maxItems=8", totalLeaves: 15, maxItems: 8, wantSubtreeSize: 8, wantK: 2, wantFinalLeaves: 7},
		{name: "adversarial N=23 (binary 10111), maxItems=8", totalLeaves: 23, maxItems: 8, wantSubtreeSize: 8, wantK: 3, wantFinalLeaves: 7},

		{name: "error: totalLeaves zero", totalLeaves: 0, maxItems: 4, wantErr: true},
		{name: "error: totalLeaves negative", totalLeaves: -1, maxItems: 4, wantErr: true},
		{name: "error: maxItems zero", totalLeaves: 5, maxItems: 0, wantErr: true},
		{name: "error: maxItems negative", totalLeaves: 5, maxItems: -2, wantErr: true},
		{name: "error: maxItems not power of two", totalLeaves: 5, maxItems: 3, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, k, r, err := partitionLegacyBlock(tc.totalLeaves, tc.maxItems)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantSubtreeSize, s, "subtreeSize")
			require.Equal(t, tc.wantK, k, "K")
			require.Equal(t, tc.wantFinalLeaves, r, "finalLeafCount")

			require.Equal(t, tc.totalLeaves, (tc.wantK-1)*tc.wantSubtreeSize+tc.wantFinalLeaves,
				"partition should account for every leaf")

			// Final subtree leaf count must be in [1, subtreeSize].
			require.GreaterOrEqual(t, r, 1, "finalLeafCount >= 1")
			require.LessOrEqual(t, r, s, "finalLeafCount <= subtreeSize")
		})
	}
}

// TestPartitionLegacyBlock_BoundedK proves that the partition never produces
// more than ceil(totalLeaves / maxItems) subtrees, regardless of how adversarial
// the leaf count is. Pre-#901 the algorithm could explode this to N/2 subtrees.
func TestPartitionLegacyBlock_BoundedK(t *testing.T) {
	maxItems := 32768

	// Sample a range of leaf counts including the production case and other
	// numbers chosen to have many low-bit-set patterns that broke the previous
	// algorithm.
	cases := []int{900679, 1_000_000, 2_097_151, 524_287, 524_289, 1023, 1025}

	for _, total := range cases {
		s, k, r, err := partitionLegacyBlock(total, maxItems)
		require.NoError(t, err)

		expectedK := (total + maxItems - 1) / maxItems
		if total <= maxItems {
			expectedK = 1
		}

		require.Equal(t, expectedK, k, "K must equal ceil(total/maxItems) for total=%d", total)
		require.LessOrEqual(t, s, maxItems, "subtreeSize must not exceed maxItems for total=%d", total)
		require.Equal(t, total, (k-1)*s+r, "partition must cover every leaf for total=%d", total)
	}
}
