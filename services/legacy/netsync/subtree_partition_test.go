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
		{name: "fall back to smaller s for non-pow2 remainder (N=7)", totalLeaves: 7, maxItems: 4, wantSubtreeSize: 2, wantK: 4, wantFinalLeaves: 1},
		{name: "two subtrees, last full", totalLeaves: 8, maxItems: 4, wantSubtreeSize: 4, wantK: 2, wantFinalLeaves: 4},
		{name: "fall back to smaller s for N=11", totalLeaves: 11, maxItems: 4, wantSubtreeSize: 2, wantK: 6, wantFinalLeaves: 1},
		{name: "three subtrees, last full", totalLeaves: 12, maxItems: 4, wantSubtreeSize: 4, wantK: 3, wantFinalLeaves: 4},
		{name: "production-sized, last 1 leaf", totalLeaves: 1048577, maxItems: 1048576, wantSubtreeSize: 1048576, wantK: 2, wantFinalLeaves: 1},
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
		})
	}
}
