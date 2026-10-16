package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestListItemOptions pins listItemOptions, which DefaultSpendAndCreateMulti uses to give each
// transaction of a list its options once per list instead of on every parse of a call's options.
func TestListItemOptions(t *testing.T) {
	newList := func(subtreeIdxs []int) *SpendAndCreateMultiList {
		locked := true

		return &SpendAndCreateMultiList{
			Options: &CreateOptions{
				MinedBlockInfos: []MinedBlockInfo{{BlockID: 7, BlockHeight: 70}},
				TxIDs:           []chainhash.Hash{{1}, {2}, {3}},
				SubtreeIdxs:     subtreeIdxs,
				BelowCheckpoint: true,
				IsCoinbase:      &locked,
				Locked:          true,
				IgnoreFlags:     IgnoreFlags{IgnoreLocked: true, SkipUTXOHashCheck: true},
			},
			TxIDs: []chainhash.Hash{{1}, {2}, {3}},
		}
	}

	for _, tc := range []struct {
		name        string
		subtreeIdxs []int
	}{
		{name: "with a subtree index per transaction", subtreeIdxs: []int{4, 5, 6}},
		{name: "without subtree indexes", subtreeIdxs: nil},
	} {
		t.Run(tc.name+": the same options as ItemOptions with the list's txid", func(t *testing.T) {
			list := newList(tc.subtreeIdxs)
			fns := listItemOptions(list)
			require.Len(t, fns, 3)

			for i, fn := range fns {
				got, err := ParseCreateOptions(fn)
				require.NoError(t, err)

				want := list.Options.ItemOptions(i)
				want.TxID = &list.TxIDs[i]

				require.Equal(t, want, got, "transaction %d", i)
			}
		})
	}

	t.Run("an option appended after a transaction's own reaches no other transaction", func(t *testing.T) {
		list := newList([]int{4, 5, 6})
		fns := listItemOptions(list)

		first, err := ParseCreateOptions(fns[0], WithMinedBlockInfo(MinedBlockInfo{BlockID: 99}))
		require.NoError(t, err)
		require.Len(t, first.MinedBlockInfos, 2)

		second, err := ParseCreateOptions(fns[1])
		require.NoError(t, err)
		require.Equal(t, []MinedBlockInfo{{BlockID: 7, BlockHeight: 70, SubtreeIdx: 5}}, second.MinedBlockInfos)

		require.Equal(t, []MinedBlockInfo{{BlockID: 7, BlockHeight: 70}}, list.Options.MinedBlockInfos,
			"the list's own options must not change")
	})

	t.Run("applying a transaction's options allocates nothing", func(t *testing.T) {
		fns := listItemOptions(newList([]int{4, 5, 6}))

		var o CreateOptions

		allocs := testing.AllocsPerRun(100, func() { fns[1](&o) })
		require.Zero(t, allocs)
	})
}
