package utxoset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// unit returns n transaction sizes of 1.
func unit(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = 1
	}

	return out
}

// requireWellFormed checks what every plan must hold, whatever the list: each transaction in
// exactly one chunk, positions ascending in a chunk, the bounds respected, and every parent
// either in the same chunk or in a chunk the child's chunk lists as a dependency, which is an
// earlier one.
func requireWellFormed(t *testing.T, plan *chunkPlan, parents [][]int, sizes []int, maxTxs, maxBytes int) {
	t.Helper()

	seen := make([]int, len(parents))

	for _, ch := range plan.chunks {
		require.NotEmpty(t, ch.pos)
		require.LessOrEqual(t, len(ch.pos), maxTxs, "chunk %d is over the transaction bound", ch.idx)

		bytes := 0

		for k, i := range ch.pos {
			seen[i]++
			bytes += sizes[i]

			if k > 0 {
				require.Greater(t, i, ch.pos[k-1])
			}

			for _, p := range parents[i] {
				if plan.chunkOf[p] != ch.idx {
					require.Contains(t, ch.deps, plan.chunkOf[p])
					require.Less(t, plan.chunkOf[p], ch.idx, "a chunk waits only on an earlier one")
				}
			}
		}

		if len(ch.pos) > 1 {
			require.LessOrEqual(t, bytes, maxBytes, "chunk %d is over the byte bound", ch.idx)
		}
	}

	for i, n := range seen {
		require.Equal(t, 1, n, "transaction %d is in %d chunks", i, n)
	}
}

func TestPlanFamilyChunksPacksWholeFamilies(t *testing.T) {
	// Families: {0,1,2} a chain, {3}, {4,5} a pair, {6}.
	parents := [][]int{nil, {0}, {1}, nil, nil, {4}, nil}

	plan := planFamilyChunks(parents, unit(7), 4, 1<<20)
	requireWellFormed(t, plan, parents, unit(7), 4, 1<<20)

	require.Equal(t, 4, plan.families)
	require.Equal(t, 3, plan.largest)
	require.Zero(t, plan.cutEdges, "every family fits in a chunk, so no edge is cut")
	require.Len(t, plan.chunks, 2)
	require.Equal(t, []int{0, 1, 2, 3}, plan.chunks[0].pos)
	require.Equal(t, []int{4, 5, 6}, plan.chunks[1].pos)
	require.Empty(t, plan.chunks[1].deps)
}

func TestPlanFamilyChunksNeverSplitsAFamilyThatFits(t *testing.T) {
	// A family of 3 that would straddle a chunk of 4 moves whole to the next chunk.
	parents := [][]int{nil, nil, {0}, {2}}
	plan := planFamilyChunks(parents, unit(4), 3, 1<<20)
	requireWellFormed(t, plan, parents, unit(4), 3, 1<<20)

	// Family {0,2,3} and singleton {1}: the family fills chunk 0, the singleton is chunk 1.
	require.Zero(t, plan.cutEdges)
	require.Equal(t, []int{0, 2, 3}, plan.chunks[0].pos)
	require.Equal(t, []int{1}, plan.chunks[1].pos)
}

func TestPlanFamilyChunksCutsAChainAtTheBound(t *testing.T) {
	const n = 10

	parents := make([][]int, n)
	for i := 1; i < n; i++ {
		parents[i] = []int{i - 1}
	}

	plan := planFamilyChunks(parents, unit(n), 4, 1<<20)
	requireWellFormed(t, plan, parents, unit(n), 4, 1<<20)

	require.Len(t, plan.chunks, 3)
	require.Equal(t, 2, plan.cutEdges, "a chain of ten in chunks of four is cut twice")
	require.Equal(t, []int{0}, plan.chunks[1].deps)
	require.Equal(t, []int{1}, plan.chunks[2].deps)
}

func TestPlanFamilyChunksSharesTheLastSliceWithASmallFamily(t *testing.T) {
	// A chain of 5 in chunks of 4 leaves one transaction in the last slice, which a following
	// singleton fills.
	parents := [][]int{nil, {0}, {1}, {2}, {3}, nil}
	plan := planFamilyChunks(parents, unit(6), 4, 1<<20)
	requireWellFormed(t, plan, parents, unit(6), 4, 1<<20)

	require.Len(t, plan.chunks, 2)
	require.Equal(t, []int{4, 5}, plan.chunks[1].pos)
}

func TestPlanFamilyChunksFollowsChainsWhenThatCutsFewerEdges(t *testing.T) {
	// One root, then two chains of six interleaved in list order: A1 B1 A2 B2 and so on. In list
	// order a run of four takes parts of both chains and cuts each chain again and again.
	// Following one chain to its end before the other keeps each chain whole between cuts.
	const links = 6

	parents := [][]int{nil}

	for k := 0; k < links; k++ {
		for chain := 0; chain < 2; chain++ {
			if k == 0 {
				parents = append(parents, []int{0})
			} else {
				parents = append(parents, []int{len(parents) - 2})
			}
		}
	}

	sizes := unit(len(parents))

	fam := make([]int, len(parents))
	for i := range fam {
		fam[i] = i
	}

	byList := cutsOf(cutRuns(fam, sizes, 4, 1<<20), parents)
	byChain := cutsOf(cutRuns(chainOrder(fam, parents), sizes, 4, 1<<20), parents)
	require.Less(t, byChain, byList, "the chain-following order must cut fewer edges here")

	plan := planFamilyChunks(parents, sizes, 4, 1<<20)
	requireWellFormed(t, plan, parents, sizes, 4, 1<<20)
	require.Equal(t, byChain, plan.cutEdges, "the plan keeps the better of the two orders")
}

func TestPlanFamilyChunksRespectsTheByteBound(t *testing.T) {
	parents := [][]int{nil, nil, nil, nil}
	sizes := []int{6, 6, 6, 40}

	plan := planFamilyChunks(parents, sizes, 100, 12)
	requireWellFormed(t, plan, parents, sizes, 100, 12)

	require.Equal(t, []int{0, 1}, plan.chunks[0].pos)
	require.Equal(t, []int{2}, plan.chunks[1].pos)
	require.Equal(t, []int{3}, plan.chunks[2].pos, "a transaction over the bound still gets a chunk to itself")
}

func TestPlanFamilyChunksEmptyAndSingle(t *testing.T) {
	require.Empty(t, planFamilyChunks(nil, nil, 4, 100).chunks)

	plan := planFamilyChunks([][]int{nil}, unit(1), 4, 100)
	require.Len(t, plan.chunks, 1)
	require.Zero(t, plan.cutEdges)
}
