package blockassembly

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// referenceSortUnmined is the single-threaded order the parallel sort must
// reproduce exactly: a stable sort by CreatedAt, then parents first within each
// run of equal CreatedAt, both as originally written.
func referenceSortUnmined(txs []*utxo.UnminedTransaction) {
	sort.SliceStable(txs, func(i, j int) bool {
		return txs[i].CreatedAt < txs[j].CreatedAt
	})

	for start := 0; start < len(txs); {
		end := start + 1
		for end < len(txs) && txs[end].CreatedAt == txs[start].CreatedAt {
			end++
		}

		if end-start > 1 {
			referenceOrderRunParentsFirst(txs[start:end])
		}

		start = end
	}
}

// referenceOrderRunParentsFirst is the original per-run parents-first reorder,
// kept unchanged as the order the optimised one must reproduce.
func referenceOrderRunParentsFirst(run []*utxo.UnminedTransaction) {
	n := len(run)

	// Map each hash in the run to its slot so intra-run parent edges can be found.
	pos := make(map[chainhash.Hash]int, n)
	for i, tx := range run {
		pos[tx.Node.Hash] = i
	}

	// parents[i] = slots within the run whose outputs transaction i spends.
	parents := make([][]int, n)
	for i, tx := range run {
		if tx.TxInpoints == nil {
			continue
		}

		for _, parentHash := range tx.TxInpoints.GetParentTxHashes() {
			if p, ok := pos[parentHash]; ok && p != i {
				parents[i] = append(parents[i], p)
			}
		}
	}

	// depth[i] = length of the longest in-run ancestor chain ending at i. A
	// parent always has a strictly smaller depth than its child, so ordering by
	// depth places parents before children. Computed iteratively (explicit
	// stack) to avoid deep recursion on long dependency chains.
	const (
		unvisited int8 = iota
		active
		done
	)

	depth := make([]int, n)
	state := make([]int8, n)
	stack := make([]int, 0, n)

	for i := 0; i < n; i++ {
		if state[i] != unvisited {
			continue
		}

		stack = append(stack, i)

		for len(stack) > 0 {
			v := stack[len(stack)-1]

			if state[v] == unvisited {
				state[v] = active
				for _, p := range parents[v] {
					if state[p] == unvisited {
						stack = append(stack, p)
					}
				}

				continue
			}

			stack = stack[:len(stack)-1]
			if state[v] == done {
				continue
			}

			d := 0
			for _, p := range parents[v] {
				// Parents still active indicate a cycle (impossible for a tx
				// DAG); skip them so this terminates rather than looping.
				if state[p] == done && depth[p]+1 > d {
					d = depth[p] + 1
				}
			}

			depth[v] = d
			state[v] = done
		}
	}

	// Stable-sort slot indices by depth; equal depths keep original order.
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}

	sort.SliceStable(order, func(a, b int) bool {
		return depth[order[a]] < depth[order[b]]
	})

	reordered := make([]*utxo.UnminedTransaction, n)
	for k, idx := range order {
		reordered[k] = run[idx]
	}

	copy(run, reordered)
}

// randomUnmined builds n unmined txs in a scrambled order, the way the store's
// iterator returns them. CreatedAt values collide in runs, and about half of the
// txs spend a tx created at the same time, so the parents-first reorder matters.
func randomUnmined(r *rand.Rand, n, distinctCreatedAt int) []*utxo.UnminedTransaction {
	txs := make([]*utxo.UnminedTransaction, n)
	byCreatedAt := make(map[int][]chainhash.Hash)

	for i := range txs {
		var h chainhash.Hash

		r.Read(h[:])

		createdAt := r.Intn(distinctCreatedAt)

		parents := []chainhash.Hash{minedParent}
		if siblings := byCreatedAt[createdAt]; len(siblings) > 0 && r.Intn(2) == 0 {
			parents = []chainhash.Hash{siblings[r.Intn(len(siblings))]}

			// some txs spend several txs of the same run
			for extra := r.Intn(3); extra > 0; extra-- {
				parents = append(parents, siblings[r.Intn(len(siblings))])
			}
		}

		byCreatedAt[createdAt] = append(byCreatedAt[createdAt], h)

		txs[i] = &utxo.UnminedTransaction{
			Node:       &subtree.Node{Hash: h},
			TxInpoints: multiParentInpointsPtr(parents...),
			CreatedAt:  createdAt,
		}
	}

	// children were generated after their parents; scramble so parents can come later
	r.Shuffle(len(txs), func(i, j int) { txs[i], txs[j] = txs[j], txs[i] })

	return txs
}

func requireSameOrder(t *testing.T, want, got []*utxo.UnminedTransaction) {
	t.Helper()

	require.Len(t, got, len(want))

	for i := range want {
		require.Same(t, want[i], got[i], "order differs at index %d", i)
	}
}

// TestRadixSortUnminedMatchesSingleThreadedOrder pins that the parallel radix
// reload sort produces exactly the order of the single-threaded stable sort with
// the parents-first tiebreak, whatever the worker count, including runs of equal
// CreatedAt that span every worker's share.
func TestRadixSortUnminedMatchesSingleThreadedOrder(t *testing.T) {
	r := rand.New(rand.NewSource(1))

	for _, tc := range []struct {
		n, distinct, workers int
	}{
		{0, 1, 4},
		{1, 1, 4},
		{2, 1, 4},
		{17, 3, 4},
		{1000, 10, 3},
		{1000, 1, 5}, // one run spanning every worker
		{5000, 200, 7},
		{5000, 5000, 8},
		{20000, 300, 16},
		{20000, 300, 1},    // single worker
		{20000, 70000, 64}, // more workers than useful, wide key range
	} {
		in := randomUnmined(r, tc.n, tc.distinct)

		want := append([]*utxo.UnminedTransaction(nil), in...)
		referenceSortUnmined(want)

		got := append([]*utxo.UnminedTransaction(nil), in...)
		sortUnminedByCreatedAt(got, tc.workers)

		requireSameOrder(t, want, got)
	}
}

// TestRadixSortUnminedFallsBackOnWideKeys pins the fallback: when the CreatedAt
// range and the position need more than 64 bits together, the comparison sort
// runs, with the same order.
func TestRadixSortUnminedFallsBackOnWideKeys(t *testing.T) {
	in := randomUnmined(rand.New(rand.NewSource(3)), 3000, 50)

	// stretch CreatedAt across a range wider than 64-12 bits, keeping ties
	for _, tx := range in {
		tx.CreatedAt *= 1 << 56
	}

	require.False(t, radixSortFits(in), "fixture must exceed the packed key width")

	want := append([]*utxo.UnminedTransaction(nil), in...)
	referenceSortUnmined(want)

	got := append([]*utxo.UnminedTransaction(nil), in...)
	sortUnminedByCreatedAt(got, 8)

	requireSameOrder(t, want, got)
}

// TestRadixSortUnminedSignedCreatedAt pins CreatedAt values below zero and at the
// ends of the int range: negative values take the radix path, a range spanning
// MinInt64 to MaxInt64 takes the fallback, and both give the stable sort's order.
func TestRadixSortUnminedSignedCreatedAt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shift func(createdAt int) int
		radix bool
	}{
		{"negative", func(c int) int { return c - 1_700_000_000_123 }, true},
		{"near MinInt64", func(c int) int { return math.MinInt64 + 12_345 + c }, true},
		{"near MaxInt64", func(c int) int { return math.MaxInt64 - 12_345 - c }, true},
		{"MinInt64 to MaxInt64", func(c int) int {
			if c%2 == 0 {
				return math.MinInt64 + c
			}

			return math.MaxInt64 - c
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := randomUnmined(rand.New(rand.NewSource(5)), 3000, 50)
			for _, tx := range in {
				tx.CreatedAt = tc.shift(tx.CreatedAt)
			}

			require.Equal(t, tc.radix, radixSortFits(in))

			want := append([]*utxo.UnminedTransaction(nil), in...)
			referenceSortUnmined(want)

			got := append([]*utxo.UnminedTransaction(nil), in...)
			require.Equal(t, tc.radix, sortUnminedByCreatedAt(got, 8))

			requireSameOrder(t, want, got)
		})
	}
}

// TestStableSortUnminedByCreatedAtUsesTheSameOrder pins the production entry point.
func TestStableSortUnminedByCreatedAtUsesTheSameOrder(t *testing.T) {
	in := randomUnmined(rand.New(rand.NewSource(2)), 200_000, 2_000)

	want := append([]*utxo.UnminedTransaction(nil), in...)
	referenceSortUnmined(want)

	got := append([]*utxo.UnminedTransaction(nil), in...)
	stableSortUnminedByCreatedAt(got)

	requireSameOrder(t, want, got)
}

// BenchmarkUnminedReloadSort compares the reload sort with the single-threaded
// stable sort it replaces, on a reload-shaped set: millisecond CreatedAt at about
// 1.5M txs per second, so runs of ~1,500 equal timestamps.
func BenchmarkUnminedReloadSort(b *testing.B) {
	const n = 4_000_000

	in := randomUnmined(rand.New(rand.NewSource(4)), n, n/1500)

	b.Run("stable-single-threaded", func(b *testing.B) {
		for range b.N {
			txs := append([]*utxo.UnminedTransaction(nil), in...)
			referenceSortUnmined(txs)
		}
	})

	b.Run("radix-parallel", func(b *testing.B) {
		for range b.N {
			txs := append([]*utxo.UnminedTransaction(nil), in...)
			stableSortUnminedByCreatedAt(txs)
		}
	})
}

// TestOrderRunParentsFirstEdgeCases pins the per-run reorder against the original
// on shapes the random sets rarely produce: a parent cycle (impossible for a real
// tx DAG, but handled), and two txs of one run whose hashes share their first 8
// bytes, one spending the other.
func TestOrderRunParentsFirstEdgeCases(t *testing.T) {
	mk := func(h chainhash.Hash, parents ...chainhash.Hash) *utxo.UnminedTransaction {
		if len(parents) == 0 {
			parents = []chainhash.Hash{minedParent}
		}

		return &utxo.UnminedTransaction{Node: &subtree.Node{Hash: h}, TxInpoints: multiParentInpointsPtr(parents...), CreatedAt: 7}
	}

	check := func(t *testing.T, run []*utxo.UnminedTransaction) {
		t.Helper()

		want := append([]*utxo.UnminedTransaction(nil), run...)
		referenceOrderRunParentsFirst(want)

		got := append([]*utxo.UnminedTransaction(nil), run...)
		orderEqualCreatedAtRunsParentsFirst(got)

		requireSameOrder(t, want, got)
	}

	t.Run("cycle", func(t *testing.T) {
		a, b, c := txHash(1), txHash(2), txHash(3)
		check(t, []*utxo.UnminedTransaction{mk(a, b), mk(b, a), mk(c, a)})
	})

	t.Run("hash prefix collision", func(t *testing.T) {
		parent, child, other := txHash(9), txHash(9), txHash(4)
		child[31] = 1 // same first 8 bytes as parent, different tx

		check(t, []*utxo.UnminedTransaction{mk(other, child), mk(child, parent), mk(parent)})
	})

	t.Run("parent not in the run, sharing a prefix with one in it", func(t *testing.T) {
		inRun, outside, child := txHash(5), txHash(5), txHash(6)
		outside[31] = 2

		check(t, []*utxo.UnminedTransaction{mk(child, outside), mk(inRun)})
	})
}
