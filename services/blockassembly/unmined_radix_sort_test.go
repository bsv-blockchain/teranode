package blockassembly

import (
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
// run of equal CreatedAt.
func referenceSortUnmined(txs []*utxo.UnminedTransaction) {
	sort.SliceStable(txs, func(i, j int) bool {
		return txs[i].CreatedAt < txs[j].CreatedAt
	})

	orderEqualCreatedAtRunsParentsFirst(txs)
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
