package netsync

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// ParentsFirst returns the indexes 0..len(hashes)-1 reordered so that every
// tx comes after any of its parents that are also in hashes. SV Node parks a
// child that arrives before its parent in its orphan pool, which is bounded
// and expires entries, so the child can be lost before the parent turns up.
// Sending the parent first avoids relying on that pool.
//
// Input order is otherwise kept as far as the dependencies allow: a parent
// is pulled forward to just ahead of the first tx that spends it, so a tx
// unrelated to either can end up behind the moved parent. For [C, X, P],
// where C spends P, the result is [P, C, X].
//
// parents(i) returns the parent tx hashes of hashes[i]; parents outside the
// set are ignored. Duplicate hashes keep only their first position as a
// dependency target. Cycles cannot occur between valid txs, but a tx is
// marked visited before its parents are walked, so malformed input still
// terminates.
//
// The walk recurses once per parent hop, so its depth is bounded by the
// longest in-set chain, at most len(hashes): maxRequestedTxns (50,000) on the
// announce path and maxRebroadcastInventory (4096) on the rebroadcast path.
// Revisit if either grows by an order of magnitude.
func ParentsFirst(hashes []chainhash.Hash, parents func(i int) []chainhash.Hash) []int {
	index := make(map[chainhash.Hash]int, len(hashes))

	for i, hash := range hashes {
		if _, dup := index[hash]; !dup {
			index[hash] = i
		}
	}

	order := make([]int, 0, len(hashes))
	visited := make([]bool, len(hashes))

	var visit func(i int)

	visit = func(i int) {
		if visited[i] {
			return
		}

		visited[i] = true

		for _, parent := range parents(i) {
			if j, ok := index[parent]; ok {
				visit(j)
			}
		}

		order = append(order, i)
	}

	for i := range hashes {
		visit(i)
	}

	return order
}
