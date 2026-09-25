package netsync

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// ParentsFirst returns the indexes 0..len(hashes)-1 reordered so that every
// tx comes after any of its parents that are also in hashes. SV Node only
// accepts a child once it has the parent, so invs must reach a peer in this
// order. Txs with no dependency between them keep their relative order.
//
// parents(i) returns the parent tx hashes of hashes[i]; parents outside the
// set are ignored. Duplicate hashes keep only their first position as a
// dependency target. Cycles cannot occur between valid txs, but a tx is
// marked visited before its parents are walked, so malformed input still
// terminates.
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
