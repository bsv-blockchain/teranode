package util

import (
	"fmt"

	"github.com/libsv/go-bt/v2/chainhash"
)

func GetMerkleProofForCoinbase(subtrees []*Subtree) ([]*chainhash.Hash, error) {
	if len(subtrees) == 0 {
		return nil, fmt.Errorf("no subtrees available")
	}

	merkleProof, err := subtrees[0].GetMerkleProof(0)
	if err != nil {
		return nil, fmt.Errorf("failed creating merkle proof for subtree: %v", err)
	}

	// Create a new tree with the subtreeHashes of the subtrees
	topTree := NewTreeByLeafCount(CeilPowerOfTwo(len(subtrees)))
	for _, subtree := range subtrees {
		err = topTree.AddNode(subtree.RootHash(), subtree.Fees, subtree.SizeInBytes)
		if err != nil {
			return nil, err
		}
	}

	topMerkleProof, err := topTree.GetMerkleProof(0)
	if err != nil {
		return nil, fmt.Errorf("failed creating merkle proofs for toptree: %v", err)
	}

	return append(merkleProof, topMerkleProof...), nil
}
