package model

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// CheckSubtreeSlicesForDuplicateTxs is a MANDATORY consensus security floor —
// not an optimization. It enforces the CVE-2012-2459 duplicate-transaction
// check on any code path that holds the block's subtree slices in memory but
// does not go through Block.Valid (which runs the full pooled/disk-backed
// checkDuplicateTransactions). Every such path MUST call it before creating or
// spending UTXOs.
//
// WHY IT IS REQUIRED (do not remove or gate it on "the block is trusted"):
// the merkle root CANNOT detect a CVE-2012-2459 duplicate. The merkle tree's
// duplicate-last-node-when-odd rule means a block whose trailing transactions
// are duplicated in a specific pattern produces the SAME merkle root — and
// therefore the same block header and the same block hash — as the honest
// block. So CheckMerkleRoot passing does NOT imply the transaction list is
// duplicate-free; this scan is the only thing that catches it.
//
// WHY IT STILL APPLIES BELOW THE CHECKPOINT: "below checkpoint" trusts the
// chain of block HASHES (the header chain is checkpoint-anchored), NOT the
// block BODIES, which are still downloaded from untrusted peers. A CVE-2012-2459
// mutation produces a body that hashes to the same, trusted block hash while
// carrying a repeated transaction — so a below-checkpoint block being "safe"
// (its hash is on the trusted chain) does not make a peer-supplied body safe.
// Skipping this on a below-checkpoint fast path is a cheap, peer-triggerable
// sync-DoS / state-corruption vector, because the mutated body reuses the
// honest block's proof-of-work (no mining required).
//
// It scans every node hash across all subtree slices into a plain Go map,
// skipping the coinbase placeholder at position [0][0], and returns a
// BlockInvalidError on the first duplicate. It is intentionally lightweight —
// O(N) in the total transaction count with one map allocation — so it is cheap
// enough to run unconditionally on every slice-only path. Callers on the full
// validation path (Block.Valid) instead use the pooled/disk-backed
// checkDuplicateTransactions; this helper is the independent floor for the
// paths that skip it.
func CheckSubtreeSlicesForDuplicateTxs(slices []*subtreepkg.Subtree) error {
	// Estimate total node count for the initial map capacity to avoid rehashing.
	totalNodes := 0
	for _, st := range slices {
		if st != nil {
			totalNodes += len(st.Nodes)
		}
	}

	seen := make(map[chainhash.Hash]struct{}, totalNodes)

	for subIdx, subtree := range slices {
		if subtree == nil {
			continue
		}
		for txIdx, node := range subtree.Nodes {
			// Skip the coinbase placeholder (all-0xFF hash) that occupies the
			// first position of the first subtree. It is not a real transaction
			// hash and must not be deduped against itself.
			if subIdx == 0 && txIdx == 0 && node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				continue
			}

			if _, exists := seen[node.Hash]; exists {
				return errors.NewBlockInvalidError(
					"[CheckSubtreeSlicesForDuplicateTxs] block contains duplicate transaction %s (CVE-2012-2459)",
					node.Hash.String(),
				)
			}
			seen[node.Hash] = struct{}{}
		}
	}

	return nil
}
