package model

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// CheckSubtreeShape enforces the body-shape rules the top-level merkle composition
// depends on, for ONE subtree.
//
// Extracted from Block.CheckMerkleRoot so the quick-validation binding pass can run the
// identical arithmetic without a second implementation of it. The binding pass walks the
// block one chunk of subtrees at a time and never holds them all, so it cannot call
// CheckMerkleRoot; before this extraction it carried its own partial copy of the
// size-uniformity rule, which is exactly the kind of duplicate that drifts
// (bitcoin-sv/teranode#4838).
//
// The rules, and why each exists:
//
//   - The FIRST subtree's leaf count must be a power of two. Lifting a short final
//     subtree to the first subtree's height only reproduces the canonical flat merkle
//     root when that height is exact, so a non-power-of-two first subtree lets a peer
//     craft a body whose composed root a canonical SV Node would not agree with.
//   - Every non-final subtree must match the first exactly. Only the final one may be
//     short, and it may never be longer.
//
// NOT REACHABLE FOR A ONE-SUBTREE BLOCK, on either caller, and that is a rule about the
// call sites rather than about this function. Block.CheckMerkleRoot returns the single
// subtree's root directly from its len(hashes) == 1 branch, BEFORE any of this; a single
// subtree with a non-power-of-two leaf count is therefore a legitimate body shape today
// and applying the guard to it would reject genuine blocks. Keep that early exit where it
// is, and do not fold it in here.
//
// label names the calling site for the error text, which is otherwise identical between
// the two callers.
func CheckSubtreeShape(label string, idx, length, targetLength int, isLast bool) error {
	if idx == 0 && !subtreepkg.IsPowerOfTwo(targetLength) {
		return errors.NewBlockCorruptError(
			"[%s] first subtree leaf count is not a power of two: %d",
			label, targetLength,
		)
	}

	if !isLast && length != targetLength {
		return errors.NewBlockCorruptError(
			"[%s] only the final subtree may be incomplete (index %d, length %d, targetLength %d)",
			label, idx, length, targetLength,
		)
	}

	if isLast && length > targetLength {
		return errors.NewBlockCorruptError(
			"[%s] final subtree exceeds first subtree size (length %d, targetLength %d)",
			label, length, targetLength,
		)
	}

	return nil
}

// ComposeSubtreeRootsToMerkleRoot builds the top-level merkle root from one root per
// subtree, rejecting a repeated root on the way.
//
// The duplicate-root scan is part of the composition and not a separate courtesy check:
// two identical subtree roots mean two identical subtrees, i.e. every transaction in
// them duplicated, which is the CVE-2012-2459 shape at subtree granularity. It is
// checked here because this is the one place that has all the roots in hand.
//
// Callers must pass the roots already adjusted for their position — the first subtree's
// root with the coinbase substituted for the placeholder, and a short final subtree's
// root lifted to the first subtree's height. Those two adjustments need the subtree
// objects, which this function deliberately does not take, so that a caller holding only
// 32 bytes per subtree can still compose.
func ComposeSubtreeRootsToMerkleRoot(label string, roots []chainhash.Hash) (*chainhash.Hash, error) {
	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(roots))
	if err != nil {
		return nil, errors.NewProcessingError("[%s] error creating new root tree", label, err)
	}

	seen := make(map[chainhash.Hash]struct{}, len(roots))

	for _, hash := range roots {
		if _, dup := seen[hash]; dup {
			return nil, errors.NewBlockCorruptError("[%s] duplicate subtree root hash in top-level merkle tree: %s", label, hash.String())
		}

		seen[hash] = struct{}{}

		if err = st.AddNode(hash, 1, 0); err != nil {
			return nil, errors.NewProcessingError("[%s] error adding node to root tree", label, err)
		}
	}

	calculated := st.RootHash()

	root, err := chainhash.NewHash(calculated[:])
	if err != nil {
		return nil, errors.NewProcessingError("[%s] error creating calculated merkle root hash", label, err)
	}

	return root, nil
}
