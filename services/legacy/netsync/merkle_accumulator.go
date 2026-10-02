package netsync

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// merkleAccumulator computes a block's merkle root from its subtrees one at a
// time, so the caller never has to hold them all.
//
// It exists because CheckMerkleRoot (model/Block.go:2179) needs every subtree
// slice present at once, and that is the last thing forcing legacy sync to keep
// a whole block in memory. What that function actually asks each subtree for is
// only its root hash, its leaf count and its height, none of which need the
// transactions, so the computation can be fed incrementally.
//
// Every rule below is transcribed from CheckMerkleRoot and none of them is
// optional. The power-of-two guard stops a peer crafting a partition whose root
// a canonical validator would not agree with. The duplicate-root check guards
// TOP-TREE malleability — two distinct subtrees folding to the same root hash —
// which is a different mutation from CVE-2012-2459's transaction-level
// duplication inside a single subtree. TestMerkleAccumulator_MatchesCheckMerkleRoot
// asserts this produces the identical root, which is the only reason it is safe
// to use instead.
//
// The CVE-2012-2459 transaction-level floor IS present, but not here: it is
// applied per transaction as each one streams through newBlockStreamBuilder's
// AddTx, via the dedup map every caller of that constructor must now supply.
// This accumulator only ever sees a subtree once AddTx has already accepted
// every transaction in it, so it is never the thing enforcing that floor —
// its own duplicate-root check above is the top-tree analogue, not a
// substitute for it.
type merkleAccumulator struct {
	coinbaseTxID  *chainhash.Hash
	coinbaseSize  uint64
	expectedCount int
	hashes        []chainhash.Hash
	seen          map[chainhash.Hash]struct{}
	targetLength  int
	targetHeight  int
	added         int
	finished      bool
}

// newMerkleAccumulator prepares an accumulator for a block already known to hold
// subtreeCount subtrees. The count is known before any transaction arrives,
// because a block message carries its transaction count immediately after the
// 80-byte header, so the partition is decided up front rather than discovered.
func newMerkleAccumulator(subtreeCount int, coinbaseTxID *chainhash.Hash, coinbaseSize uint64) (*merkleAccumulator, error) {
	if subtreeCount <= 0 {
		return nil, errors.NewProcessingError("[merkleAccumulator] subtree count must be positive, got %d", subtreeCount)
	}

	if coinbaseTxID == nil {
		return nil, errors.NewProcessingError("[merkleAccumulator] no coinbase transaction id")
	}

	return &merkleAccumulator{
		coinbaseTxID:  coinbaseTxID,
		coinbaseSize:  coinbaseSize,
		expectedCount: subtreeCount,
		hashes:        make([]chainhash.Hash, 0, subtreeCount),
		seen:          make(map[chainhash.Hash]struct{}, subtreeCount),
	}, nil
}

// Add folds one completed subtree into the accumulation and keeps nothing but
// its root hash. isLast says whether this is the final subtree of the block,
// which the caller knows from the partition it computed up front.
func (m *merkleAccumulator) Add(st *subtreepkg.Subtree, isLast bool) error {
	if m.finished {
		return errors.NewProcessingError("[merkleAccumulator] Add called after the root was taken")
	}

	if st == nil {
		return errors.NewProcessingError("[merkleAccumulator] subtree %d of %d is nil", m.added, m.expectedCount)
	}

	if m.added >= m.expectedCount {
		return errors.NewProcessingError("[merkleAccumulator] more subtrees than the %d this block was partitioned into", m.expectedCount)
	}

	var root chainhash.Hash

	if m.added == 0 {
		// The coinbase sits in slot zero of the first subtree as a placeholder,
		// so its real transaction id has to be substituted before the first
		// subtree's root means anything.
		replaced, err := st.RootHashWithReplaceRootNode(m.coinbaseTxID, 0, m.coinbaseSize)
		if err != nil {
			return errors.NewProcessingError("[merkleAccumulator] failed replacing the coinbase placeholder in the first subtree", err)
		}

		root = *replaced

		m.targetLength = st.Length()
		m.targetHeight = st.Height
	} else {
		// Lift correctness depends on the first subtree's leaf count being a
		// power of two: that is what makes the partitioned top tree match the
		// canonical flat merkle root. Lengths like [3, 2] otherwise produce a
		// root a canonical validator rejects. This can only be checked once a
		// second subtree has actually shown up, because a single-subtree block
		// is exempt (Root() never builds a top tree for it); reaching the else
		// branch at all is proof there is one.
		if !subtreepkg.IsPowerOfTwo(m.targetLength) {
			return errors.NewBlockInvalidError("[merkleAccumulator] first subtree leaf count is not a power of two: %d", m.targetLength)
		}

		if !isLast && st.Length() != m.targetLength {
			return errors.NewBlockInvalidError("[merkleAccumulator] only the final subtree may be incomplete (index %d, length %d, target %d)",
				m.added, st.Length(), m.targetLength)
		}

		if isLast && st.Length() > m.targetLength {
			return errors.NewBlockInvalidError("[merkleAccumulator] final subtree exceeds the first subtree's size (length %d, target %d)",
				st.Length(), m.targetLength)
		}

		if isLast && st.Length() < m.targetLength {
			// Lift the short final subtree's root to the target height so it
			// occupies the slot of a same-capacity subtree. Its leaf count does
			// not have to be a power of two, because the duplicate-when-odd rule
			// already puts its own root at ceil(log2(length)).
			lifted, err := st.RootHashPadded(m.targetHeight)
			if err != nil {
				return errors.NewProcessingError("[merkleAccumulator] failed lifting the final subtree", err)
			}

			root = *lifted
		} else {
			rh := st.RootHash()
			if rh == nil {
				return errors.NewProcessingError("[merkleAccumulator] subtree %d returned a nil root hash", m.added)
			}

			root = *rh
		}
	}

	if _, dup := m.seen[root]; dup {
		return errors.NewBlockInvalidError("[merkleAccumulator] duplicate subtree root hash in the top-level merkle tree: %s", root.String())
	}

	m.seen[root] = struct{}{}
	m.hashes = append(m.hashes, root)
	m.added++

	return nil
}

// Root returns the block's merkle root. It may be called once, after every
// subtree has been added.
func (m *merkleAccumulator) Root() (*chainhash.Hash, error) {
	if m.added != m.expectedCount {
		return nil, errors.NewProcessingError("[merkleAccumulator] have %d of %d subtrees", m.added, m.expectedCount)
	}

	m.finished = true

	if len(m.hashes) == 1 {
		root := m.hashes[0]

		return &root, nil
	}

	top, err := subtreepkg.NewIncompleteTreeByLeafCount(len(m.hashes))
	if err != nil {
		return nil, errors.NewProcessingError("[merkleAccumulator] failed creating the top-level tree", err)
	}

	for _, h := range m.hashes {
		if err = top.AddNode(h, 1, 0); err != nil {
			return nil, errors.NewProcessingError("[merkleAccumulator] failed adding a subtree root to the top-level tree", err)
		}
	}

	root := top.RootHash()

	return chainhash.NewHash(root[:])
}
