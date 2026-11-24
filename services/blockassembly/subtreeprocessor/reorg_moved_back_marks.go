package subtreeprocessor

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// movedBackMarks holds what the slow reorg needs of the moved-back blocks' transactions to decide the
// two mark sets (issue 2269): a frozen set of their hashes, and one bit per position of it that a
// moveForward block sets when it mines that transaction. It replaces a Go map of hash to bool, which
// cost a sequential insert per moved-back transaction and a sequential probe per forward transaction.
//
// Build it with add, which is safe for concurrent use, then freeze it once; markForward and stillIn
// read the frozen set. A set holds the coinbase placeholder of no block.
type movedBackMarks struct {
	set   *SortedHashSet
	mined []atomic.Uint64
}

// newMovedBackMarks returns an empty builder sized for about capacity hashes; a wrong guess changes
// speed only.
func newMovedBackMarks(capacity int) *movedBackMarks {
	return &movedBackMarks{set: NewSortedHashSet(capacity)}
}

// add puts the hashes of nodes in the set, leaving out the coinbase placeholder.
func (m *movedBackMarks) add(nodes []subtreepkg.Node) {
	for i := range nodes {
		if !nodes[i].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			_ = m.set.Put(nodes[i].Hash) // only fails once frozen, which add is never called after
		}
	}
}

// freeze ends the build and sizes the mined bitmap.
func (m *movedBackMarks) freeze(workers int) error {
	if err := m.set.Freeze(workers); err != nil {
		return err
	}

	m.mined = make([]atomic.Uint64, (m.set.Positions()+63)/64)

	return nil
}

// markForward records the transactions of a moveForward block that the moved-back blocks had as mined
// forward, and returns the ones they had not, ascending: the block's share of the transactions to mark
// as on the longest chain. The coinbase placeholder is in neither. It reads the block's set on up to
// workers goroutines, a partition each, and the partitions' answers join in order.
func (m *movedBackMarks) markForward(block *SortedHashSet, workers int) []chainhash.Hash {
	parts := make([][]chainhash.Hash, block.Partitions())

	block.IterPositions(workers, func(part int, _ uint64, hash *chainhash.Hash) {
		if hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			return
		}

		pos, ok := m.set.Find(*hash)
		if !ok {
			parts[part] = append(parts[part], *hash)

			return
		}

		// load first: the word is shared by 64 hashes and most are set once
		if word, bit := &m.mined[pos/64], uint64(1)<<(pos%64); word.Load()&bit == 0 {
			word.Or(bit)
		}
	})

	return joinParts(parts)
}

// stillIn returns the moved-back transactions no moveForward block mined and txMap still holds, in no
// particular order across calls but each once: the moved-back transactions block assembly must mark as
// not on the longest chain. txMap is probed from up to workers goroutines.
func (m *movedBackMarks) stillIn(txMap interface{ Exists(chainhash.Hash) bool }, workers int) []chainhash.Hash {
	parts := make([][]chainhash.Hash, m.set.Partitions())

	m.set.IterPositions(workers, func(part int, pos uint64, hash *chainhash.Hash) {
		if m.mined[pos/64].Load()&(1<<(pos%64)) == 0 && txMap.Exists(*hash) {
			parts[part] = append(parts[part], *hash)
		}
	})

	return joinParts(parts)
}

// joinParts concatenates the per-partition lists, in partition order, into one slice sized exactly.
func joinParts(parts [][]chainhash.Hash) []chainhash.Hash {
	total := 0
	for _, p := range parts {
		total += len(p)
	}

	if total == 0 {
		return nil
	}

	out := make([]chainhash.Hash, 0, total)
	for _, p := range parts {
		out = append(out, p...)
	}

	return out
}
