package backfill

import (
	"bytes"
	"context"
	"sort"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// SourceBlock is one block of the frozen source: its subtrees in block order, each with its
// transaction IDs in subtree node order. The coinbase placeholder may stand at the first node.
type SourceBlock struct {
	BlockID  uint32
	Height   uint32
	Hash     chainhash.Hash
	Coinbase chainhash.Hash
	Subtrees []SourceSubtree
}

// SourceSubtree is one subtree of a source block.
type SourceSubtree struct {
	Hash  chainhash.Hash
	TxIDs []chainhash.Hash
}

// Source is everything the numbering of one segment depends on, frozen: the source tip, the
// main-chain blocks to number in rising height, and the readable transactions no main-chain block
// holds. The tip may be later than the blocks this segment numbers.
type Source struct {
	SourceTipBlockID uint32
	SourceTipHash    chainhash.Hash
	Blocks           []SourceBlock
	Suffix           []chainhash.Hash
}

// Facts answers what the frozen stores hold about a transaction. Both calls must give the same
// answer every time for the same frozen source; a resumed numbering relies on it.
type Facts interface {
	// Readable reports whether any copy of the transaction a lookup or a restore can reach is
	// still stored: an output, a journal or undo copy, an identity, containment or preserved row,
	// or the equivalent record of another store. Only a readable transaction is numbered.
	Readable(txid chainhash.Hash) (bool, error)

	// Existing returns a valid lane position the transaction already holds, from a store copy or
	// an earlier segment's manifest. A transaction with one keeps it and is not numbered again.
	Existing(txid chainhash.Hash) (uint64, bool, error)
}

// Seen records the transactions a numbering has placed, to refuse one placed twice. A conversion
// over a store too large for memory supplies a disk-backed one. Each call to Number needs an empty
// one, a resume included: Number places the blocks already written again as it checks them.
type Seen interface {
	// Add records txid and reports whether it was already recorded.
	Add(txid chainhash.Hash) (bool, error)
}

// MemorySeen is a Seen held in memory.
type MemorySeen map[chainhash.Hash]struct{}

// Add implements Seen.
func (s MemorySeen) Add(txid chainhash.Hash) (bool, error) {
	if _, ok := s[txid]; ok {
		return true, nil
	}

	s[txid] = struct{}{}

	return false, nil
}

// BlockOrder returns the transactions of a source block that take new backfill positions, in the
// order they take them: subtree by subtree, node by node, leaving out the coinbase placeholder, the
// coinbase, every transaction no copy holds any more, and every transaction that already has a
// position. It is the one definition of a block's order: numbering and its resumption use it.
// ImportBlock does not: it places whatever the assignment holds, in subtree and node order.
func BlockOrder(b SourceBlock, facts Facts) ([]chainhash.Hash, error) {
	var out []chainhash.Hash

	for si, st := range b.Subtrees {
		for ni, txid := range st.TxIDs {
			if txid == subtreepkg.CoinbasePlaceholderHashValue || txid == b.Coinbase {
				continue
			}

			take, err := takesPosition(txid, facts)
			if err != nil {
				return nil, errors.NewProcessingError("block %d subtree %d node %d", b.BlockID, si, ni, err)
			}

			if take {
				out = append(out, txid)
			}
		}
	}

	return out, nil
}

// SuffixOrder returns the suffix transactions that take new positions, sorted by txid bytes.
func SuffixOrder(txids []chainhash.Hash, facts Facts) ([]chainhash.Hash, error) {
	sorted := append([]chainhash.Hash(nil), txids...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i][:], sorted[j][:]) < 0 })

	out := make([]chainhash.Hash, 0, len(sorted))

	for i, txid := range sorted {
		if i > 0 && txid == sorted[i-1] {
			return nil, errors.NewProcessingError("suffix lists %s twice", txid.String())
		}

		take, err := takesPosition(txid, facts)
		if err != nil {
			return nil, errors.NewProcessingError("suffix", err)
		}

		if take {
			out = append(out, txid)
		}
	}

	return out, nil
}

func takesPosition(txid chainhash.Hash, facts Facts) (bool, error) {
	readable, err := facts.Readable(txid)
	if err != nil || !readable {
		return false, err
	}

	pos, ok, err := facts.Existing(txid)
	if err != nil {
		return false, err
	}

	if ok {
		if err = lane.CheckPosition(pos); err != nil {
			return false, errors.NewProcessingError("existing position of %s", txid.String(), err)
		}

		return false, nil
	}

	return true, nil
}

// Number writes the segment's assignment into m from the frozen source, block by block and then
// the suffix, and finishes it. Against a manifest already finished, as a crash between Finish and
// recording the segment leaves, it checks the source against everything recorded and returns the
// segment, writing nothing. Against a manifest a crash left part-written it resumes: each block
// already recorded and the source tip must match the manifest, or it fails, since a changed source
// would renumber transactions already given positions. A transaction placed twice, in two blocks or
// a block and the suffix, fails. It allocates nothing: positions follow from the manifest's first
// position and the order.
func Number(ctx context.Context, m *Manifest, src Source, facts Facts, seen Seen) (lane.ConversionSegment, error) {
	written := m.Blocks()

	if len(written) > len(src.Blocks) || (m.Finished() && len(written) != len(src.Blocks)) {
		return lane.ConversionSegment{}, errors.NewProcessingError("manifest holds %d blocks, the source %d", len(written), len(src.Blocks))
	}

	header := m.Header()
	if src.SourceTipBlockID != header.SourceTipBlockID || src.SourceTipHash != header.SourceTipHash {
		return lane.ConversionSegment{}, errors.NewProcessingError("source tip block %d (%s) differs from manifest tip block %d (%s); the source changed", src.SourceTipBlockID, src.SourceTipHash.String(), header.SourceTipBlockID, header.SourceTipHash.String())
	}

	for i, b := range src.Blocks {
		if err := ctx.Err(); err != nil {
			return lane.ConversionSegment{}, err
		}

		if i > 0 && b.Height <= src.Blocks[i-1].Height {
			return lane.ConversionSegment{}, errors.NewProcessingError("source block %d at height %d does not rise above %d", i, b.Height, src.Blocks[i-1].Height)
		}

		order, err := BlockOrder(b, facts)
		if err != nil {
			return lane.ConversionSegment{}, err
		}

		if err = placeOnce(seen, order); err != nil {
			return lane.ConversionSegment{}, err
		}

		if i < len(written) {
			if err = sameBlock(m, written[i], b, order); err != nil {
				return lane.ConversionSegment{}, err
			}

			continue
		}

		if _, err = m.AppendBlock(b.BlockID, b.Height, order); err != nil {
			return lane.ConversionSegment{}, err
		}
	}

	suffix, err := SuffixOrder(src.Suffix, facts)
	if err != nil {
		return lane.ConversionSegment{}, err
	}

	if err = placeOnce(seen, suffix); err != nil {
		return lane.ConversionSegment{}, err
	}

	if m.HasSuffix() {
		stored, _, err := m.SuffixTxIDs()
		if err != nil {
			return lane.ConversionSegment{}, err
		}

		if !sameTxIDs(stored, suffix) {
			return lane.ConversionSegment{}, errors.NewProcessingError("the suffix differs from the one already recorded; the source changed")
		}
	} else if _, err = m.AppendSuffix(suffix); err != nil {
		return lane.ConversionSegment{}, err
	}

	return m.Finish()
}

func placeOnce(seen Seen, txids []chainhash.Hash) error {
	for _, txid := range txids {
		dup, err := seen.Add(txid)
		if err != nil {
			return err
		}

		if dup {
			return errors.NewProcessingError("transaction %s is placed twice", txid.String())
		}
	}

	return nil
}

func sameBlock(m *Manifest, rec lane.ConversionBlock, b SourceBlock, order []chainhash.Hash) error {
	if rec.BlockID != b.BlockID || rec.Height != b.Height {
		return errors.NewProcessingError("manifest block %d is block %d at %d, the source has block %d at %d", rec.Seq, rec.BlockID, rec.Height, b.BlockID, b.Height)
	}

	stored, err := m.BlockTxIDs(rec.Seq)
	if err != nil {
		return err
	}

	if !sameTxIDs(stored, order) {
		return errors.NewProcessingError("manifest block %d (block %d) differs from the source; the source changed", rec.Seq, b.BlockID)
	}

	return nil
}

func sameTxIDs(a, b []chainhash.Hash) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
