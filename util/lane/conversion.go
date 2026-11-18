package lane

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// Lanes no lease is ever granted.
const (
	// CoinbaseLane holds coinbase positions: the coinbase of a block takes its block's ID.
	CoinbaseLane uint8 = 254

	// BackfillLane holds the positions the historical conversion assigns.
	BackfillLane uint8 = 255

	// BackfillSlotSentinel is position 1 of the backfill lane. No record carries it: it marks an
	// old spent slot whose spender legacy pruning had already deleted. Conversion numbering starts
	// at BackfillFirstPosition.
	BackfillSlotSentinel uint64 = 1

	// BackfillFirstPosition is the lowest position the conversion assigns to a transaction.
	BackfillFirstPosition uint64 = 2
)

// BlockMapRow is a map row with the block it belongs to.
type BlockMapRow struct {
	BlockID uint32
	MapRow
}

// ConversionSegment is one run of the historical conversion: the positions it assigned in the
// backfill lane and the frozen source it assigned them from. Positions First to PrefixLast are its
// main-chain prefix, numbered block by block in height order; PrefixLast+1 to Last are its suffix
// (records no main-chain block of the source holds: unmined, fork-only or conflict losers). A
// segment's bounds are immutable once recorded, and a later segment starts above every earlier
// one.
type ConversionSegment struct {
	ID         uint32
	First      uint64
	PrefixLast uint64
	Last       uint64
	// SourceTipBlockID and SourceTipHash name the frozen tip the segment was assigned against.
	SourceTipBlockID uint32
	SourceTipHash    chainhash.Hash
	// ManifestDigest identifies the immutable assignment manifest the segment was numbered from.
	ManifestDigest chainhash.Hash
}

// Check validates a segment's own bounds. An empty prefix has PrefixLast = First-1; an empty
// suffix has Last = PrefixLast.
func (s ConversionSegment) Check() error {
	if s.ID == 0 {
		return errors.NewProcessingError("conversion segment ID 0 is not a segment")
	}

	if s.First < BackfillFirstPosition || s.Last > MaxPosition {
		return errors.NewProcessingError("conversion segment %d: positions %d to %d are outside %d to %d", s.ID, s.First, s.Last, BackfillFirstPosition, MaxPosition)
	}

	if s.PrefixLast+1 < s.First || s.PrefixLast > s.Last || s.Last < s.First {
		return errors.NewProcessingError("conversion segment %d: prefix end %d is not within %d to %d", s.ID, s.PrefixLast, s.First, s.Last)
	}

	return nil
}

// ConversionBlock is one main-chain block of a segment's prefix, in the segment's order: Seq
// counts from 0, Height is the block's original height, and the block's assigned transactions
// hold positions First to First+Count-1 of the backfill lane.
type ConversionBlock struct {
	Seq     uint32
	BlockID uint32
	Height  uint32
	First   uint64
	Count   uint64
}

// CheckConversionBlocks validates a segment's complete block list: sequence numbers from 0,
// strictly rising heights, distinct blocks, and stretches that tile the prefix exactly from First
// to PrefixLast with no gap or overlap. A block with no assigned transaction has Count 0.
func CheckConversionBlocks(s ConversionSegment, blocks []ConversionBlock) error {
	if err := s.Check(); err != nil {
		return err
	}

	next := s.First
	seen := make(map[uint32]struct{}, len(blocks))

	for i, b := range blocks {
		if b.Seq != uint32(i) { //nolint:gosec // a segment has far fewer than 2^32 blocks
			return errors.NewProcessingError("conversion segment %d: block %d has sequence %d", s.ID, i, b.Seq)
		}

		if i > 0 && b.Height <= blocks[i-1].Height {
			return errors.NewProcessingError("conversion segment %d: block %d at height %d does not rise above %d", s.ID, i, b.Height, blocks[i-1].Height)
		}

		if _, ok := seen[b.BlockID]; ok {
			return errors.NewProcessingError("conversion segment %d: block ID %d listed twice", s.ID, b.BlockID)
		}

		seen[b.BlockID] = struct{}{}

		if b.First != next {
			return errors.NewProcessingError("conversion segment %d: block %d starts at %d, want %d", s.ID, i, b.First, next)
		}

		if b.Count > MaxPosition-next+1 {
			return errors.NewProcessingError("conversion segment %d: block %d's %d positions run past the lane", s.ID, i, b.Count)
		}

		next += b.Count
	}

	if next != s.PrefixLast+1 {
		return errors.NewProcessingError("conversion segment %d: blocks end at %d, prefix ends at %d", s.ID, next-1, s.PrefixLast)
	}

	return nil
}
