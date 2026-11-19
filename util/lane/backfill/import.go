package backfill

import (
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// Assignment is an explicit, frozen txid-to-position assignment: the backfill positions a manifest
// records, the existing positions the stores already hold, and the coinbase positions. The import
// reads positions from it and from nowhere else.
type Assignment interface {
	Position(txid chainhash.Hash) (uint64, bool)
}

// MapAssignment is an Assignment held in memory.
type MapAssignment map[chainhash.Hash]uint64

// Position implements Assignment.
func (a MapAssignment) Position(txid chainhash.Hash) (uint64, bool) {
	p, ok := a[txid]

	return p, ok
}

// AddBlock adds a manifest block record's assignment: the transaction at index i takes position
// First+i of the backfill lane. A transaction already present with another position is an error.
func (a MapAssignment) AddBlock(rec lane.ConversionBlock, txids []chainhash.Hash) error {
	if uint64(len(txids)) != rec.Count {
		return errors.NewProcessingError("manifest block %d holds %d txids, its record says %d", rec.Seq, len(txids), rec.Count)
	}

	return a.addRun(rec.First, txids)
}

// AddSuffix adds a manifest's suffix assignment.
func (a MapAssignment) AddSuffix(first uint64, txids []chainhash.Hash) error {
	return a.addRun(first, txids)
}

func (a MapAssignment) addRun(first uint64, txids []chainhash.Hash) error {
	// Position 0 needs no rule here: Add refuses it for the first txid before anything is added.
	if first > lane.MaxPosition || uint64(len(txids)) > lane.MaxPosition-first+1 {
		return errors.NewProcessingError("a run of %d txids from position %d does not fit 1 to %d", len(txids), first, lane.MaxPosition)
	}

	for i, txid := range txids {
		if err := a.Add(txid, lane.Pack(lane.BackfillLane, first+uint64(i))); err != nil {
			return err
		}
	}

	return nil
}

// Add records one transaction's position, refusing an invalid position or a second, different one.
func (a MapAssignment) Add(txid chainhash.Hash, position uint64) error {
	if err := lane.CheckPosition(position); err != nil {
		return errors.NewProcessingError("position of %s", txid.String(), err)
	}

	if p, ok := a[txid]; ok && p != position {
		return errors.NewProcessingError("%s is assigned %#x and %#x", txid.String(), p, position)
	}

	a[txid] = position

	return nil
}

// CoinbasePosition is the position of the coinbase of the block with blockID, in the coinbase
// lane, for a coinbase that has none yet. An already positioned coinbase keeps the position it has.
// Block ID 0 cannot be a position.
func CoinbasePosition(blockID uint32) (uint64, error) {
	if blockID == 0 {
		return lane.None, errors.NewProcessingError("block ID 0 cannot be a coinbase lane position")
	}

	return lane.Pack(lane.CoinbaseLane, uint64(blockID)), nil
}

// ImportedBlock is what the import produces for one historical block: its map rows, its proof file
// and how many of its transactions the assignment placed.
type ImportedBlock struct {
	BlockID uint32
	Rows    []lane.MapRow
	Proof   lane.ProofFile
	// Members is the number of the block's transactions placed: those the assignment holds, less
	// a duplicate coinbase whose coinbase-lane position names another block. A historical block
	// may have fewer than it contains, since a fully removed transaction needs no position, so this
	// is not a block count and must never stand in for one at a normal block gate.
	Members int
}

// ImportBlock builds a historical block's map rows and proof file from an explicit assignment:
// each transaction of each subtree the assignment holds is placed at its position, in its lane, in
// that subtree's bitmap. A transaction it does not hold is left out. It reads no store, decoder,
// lease or bitmap, and fails if any transaction appears twice in the block, held or not, or if two
// of its transactions share a position.
//
// A coinbase whose assigned position is in the coinbase lane but names another block is left out:
// that coinbase is the same transaction as a later block's (BSV has two such historical pairs,
// the BIP30 exceptions), the assignment is keyed by txid, and the position belongs to the other
// block, which places it.
func ImportBlock(b SourceBlock, assign Assignment) (ImportedBlock, error) {
	out := ImportedBlock{BlockID: b.BlockID, Proof: lane.ProofFile{BlockHash: b.Hash}}

	total := 0
	for _, st := range b.Subtrees {
		total += len(st.TxIDs)
	}

	seen := make(map[chainhash.Hash]struct{}, total)
	placed := map[uint64]chainhash.Hash{}

	for si, st := range b.Subtrees {
		lanes := lane.LaneBitmaps{}

		for ni, txid := range st.TxIDs {
			if txid == subtreepkg.CoinbasePlaceholderHashValue {
				txid = b.Coinbase
			}

			if _, dup := seen[txid]; dup {
				return ImportedBlock{}, errors.NewProcessingError("block %d holds %s twice", b.BlockID, txid.String())
			}

			seen[txid] = struct{}{}

			pos, ok := assign.Position(txid)
			if !ok {
				continue
			}

			if err := lane.CheckPosition(pos); err != nil {
				return ImportedBlock{}, errors.NewProcessingError("block %d subtree %d node %d", b.BlockID, si, ni, err)
			}

			// The placeholder was replaced by the coinbase above, so this is the coinbase in either form.
			if txid == b.Coinbase {
				if l, p := lane.Unpack(pos); l == lane.CoinbaseLane && p != uint64(b.BlockID) {
					continue
				}
			}

			if other, dup := placed[pos]; dup {
				return ImportedBlock{}, errors.NewProcessingError("block %d: %s and %s share position %#x", b.BlockID, other.String(), txid.String(), pos)
			}

			placed[pos] = txid

			l, p := lane.Unpack(pos)
			bm, ok := lanes[l]

			if !ok {
				bm = roaring64.New()
				lanes[l] = bm
			}

			bm.Add(p)
			out.Members++
		}

		out.Proof.Subtrees = append(out.Proof.Subtrees, lane.SubtreeBitmaps{SubtreeHash: st.Hash, Lanes: lanes})
	}

	union := out.Proof.LaneUnion()

	for l := 0; l <= lane.MaxLane; l++ {
		bm, ok := union[uint8(l)] //nolint:gosec // l is at most 255
		if !ok {
			continue
		}

		rows, err := lane.BuildMapRows(uint8(l), bm) //nolint:gosec // l is at most 255
		if err != nil {
			return ImportedBlock{}, errors.NewProcessingError("block %d", b.BlockID, err)
		}

		out.Rows = append(out.Rows, rows...)
	}

	return out, nil
}

// CheckBackfillStretch verifies an imported block against its manifest record: the block is the
// record's block, the record holds Count txids, and each of them sits in a subtree whose bitmap
// holds the position the manifest assigned it, First+i for the txid at index i. So every position
// of the stretch, First to First+Count-1, is in the import. It checks subtrees, not nodes: two
// transactions of one subtree whose positions were swapped pass. Positions outside the stretch,
// which an earlier segment gave, are not this record's to check.
func CheckBackfillStretch(imported ImportedBlock, rec lane.ConversionBlock, txids []chainhash.Hash, b SourceBlock) error {
	if imported.BlockID != rec.BlockID || b.BlockID != rec.BlockID {
		return errors.NewProcessingError("manifest block %d is block %d, not %d", rec.Seq, rec.BlockID, imported.BlockID)
	}

	if uint64(len(txids)) != rec.Count {
		return errors.NewProcessingError("manifest block %d holds %d txids, its record says %d", rec.Seq, len(txids), rec.Count)
	}

	// Each assigned transaction sits in the subtree whose bitmap holds its position.
	where := map[chainhash.Hash]int{}

	for si, st := range b.Subtrees {
		for _, txid := range st.TxIDs {
			where[txid] = si
		}
	}

	for i, txid := range txids {
		si, ok := where[txid]
		if !ok {
			return errors.NewProcessingError("block %d does not hold manifest transaction %s", rec.BlockID, txid.String())
		}

		var bm *roaring64.Bitmap
		if si < len(imported.Proof.Subtrees) {
			bm = imported.Proof.Subtrees[si].Lanes[lane.BackfillLane]
		}

		if bm == nil || !bm.Contains(rec.First+uint64(i)) {
			return errors.NewProcessingError("block %d: %s is not in the subtree holding its position", rec.BlockID, txid.String())
		}
	}

	return nil
}
