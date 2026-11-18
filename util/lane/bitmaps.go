package lane

import (
	"encoding/binary"
	"sort"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// The two files below carry the positions a subtree took, one 64-bit Roaring bitmap per lane.
//
// The subtree lane bitmaps file sits beside a subtree's files, keyed by the subtree's hash:
//
//	[4]  magic "LNSB"
//	[1]  format version, 1
//	[32] subtree root hash
//	lanes
//
// The proof file sits beside a block's subtree files, one per block:
//
//	[4]  magic "LNPF"
//	[1]  format version, 1
//	[32] block hash
//	[4]  subtree count, uint32 LE
//	per subtree, in the block's subtree order: [32] subtree root hash, then lanes
//
// lanes is
//
//	[2]  lane count, uint16 LE, at most 256
//	per lane, rising lane number: [1] lane, [4] bitmap length uint32 LE, then the bitmap in the
//	portable 64-bit Roaring format, canonical, non-empty, positions 1 to MaxPosition.
//
// A decoder rejects any other version, a short or long buffer, lanes out of order, an empty or
// malformed bitmap and, in a proof file, a position two subtrees both claim in one lane.

const (
	// BitmapFormatVersion is the only version of both files.
	BitmapFormatVersion = byte(1)

	subtreeBitmapsMagic = "LNSB"
	proofFileMagic      = "LNPF"

	// minLaneEntry is the smallest encoded lane: lane, length and an 8-byte empty bucket count.
	minLaneEntry = 1 + 4 + 8
)

// LaneBitmaps maps a lane to the positions taken in it. A lane with no positions is absent.
type LaneBitmaps map[uint8]*roaring64.Bitmap

// SubtreeBitmaps is the positions one subtree took, by lane.
type SubtreeBitmaps struct {
	SubtreeHash chainhash.Hash
	Lanes       LaneBitmaps
}

// ProofFile is one block's subtrees, each with the positions it took by lane, in the block's
// subtree order. The subtree whose bitmap for a lane holds a position holds the transaction.
type ProofFile struct {
	BlockHash chainhash.Hash
	Subtrees  []SubtreeBitmaps
}

// EncodeSubtreeBitmaps serializes s.
func EncodeSubtreeBitmaps(s SubtreeBitmaps) ([]byte, error) {
	buf := append([]byte(subtreeBitmapsMagic), BitmapFormatVersion)
	buf = append(buf, s.SubtreeHash[:]...)

	return appendLanes(buf, s.Lanes)
}

// DecodeSubtreeBitmaps parses and checks a subtree lane bitmaps file.
func DecodeSubtreeBitmaps(b []byte) (SubtreeBitmaps, error) {
	rest, err := readHeader(b, subtreeBitmapsMagic)
	if err != nil {
		return SubtreeBitmaps{}, err
	}

	if len(rest) < chainhash.HashSize {
		return SubtreeBitmaps{}, errors.NewProcessingError("subtree lane bitmaps truncated in the subtree hash")
	}

	var s SubtreeBitmaps

	copy(s.SubtreeHash[:], rest)

	s.Lanes, rest, err = readLanes(rest[chainhash.HashSize:])
	if err != nil {
		return SubtreeBitmaps{}, errors.NewProcessingError("subtree %s", s.SubtreeHash.String(), err)
	}

	if len(rest) != 0 {
		return SubtreeBitmaps{}, errors.NewProcessingError("subtree lane bitmaps have %d trailing bytes", len(rest))
	}

	return s, nil
}

// EncodeProofFile serializes p after checking that no position is in two subtrees of one lane.
func EncodeProofFile(p ProofFile) ([]byte, error) {
	if err := checkDisjoint(p.Subtrees); err != nil {
		return nil, err
	}

	buf := append([]byte(proofFileMagic), BitmapFormatVersion)
	buf = append(buf, p.BlockHash[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(p.Subtrees))) //nolint:gosec // a block has far fewer than 2^32 subtrees

	var err error

	for i := range p.Subtrees {
		buf = append(buf, p.Subtrees[i].SubtreeHash[:]...)

		if buf, err = appendLanes(buf, p.Subtrees[i].Lanes); err != nil {
			return nil, errors.NewProcessingError("subtree %d", i, err)
		}
	}

	return buf, nil
}

// DecodeProofFile parses and checks a proof file.
func DecodeProofFile(b []byte) (ProofFile, error) {
	rest, err := readHeader(b, proofFileMagic)
	if err != nil {
		return ProofFile{}, err
	}

	if len(rest) < chainhash.HashSize+4 {
		return ProofFile{}, errors.NewProcessingError("proof file truncated in its header")
	}

	var p ProofFile

	copy(p.BlockHash[:], rest)
	n := binary.LittleEndian.Uint32(rest[chainhash.HashSize:])
	rest = rest[chainhash.HashSize+4:]

	// Each subtree is at least its hash and a lane count.
	if uint64(n)*(chainhash.HashSize+2) > uint64(len(rest)) {
		return ProofFile{}, errors.NewProcessingError("proof file claims %d subtrees in %d bytes", n, len(rest))
	}

	p.Subtrees = make([]SubtreeBitmaps, n)

	for i := range p.Subtrees {
		if len(rest) < chainhash.HashSize {
			return ProofFile{}, errors.NewProcessingError("proof file truncated in subtree %d", i)
		}

		copy(p.Subtrees[i].SubtreeHash[:], rest)

		p.Subtrees[i].Lanes, rest, err = readLanes(rest[chainhash.HashSize:])
		if err != nil {
			return ProofFile{}, errors.NewProcessingError("proof file subtree %d", i, err)
		}
	}

	if len(rest) != 0 {
		return ProofFile{}, errors.NewProcessingError("proof file has %d trailing bytes", len(rest))
	}

	if err = checkDisjoint(p.Subtrees); err != nil {
		return ProofFile{}, err
	}

	return p, nil
}

// LaneUnion merges the subtrees' bitmaps lane by lane: the positions the whole block took from
// each lane, which BuildMapRows turns into the block's rows.
func (p ProofFile) LaneUnion() LaneBitmaps {
	out := LaneBitmaps{}

	for _, s := range p.Subtrees {
		for l, bm := range s.Lanes {
			if u, ok := out[l]; ok {
				u.Or(bm)
			} else {
				out[l] = bm.Clone()
			}
		}
	}

	return out
}

// SubtreeOf returns the index of the subtree that took lane position v, or -1 if none did.
func (p ProofFile) SubtreeOf(v uint64) int {
	l, pos := Unpack(v)

	for i, s := range p.Subtrees {
		if bm, ok := s.Lanes[l]; ok && bm.Contains(pos) {
			return i
		}
	}

	return -1
}

func checkDisjoint(subtrees []SubtreeBitmaps) error {
	seen := LaneBitmaps{}

	for i, s := range subtrees {
		for l, bm := range s.Lanes {
			u, ok := seen[l]
			if !ok {
				seen[l] = bm.Clone()
				continue
			}

			if u.Intersects(bm) {
				return errors.NewProcessingError("subtree %d takes a lane %d position an earlier subtree took", i, l)
			}

			u.Or(bm)
		}
	}

	return nil
}

func readHeader(b []byte, magic string) ([]byte, error) {
	if len(b) < len(magic)+1 {
		return nil, errors.NewProcessingError("lane bitmap file is %d bytes, too short", len(b))
	}

	if string(b[:len(magic)]) != magic {
		return nil, errors.NewProcessingError("lane bitmap file does not start with %q", magic)
	}

	if b[len(magic)] != BitmapFormatVersion {
		return nil, errors.NewProcessingError("lane bitmap file %q has unsupported version %d", magic, b[len(magic)])
	}

	return b[len(magic)+1:], nil
}

func appendLanes(buf []byte, lanes LaneBitmaps) ([]byte, error) {
	keys := make([]int, 0, len(lanes))
	for l := range lanes {
		keys = append(keys, int(l))
	}

	sort.Ints(keys)

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(keys))) //nolint:gosec // at most 256

	for _, k := range keys {
		bm := lanes[uint8(k)] //nolint:gosec // k came from a uint8
		if bm == nil || bm.IsEmpty() {
			return nil, errors.NewProcessingError("lane %d has an empty bitmap", k)
		}

		if bm.Minimum() == 0 || bm.Maximum() > MaxPosition {
			return nil, errors.NewProcessingError("lane %d bitmap holds positions %d to %d, outside 1 to %d", k, bm.Minimum(), bm.Maximum(), MaxPosition)
		}

		c := bm.Clone()
		c.RunOptimize()

		data, err := c.ToBytes()
		if err != nil {
			return nil, errors.NewProcessingError("lane %d bitmap", k, err)
		}

		if uint64(len(data)) > 1<<32-1 {
			return nil, errors.NewProcessingError("lane %d bitmap is %d bytes", k, len(data))
		}

		buf = append(buf, uint8(k)) //nolint:gosec // k came from a uint8
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(data)))
		buf = append(buf, data...)
	}

	return buf, nil
}

func readLanes(b []byte) (LaneBitmaps, []byte, error) {
	if len(b) < 2 {
		return nil, nil, errors.NewProcessingError("truncated in the lane count")
	}

	n := int(binary.LittleEndian.Uint16(b))
	b = b[2:]

	if n > MaxLane+1 || n*minLaneEntry > len(b) {
		return nil, nil, errors.NewProcessingError("claims %d lanes in %d bytes", n, len(b))
	}

	lanes := make(LaneBitmaps, n)
	prev := -1

	for i := 0; i < n; i++ {
		if len(b) < 5 {
			return nil, nil, errors.NewProcessingError("truncated in lane entry %d", i)
		}

		l := int(b[0])
		size := uint64(binary.LittleEndian.Uint32(b[1:5]))
		b = b[5:]

		if l <= prev {
			return nil, nil, errors.NewProcessingError("lane %d follows lane %d", l, prev)
		}

		prev = l

		if size > uint64(len(b)) {
			return nil, nil, errors.NewProcessingError("lane %d bitmap of %d bytes in %d", l, size, len(b))
		}

		bm, err := DecodeHoleData(b[:size])
		if err != nil {
			return nil, nil, errors.NewProcessingError("lane %d", l, err)
		}

		lanes[uint8(l)] = bm //nolint:gosec // l came from a byte
		b = b[size:]
	}

	return lanes, b, nil
}
