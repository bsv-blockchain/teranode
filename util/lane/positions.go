package lane

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// PositionSize is the encoded size of one lane position in every lane format: eight bytes,
// little-endian, the packed value bit for bit.
const PositionSize = 8

// CheckPosition returns an error unless v names a real position. Every lane format calls it on
// each required position it decodes or encodes: a zero position, or one whose seven-byte part is
// zero, is never a valid required value. Lanes 0, 254 and 255 are valid lanes.
func CheckPosition(v uint64) error {
	if !Valid(v) {
		return errors.NewProcessingError("lane position %#x is not a valid position", v)
	}

	return nil
}

// TxPositions is the lane data a transaction's metadata carries: the transaction's own lane
// position and one lane position per distinct parent, in the order of the parents in its
// TxInpoints (go-subtree), which lists each parent once. Eight bytes per distinct parent, not
// per input.
type TxPositions struct {
	Own     uint64
	Parents []uint64
}

// Check returns an error unless every position is valid and there is exactly one parent position
// per distinct parent. parentHashes is the transaction's distinct parent list; a duplicate in it
// is an error too, because the positions could then not be matched to parents one to one.
func (p TxPositions) Check(parentHashes []chainhash.Hash) error {
	if err := CheckPosition(p.Own); err != nil {
		return errors.NewProcessingError("own lane position", err)
	}

	if len(p.Parents) != len(parentHashes) {
		return errors.NewProcessingError("%d parent lane positions for %d distinct parents", len(p.Parents), len(parentHashes))
	}

	for i, v := range p.Parents {
		if err := CheckPosition(v); err != nil {
			return errors.NewProcessingError("lane position of parent %d", i, err)
		}
	}

	return CheckDistinctParents(parentHashes)
}

// CheckDistinctParents returns an error if a parent hash appears twice.
func CheckDistinctParents(parentHashes []chainhash.Hash) error {
	if len(parentHashes) < 2 {
		return nil
	}

	if len(parentHashes) <= 16 {
		for i := 1; i < len(parentHashes); i++ {
			for j := 0; j < i; j++ {
				if parentHashes[i] == parentHashes[j] {
					return errors.NewProcessingError("parent %s is listed twice", parentHashes[i].String())
				}
			}
		}

		return nil
	}

	seen := make(map[chainhash.Hash]struct{}, len(parentHashes))
	for i := range parentHashes {
		if _, ok := seen[parentHashes[i]]; ok {
			return errors.NewProcessingError("parent %s is listed twice", parentHashes[i].String())
		}

		seen[parentHashes[i]] = struct{}{}
	}

	return nil
}

// AppendPosition appends one position in its encoded form.
func AppendPosition(dst []byte, v uint64) []byte {
	return binary.LittleEndian.AppendUint64(dst, v)
}

// AppendPositions appends each position in its encoded form, with no count: every format that
// uses it takes the count from a field it already has.
func AppendPositions(dst []byte, vs []uint64) []byte {
	for _, v := range vs {
		dst = binary.LittleEndian.AppendUint64(dst, v)
	}

	return dst
}

// ReadPositions decodes n positions from the start of b, checks each one and returns them with
// the rest of b. It allocates only after it knows b holds all n.
func ReadPositions(b []byte, n int) ([]uint64, []byte, error) {
	if n < 0 || uint64(n)*PositionSize > uint64(len(b)) {
		return nil, nil, errors.NewProcessingError("lane positions truncated: want %d, have %d bytes", n, len(b))
	}

	if n == 0 {
		return nil, b, nil
	}

	vs := make([]uint64, n)
	for i := range vs {
		vs[i] = binary.LittleEndian.Uint64(b[i*PositionSize:])
		if err := CheckPosition(vs[i]); err != nil {
			return nil, nil, errors.NewProcessingError("lane position %d", i, err)
		}
	}

	return vs, b[n*PositionSize:], nil
}
