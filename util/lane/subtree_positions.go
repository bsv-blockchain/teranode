package lane

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// The subtree lane positions file sits beside a subtree's meta file (go-subtree Meta, which has
// no version and is not changed). It carries, for each node of the subtree in node order, the
// transaction's own lane position and one lane position per distinct parent, in the parent order
// of that node's TxInpoints in the meta file:
//
//	[4]  magic "LNSP"
//	[1]  format version, 1
//	[32] subtree root hash
//	[4]  node count, uint32 LE
//	per node: [8] own position, [4] parent count uint32 LE, [8] per parent
//
// The coinbase placeholder, node 0 of a block's first subtree, has no position yet: it is written
// as own position 0 with no parents. No other node may carry a zero or invalid position.

const (
	// SubtreePositionsVersion is the only version of the subtree lane positions file.
	SubtreePositionsVersion = byte(1)

	subtreePositionsMagic = "LNSP"

	// minNodeEntry is the smallest encoded node: own position and a parent count.
	minNodeEntry = PositionSize + 4
)

// SubtreePositions is the decoded subtree lane positions file.
type SubtreePositions struct {
	RootHash chainhash.Hash
	Nodes    []TxPositions
}

// EncodeSubtreePositions serializes s. It checks every position first, so it never writes a file
// its own decoder would refuse.
func EncodeSubtreePositions(s SubtreePositions) ([]byte, error) {
	size := len(subtreePositionsMagic) + 1 + chainhash.HashSize + 4

	for i, n := range s.Nodes {
		if err := checkNode(i, n); err != nil {
			return nil, err
		}

		size += minNodeEntry + len(n.Parents)*PositionSize
	}

	buf := make([]byte, 0, size)
	buf = append(buf, subtreePositionsMagic...)
	buf = append(buf, SubtreePositionsVersion)
	buf = append(buf, s.RootHash[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s.Nodes))) //nolint:gosec // a subtree holds far fewer than 2^32 nodes

	for _, n := range s.Nodes {
		buf = AppendPosition(buf, n.Own)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(n.Parents))) //nolint:gosec // bounded by the inputs of one transaction
		buf = AppendPositions(buf, n.Parents)
	}

	return buf, nil
}

// DecodeSubtreePositions parses a subtree lane positions file and checks every position. It does
// not compare it with the subtree; CheckSubtreePositions does.
func DecodeSubtreePositions(b []byte) (SubtreePositions, error) {
	head := len(subtreePositionsMagic) + 1 + chainhash.HashSize + 4
	if len(b) < head {
		return SubtreePositions{}, errors.NewProcessingError("subtree lane positions file is %d bytes, too short", len(b))
	}

	if string(b[:len(subtreePositionsMagic)]) != subtreePositionsMagic {
		return SubtreePositions{}, errors.NewProcessingError("subtree lane positions file does not start with %q", subtreePositionsMagic)
	}

	if v := b[len(subtreePositionsMagic)]; v != SubtreePositionsVersion {
		return SubtreePositions{}, errors.NewProcessingError("subtree lane positions file has unsupported version %d", v)
	}

	var s SubtreePositions

	copy(s.RootHash[:], b[len(subtreePositionsMagic)+1:])
	n := binary.LittleEndian.Uint32(b[head-4:])
	rest := b[head:]

	if uint64(n)*minNodeEntry > uint64(len(rest)) {
		return SubtreePositions{}, errors.NewProcessingError("subtree lane positions file claims %d nodes in %d bytes", n, len(rest))
	}

	s.Nodes = make([]TxPositions, n)

	for i := range s.Nodes {
		if len(rest) < minNodeEntry {
			return SubtreePositions{}, errors.NewProcessingError("subtree lane positions file truncated at node %d", i)
		}

		s.Nodes[i].Own = binary.LittleEndian.Uint64(rest)
		k := binary.LittleEndian.Uint32(rest[PositionSize:])
		rest = rest[minNodeEntry:]

		parents, more, err := readNodeParents(rest, k, i)
		if err != nil {
			return SubtreePositions{}, err
		}

		s.Nodes[i].Parents = parents
		rest = more

		if err := checkNode(i, s.Nodes[i]); err != nil {
			return SubtreePositions{}, err
		}
	}

	if len(rest) != 0 {
		return SubtreePositions{}, errors.NewProcessingError("subtree lane positions file has %d trailing bytes", len(rest))
	}

	return s, nil
}

func readNodeParents(rest []byte, k uint32, node int) ([]uint64, []byte, error) {
	if uint64(k)*PositionSize > uint64(len(rest)) {
		return nil, nil, errors.NewProcessingError("subtree lane positions file truncated in node %d's %d parents", node, k)
	}

	if k == 0 {
		return nil, rest, nil
	}

	parents := make([]uint64, k)
	for j := range parents {
		parents[j] = binary.LittleEndian.Uint64(rest[j*PositionSize:])
	}

	return parents, rest[int(k)*PositionSize:], nil
}

// checkNode checks one node's positions: all valid, except the coinbase placeholder form (own 0,
// no parents) which only node 0 may have.
func checkNode(i int, n TxPositions) error {
	if n.Own == None && len(n.Parents) == 0 && i == 0 {
		return nil
	}

	if err := CheckPosition(n.Own); err != nil {
		return errors.NewProcessingError("subtree node %d own lane position", i, err)
	}

	for j, p := range n.Parents {
		if err := CheckPosition(p); err != nil {
			return errors.NewProcessingError("subtree node %d parent %d lane position", i, j, err)
		}
	}

	return nil
}

// CheckSubtreePositions checks a decoded positions file against the subtree and its meta: the
// same root hash and node count, one parent position per distinct parent of each node, and the
// position-less form only on a coinbase placeholder at node 0.
func CheckSubtreePositions(s SubtreePositions, st *subtreepkg.Subtree, meta *subtreepkg.Meta) error {
	if st == nil || meta == nil {
		return errors.NewProcessingError("subtree lane positions: subtree and meta are required")
	}

	if root := st.RootHash(); root == nil || *root != s.RootHash {
		return errors.NewProcessingError("subtree lane positions are for subtree %s, not this one", s.RootHash.String())
	}

	if len(s.Nodes) != st.Length() || len(meta.TxInpoints) < st.Length() {
		return errors.NewProcessingError("subtree lane positions hold %d nodes, the subtree %d and its meta %d", len(s.Nodes), st.Length(), len(meta.TxInpoints))
	}

	for i, n := range s.Nodes {
		placeholder := i == 0 && st.Nodes[0].Hash == subtreepkg.CoinbasePlaceholderHashValue

		if placeholder != (n.Own == None) {
			return errors.NewProcessingError("subtree node %d: only the coinbase placeholder has no lane position, and it has none", i)
		}

		if placeholder {
			if len(n.Parents) != 0 {
				return errors.NewProcessingError("subtree node 0, the coinbase placeholder, has parent positions")
			}

			continue
		}

		parents := meta.TxInpoints[i].ParentTxHashes
		if err := n.Check(parents); err != nil {
			return errors.NewProcessingError("subtree node %d (%s)", i, st.Nodes[i].Hash.String(), err)
		}
	}

	return nil
}
