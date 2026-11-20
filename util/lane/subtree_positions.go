package lane

import (
	"encoding/binary"
	"math"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// The subtree lane positions file sits beside a subtree's meta file (go-subtree Meta, which has
// no version and is not changed). For each node of the subtree, in node order, it carries the
// transaction's own lane position and one lane position per distinct parent, in the parent order
// of that node's TxInpoints in the meta file. The file is laid out in columns, so it decodes into
// three flat arrays, the same shape SubtreePositions holds in memory, reading each byte once:
//
//	[4]    magic "LNSP"
//	[1]    format version, 1
//	[32]   subtree root hash
//	[4]    node count n, uint32 LE
//	[8*n]  own position of each node, uint64 LE
//	[4*n]  parent count of each node, uint32 LE
//	[8*p]  every node's parent positions, node by node, where p is the sum of the counts
//
// The coinbase placeholder, node 0 of a block's first subtree, has no position yet: it is written
// as own position 0 with no parents. No other node may carry a zero or invalid position.
//
// A reader that needs both the meta and the positions should fetch the two files concurrently;
// decoding the positions reads each byte once and allocates the result and its three arrays,
// however many nodes the subtree holds.

const (
	// SubtreePositionsVersion is the only version of the subtree lane positions file.
	SubtreePositionsVersion = byte(1)

	subtreePositionsMagic = "LNSP"

	subtreePositionsHead = len(subtreePositionsMagic) + 1 + chainhash.HashSize + 4

	// perNodeFixed is each node's own position and parent count.
	perNodeFixed = PositionSize + 4
)

// SubtreePositions holds a subtree's lane positions in three flat arrays indexed by node, so a
// million-node subtree costs three array allocations, not one per node. Node i's own position is Own[i]
// and its parent positions are Parents[ParentStart[i]:ParentStart[i+1]]. ParentStart has one more
// entry than Own. Build one with NewSubtreePositions and AppendNode, or by decoding a file.
type SubtreePositions struct {
	RootHash    chainhash.Hash
	Own         []uint64
	ParentStart []uint32
	Parents     []uint64
}

// NewSubtreePositions starts an empty positions set for a subtree, with room for nodes nodes and
// parents parent positions in total.
func NewSubtreePositions(root chainhash.Hash, nodes, parents int) *SubtreePositions {
	start := make([]uint32, 1, nodes+1)

	return &SubtreePositions{
		RootHash:    root,
		Own:         make([]uint64, 0, nodes),
		ParentStart: start,
		Parents:     make([]uint64, 0, parents),
	}
}

// AppendNode adds the next node's own position and parent positions, copying the parents.
func (s *SubtreePositions) AppendNode(own uint64, parents []uint64) error {
	if uint64(len(s.Parents))+uint64(len(parents)) > math.MaxUint32 {
		return errors.NewProcessingError("subtree lane positions: more than 2^32 parent positions")
	}

	s.Own = append(s.Own, own)
	s.Parents = append(s.Parents, parents...)
	s.ParentStart = append(s.ParentStart, uint32(len(s.Parents))) //nolint:gosec // bounded above

	return nil
}

// Len is the number of nodes.
func (s *SubtreePositions) Len() int {
	return len(s.Own)
}

// NodeParents returns node i's parent positions, a view into Parents capped so an append cannot
// run into the next node's.
func (s *SubtreePositions) NodeParents(i int) []uint64 {
	lo, hi := s.ParentStart[i], s.ParentStart[i+1]

	return s.Parents[lo:hi:hi]
}

// checkShape validates the arrays' shape and every position: all valid, except the coinbase
// placeholder form (own 0, no parents) which only node 0 may have.
func (s *SubtreePositions) checkShape() error {
	if len(s.ParentStart) != len(s.Own)+1 || s.ParentStart[0] != 0 || int(s.ParentStart[len(s.Own)]) != len(s.Parents) {
		return errors.NewProcessingError("subtree lane positions: offsets do not match %d nodes and %d parents", len(s.Own), len(s.Parents))
	}

	for i, own := range s.Own {
		lo, hi := s.ParentStart[i], s.ParentStart[i+1]
		if hi < lo {
			return errors.NewProcessingError("subtree node %d: parent offsets run backwards", i)
		}

		if own == None && lo == hi && i == 0 {
			continue
		}

		if err := CheckPosition(own); err != nil {
			return errors.NewProcessingError("subtree node %d own lane position", i, err)
		}
	}

	for j, p := range s.Parents {
		if err := CheckPosition(p); err != nil {
			return errors.NewProcessingError("subtree parent lane position %d", j, err)
		}
	}

	return nil
}

// EncodeSubtreePositions serializes s in one exactly sized allocation. It checks every position
// first, so it never writes a file its own decoder would refuse.
func EncodeSubtreePositions(s *SubtreePositions) ([]byte, error) {
	if err := s.checkShape(); err != nil {
		return nil, err
	}

	n := len(s.Own)

	buf := make([]byte, 0, subtreePositionsHead+n*perNodeFixed+len(s.Parents)*PositionSize)
	buf = append(buf, subtreePositionsMagic...)
	buf = append(buf, SubtreePositionsVersion)
	buf = append(buf, s.RootHash[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(n)) //nolint:gosec // a subtree holds far fewer than 2^32 nodes
	buf = AppendPositions(buf, s.Own)

	for i := 0; i < n; i++ {
		buf = binary.LittleEndian.AppendUint32(buf, s.ParentStart[i+1]-s.ParentStart[i])
	}

	return AppendPositions(buf, s.Parents), nil
}

// DecodeSubtreePositions parses a subtree lane positions file and checks every position. It reads
// each byte once and allocates the result and its three arrays, each sized only after the buffer is
// known to hold what it claims: the own and offset arrays from the checked node count, the parent
// array from the counted total once the parent block's length matches it. It does not compare the result with the subtree; CheckSubtreePositions
// does.
func DecodeSubtreePositions(b []byte) (*SubtreePositions, error) {
	if len(b) < subtreePositionsHead {
		return nil, errors.NewProcessingError("subtree lane positions file is %d bytes, too short", len(b))
	}

	if string(b[:len(subtreePositionsMagic)]) != subtreePositionsMagic {
		return nil, errors.NewProcessingError("subtree lane positions file does not start with %q", subtreePositionsMagic)
	}

	if v := b[len(subtreePositionsMagic)]; v != SubtreePositionsVersion {
		return nil, errors.NewProcessingError("subtree lane positions file has unsupported version %d", v)
	}

	n := uint64(binary.LittleEndian.Uint32(b[subtreePositionsHead-4:]))
	rest := b[subtreePositionsHead:]

	if n*perNodeFixed > uint64(len(rest)) {
		return nil, errors.NewProcessingError("subtree lane positions file claims %d nodes in %d bytes", n, len(rest))
	}

	ownBytes := rest[:n*PositionSize]
	countBytes := rest[n*PositionSize : n*perNodeFixed]
	parentBytes := rest[n*perNodeFixed:]

	// The own and offset arrays are sized from n, which the buffer has already been checked to
	// hold. One pass over the own and count columns fills both and totals the parents.
	s := &SubtreePositions{
		Own:         make([]uint64, n),
		ParentStart: make([]uint32, n+1),
	}
	copy(s.RootHash[:], b[len(subtreePositionsMagic)+1:])

	var total uint64

	for i := range s.Own {
		s.Own[i] = binary.LittleEndian.Uint64(ownBytes[i*PositionSize:])
		total += uint64(binary.LittleEndian.Uint32(countBytes[i*4:]))

		// The offsets are 32-bit: refuse a total they cannot hold before it wraps. It runs before
		// the parent block's length is compared, so a total past 2^32 is refused here even when
		// the block's length would match it.
		if total > math.MaxUint32 {
			return nil, errors.NewProcessingError("subtree lane positions file: counts sum past 2^32 parents")
		}

		s.ParentStart[i+1] = uint32(total)
	}

	// The parent block must hold exactly the counted positions. Compare by division, so a large
	// total cannot wrap a multiplication into a match.
	if uint64(len(parentBytes))%PositionSize != 0 || uint64(len(parentBytes))/PositionSize != total {
		return nil, errors.NewProcessingError("subtree lane positions file: counts sum to %d parents, %d bytes remain", total, len(parentBytes))
	}

	s.Parents = make([]uint64, total)
	for j := range s.Parents {
		s.Parents[j] = binary.LittleEndian.Uint64(parentBytes[j*PositionSize:])
	}

	if err := s.checkShape(); err != nil {
		return nil, err
	}

	return s, nil
}

// CheckSubtreePositions checks a decoded positions file against the subtree and its meta: the
// same root hash and node count, one parent position per distinct parent of each node, and the
// position-less form only on a coinbase placeholder at node 0.
func CheckSubtreePositions(s *SubtreePositions, st *subtreepkg.Subtree, meta *subtreepkg.Meta) error {
	if s == nil || st == nil || meta == nil {
		return errors.NewProcessingError("subtree lane positions: positions, subtree and meta are required")
	}

	if root := st.RootHash(); root == nil || *root != s.RootHash {
		return errors.NewProcessingError("subtree lane positions are for subtree %s, not this one", s.RootHash.String())
	}

	if s.Len() != st.Length() || len(meta.TxInpoints) < st.Length() {
		return errors.NewProcessingError("subtree lane positions hold %d nodes, the subtree %d and its meta %d", s.Len(), st.Length(), len(meta.TxInpoints))
	}

	for i := 0; i < s.Len(); i++ {
		placeholder := i == 0 && st.Nodes[0].Hash == subtreepkg.CoinbasePlaceholderHashValue
		parents := s.NodeParents(i)

		if placeholder != (s.Own[i] == None) {
			return errors.NewProcessingError("subtree node %d: only the coinbase placeholder has no lane position, and it has none", i)
		}

		if placeholder {
			if len(parents) != 0 {
				return errors.NewProcessingError("subtree node 0, the coinbase placeholder, has parent positions")
			}

			continue
		}

		p := TxPositions{Own: s.Own[i], Parents: parents}
		if err := p.Check(meta.TxInpoints[i].ParentTxHashes); err != nil {
			return errors.NewProcessingError("subtree node %d (%s)", i, st.Nodes[i].Hash.String(), err)
		}
	}

	return nil
}
