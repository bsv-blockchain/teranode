package lane

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// subtreeWithMeta builds a subtree whose first node is the coinbase placeholder, then two
// transactions: one with parents a and b, one with no parents.
func subtreeWithMeta(t *testing.T) (*subtreepkg.Subtree, *subtreepkg.Meta) {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(chainhash.Hash{0x11}, 1, 1))
	require.NoError(t, st.AddNode(chainhash.Hash{0x12}, 1, 1))

	meta := subtreepkg.NewSubtreeMeta(st)
	meta.TxInpoints[1] = subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{{0xA}, {0xB}}, []uint32{1, 0, 2, 0, 1})
	meta.TxInpoints[2] = subtreepkg.NewTxInpoints()

	return st, meta
}

// positionsOf builds a positions set node by node.
func positionsOf(t *testing.T, root chainhash.Hash, nodes ...TxPositions) *SubtreePositions {
	t.Helper()

	s := NewSubtreePositions(root, len(nodes), 0)
	for _, n := range nodes {
		require.NoError(t, s.AppendNode(n.Own, n.Parents))
	}

	return s
}

func samplePositions(t *testing.T, st *subtreepkg.Subtree) *SubtreePositions {
	t.Helper()

	return positionsOf(t, *st.RootHash(),
		TxPositions{},
		TxPositions{Own: Pack(7, 100), Parents: []uint64{Pack(255, 2), Pack(0, 1)}},
		TxPositions{Own: Pack(7, 101)},
	)
}

func TestSubtreePositionsRoundTrip(t *testing.T) {
	st, meta := subtreeWithMeta(t)
	s := samplePositions(t, st)

	require.Equal(t, 3, s.Len())
	require.Equal(t, []uint64{Pack(255, 2), Pack(0, 1)}, s.NodeParents(1))
	require.Empty(t, s.NodeParents(2))

	b, err := EncodeSubtreePositions(s)
	require.NoError(t, err)

	// The columns: three own positions, three parent counts, then the two parent positions.
	head := subtreePositionsHead
	require.Equal(t, "LNSP", string(b[:4]))
	require.Equal(t, SubtreePositionsVersion, b[4])
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(b[head-4:]))
	require.Equal(t, AppendPositions(nil, []uint64{0, Pack(7, 100), Pack(7, 101)}), b[head:head+24])
	require.Equal(t, []byte{0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}, b[head+24:head+36])
	require.Equal(t, AppendPositions(nil, []uint64{Pack(255, 2), Pack(0, 1)}), b[head+36:])

	got, err := DecodeSubtreePositions(b)
	require.NoError(t, err)
	require.Equal(t, s.RootHash, got.RootHash)
	require.Equal(t, s.Own, got.Own)
	require.Equal(t, s.ParentStart, got.ParentStart)
	require.Equal(t, s.Parents, got.Parents)
	require.NoError(t, CheckSubtreePositions(got, st, meta))

	// A node's parents are a capped view: appending to one cannot overwrite the next node's.
	view := got.NodeParents(1)
	require.Equal(t, len(view), cap(view))
}

// Decoding costs four allocations, the result and its three arrays, however many nodes.
func TestDecodeSubtreePositionsAllocatesPerSubtreeNotPerNode(t *testing.T) {
	s := NewSubtreePositions(chainhash.Hash{1}, 10_000, 10_000)
	for i := 0; i < 10_000; i++ {
		require.NoError(t, s.AppendNode(Pack(3, uint64(i)+1), []uint64{Pack(4, uint64(i)+1)}))
	}

	b, err := EncodeSubtreePositions(s)
	require.NoError(t, err)

	allocs := testing.AllocsPerRun(10, func() {
		if _, err := DecodeSubtreePositions(b); err != nil {
			panic(err)
		}
	})
	require.LessOrEqual(t, allocs, float64(4), "the struct and its three arrays")
}

func TestCheckSubtreePositionsRejectsMismatch(t *testing.T) {
	st, meta := subtreeWithMeta(t)

	wrongRoot := samplePositions(t, st)
	wrongRoot.RootHash = chainhash.Hash{9}
	require.Error(t, CheckSubtreePositions(wrongRoot, st, meta))

	missingParent := positionsOf(t, *st.RootHash(), TxPositions{}, TxPositions{Own: Pack(7, 100), Parents: []uint64{Pack(255, 2)}}, TxPositions{Own: Pack(7, 101)})
	require.Error(t, CheckSubtreePositions(missingParent, st, meta))

	extraNode := samplePositions(t, st)
	require.NoError(t, extraNode.AppendNode(Pack(1, 1), nil))
	require.Error(t, CheckSubtreePositions(extraNode, st, meta))

	require.Error(t, CheckSubtreePositions(samplePositions(t, st), nil, meta))
	require.Error(t, CheckSubtreePositions(nil, st, meta))

	positioned := samplePositions(t, st)
	positioned.Own[0] = Pack(254, 9)
	require.Error(t, CheckSubtreePositions(positioned, st, meta), "the coinbase placeholder has no position yet")

	// Node 0 may be position-less only when it is the coinbase placeholder.
	plain, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, plain.AddNode(chainhash.Hash{0x21}, 1, 1))
	require.NoError(t, plain.AddNode(chainhash.Hash{0x22}, 1, 1))
	plainMeta := subtreepkg.NewSubtreeMeta(plain)
	plainMeta.TxInpoints[0] = subtreepkg.NewTxInpoints()
	plainMeta.TxInpoints[1] = subtreepkg.NewTxInpoints()

	require.Error(t, CheckSubtreePositions(positionsOf(t, *plain.RootHash(), TxPositions{}, TxPositions{Own: Pack(1, 2)}), plain, plainMeta))
	require.NoError(t, CheckSubtreePositions(positionsOf(t, *plain.RootHash(), TxPositions{Own: Pack(1, 1)}, TxPositions{Own: Pack(1, 2)}), plain, plainMeta))
}

func TestSubtreePositionsRefusesInvalid(t *testing.T) {
	st, _ := subtreeWithMeta(t)

	bad := samplePositions(t, st)
	bad.Own[2] = None
	_, err := EncodeSubtreePositions(bad)
	require.Error(t, err, "only node 0 may lack a position")

	bad = samplePositions(t, st)
	bad.Parents[1] = Pack(3, 0)
	_, err = EncodeSubtreePositions(bad)
	require.Error(t, err, "zero parent position")

	bad = samplePositions(t, st)
	bad.ParentStart[3] = 1
	_, err = EncodeSubtreePositions(bad)
	require.Error(t, err, "offsets that do not match the parents")

	good, err := EncodeSubtreePositions(samplePositions(t, st))
	require.NoError(t, err)

	head := subtreePositionsHead
	countsAt := head + 3*PositionSize

	for name, b := range map[string][]byte{
		"short":          good[:10],
		"magic":          append([]byte("LNSB"), good[4:]...),
		"version":        append(append([]byte("LNSP"), 9), good[5:]...),
		"truncated":      good[:len(good)-1],
		"trailing":       append(append([]byte{}, good...), 0),
		"node count":     withUint32(good, head-4, 1<<30),
		"counts too big": withUint32(good, countsAt+4, 3),
		"counts short":   withUint32(good, countsAt+4, 1),
	} {
		_, err := DecodeSubtreePositions(b)
		require.Error(t, err, name)
	}

	// A zero position written into node 2's own slot is refused on decode.
	zeroed := append([]byte{}, good...)
	copy(zeroed[head+2*PositionSize:], make([]byte, PositionSize))
	_, err = DecodeSubtreePositions(zeroed)
	require.Error(t, err)
}

// A positions file claiming 2^31 nodes in a few bytes is refused before anything is sized from
// the claim.
func TestDecodeSubtreePositionsBoundsItsAllocationByTheBuffer(t *testing.T) {
	b := append([]byte(subtreePositionsMagic), SubtreePositionsVersion)
	b = append(b, make([]byte, 32)...)
	b = append(b, 0xff, 0xff, 0xff, 0x7f)
	b = append(b, make([]byte, 64)...)

	var err error

	n := allocatedBy(func() { _, err = DecodeSubtreePositions(b) })
	require.Error(t, err)
	require.Less(t, n, uint64(1<<20), "allocated %d bytes for a %d-byte file", n, len(b))

	// Counts that sum past the parent block are refused before the parent array is made.
	one := append([]byte(subtreePositionsMagic), SubtreePositionsVersion)
	one = append(one, make([]byte, 32)...)
	one = binary.LittleEndian.AppendUint32(one, 1)
	one = AppendPosition(one, Pack(1, 1))
	one = binary.LittleEndian.AppendUint32(one, 1<<31)

	n = allocatedBy(func() { _, err = DecodeSubtreePositions(one) })
	require.Error(t, err)
	require.Less(t, n, uint64(1<<20), "allocated %d bytes for counts claiming 2^31 parents", n)

	// Counts that sum past 2^32 are refused by the 32-bit total check, which runs before the
	// length check. Its own error text pins it: with the check gone, this file would fail only
	// the length check, and a parent block that matches a 2^32 total is over 34 GB.
	two := append([]byte(subtreePositionsMagic), SubtreePositionsVersion)
	two = append(two, make([]byte, 32)...)
	two = binary.LittleEndian.AppendUint32(two, 2)
	two = AppendPositions(two, []uint64{Pack(1, 1), Pack(1, 2)})
	two = binary.LittleEndian.AppendUint32(two, 0xFFFFFFFF)
	two = binary.LittleEndian.AppendUint32(two, 1)

	_, err = DecodeSubtreePositions(two)
	require.ErrorContains(t, err, "past 2^32")
}

func withUint32(b []byte, at int, v uint32) []byte {
	out := append([]byte{}, b...)
	binary.LittleEndian.PutUint32(out[at:], v)

	return out
}

// BenchmarkDecodeSubtreePositions decodes a million-node subtree with one parent per node.
func BenchmarkDecodeSubtreePositions(b *testing.B) {
	const nodes = 1 << 20

	s := NewSubtreePositions(chainhash.Hash{1}, nodes, nodes)
	for i := 0; i < nodes; i++ {
		if err := s.AppendNode(Pack(3, uint64(i)+1), []uint64{Pack(4, uint64(i)+1)}); err != nil {
			b.Fatal(err)
		}
	}

	data, err := EncodeSubtreePositions(s)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := DecodeSubtreePositions(data); err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzDecodeSubtreePositions(f *testing.F) {
	s := NewSubtreePositions(chainhash.Hash{1}, 2, 1)
	_ = s.AppendNode(None, nil)
	_ = s.AppendNode(Pack(1, 1), []uint64{Pack(2, 2)})

	b, err := EncodeSubtreePositions(s)
	if err != nil {
		f.Fatal(err)
	}

	f.Add(b)

	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := DecodeSubtreePositions(data)
		if err != nil {
			return
		}

		again, err := EncodeSubtreePositions(got)
		require.NoError(t, err)
		require.Equal(t, data, again)
	})
}
