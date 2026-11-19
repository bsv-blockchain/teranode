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

func samplePositions(st *subtreepkg.Subtree) SubtreePositions {
	return SubtreePositions{
		RootHash: *st.RootHash(),
		Nodes: []TxPositions{
			{},
			{Own: Pack(7, 100), Parents: []uint64{Pack(255, 2), Pack(0, 1)}},
			{Own: Pack(7, 101)},
		},
	}
}

func TestSubtreePositionsRoundTrip(t *testing.T) {
	st, meta := subtreeWithMeta(t)
	s := samplePositions(st)

	b, err := EncodeSubtreePositions(s)
	require.NoError(t, err)

	// Golden layout of the tail: node 2 is own position then a zero parent count.
	tail := b[len(b)-12:]
	require.Equal(t, Pack(7, 101), binary.LittleEndian.Uint64(tail))
	require.Equal(t, uint32(0), binary.LittleEndian.Uint32(tail[8:]))
	require.Equal(t, "LNSP", string(b[:4]))
	require.Equal(t, SubtreePositionsVersion, b[4])

	got, err := DecodeSubtreePositions(b)
	require.NoError(t, err)
	require.Equal(t, s, got)
	require.NoError(t, CheckSubtreePositions(got, st, meta))
}

func TestCheckSubtreePositionsRejectsMismatch(t *testing.T) {
	st, meta := subtreeWithMeta(t)

	wrongRoot := samplePositions(st)
	wrongRoot.RootHash = chainhash.Hash{9}
	require.Error(t, CheckSubtreePositions(wrongRoot, st, meta))

	missingParent := samplePositions(st)
	missingParent.Nodes[1].Parents = missingParent.Nodes[1].Parents[:1]
	require.Error(t, CheckSubtreePositions(missingParent, st, meta))

	extraNode := samplePositions(st)
	extraNode.Nodes = append(extraNode.Nodes, TxPositions{Own: Pack(1, 1)})
	require.Error(t, CheckSubtreePositions(extraNode, st, meta))

	require.Error(t, CheckSubtreePositions(samplePositions(st), nil, meta))

	positioned := samplePositions(st)
	positioned.Nodes[0].Own = Pack(254, 9)
	require.Error(t, CheckSubtreePositions(positioned, st, meta), "the coinbase placeholder has no position yet")

	// Node 0 may be position-less only when it is the coinbase placeholder.
	plain, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, plain.AddNode(chainhash.Hash{0x21}, 1, 1))
	require.NoError(t, plain.AddNode(chainhash.Hash{0x22}, 1, 1))
	plainMeta := subtreepkg.NewSubtreeMeta(plain)
	plainMeta.TxInpoints[0] = subtreepkg.NewTxInpoints()
	plainMeta.TxInpoints[1] = subtreepkg.NewTxInpoints()

	require.Error(t, CheckSubtreePositions(SubtreePositions{RootHash: *plain.RootHash(), Nodes: []TxPositions{{}, {Own: Pack(1, 2)}}}, plain, plainMeta))
	require.NoError(t, CheckSubtreePositions(SubtreePositions{RootHash: *plain.RootHash(), Nodes: []TxPositions{{Own: Pack(1, 1)}, {Own: Pack(1, 2)}}}, plain, plainMeta))
}

func TestSubtreePositionsRefusesInvalid(t *testing.T) {
	st, _ := subtreeWithMeta(t)

	bad := samplePositions(st)
	bad.Nodes[2].Own = None
	_, err := EncodeSubtreePositions(bad)
	require.Error(t, err, "only node 0 may lack a position")

	bad = samplePositions(st)
	bad.Nodes[1].Parents[1] = Pack(3, 0)
	_, err = EncodeSubtreePositions(bad)
	require.Error(t, err, "zero parent position")

	good, err := EncodeSubtreePositions(samplePositions(st))
	require.NoError(t, err)

	for name, b := range map[string][]byte{
		"short":     good[:10],
		"magic":     append([]byte("LNSB"), good[4:]...),
		"version":   append(append([]byte("LNSP"), 9), good[5:]...),
		"truncated": good[:len(good)-1],
		"trailing":  append(append([]byte{}, good...), 0),
		"count":     withUint32(good, 37, 1<<30),
	} {
		_, err := DecodeSubtreePositions(b)
		require.Error(t, err, name)
	}

	// A zero position written into node 2 is refused on decode.
	zeroed := append([]byte{}, good...)
	copy(zeroed[len(zeroed)-12:], make([]byte, 8))
	_, err = DecodeSubtreePositions(zeroed)
	require.Error(t, err)
}

func withUint32(b []byte, at int, v uint32) []byte {
	out := append([]byte{}, b...)
	binary.LittleEndian.PutUint32(out[at:], v)

	return out
}

func FuzzDecodeSubtreePositions(f *testing.F) {
	st, err := subtreepkg.NewTreeByLeafCount(4)
	if err != nil {
		f.Fatal(err)
	}

	if err = st.AddCoinbaseNode(); err != nil {
		f.Fatal(err)
	}

	b, err := EncodeSubtreePositions(SubtreePositions{RootHash: chainhash.Hash{1}, Nodes: []TxPositions{{}, {Own: Pack(1, 1), Parents: []uint64{Pack(2, 2)}}}})
	if err != nil {
		f.Fatal(err)
	}

	f.Add(b)

	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := DecodeSubtreePositions(data)
		if err != nil {
			return
		}

		again, err := EncodeSubtreePositions(s)
		require.NoError(t, err)
		require.Equal(t, data, again)
	})
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
}
