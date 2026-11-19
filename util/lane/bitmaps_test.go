package lane

import (
	"encoding/hex"
	"runtime"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func sampleProof() ProofFile {
	return ProofFile{
		BlockHash: chainhash.Hash{0xB1},
		Subtrees: []SubtreeBitmaps{
			{SubtreeHash: chainhash.Hash{0x51}, Lanes: LaneBitmaps{7: bitmapOf(1, 2, 3), 254: bitmapOf(42)}},
			{SubtreeHash: chainhash.Hash{0x52}, Lanes: LaneBitmaps{7: bitmapOf(4, 9), 0: bitmapOf(1)}},
		},
	}
}

func TestSubtreeBitmapsGolden(t *testing.T) {
	s := SubtreeBitmaps{SubtreeHash: chainhash.Hash{0xAA}, Lanes: LaneBitmaps{7: bitmapOf(1)}}

	b, err := EncodeSubtreeBitmaps(s)
	require.NoError(t, err)

	// magic, version, hash, lane count 1, lane 7, length, then the portable bitmap {1}.
	bm := mustBytes(t, bitmapOf(1))
	want := "4c4e534201" + "aa" + hex.EncodeToString(make([]byte, 31)) + "0100" + "07" +
		hex.EncodeToString([]byte{byte(len(bm)), 0, 0, 0}) + hex.EncodeToString(bm)
	require.Equal(t, want, hex.EncodeToString(b))

	got, err := DecodeSubtreeBitmaps(b)
	require.NoError(t, err)
	require.Equal(t, s.SubtreeHash, got.SubtreeHash)
	require.True(t, got.Lanes[7].Equals(s.Lanes[7]))
}

func TestProofFileRoundTrip(t *testing.T) {
	p := sampleProof()

	b, err := EncodeProofFile(p)
	require.NoError(t, err)

	got, err := DecodeProofFile(b)
	require.NoError(t, err)
	require.Equal(t, p.BlockHash, got.BlockHash)
	require.Len(t, got.Subtrees, 2)

	for i := range p.Subtrees {
		require.Equal(t, p.Subtrees[i].SubtreeHash, got.Subtrees[i].SubtreeHash)
		require.Len(t, got.Subtrees[i].Lanes, len(p.Subtrees[i].Lanes))

		for l, bm := range p.Subtrees[i].Lanes {
			require.True(t, got.Subtrees[i].Lanes[l].Equals(bm))
		}
	}

	require.Equal(t, 0, got.SubtreeOf(Pack(7, 2)))
	require.Equal(t, 1, got.SubtreeOf(Pack(7, 9)))
	require.Equal(t, 0, got.SubtreeOf(Pack(254, 42)))
	require.Equal(t, -1, got.SubtreeOf(Pack(7, 5)))
	require.Equal(t, -1, got.SubtreeOf(Pack(8, 1)))

	union := got.LaneUnion()
	require.True(t, union[7].Equals(bitmapOf(1, 2, 3, 4, 9)))
	require.True(t, union[0].Equals(bitmapOf(1)))

	// The union does not alias a subtree's bitmap.
	union[7].Add(100)
	require.False(t, got.Subtrees[0].Lanes[7].Contains(100))
}

func TestProofFileRefusesOverlap(t *testing.T) {
	p := sampleProof()
	p.Subtrees[1].Lanes[7] = bitmapOf(3, 4)

	_, err := EncodeProofFile(p)
	require.Error(t, err)

	// Hand-built bytes with the overlap are refused on decode too.
	good := sampleProof()
	good.Subtrees[1].Lanes[7] = bitmapOf(5, 4)
	b, err := EncodeProofFile(good)
	require.NoError(t, err)

	bad := append([]byte{}, b...)
	five := mustBytes(t, bitmapOf(4, 5))
	three := mustBytes(t, bitmapOf(3, 4))
	require.Len(t, three, len(five))
	i := indexOf(bad, five)
	require.GreaterOrEqual(t, i, 0)
	copy(bad[i:], three)

	_, err = DecodeProofFile(bad)
	require.Error(t, err)

	// A third subtree overlapping the second, not the first, is refused too: the check compares
	// with every earlier subtree, not only the first.
	three3 := ProofFile{Subtrees: []SubtreeBitmaps{
		{SubtreeHash: chainhash.Hash{1}, Lanes: LaneBitmaps{7: bitmapOf(1)}},
		{SubtreeHash: chainhash.Hash{2}, Lanes: LaneBitmaps{7: bitmapOf(2)}},
		{SubtreeHash: chainhash.Hash{3}, Lanes: LaneBitmaps{7: bitmapOf(2)}},
	}}
	_, err = EncodeProofFile(three3)
	require.Error(t, err)
}

func TestLaneFilesRejectMalformedInput(t *testing.T) {
	p, err := EncodeProofFile(sampleProof())
	require.NoError(t, err)

	s, err := EncodeSubtreeBitmaps(sampleProof().Subtrees[0])
	require.NoError(t, err)

	for name, b := range map[string][]byte{
		"empty":       {},
		"wrong magic": append([]byte("XXXX"), p[4:]...),
		"version 2":   append(append([]byte("LNPF"), 2), p[5:]...),
		"truncated":   p[:len(p)-1],
		"trailing":    append(append([]byte{}, p...), 0),
		"huge count":  append(append(append([]byte{}, p[:37]...), 0xff, 0xff, 0xff, 0x7f), p[41:]...),
	} {
		_, err := DecodeProofFile(b)
		require.Error(t, err, name)
	}

	for name, b := range map[string][]byte{
		"proof magic": append([]byte("LNPF"), s[4:]...),
		"truncated":   s[:len(s)-3],
		"trailing":    append(append([]byte{}, s...), 1),
		"no hash":     s[:20],
	} {
		_, err := DecodeSubtreeBitmaps(b)
		require.Error(t, err, name)
	}

	// Lanes out of order.
	two := SubtreeBitmaps{Lanes: LaneBitmaps{1: bitmapOf(1), 2: bitmapOf(1)}}
	b, err := EncodeSubtreeBitmaps(two)
	require.NoError(t, err)

	off := 4 + 1 + 32 + 2
	first := 5 + int(b[off+1])
	b[off], b[off+first] = 2, 1
	_, err = DecodeSubtreeBitmaps(b)
	require.Error(t, err)
}

func TestLaneFilesRefuseBadBitmaps(t *testing.T) {
	_, err := EncodeSubtreeBitmaps(SubtreeBitmaps{Lanes: LaneBitmaps{1: roaring64.New()}})
	require.Error(t, err, "empty bitmap")

	_, err = EncodeSubtreeBitmaps(SubtreeBitmaps{Lanes: LaneBitmaps{1: bitmapOf(0)}})
	require.Error(t, err, "position 0")

	_, err = EncodeSubtreeBitmaps(SubtreeBitmaps{Lanes: LaneBitmaps{1: bitmapOf(MaxPosition + 1)}})
	require.Error(t, err)

	_, err = EncodeSubtreeBitmaps(SubtreeBitmaps{Lanes: LaneBitmaps{1: nil}})
	require.Error(t, err, "nil bitmap")

	// The overlap check runs first in a proof file, so it must refuse a nil or empty bitmap
	// itself rather than panic on it, in any subtree.
	for name, lanes := range map[string]LaneBitmaps{
		"nil":   {7: nil},
		"empty": {7: roaring64.New()},
	} {
		for _, at := range []int{0, 1} {
			p := sampleProof()
			p.Subtrees[at].Lanes = lanes

			require.NotPanics(t, func() {
				_, err = EncodeProofFile(p)
			}, name)
			require.Error(t, err, "%s bitmap in subtree %d", name, at)
		}
	}
}

// A lane written twice in one subtree is refused, in both files, rather than the second copy
// silently replacing the first.
func TestLaneFilesRefuseARepeatedLane(t *testing.T) {
	two := SubtreeBitmaps{SubtreeHash: chainhash.Hash{0x61}, Lanes: LaneBitmaps{1: bitmapOf(1), 2: bitmapOf(5)}}

	s, err := EncodeSubtreeBitmaps(two)
	require.NoError(t, err)

	off := 4 + 1 + 32 + 2
	first := 5 + int(s[off+1])
	s[off+first] = 1 // the second entry now claims lane 1 again

	_, err = DecodeSubtreeBitmaps(s)
	require.Error(t, err)

	p, err := EncodeProofFile(ProofFile{BlockHash: chainhash.Hash{0x62}, Subtrees: []SubtreeBitmaps{two}})
	require.NoError(t, err)

	off = 4 + 1 + 32 + 4 + 32 + 2
	p[off+first] = 1

	_, err = DecodeProofFile(p)
	require.Error(t, err)
}

func indexOf(b, sub []byte) int {
	for i := 0; i+len(sub) <= len(b); i++ {
		if string(b[i:i+len(sub)]) == string(sub) {
			return i
		}
	}

	return -1
}

func FuzzDecodeProofFile(f *testing.F) {
	b, err := EncodeProofFile(sampleProof())
	if err != nil {
		f.Fatal(err)
	}

	f.Add(b)
	f.Add([]byte("LNPF\x01"))

	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := DecodeProofFile(data)
		if err != nil {
			return
		}

		again, err := EncodeProofFile(p)
		require.NoError(t, err)

		q, err := DecodeProofFile(again)
		require.NoError(t, err)
		require.Equal(t, p.BlockHash, q.BlockHash)
		require.Len(t, q.Subtrees, len(p.Subtrees))

		for i := range p.Subtrees {
			for l, bm := range p.Subtrees[i].Lanes {
				require.True(t, q.Subtrees[i].Lanes[l].Equals(bm))
			}
		}
	})
}

func FuzzDecodeSubtreeBitmaps(f *testing.F) {
	b, err := EncodeSubtreeBitmaps(sampleProof().Subtrees[0])
	if err != nil {
		f.Fatal(err)
	}

	f.Add(b)
	f.Add([]byte("LNSB\x01"))

	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := DecodeSubtreeBitmaps(data)
		if err != nil {
			return
		}

		again, err := EncodeSubtreeBitmaps(s)
		require.NoError(t, err)

		q, err := DecodeSubtreeBitmaps(again)
		require.NoError(t, err)
		require.Equal(t, s.SubtreeHash, q.SubtreeHash)
		require.Len(t, q.Lanes, len(s.Lanes))

		for l, bm := range s.Lanes {
			require.True(t, q.Lanes[l].Equals(bm))
		}
	})
}

// allocatedBy returns the bytes fn allocates.
func allocatedBy(fn func()) uint64 {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}

// A proof file claiming 2^31 subtrees in a few bytes is refused before anything is sized from
// the claim.
func TestDecodeProofFileBoundsItsAllocationByTheBuffer(t *testing.T) {
	b := append([]byte(proofFileMagic), BitmapFormatVersion)
	b = append(b, make([]byte, 32)...)
	b = append(b, 0xff, 0xff, 0xff, 0x7f)
	b = append(b, make([]byte, 64)...)

	var err error

	n := allocatedBy(func() { _, err = DecodeProofFile(b) })
	require.Error(t, err)
	require.Less(t, n, uint64(1<<20), "allocated %d bytes for a %d-byte file", n, len(b))
}
