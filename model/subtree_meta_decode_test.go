package model

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"math/rand"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// metaEntry is one tx of a generated subtree meta: its parents, each with its vouts. A nil
// entry is written as a tx with no parents.
type metaEntry []struct {
	parent chainhash.Hash
	vouts  []uint32
}

// encodeMetaBody writes entries in the .subtreeMeta wire format after the header: per tx the
// parent count, the parent hashes, then per parent the vout count and the vouts.
func encodeMetaBody(entries []metaEntry) []byte {
	var buf bytes.Buffer

	var word [4]byte

	put := func(v uint32) {
		binary.LittleEndian.PutUint32(word[:], v)
		buf.Write(word[:])
	}

	for _, e := range entries {
		put(uint32(len(e))) // nolint: gosec

		for _, p := range e {
			buf.Write(p.parent[:])
		}

		for _, p := range e {
			put(uint32(len(p.vouts))) // nolint: gosec

			for _, v := range p.vouts {
				put(v)
			}
		}
	}

	return buf.Bytes()
}

// randomMetaEntries returns n entries of 0-5 parents with 0-4 vouts each, and every 97th of
// bigParents parents, so some entries do not fit a small read buffer.
func randomMetaEntries(rng *rand.Rand, n, bigParents int) []metaEntry {
	entries := make([]metaEntry, n)

	for i := range entries {
		parents := rng.Intn(6)
		if bigParents > 0 && i%97 == 96 {
			parents = bigParents
		}

		if parents == 0 {
			continue
		}

		e := make(metaEntry, parents)
		for p := range e {
			rng.Read(e[p].parent[:])

			e[p].vouts = make([]uint32, rng.Intn(5))
			for v := range e[p].vouts {
				e[p].vouts[v] = rng.Uint32()
			}
		}

		entries[i] = e
	}

	return entries
}

// subtreeOfSize returns a subtree with room for at least size nodes.
func subtreeOfSize(t testing.TB, size int) *subtreepkg.Subtree {
	t.Helper()

	leaves := 1
	for leaves < size {
		leaves *= 2
	}

	st, err := subtreepkg.NewTreeByLeafCount(leaves)
	require.NoError(t, err)

	return st
}

// referenceMeta decodes body with go-subtree's own decoder.
func referenceMeta(st *subtreepkg.Subtree, count uint32, body []byte) (*subtreepkg.Meta, error) {
	var header [subtreeMetaHeaderSize]byte

	binary.LittleEndian.PutUint32(header[chainhash.HashSize:], count)

	return subtreepkg.NewSubtreeMetaFromReader(st, bytes.NewReader(append(header[:], body...)))
}

func decodeWithBuffer(st *subtreepkg.Subtree, count uint32, body []byte, size int) (*subtreepkg.Meta, error) {
	return decodeSubtreeMetaBody(st, count, bufio.NewReaderSize(bytes.NewReader(body), size))
}

func requireSameMeta(t *testing.T, want, got *subtreepkg.Meta) {
	t.Helper()

	require.Same(t, want.Subtree, got.Subtree)
	require.Len(t, got.TxInpoints, len(want.TxInpoints))

	for i := range want.TxInpoints {
		w, g := &want.TxInpoints[i], &got.TxInpoints[i]

		require.Equal(t, w.ParentTxHashes == nil, g.ParentTxHashes == nil, "tx %d: nil parents mean the tx is not in the meta", i)
		require.Equal(t, w.ParentTxHashes, g.ParentTxHashes, "tx %d", i)
		require.Equal(t, w.GetTxInpoints(), g.GetTxInpoints(), "tx %d", i)

		ws, werr := w.Serialize()
		gs, gerr := g.Serialize()
		require.Equal(t, werr == nil, gerr == nil, "tx %d", i)
		require.Equal(t, ws, gs, "tx %d", i)

		for p := range w.ParentTxHashes {
			wv, err := w.GetParentVoutsAtIndex(p)
			require.NoError(t, err)

			gv, err := g.GetParentVoutsAtIndex(p)
			require.NoError(t, err)
			require.Equal(t, wv, gv, "tx %d parent %d", i, p)
		}
	}
}

// The decoder gives what go-subtree's own decoder gives, whatever the read buffer size and
// wherever its boundaries fall.
func TestDecodeSubtreeMetaBody_MatchesGoSubtree(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	for _, tc := range []struct {
		name       string
		n          int
		bigParents int
	}{
		{"small entries", 5000, 0},
		{"entries larger than a small buffer", 2000, 300},
		{"entries larger than the fast path", 400, 5000},
		{"empty", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := randomMetaEntries(rng, tc.n, tc.bigParents)
			body := encodeMetaBody(entries)
			st := subtreeOfSize(t, max(1, 2*tc.n))
			count := uint32(tc.n) // nolint: gosec

			want, err := referenceMeta(st, count, body)
			require.NoError(t, err)

			for _, size := range []int{16, 64, 4096, 1 << 20} {
				got, err := decodeWithBuffer(st, count, body, size)
				require.NoError(t, err, "buffer %d", size)
				requireSameMeta(t, want, got)
			}
		})
	}
}

// A tx with no parents decodes to nil parents, as go-subtree's decoder gives: block validation
// reads that as a tx that is not in the meta.
func TestDecodeSubtreeMetaBody_NoParentsIsNil(t *testing.T) {
	body := encodeMetaBody([]metaEntry{nil, {{parent: chainhash.HashH([]byte("p")), vouts: []uint32{1}}}, nil})
	st := subtreeOfSize(t, 4)

	got, err := decodeWithBuffer(st, 3, body, 1<<20)
	require.NoError(t, err)
	require.Nil(t, got.TxInpoints[0].ParentTxHashes)
	require.NotNil(t, got.TxInpoints[1].ParentTxHashes)
	require.Nil(t, got.TxInpoints[2].ParentTxHashes)
	require.Nil(t, got.TxInpoints[3].ParentTxHashes, "past the count")
}

// A torn file fails wherever it is cut, exactly where go-subtree's decoder fails, and trailing
// bytes are ignored as they are there.
func TestDecodeSubtreeMetaBody_TornFile(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	entries := randomMetaEntries(rng, 40, 50)
	body := encodeMetaBody(entries)
	st := subtreeOfSize(t, 64)

	for cut := 0; cut <= len(body); cut++ {
		_, wantErr := referenceMeta(st, 40, body[:cut])

		for _, size := range []int{16, 1 << 20} {
			_, err := decodeWithBuffer(st, 40, body[:cut], size)
			require.Equal(t, wantErr != nil, err != nil, "cut at %d of %d, buffer %d: %v", cut, len(body), size, err)
		}
	}

	want, err := referenceMeta(st, 40, append(body, 1, 2, 3))
	require.NoError(t, err)

	got, err := decodeWithBuffer(st, 40, append(body, 1, 2, 3), 16)
	require.NoError(t, err)
	requireSameMeta(t, want, got)
}

// Decoded txs share their backing arrays, so an append to one must not write into the next.
func TestDecodeSubtreeMetaBody_TxsDoNotShareCapacity(t *testing.T) {
	a, b := chainhash.HashH([]byte("a")), chainhash.HashH([]byte("b"))
	body := encodeMetaBody([]metaEntry{
		{{parent: a, vouts: []uint32{1}}},
		{{parent: b, vouts: []uint32{2}}},
	})

	got, err := decodeWithBuffer(subtreeOfSize(t, 2), 2, body, 1<<20)
	require.NoError(t, err)

	_ = append(got.TxInpoints[0].ParentTxHashes, chainhash.HashH([]byte("x")))
	require.Equal(t, []chainhash.Hash{b}, got.TxInpoints[1].ParentTxHashes)
	require.Equal(t, []subtreepkg.Inpoint{{Hash: b, Index: 2}}, got.TxInpoints[1].GetTxInpoints())
}

// The decoder allocates per block of txs, not per tx.
func TestDecodeSubtreeMetaBody_AllocationsDoNotGrowWithTxs(t *testing.T) {
	rng := rand.New(rand.NewSource(3))

	const n = 1 << 16

	body := encodeMetaBody(randomMetaEntries(rng, n, 0))
	st := subtreeOfSize(t, n)
	br := bufio.NewReaderSize(nil, 1<<20)

	allocs := testing.AllocsPerRun(5, func() {
		br.Reset(bytes.NewReader(body))

		if _, err := decodeSubtreeMetaBody(st, n, br); err != nil {
			panic(err)
		}
	})

	require.Less(t, allocs, float64(64), "%d txs", n)
}

// A small meta allocates about what it holds, not a slab sized for a full subtree.
func TestDecodeSubtreeMetaBody_SmallMetaAllocatesLittle(t *testing.T) {
	rng := rand.New(rand.NewSource(6))
	body := encodeMetaBody(randomMetaEntries(rng, 10, 0))
	st := subtreeOfSize(t, 16)
	br := bufio.NewReaderSize(nil, 1<<20)

	var before, after runtime.MemStats

	runtime.ReadMemStats(&before)

	for i := 0; i < 10; i++ {
		br.Reset(bytes.NewReader(body))

		if _, err := decodeSubtreeMetaBody(st, 10, br); err != nil {
			t.Fatal(err)
		}
	}

	runtime.ReadMemStats(&after)

	require.Less(t, (after.TotalAlloc-before.TotalAlloc)/10, uint64(16<<10), "bytes per decode of a %d-byte body", len(body))
}

func FuzzDecodeSubtreeMetaBody(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add(encodeMetaBody([]metaEntry{nil, {{vouts: []uint32{1, 2}}}}), uint8(2))
	f.Add(encodeMetaBody(randomMetaEntries(rand.New(rand.NewSource(4)), 8, 3)), uint8(8))

	f.Fuzz(func(t *testing.T, body []byte, count uint8) {
		st := subtreeOfSize(t, 256)

		want, wantErr := referenceMeta(st, uint32(count), body)

		for _, size := range []int{16, 1 << 20} {
			got, err := decodeWithBuffer(st, uint32(count), body, size)
			require.Equal(t, wantErr != nil, err != nil, "buffer %d: %v / %v", size, wantErr, err)

			if err == nil {
				requireSameMeta(t, want, got)
			}
		}
	})
}

// BenchmarkDecodeSubtreeMeta compares go-subtree's decoder with decodeSubtreeMetaBody on a full
// subtree of 1M txs with 1-3 parents each.
func BenchmarkDecodeSubtreeMeta(b *testing.B) {
	const n = 1 << 20

	rng := rand.New(rand.NewSource(5))
	entries := make([]metaEntry, n)

	for i := range entries {
		e := make(metaEntry, 1+rng.Intn(3))
		for p := range e {
			rng.Read(e[p].parent[:])
			e[p].vouts = []uint32{rng.Uint32() % 4}
		}

		entries[i] = e
	}

	body := encodeMetaBody(entries)
	st := subtreeOfSize(b, n)

	var header [subtreeMetaHeaderSize]byte

	binary.LittleEndian.PutUint32(header[chainhash.HashSize:], n)
	full := append(header[:], body...)

	b.Run("go-subtree", func(b *testing.B) {
		b.SetBytes(int64(len(full)))

		br := bufio.NewReaderSize(nil, 1<<20)

		for it := 0; it < b.N; it++ {
			br.Reset(bytes.NewReader(full))

			if _, err := subtreepkg.NewSubtreeMetaFromReader(st, br); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("packed", func(b *testing.B) {
		b.SetBytes(int64(len(body)))

		br := bufio.NewReaderSize(nil, 1<<20)

		for it := 0; it < b.N; it++ {
			br.Reset(bytes.NewReader(body))

			if _, err := decodeSubtreeMetaBody(st, n, br); err != nil {
				b.Fatal(err)
			}
		}
	})

	// as block validation decodes, one subtree per loader on every core
	b.Run("go-subtree/parallel", func(b *testing.B) {
		b.SetBytes(int64(len(full)))

		b.RunParallel(func(pb *testing.PB) {
			br := bufio.NewReaderSize(nil, 1<<20)

			for pb.Next() {
				br.Reset(bytes.NewReader(full))

				if _, err := subtreepkg.NewSubtreeMetaFromReader(st, br); err != nil {
					panic(err)
				}
			}
		})
	})

	b.Run("packed/parallel", func(b *testing.B) {
		b.SetBytes(int64(len(body)))

		b.RunParallel(func(pb *testing.PB) {
			br := bufio.NewReaderSize(nil, 1<<20)

			for pb.Next() {
				br.Reset(bytes.NewReader(body))

				if _, err := decodeSubtreeMetaBody(st, n, br); err != nil {
					panic(err)
				}
			}
		})
	})
}
