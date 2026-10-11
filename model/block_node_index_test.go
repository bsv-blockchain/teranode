package model

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// testNodeHashes is a block's node hashes addressed by global tx index.
type testNodeHashes []chainhash.Hash

func (n testNodeHashes) hashAt(idx uint32) *chainhash.Hash { return &n[idx] }

func seqHash(i uint64) chainhash.Hash {
	var h chainhash.Hash

	for k := 0; k < 4; k++ {
		x := i*4 + uint64(k) + 0x9E3779B97F4A7C15
		x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
		x = (x ^ (x >> 27)) * 0x94D049BB133111EB
		binary.LittleEndian.PutUint64(h[k*8:], x^(x>>31))
	}

	return h
}

func newTestNodeHashes(count int) testNodeHashes {
	nodes := make(testNodeHashes, count)
	for i := range nodes {
		nodes[i] = seqHash(uint64(i))
	}

	return nodes
}

// seededHash is a well mixed hash of the whole key; the seed only changes the layout.
func seededHash(seed uint64) func(chainhash.Hash) uint64 {
	return func(h chainhash.Hash) uint64 {
		x := seed + 0x9E3779B97F4A7C15
		for k := 0; k < 4; k++ {
			x ^= binary.LittleEndian.Uint64(h[k*8:])
			x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
			x = (x ^ (x >> 27)) * 0x94D049BB133111EB
			x ^= x >> 31
		}

		return x
	}
}

func constHash(v uint64) func(chainhash.Hash) uint64 {
	return func(chainhash.Hash) uint64 { return v }
}

// weakHash has only 2^20 distinct values so fingerprints and buckets collide a lot.
func weakHash(h chainhash.Hash) uint64 {
	return (uint64(h[0]) | uint64(h[1])<<8 | uint64(h[2]&0x0F)<<16) * 0x9E3779B97F4A7C15
}

func TestNodeIndex_RoundTripAndMiss(t *testing.T) {
	nodes := newTestNodeHashes(1000)
	x := newNodeIndex(len(nodes), nodes.hashAt, seededHash(1))

	for i := range nodes {
		require.NoError(t, x.Put(nodes[i], uint32(i)))
	}

	require.Equal(t, 1000, x.Length())

	for i := range nodes {
		idx, ok := x.Get(nodes[i])
		require.True(t, ok, "tx %d", i)
		require.Equal(t, uint32(i), idx)
	}

	_, ok := x.Get(seqHash(5000))
	require.False(t, ok)
}

func TestNodeIndex_DuplicateKeepsFirstIndex(t *testing.T) {
	nodes := newTestNodeHashes(10)
	nodes[7] = nodes[3]
	x := newNodeIndex(len(nodes), nodes.hashAt, seededHash(1))

	for i := 0; i < 7; i++ {
		require.NoError(t, x.Put(nodes[i], uint32(i)))
	}

	require.ErrorIs(t, x.Put(nodes[7], 7), errors.ErrTxExists)
	require.Contains(t, x.Put(nodes[7], 7).Error(), "hash already exists in map")

	idx, ok := x.Get(nodes[3])
	require.True(t, ok)
	require.Equal(t, uint32(3), idx)
	require.Equal(t, 7, x.Length())
}

// Every key lands in the same shard, bucket and fingerprint: only the node hash tells them apart.
func TestNodeIndex_ForcedFingerprintCollisions(t *testing.T) {
	for _, n := range []int{1, 2, 7, 8, 9, 50, 500} {
		nodes := newTestNodeHashes(n + 1)
		x := newNodeIndex(n, nodes.hashAt, constHash(42))

		for i := 0; i < n; i++ {
			require.NoError(t, x.Put(nodes[i], uint32(i)))
		}

		for i := 0; i < n; i++ {
			idx, ok := x.Get(nodes[i])
			require.True(t, ok, "n=%d tx %d", n, i)
			require.Equal(t, uint32(i), idx)
		}

		// a miss that collides on shard, bucket and fingerprint with every stored key
		_, ok := x.Get(nodes[n])
		require.False(t, ok, "n=%d", n)
		_, ok = x.Get(seqHash(1 << 40))
		require.False(t, ok, "n=%d", n)

		// a duplicate is detected although the first matching fingerprint is another key
		require.ErrorIs(t, x.Put(nodes[n-1], uint32(n)), errors.ErrTxExists, "n=%d", n)
	}
}

// Keys that share their first 28 bytes, as ground hashes would.
func TestNodeIndex_SharedPrefixKeys(t *testing.T) {
	const n = 5000

	nodes := make(testNodeHashes, n)
	base := seqHash(7)

	for i := range nodes {
		nodes[i] = base
		binary.LittleEndian.PutUint32(nodes[i][28:], uint32(i))
	}

	x := newNodeIndex(16, nodes.hashAt, seededHash(99))

	for i := range nodes {
		require.NoError(t, x.Put(nodes[i], uint32(i)))
	}

	for i := range nodes {
		idx, ok := x.Get(nodes[i])
		require.True(t, ok)
		require.Equal(t, uint32(i), idx)
	}
}

func TestNodeIndex_GrowsPastCapacity(t *testing.T) {
	nodes := newTestNodeHashes(20000)
	x := newNodeIndex(16, nodes.hashAt, seededHash(3))

	for i := range nodes {
		require.NoError(t, x.Put(nodes[i], uint32(i)))
	}

	for i := range nodes {
		idx, ok := x.Get(nodes[i])
		require.True(t, ok, "tx %d", i)
		require.Equal(t, uint32(i), idx)
	}
}

func TestNodeIndex_PresizedDoesNotGrow(t *testing.T) {
	nodes := newTestNodeHashes(100000)
	x := newNodeIndex(len(nodes), nodes.hashAt, seededHash(5))
	before := x.slots()

	for i := range nodes {
		require.NoError(t, x.Put(nodes[i], uint32(i)))
	}

	require.Equal(t, before, x.slots())
}

// Sizes around the shard-count and bucket-load thresholds, against a plain Go map.
func TestNodeIndex_BoundarySizes(t *testing.T) {
	sizes := []int{0, 1, 2, 6, 7, 8, 9, 14, 15, 16, 17, 55, 56, 57, 2047, 2048, 2049, 4095, 4096, 4097, 8192}

	for _, hint := range []int{0, 1, 2048, 4097} {
		for _, n := range sizes {
			nodes := newTestNodeHashes(n + 1)
			x := newNodeIndex(hint, nodes.hashAt, seededHash(uint64(n)))
			want := make(map[chainhash.Hash]uint32, n)

			for i := 0; i < n; i++ {
				require.NoError(t, x.Put(nodes[i], uint32(i)))
				want[nodes[i]] = uint32(i)
			}

			require.Equal(t, n, x.Length())

			for i := 0; i <= n; i++ {
				idx, ok := x.Get(nodes[i])
				wantIdx, wantOK := want[nodes[i]]
				require.Equal(t, wantOK, ok, "hint=%d n=%d tx %d", hint, n, i)
				require.Equal(t, wantIdx, idx)
			}
		}
	}
}

func TestNodeIndex_EmptyAndTiny(t *testing.T) {
	x := newNodeIndex(0, newTestNodeHashes(0).hashAt, seededHash(1))
	_, ok := x.Get(seqHash(1))
	require.False(t, ok)
	require.Equal(t, 0, x.Length())
	x.Freeze()
	_, ok = x.Get(seqHash(1))
	require.False(t, ok)

	nodes := newTestNodeHashes(1)
	x = newNodeIndex(1, nodes.hashAt, seededHash(1))
	require.NoError(t, x.Put(nodes[0], 0))
	idx, ok := x.Get(nodes[0])
	require.True(t, ok)
	require.Equal(t, uint32(0), idx)

	// no large fixed preallocation for a tiny block
	require.LessOrEqual(t, newNodeIndex(1, nodes.hashAt, seededHash(1)).slots(), 64)
	require.LessOrEqual(t, newNodeIndex(0, nodes.hashAt, seededHash(1)).slots(), 64)
}

// Weak hash, half the keys inserted, every answer checked against a Go map.
func TestNodeIndex_MatchesGoMap(t *testing.T) {
	const n = 100000

	nodes := newTestNodeHashes(n)
	x := newNodeIndex(1024, nodes.hashAt, weakHash)
	want := make(map[chainhash.Hash]uint32, n)

	for i := 0; i < n; i += 2 {
		require.NoError(t, x.Put(nodes[i], uint32(i)))
		want[nodes[i]] = uint32(i)
	}

	for i := 0; i < n; i++ {
		idx, ok := x.Get(nodes[i])
		wantIdx, wantOK := want[nodes[i]]
		require.Equal(t, wantOK, ok, "tx %d", i)
		require.Equal(t, wantIdx, idx, "tx %d", i)
	}

	for i := uint64(n); i < 2*n; i++ {
		_, ok := x.Get(seqHash(i))
		require.False(t, ok, "absent %d", i)
	}
}

// The seed, the insertion order and the number of workers only change the layout.
func TestNodeIndex_Deterministic(t *testing.T) {
	const n = 30000

	nodes := newTestNodeHashes(n + 100)
	// every 5th entry repeats the hash of an earlier one
	for i := 5; i < n; i += 5 {
		nodes[i] = nodes[i-3]
	}

	var ref map[chainhash.Hash]uint32

	var refDup map[chainhash.Hash]bool

	for run, workers := range []int{1, 2, 7, 16} {
		x := newNodeIndex(n/2, nodes.hashAt, seededHash(uint64(run)+1000))
		order := rand.New(rand.NewSource(int64(run))).Perm(n)
		dups := make([]bool, n)

		var wg sync.WaitGroup

		for w := 0; w < workers; w++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for j := w; j < n; j += workers {
					i := order[j]
					if err := x.Put(nodes[i], uint32(i)); err != nil {
						require.ErrorIs(t, err, errors.ErrTxExists)

						dups[i] = true
					}
				}
			}()
		}

		wg.Wait()
		x.Freeze()

		got := make(map[chainhash.Hash]uint32)
		gotDup := make(map[chainhash.Hash]bool)

		for i := 0; i < n; i++ {
			if idx, ok := x.Get(nodes[i]); ok {
				got[nodes[i]] = 0
				_ = idx
			}

			if dups[i] {
				gotDup[nodes[i]] = true
			}
		}

		// which of two equal keys wins depends on the schedule, the key set and the set of
		// keys seen twice must not
		if ref == nil {
			ref, refDup = got, gotDup
			continue
		}

		require.Equal(t, ref, got, "workers=%d", workers)
		require.Equal(t, refDup, gotDup, "workers=%d", workers)
	}
}

func TestNodeIndex_ConcurrentPutThenFrozenGet(t *testing.T) {
	nodes := newTestNodeHashes(64000)
	x := newNodeIndex(len(nodes), nodes.hashAt, seededHash(11))

	const workers = 16

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := w; i < len(nodes); i += workers {
				require.NoError(t, x.Put(nodes[i], uint32(i)))
			}
		}()
	}

	wg.Wait()
	x.Freeze()
	require.ErrorContains(t, x.Put(seqHash(1<<40), 0), "frozen")

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := w; i < len(nodes); i += workers {
				idx, ok := x.Get(nodes[i])
				require.True(t, ok)
				require.Equal(t, uint32(i), idx)
			}
		}()
	}

	wg.Wait()
}

// Concurrent readers and writers on a not yet frozen index, for the race detector.
func TestNodeIndex_ConcurrentPutAndGet(t *testing.T) {
	nodes := newTestNodeHashes(20000)
	x := newNodeIndex(16, nodes.hashAt, seededHash(13))

	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Add(2)

		go func() {
			defer wg.Done()

			for i := w; i < len(nodes); i += 8 {
				require.NoError(t, x.Put(nodes[i], uint32(i)))
			}
		}()

		go func() {
			defer wg.Done()

			for i := 0; i < len(nodes); i++ {
				if idx, ok := x.Get(nodes[i]); ok {
					require.Equal(t, uint32(i), idx)
				}
			}
		}()
	}

	wg.Wait()
}

func FuzzNodeIndexMatchesGoMap(f *testing.F) {
	f.Add([]byte{1, 2, 3, 1, 2, 3}, uint64(1), uint8(0))
	f.Add([]byte{}, uint64(2), uint8(1))
	f.Add([]byte{0, 0, 0, 0, 0}, uint64(3), uint8(2))

	f.Fuzz(func(t *testing.T, keys []byte, seed uint64, mode uint8) {
		if len(keys) > 4096 {
			keys = keys[:4096]
		}

		// the key space is small so repeats and, with the weak hashes, collisions are common
		hashes := make(testNodeHashes, len(keys))
		for i, k := range keys {
			hashes[i] = seqHash(uint64(k))
		}

		var hash func(chainhash.Hash) uint64

		switch mode % 3 {
		case 0:
			hash = seededHash(seed)
		case 1:
			hash = constHash(seed)
		default:
			hash = weakHash
		}

		x := newNodeIndex(int(seed%64), hashes.hashAt, hash)
		want := make(map[chainhash.Hash]uint32)

		for i := range hashes {
			err := x.Put(hashes[i], uint32(i))
			if _, dup := want[hashes[i]]; dup {
				require.ErrorIs(t, err, errors.ErrTxExists)
				continue
			}

			require.NoError(t, err)
			want[hashes[i]] = uint32(i)
		}

		require.Equal(t, len(want), x.Length())

		for k := 0; k < 300; k++ {
			h := seqHash(uint64(k))
			idx, ok := x.Get(h)
			wantIdx, wantOK := want[h]
			require.Equal(t, wantOK, ok)
			require.Equal(t, wantIdx, idx)
		}
	})
}

// buildLayout makes subtrees of the given node counts, all with capacity size, and
// returns them with every node hash by global index (subIdx*size+txIdx).
func buildLayout(t *testing.T, size int, lens []int) ([]*subtreepkg.Subtree, map[uint64]chainhash.Hash) {
	t.Helper()

	subs := make([]*subtreepkg.Subtree, len(lens))
	byIdx := make(map[uint64]chainhash.Hash)
	n := uint64(0)

	for s, l := range lens {
		capacity := size
		for capacity < l {
			capacity *= 2
		}

		st, err := subtreepkg.NewTreeByLeafCount(capacity)
		require.NoError(t, err)

		for i := 0; i < l; i++ {
			n++

			h := seqHash(n + 1<<32)
			if s == 0 && i == 0 {
				h = subtreepkg.CoinbasePlaceholderHashValue
				require.NoError(t, st.AddCoinbaseNode())
			} else {
				require.NoError(t, st.AddNode(h, 1, 0))
			}

			byIdx[uint64(s*size+i)] = h
		}

		subs[s] = st
	}

	return subs, byIdx
}

func TestBlockNodeIndex_MatchesLegacyMap(t *testing.T) {
	cases := map[string]struct {
		size int
		lens []int
	}{
		"single subtree":          {4, []int{4}},
		"coinbase only":           {4, []int{1}},
		"full subtrees":           {4, []int{4, 4, 4}},
		"short last":              {4, []int{4, 4, 2}},
		"short non-last":          {4, []int{4, 2, 4}},
		"oversized last":          {4, []int{4, 4, 7}},
		"oversized last of two":   {2, []int{2, 7}},
		"many subtrees":           {16, []int{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 3}},
		"one tx next to coinbase": {2, []int{2}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			subs, byIdx := buildLayout(t, tc.size, tc.lens)
			m := newBlockNodeIndex(0, len(subs), tc.size)
			legacy := txmap.NewSplitSwissMapUint64(10)

			for s, st := range subs {
				require.NoError(t, m.register(s, st.Nodes))

				for i := range st.Nodes {
					if s == 0 && i == 0 {
						continue
					}

					idx := uint64(s*tc.size + i)
					require.NoError(t, m.Put(st.Nodes[i].Hash, idx))
					require.NoError(t, legacy.Put(st.Nodes[i].Hash, idx))
				}
			}

			m.Freeze()

			for idx, h := range byIdx {
				got, gotOK := m.Get(h)
				want, wantOK := legacy.Get(h)
				require.Equal(t, wantOK, gotOK, "idx %d", idx)
				require.Equal(t, want, got, "idx %d", idx)
			}

			// coinbase placeholder is never stored
			_, ok := m.Get(subtreepkg.CoinbasePlaceholderHashValue)
			require.False(t, ok)

			_, ok = m.Get(seqHash(12345))
			require.False(t, ok)
		})
	}
}

func TestBlockNodeIndex_DuplicateErrorLikeLegacy(t *testing.T) {
	subs, _ := buildLayout(t, 4, []int{4, 4})
	m := newBlockNodeIndex(0, 2, 4)

	for s, st := range subs {
		require.NoError(t, m.register(s, st.Nodes))
	}

	require.NoError(t, m.Put(subs[0].Nodes[1].Hash, 1))

	err := m.Put(subs[1].Nodes[2].Hash, 6)
	require.NoError(t, err)

	err = m.Put(subs[0].Nodes[1].Hash, 5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash already exists in map")

	legacy := txmap.NewSplitSwissMapUint64(10)
	require.NoError(t, legacy.Put(subs[0].Nodes[1].Hash, 1))
	legacyErr := legacy.Put(subs[0].Nodes[1].Hash, 5)
	require.Contains(t, legacyErr.Error(), "hash already exists in map", "putSubtreeInTxMap matches on this text")
}

func TestBlockNodeIndex_RejectsIndexAboveUint32(t *testing.T) {
	subs, _ := buildLayout(t, 4, []int{4})
	m := newBlockNodeIndex(0, 1, 4)
	require.NoError(t, m.register(0, subs[0].Nodes))

	for _, idx := range []uint64{math.MaxUint32 + 1, 1 << 40, math.MaxUint64} {
		err := m.Put(subs[0].Nodes[1].Hash, idx)
		require.Error(t, err, "idx %d", idx)
		require.Contains(t, err.Error(), "32-bit")
	}

	_, ok := m.Get(subs[0].Nodes[1].Hash)
	require.False(t, ok, "a rejected index must not be stored")

	// the largest index is fine
	require.NoError(t, m.Put(subs[0].Nodes[1].Hash, math.MaxUint32))
}

func TestBlockNodeIndex_RegisterRejectsOversizedNonLastSubtree(t *testing.T) {
	subs, _ := buildLayout(t, 2, []int{5, 2})
	m := newBlockNodeIndex(0, 2, 2)

	err := m.register(0, subs[0].Nodes)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockCorrupt), "got %v", err)

	// the last subtree may be longer than the first
	require.NoError(t, m.register(1, subs[1].Nodes))
	require.Error(t, m.register(2, subs[1].Nodes), "out of range subtree")
}

// Which map checkDuplicateTransactions and the load path build.
func TestNewInMemoryBlockTxMap_Selection(t *testing.T) {
	_, ok := newInMemoryBlockTxMap(0, 3, 4, 12).(*blockNodeIndex)
	require.True(t, ok)

	// highest usable index is 2^32-1, so 2^32 entries still fit
	_, ok = newInMemoryBlockTxMap(0, 2, 1<<31, 1<<32).(*blockNodeIndex)
	require.True(t, ok)

	// beyond that the 64-bit map is kept (issue 1428): never wrap, never fail a valid block
	legacy, ok := newInMemoryBlockTxMap(0, 2, 1<<31, 1<<32+1).(*txmap.SplitSwissMapUint64)
	require.True(t, ok)
	require.NoError(t, legacy.Put(seqHash(1), 1<<33))

	_, ok = newInMemoryBlockTxMap(0, 1<<40, 1<<20, math.MaxUint64).(*txmap.SplitSwissMapUint64)
	require.True(t, ok)

	// a zero subtree size cannot address anything
	_, ok = newInMemoryBlockTxMap(0, 1, 0, 0).(*txmap.SplitSwissMapUint64)
	require.True(t, ok)
}

func TestMaxTxIndex(t *testing.T) {
	subs, _ := buildLayout(t, 4, []int{4, 4, 2})
	require.Equal(t, uint64(10), maxTxIndex(subs, 4))
	require.Equal(t, uint64(0), maxTxIndex(nil, 4))
	require.Equal(t, uint64(10), maxTxIndex([]*subtreepkg.Subtree{subs[0], nil, subs[2], nil}, 4))

	// saturates instead of wrapping
	require.Equal(t, uint64(math.MaxUint64), maxTxIndex(subs, math.MaxInt))
}

// BenchmarkBlockTxMap compares the in-memory tx map block validation builds (nodeindex) with the
// SplitSwissMapUint64 it replaced: concurrent fill by subtree as in checkDuplicateTransactions,
// then frozen lookups of every tx and one parent each, as in validOrderAndBlessed.
func BenchmarkBlockTxMap(b *testing.B) {
	const subtreeSize = 1 << 14

	for _, subtrees := range []int{1, 16, 256} {
		n := subtrees * subtreeSize
		subs := make([]*subtreepkg.Subtree, subtrees)

		for s := range subs {
			st, err := subtreepkg.NewTreeByLeafCount(subtreeSize)
			require.NoError(b, err)

			for i := 0; i < subtreeSize; i++ {
				if s == 0 && i == 0 {
					require.NoError(b, st.AddCoinbaseNode())
					continue
				}

				require.NoError(b, st.AddNode(seqHash(uint64(s*subtreeSize+i)), 1, 0))
			}

			subs[s] = st
		}

		impls := map[string]func() blockTxMap{
			"nodeindex": func() blockTxMap {
				return newInMemoryBlockTxMap(uint64(n), subtrees, subtreeSize, maxTxIndex(subs, subtreeSize))
			},
			"swiss": func() blockTxMap { return newBlockTxMap(uint64(n)) },
		}

		for _, name := range []string{"nodeindex", "swiss"} {
			b.Run(fmt.Sprintf("txs=%d/%s", n, name), func(b *testing.B) {
				b.ReportAllocs()

				for it := 0; it < b.N; it++ {
					m := impls[name]()

					var wg sync.WaitGroup

					for s := range subs {
						wg.Add(1)

						go func() {
							defer wg.Done()

							if ni, ok := m.(*blockNodeIndex); ok {
								_ = ni.register(s, subs[s].Nodes)
							}

							for i, node := range subs[s].Nodes {
								if s == 0 && i == 0 {
									continue
								}

								if err := m.Put(node.Hash, uint64(s*subtreeSize+i)); err != nil {
									panic(err)
								}
							}
						}()
					}

					wg.Wait()
					m.Freeze()

					for s := range subs {
						wg.Add(1)

						go func() {
							defer wg.Done()

							for i, node := range subs[s].Nodes {
								if s == 0 && i == 0 {
									continue
								}

								if _, ok := m.Get(node.Hash); !ok {
									panic("missing tx")
								}

								if i > 1 {
									if _, ok := m.Get(subs[s].Nodes[i-1].Hash); !ok && !(s == 0 && i == 1) {
										panic("missing parent")
									}
								}
							}
						}()
					}

					wg.Wait()
				}

				b.ReportMetric(float64(n), "txs/op")
			})
		}
	}
}
