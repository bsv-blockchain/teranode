package subtreeprocessor

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// randHashes returns n pseudo-random hashes, deterministic for a seed.
func randHashes(n int, seed int64) []chainhash.Hash {
	r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
	out := make([]chainhash.Hash, n)

	for i := range out {
		_, _ = r.Read(out[i][:])
	}

	return out
}

// buildSet fills a set via the partition-batched path and freezes it.
func buildSet(t testing.TB, hashes []chainhash.Hash, capacity, workers int) *SortedHashSet {
	t.Helper()

	s := NewSortedHashSet(capacity)
	batches := make(map[int][]chainhash.Hash)

	for _, h := range hashes {
		p := s.Partition(h)
		batches[p] = append(batches[p], h)
	}

	for p, b := range batches {
		require.NoError(t, s.PutMultiBucket(p, b))
	}

	require.NoError(t, s.Freeze(workers))

	return s
}

func sortedDistinct(hashes []chainhash.Hash) []chainhash.Hash {
	seen := make(map[chainhash.Hash]struct{}, len(hashes))
	var out []chainhash.Hash

	for _, h := range hashes {
		if _, ok := seen[h]; !ok {
			seen[h] = struct{}{}
			out = append(out, h)
		}
	}

	sort.Slice(out, func(i, j int) bool { return compareHash(&out[i], &out[j]) < 0 })

	return out
}

func collect(s *SortedHashSet) []chainhash.Hash {
	var out []chainhash.Hash

	s.Iter(func(h chainhash.Hash) bool {
		out = append(out, h)
		return true
	})

	return out
}

// assertMatchesReference checks the frozen set against a plain Go map model: every
// member exists, near-miss and random non-members do not, Length is the distinct
// count and Iter yields the sorted distinct members.
func assertMatchesReference(t *testing.T, s *SortedHashSet, hashes []chainhash.Hash) {
	t.Helper()

	ref := make(map[chainhash.Hash]struct{}, len(hashes))
	for _, h := range hashes {
		ref[h] = struct{}{}
	}

	require.Equal(t, len(ref), s.Length())

	for _, h := range hashes {
		require.True(t, s.Exists(h), "member missing: %s", h)

		// near misses: flip one bit at the start, middle and end of the hash
		for _, pos := range []int{0, 15, 31} {
			m := h
			m[pos] ^= 0x01

			_, isMember := ref[m]
			require.Equal(t, isMember, s.Exists(m), "near miss mismatch for %s", m)
		}
	}

	for _, h := range randHashes(2000, 987654321) {
		_, isMember := ref[h]
		require.Equal(t, isMember, s.Exists(h))
	}

	require.Equal(t, sortedDistinct(hashes), collect(s))
}

func TestSortedHashSet_ExactVsMap(t *testing.T) {
	// sizes around the slot (4/16/32) and partition (65536 per partition) thresholds
	sizes := []int{0, 1, 2, 3, 4, 5, 15, 16, 17, 31, 32, 33, 100, 1023, 1024, 1025, 4096, 65535, 65536, 65537, 131073, 270000}

	for _, n := range sizes {
		hashes := randHashes(n, int64(n)+1)

		capacities := []int{0, n, n / 3, n * 4}
		if n > 100000 {
			capacities = []int{n, n / 3}
		}

		for _, capacity := range capacities {
			s := buildSet(t, hashes, capacity, 4)
			assertMatchesReference(t, s, hashes)
		}
	}
}

func TestSortedHashSet_Duplicates(t *testing.T) {
	base := randHashes(5000, 42)

	var hashes []chainhash.Hash

	r := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test data
	for _, h := range base {
		for c := 0; c < 1+r.Intn(4); c++ {
			hashes = append(hashes, h)
		}
	}

	r.Shuffle(len(hashes), func(i, j int) { hashes[i], hashes[j] = hashes[j], hashes[i] })

	s := buildSet(t, hashes, len(hashes), 3)
	require.Equal(t, 5000, s.Length(), "Length must be the distinct count")
	assertMatchesReference(t, s, hashes)

	// only duplicates of a single hash
	one := randHashes(1, 1)[0]
	s = buildSet(t, []chainhash.Hash{one, one, one}, 3, 1)
	require.Equal(t, 1, s.Length())
	require.True(t, s.Exists(one))
	require.Equal(t, []chainhash.Hash{one}, collect(s))
}

// ground hashes share long prefixes, forcing overloaded slots (binary search path)
func TestSortedHashSet_AdversarialPrefixes(t *testing.T) {
	mk := func(sharedBytes, n int, seed int64) []chainhash.Hash {
		hashes := randHashes(n, seed)
		for i := range hashes {
			for b := 0; b < sharedBytes; b++ {
				hashes[i][b] = 0xAB
			}
		}

		return hashes
	}

	for _, shared := range []int{1, 2, 5, 8, 9, 16, 24, 31} {
		hashes := mk(shared, 3000, int64(shared))
		s := buildSet(t, hashes, len(hashes), 4)
		assertMatchesReference(t, s, hashes)
	}

	// identical first word, differing later words, and all-zero / all-ones extremes
	var ones chainhash.Hash
	for i := range ones {
		ones[i] = 0xFF
	}

	hashes := []chainhash.Hash{{}, {31: 1}, {8: 1}, {16: 1}, {24: 1}, ones}
	s := buildSet(t, hashes, 0, 2)
	assertMatchesReference(t, s, hashes)

	// every hash in one slot yet more than the 16 linear-scan limit
	big := make([]chainhash.Hash, 500)
	for i := range big {
		binary.BigEndian.PutUint64(big[i][24:], uint64(i)*7919)
	}

	s = buildSet(t, big, len(big), 2)
	assertMatchesReference(t, s, big)
}

func TestSortedHashSet_DeterministicAcrossLayouts(t *testing.T) {
	hashes := randHashes(150000, 5)
	hashes = append(hashes, hashes[:1000]...) // duplicates

	probes := append(randHashes(5000, 6), hashes[:5000]...)

	var wantIter []chainhash.Hash

	var wantExists []bool

	for i, cfg := range []struct{ capacity, workers int }{{0, 1}, {len(hashes), 1}, {len(hashes), 7}, {len(hashes) * 3, 64}, {10, 2}} {
		shuffled := append([]chainhash.Hash(nil), hashes...)
		rand.New(rand.NewSource(int64(i)+100)).Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] }) //nolint:gosec

		s := buildSet(t, shuffled, cfg.capacity, cfg.workers)

		gotExists := make([]bool, len(probes))
		for j, p := range probes {
			gotExists[j] = s.Exists(p)
		}

		if i == 0 {
			wantIter, wantExists = collect(s), gotExists
			continue
		}

		require.Equal(t, wantIter, collect(s), "iteration order must not depend on insertion order, capacity hint or workers")
		require.Equal(t, wantExists, gotExists)
	}
}

func TestSortedHashSet_ConcurrentBuildAndRead(t *testing.T) {
	hashes := randHashes(200000, 11)
	s := NewSortedHashSet(len(hashes))

	var wg sync.WaitGroup

	const writers = 8

	for w := 0; w < writers; w++ {
		wg.Add(1)

		go func(w int) {
			defer wg.Done()

			var local []chainhash.Hash
			for i := w; i < len(hashes); i += writers {
				if i%2 == 0 {
					require.NoError(t, s.Put(hashes[i]))
				} else {
					local = append(local, hashes[i])
				}
			}

			batches := make(map[int][]chainhash.Hash)
			for _, h := range local {
				batches[s.Partition(h)] = append(batches[s.Partition(h)], h)
			}

			for p, b := range batches {
				require.NoError(t, s.PutMultiBucket(p, b))
			}
		}(w)
	}

	wg.Wait()
	require.NoError(t, s.Freeze(8))

	for r := 0; r < 8; r++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for _, h := range hashes {
				if !s.Exists(h) {
					t.Errorf("missing %s", h)
					return
				}
			}
		}()
	}

	wg.Wait()
	require.Equal(t, len(hashes), s.Length())
}

func TestSortedHashSet_FrozenContract(t *testing.T) {
	s := NewSortedHashSet(10)
	h := randHashes(1, 1)[0]
	require.NoError(t, s.Put(h))

	// reads before Freeze must not silently answer
	require.PanicsWithValue(t, errNotFrozen, func() { s.Exists(h) })
	require.PanicsWithValue(t, errNotFrozen, func() { s.Length() })
	require.PanicsWithValue(t, errNotFrozen, func() { s.Iter(func(chainhash.Hash) bool { return true }) })

	require.NoError(t, s.Freeze(1))

	require.True(t, s.Exists(h))
	require.Error(t, s.Put(h), "Put after Freeze must fail")
	require.Error(t, s.PutMultiBucket(0, []chainhash.Hash{h}), "PutMultiBucket after Freeze must fail")
	require.Error(t, s.Freeze(1), "second Freeze must fail")
	require.Equal(t, 1, s.Length())
}

func TestSortedHashSet_EmptyAndSingle(t *testing.T) {
	s := NewSortedHashSet(0)
	require.NoError(t, s.Freeze(0))
	require.Equal(t, 0, s.Length())
	require.False(t, s.Exists(chainhash.Hash{}))
	require.Empty(t, collect(s))

	h := randHashes(1, 3)[0]
	s = buildSet(t, []chainhash.Hash{h}, 1, 1)
	require.Equal(t, 1, s.Length())
	require.True(t, s.Exists(h))
	require.False(t, s.Exists(chainhash.Hash{}))
}

func TestSortedHashSet_IterStopsEarly(t *testing.T) {
	s := buildSet(t, randHashes(1000, 9), 1000, 2)

	n := 0
	s.Iter(func(chainhash.Hash) bool {
		n++
		return n < 10
	})
	require.Equal(t, 10, n)
}

func TestSortedHashSet_CapacityLimit(t *testing.T) {
	require.NoError(t, checkSortedHashSetSize(math.MaxUint32))
	err := checkSortedHashSetSize(math.MaxUint32 + 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "4294967295")

	// Freeze must refuse before allocating anything
	s := NewSortedHashSet(0)
	s.parts[0].n = math.MaxUint32 + 1
	require.Error(t, s.Freeze(1))
}

func FuzzSortedHashSet(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, uint8(3))
	f.Add(make([]byte, 400), uint8(9))

	f.Fuzz(func(t *testing.T, data []byte, knob uint8) {
		// Build hashes from a tiny alphabet so prefixes collide and duplicates occur.
		// The knob picks how many leading bytes are forced equal (ground hashes), the
		// capacity hint and the worker count.
		shared := int(knob % 12)

		var hashes []chainhash.Hash

		for i := 0; i+2 <= len(data); i += 2 {
			var h chainhash.Hash
			for b := shared; b < 32; b++ {
				h[b] = data[i] % 3
			}

			h[31] = data[i+1]
			if shared < 31 {
				h[shared] = data[i]
			}

			hashes = append(hashes, h)
		}

		s := NewSortedHashSet(int(knob) * 1000)
		for _, h := range hashes {
			require.NoError(t, s.Put(h))
		}

		require.NoError(t, s.Freeze(int(knob%5)))

		ref := make(map[chainhash.Hash]struct{})
		for _, h := range hashes {
			ref[h] = struct{}{}
		}

		require.Equal(t, len(ref), s.Length())

		for _, h := range hashes {
			require.True(t, s.Exists(h))

			m := h
			m[31] ^= 0x80
			_, ok := ref[m]
			require.Equal(t, ok, s.Exists(m))
		}

		require.Equal(t, sortedDistinct(hashes), collect(s))
	})
}
