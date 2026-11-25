package subtreeprocessor

import (
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

type heldHashes map[chainhash.Hash]struct{}

func (s heldHashes) Exists(h chainhash.Hash) bool { _, ok := s[h]; return ok }

func nodesOf(hashes ...chainhash.Hash) []subtreepkg.Node {
	nodes := make([]subtreepkg.Node, len(hashes))
	for i, hash := range hashes {
		nodes[i] = subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1}
	}

	return nodes
}

func TestMovedBackMarks_SplitsForwardBlocksAndLeavesTheUnmined(t *testing.T) {
	a, b, c, d, x, y := h("a"), h("b"), h("c"), h("d"), h("x"), h("y")
	coinbase := subtreepkg.CoinbasePlaceholderHashValue

	m := newMovedBackMarks(8)
	// the placeholder is the first node of the lost block's first subtree and is never moved back
	m.add(nodesOf(coinbase, a, b))
	m.add(nodesOf(c, d))
	require.NoError(t, m.freeze(2))

	// a forward block mines b, and x and y which the lost block lacked
	forward := buildSet(t, []chainhash.Hash{b, x, y, coinbase}, 8, 2)

	notMovedBack := m.markForward(forward, 2)
	require.Equal(t, sortedDistinct([]chainhash.Hash{x, y}), notMovedBack, "ascending, without the placeholder and the moved-back hash")

	// a second block mines c
	require.Empty(t, m.markForward(buildSet(t, []chainhash.Hash{c, coinbase}, 8, 2), 2))

	// the assembly holds a, c and d, and not b: a and d were never mined forward
	left := m.stillIn(heldHashes{a: {}, c: {}, d: {}}, 2)
	require.ElementsMatch(t, []chainhash.Hash{a, d}, left)

	require.NotContains(t, left, coinbase)
}

func TestMovedBackMarks_NoForwardBlocksLeavesEverythingHeld(t *testing.T) {
	a, b := h("a"), h("b")

	m := newMovedBackMarks(2)
	m.add(nodesOf(a, b))
	require.NoError(t, m.freeze(1))

	require.ElementsMatch(t, []chainhash.Hash{a, b}, m.stillIn(heldHashes{a: {}, b: {}}, 1))
	require.Empty(t, m.stillIn(heldHashes{}, 1))
}

func TestMovedBackMarks_MatchesGoMapOnRandomSets(t *testing.T) {
	hashes := randHashes(40_000, 11)
	lost, winners := hashes[:25_000], hashes[10_000:] // 15,000 in both

	m := newMovedBackMarks(len(lost))
	for start := 0; start < len(lost); start += 4096 {
		m.add(nodesOf(lost[start:min(start+4096, len(lost))]...))
	}

	require.NoError(t, m.freeze(4))

	model := make(map[chainhash.Hash]bool, len(lost))
	for _, hash := range lost {
		model[hash] = false
	}

	var wantNotMoved []chainhash.Hash

	for _, hash := range sortedDistinct(winners) {
		if _, ok := model[hash]; ok {
			model[hash] = true
		} else {
			wantNotMoved = append(wantNotMoved, hash)
		}
	}

	require.Equal(t, wantNotMoved, m.markForward(buildSet(t, winners, len(winners), 4), 4))

	pending := make(heldHashes)
	for i, hash := range hashes {
		if i%3 == 0 {
			pending[hash] = struct{}{}
		}
	}

	want := movedBackStillInAssembly(model, pending)
	require.ElementsMatch(t, want, m.stillIn(pending, 4))
}

// retainedBytes is what a frozen movedBackMarks holds, counted from the capacity of the slices it
// keeps: the sorted hashes, the directory over them, the partition offsets, the mined bitmap and any
// build chunks not yet let go. It reads no heap statistics, so what other tests allocated cannot move it.
func (m *movedBackMarks) retainedBytes() uint64 {
	s := m.set
	bytes := uint64(cap(s.dir))*4 + uint64(cap(s.base))*8 + uint64(cap(m.mined))*8

	for i := range s.parts {
		bytes += uint64(cap(s.parts[i].keys)) * chainhash.HashSize
		bytes += uint64(cap(s.parts[i].chunks)) * 24 // slice headers

		for _, c := range s.parts[i].chunks {
			bytes += uint64(cap(c)) * chainhash.HashSize
		}
	}

	return bytes
}

// A Go map of 32-byte keys and a bool costs at least 38 bytes an entry: its slots are 33 bytes, one
// control byte each, and it grows before it is more than 7/8 full. The frozen set must stay near its 33.
// The sizes are counted from the structures' own slices rather than read from the heap, which other
// tests' garbage and goroutines skew.
func TestMovedBackMarks_HoldsFewerBytesThanAGoMap(t *testing.T) {
	const (
		n             = 400_000
		goMapMinBytes = 38 // per entry, see above
	)

	m := newMovedBackMarks(n)
	m.add(nodesOf(randHashes(n, 5)...))
	require.NoError(t, m.freeze(4))

	frozen := m.retainedBytes()
	goMap := uint64(n * goMapMinBytes)

	require.GreaterOrEqual(t, frozen, uint64(n*chainhash.HashSize), "the hashes themselves are held")
	require.Less(t, frozen, goMap*9/10, "frozen %d bytes against a Go map's at least %d", frozen, goMap)
}

// BenchmarkReorgMovedBackMarks times the marks of the slow reorg's moved-back blocks: building what is
// kept of them, probing every forward transaction against it, and listing the moved-back transactions
// block assembly still holds. gomap is what the slow path did before issue 2269 (PR 9): a Go map of hash
// to bool filled and probed by one goroutine. frozen is movedBackMarks.
func BenchmarkReorgMovedBackMarks(b *testing.B) {
	lostN, winnersN := 1<<20, 4<<20
	if testing.Short() { // the benchmark-compare job runs with -short
		lostN, winnersN = 1<<16, 1<<18
	}

	hashes := randHashes(winnersN+lostN/2, 9)
	lost := hashes[winnersN-lostN/2:] // half of the lost block is in the winners
	winners := hashes[:winnersN]
	nodes := nodesOf(lost...)
	winnerSet := buildSet(b, winners, winnersN, runtime.GOMAXPROCS(0))
	workers := runtime.GOMAXPROCS(0)

	assembly := make(heldHashes, winnersN/2)
	for _, hash := range hashes[:winnersN/2] {
		assembly[hash] = struct{}{}
	}

	b.Run("impl=gomap", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			movedBack := make(map[chainhash.Hash]bool)
			for _, node := range nodes {
				movedBack[node.Hash] = false
			}

			var notMoved []chainhash.Hash

			winnerSet.Iter(func(hash chainhash.Hash) bool {
				if _, ok := movedBack[hash]; ok {
					movedBack[hash] = true
				} else {
					notMoved = append(notMoved, hash)
				}

				return false
			})

			_ = movedBackStillInAssembly(movedBack, assembly)
			_ = notMoved
		}
	})

	b.Run("impl=frozen", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			m := newMovedBackMarks(lostN)

			parallelFor(len(nodes)/65536, workers, func(c int) { m.add(nodes[c*65536 : (c+1)*65536]) })

			if err := m.freeze(workers); err != nil {
				b.Fatal(err)
			}

			_ = m.markForward(winnerSet, workers)
			_ = m.stillIn(assembly, workers)
		}
	})
}
