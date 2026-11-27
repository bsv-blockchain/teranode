package subtreeprocessor

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// deltaCase describes the blocks of one delta test. Every block has subtrees subtrees of
// subtreeSize leaves, the first one starting with the coinbase placeholder like a real
// block's. The winners take overlap of the lost nodes, shuffled, and fresh hashes for
// the rest of their slots.
type deltaCase struct {
	lostBlocks, winBlocks, subtrees, subtreeSize int
	// winnerSubtrees is the subtrees of a winner block, subtrees when zero
	winnerSubtrees   int
	overlap          float64
	seed             int64
	dupAcrossWinners bool
	// dupIsLost makes the hash that dupAcrossWinners duplicates a lost node too
	dupIsLost bool
}

// deltaBlocks is what newDeltaBlocks stored: the blocks and the leaves of every subtree.
type deltaBlocks struct {
	lost, winners            []*model.Block
	lostLeaves, winnerLeaves [][][]chainhash.Hash // [block][subtree][node]
}

func storeDeltaBlock(t testing.TB, store *blob_memory.Memory, leaves [][]chainhash.Hash) *model.Block {
	t.Helper()

	block := &model.Block{Subtrees: make([]*chainhash.Hash, len(leaves))}

	for i, l := range leaves {
		root := storeSubtree(t, store, l)
		block.Subtrees[i] = &root
		block.TransactionCount += uint64(len(l))
	}

	return block
}

func newDeltaBlocks(t testing.TB, store *blob_memory.Memory, c deltaCase) *deltaBlocks {
	t.Helper()

	r := rand.New(rand.NewSource(c.seed)) //nolint:gosec // deterministic test data
	winnerSubtrees := cmp.Or(c.winnerSubtrees, c.subtrees)
	lostPerBlock := c.subtrees*c.subtreeSize - 1
	winPerBlock := winnerSubtrees*c.subtreeSize - 1
	pool := randHashes(c.lostBlocks*lostPerBlock+c.winBlocks*winPerBlock, c.seed)

	split := func(flat []chainhash.Hash, perBlock, subtrees int) [][][]chainhash.Hash {
		out := make([][][]chainhash.Hash, len(flat)/perBlock)

		for b := range out {
			part := flat[b*perBlock : (b+1)*perBlock]
			out[b] = make([][]chainhash.Hash, subtrees)

			for s := range out[b] {
				if s == 0 {
					out[b][s] = append([]chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue}, part[:c.subtreeSize-1]...)
					part = part[c.subtreeSize-1:]

					continue
				}

				out[b][s] = append([]chainhash.Hash(nil), part[:c.subtreeSize]...)
				part = part[c.subtreeSize:]
			}
		}

		return out
	}

	lostFlat := pool[:c.lostBlocks*lostPerBlock]
	winFlat := append([]chainhash.Hash(nil), pool[c.lostBlocks*lostPerBlock:]...)
	shared := append([]chainhash.Hash(nil), lostFlat...)
	r.Shuffle(len(shared), func(i, j int) { shared[i], shared[j] = shared[j], shared[i] })
	copy(winFlat, shared[:min(len(winFlat), int(c.overlap*float64(len(lostFlat))))])
	r.Shuffle(len(winFlat), func(i, j int) { winFlat[i], winFlat[j] = winFlat[j], winFlat[i] })

	d := &deltaBlocks{lostLeaves: split(lostFlat, lostPerBlock, c.subtrees), winnerLeaves: split(winFlat, winPerBlock, winnerSubtrees)}

	if c.dupAcrossWinners {
		// one hash in two winner blocks, besides the placeholder every block has
		d.winnerLeaves[len(d.winnerLeaves)-1][0][1] = d.winnerLeaves[0][0][1]
	}

	if c.dupIsLost {
		d.lostLeaves[0][0][1] = d.winnerLeaves[0][0][1]
	}

	for _, l := range d.lostLeaves {
		d.lost = append(d.lost, storeDeltaBlock(t, store, l))
	}

	for _, l := range d.winnerLeaves {
		d.winners = append(d.winners, storeDeltaBlock(t, store, l))
	}

	return d
}

// referenceDelta is the map-based model of computeReorgDelta.
func referenceDelta(d *deltaBlocks) *reorgDelta {
	win := make(map[chainhash.Hash]struct{})
	lostSet := make(map[chainhash.Hash]struct{})

	for _, b := range d.winnerLeaves {
		for _, st := range b {
			for _, h := range st {
				win[h] = struct{}{}
			}
		}
	}

	ref := &reorgDelta{D1: []reorgDeltaNode{}}

	for b, blk := range d.lostLeaves {
		for s, st := range blk {
			for n, h := range st {
				if h == subtreepkg.CoinbasePlaceholderHashValue {
					continue
				}

				lostSet[h] = struct{}{}

				if _, ok := win[h]; ok {
					ref.Hits++
				} else {
					ref.D1 = append(ref.D1, reorgDeltaNode{Hash: h, Block: b, Subtree: s, Node: n})
				}
			}
		}
	}

	delete(win, subtreepkg.CoinbasePlaceholderHashValue)

	for h := range win {
		if _, ok := lostSet[h]; !ok {
			ref.D2 = append(ref.D2, h)
		}
	}

	ref.D2 = sortedDistinct(ref.D2)

	return ref
}

// requireDeltaMatchesReference runs the union build and the delta over the stored blocks.
func requireDeltaMatchesReference(t testing.TB, stp *SubtreeProcessor, d *deltaBlocks, workers int) {
	t.Helper()

	ctx := context.Background()

	winners, err := stp.buildUnionSet(ctx, d.winners)
	require.NoError(t, err)

	got, err := stp.computeReorgDelta(ctx, d.lost, winners, workers)
	require.NoError(t, err)

	want := referenceDelta(d)

	require.Equal(t, want.Hits, got.Hits)
	// slices.Equal, not require.Equal: a mismatch of a million hashes would be rendered as a diff
	require.True(t, slices.Equal(want.D1, got.D1), "D1 must be the lost nodes not in the winners, in the lost blocks' order: want %d got %d", len(want.D1), len(got.D1))
	require.True(t, slices.Equal(want.D2, got.D2), "D2 must be each winner hash nobody lost, once, ascending: want %d got %d", len(want.D2), len(got.D2))
}

// TestReorgDelta_MatchesReference pins that both directions are exact against a map
// model: at every overlap with one and several blocks per side, and for duplicates in
// the winners, a duplicate that was lost, a many-partition winner set, any worker bound.
func TestReorgDelta_MatchesReference(t *testing.T) {
	stp, store := newTxMapTestProcessor(t)

	type row struct {
		name    string
		c       deltaCase
		workers int
		// fixture preconditions, so a row cannot pass without the shape it is named for
		wantDups, wantLostDup, wantPartitions bool
	}

	var rows []row

	seed := int64(100)

	for _, shape := range []struct{ lost, win int }{{1, 1}, {1, 2}, {3, 2}, {2, 3}} {
		for _, overlap := range []float64{0, 0.1, 0.5, 0.9, 1} {
			seed++

			c := deltaCase{lostBlocks: shape.lost, winBlocks: shape.win, subtrees: 3, subtreeSize: 64, overlap: overlap, seed: seed}
			rows = append(rows, row{name: testName(c), c: c, workers: 4})
		}
	}

	rows = append(rows,
		row{name: "duplicate across winners", workers: 3, wantDups: true,
			c: deltaCase{lostBlocks: 1, winBlocks: 3, subtrees: 2, subtreeSize: 64, overlap: 0.5, seed: 7, dupAcrossWinners: true}},
		row{name: "duplicate winner that was lost", workers: 3, wantDups: true, wantLostDup: true,
			c: deltaCase{lostBlocks: 2, winBlocks: 2, subtrees: 2, subtreeSize: 64, overlap: 0.3, seed: 21, dupAcrossWinners: true, dupIsLost: true}},
		row{name: "many partitions overlap 50", workers: 4, wantDups: true, wantLostDup: true, wantPartitions: true,
			c: deltaCase{lostBlocks: 1, winBlocks: 2, subtrees: 2, subtreeSize: 1 << 16, overlap: 0.5, seed: 36, dupAcrossWinners: true, dupIsLost: true}},
		row{name: "many partitions overlap 100", workers: 4, wantDups: true, wantLostDup: true, wantPartitions: true,
			c: deltaCase{lostBlocks: 1, winBlocks: 2, subtrees: 2, subtreeSize: 1 << 16, overlap: 1, seed: 41, dupAcrossWinners: true, dupIsLost: true}},
	)

	for _, w := range []int{0, 1, 2, 16} {
		rows = append(rows, row{name: fmt.Sprintf("workers %d", w), workers: w,
			c: deltaCase{lostBlocks: 2, winBlocks: 2, subtrees: 4, subtreeSize: 32, overlap: 0.7, seed: 9}})
	}

	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			d := newDeltaBlocks(t, store, r.c)

			if r.wantDups || r.wantPartitions {
				winners, err := stp.buildUnionSet(context.Background(), d.winners)
				require.NoError(t, err)

				if r.wantDups {
					require.Less(t, winners.Length(), int(winners.Positions()), "the fixture must have duplicates")
				}

				if r.wantPartitions {
					require.Greater(t, winners.Partitions(), 1)
				}
			}

			if r.wantLostDup {
				dup := d.winnerLeaves[0][0][1]
				require.Equal(t, dup, d.winnerLeaves[1][0][1])
				require.Equal(t, dup, d.lostLeaves[0][0][1])
			}

			requireDeltaMatchesReference(t, stp, d, r.workers)
		})
	}
}

func testName(c deltaCase) string {
	return "lost" + string(rune('0'+c.lostBlocks)) + "_win" + string(rune('0'+c.winBlocks)) + "_overlap" + string(rune('0'+int(c.overlap*10)/10)) + string(rune('0'+int(c.overlap*10)%10))
}

// TestReorgDelta_UnreadableSubtreeFails pins that no partial result is returned when a
// lost subtree is missing or cannot be parsed, and that a missing winner subtree fails
// the union build.
func TestReorgDelta_UnreadableSubtreeFails(t *testing.T) {
	stp, store := newTxMapTestProcessor(t)
	ctx := context.Background()
	d := newDeltaBlocks(t, store, deltaCase{lostBlocks: 2, winBlocks: 1, subtrees: 3, subtreeSize: 32, overlap: 0.5, seed: 11})

	winners, err := stp.buildUnionSet(ctx, d.winners)
	require.NoError(t, err)

	missing := chainhash.Hash{0xde, 0xad}
	garbage := chainhash.Hash{0xbe, 0xef}
	require.NoError(t, store.Set(ctx, garbage[:], fileformat.FileTypeSubtree, []byte{1, 2, 3}))

	for name, bad := range map[string]*chainhash.Hash{"missing": &missing, "garbage": &garbage} {
		t.Run("lost "+name, func(t *testing.T) {
			lost := []*model.Block{d.lost[0], {Subtrees: []*chainhash.Hash{d.lost[1].Subtrees[0], bad, d.lost[1].Subtrees[2]}}}

			got, err := stp.computeReorgDelta(ctx, lost, winners, 4)
			require.Error(t, err)
			require.Nil(t, got)
		})

		t.Run("winner "+name, func(t *testing.T) {
			set, err := stp.buildUnionSet(ctx, []*model.Block{{Subtrees: []*chainhash.Hash{d.winners[0].Subtrees[0], bad}}})
			require.Error(t, err)
			require.Nil(t, set)
		})
	}
}

// TestReorgDelta_FallsBackToSubtreeToCheck pins the read order of CreateTransactionMap: a
// subtree that is only stored as to-check is read.
func TestReorgDelta_FallsBackToSubtreeToCheck(t *testing.T) {
	stp, store := newTxMapTestProcessor(t)
	ctx := context.Background()
	d := newDeltaBlocks(t, store, deltaCase{lostBlocks: 1, winBlocks: 1, subtrees: 2, subtreeSize: 32, overlap: 0.5, seed: 13})

	root := d.lost[0].Subtrees[1]
	b, err := store.Get(ctx, root[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.NoError(t, store.Del(ctx, root[:], fileformat.FileTypeSubtree))
	require.NoError(t, store.Set(ctx, root[:], fileformat.FileTypeSubtreeToCheck, b))

	requireDeltaMatchesReference(t, stp, d, 2)
}

// FuzzReorgDelta compares the delta with the map model on small random shapes.
func FuzzReorgDelta(f *testing.F) {
	f.Add(uint8(1), uint8(1), uint8(2), uint8(3), uint8(50), int64(1), false)
	f.Add(uint8(2), uint8(3), uint8(1), uint8(4), uint8(100), int64(2), true)
	f.Add(uint8(1), uint8(2), uint8(3), uint8(2), uint8(0), int64(3), true)

	f.Fuzz(func(t *testing.T, lostBlocks, winBlocks, subtrees, sizeLog, overlap uint8, seed int64, dup bool) {
		c := deltaCase{
			lostBlocks:       1 + int(lostBlocks)%3,
			winBlocks:        1 + int(winBlocks)%3,
			subtrees:         1 + int(subtrees)%3,
			subtreeSize:      1 << (1 + int(sizeLog)%5),
			overlap:          float64(overlap%101) / 100,
			seed:             seed,
			dupAcrossWinners: dup && winBlocks%3 > 0,
		}

		stp, store := newTxMapTestProcessor(t)
		requireDeltaMatchesReference(t, stp, newDeltaBlocks(t, store, c), 1+int(subtrees)%4)
	})
}

// BenchmarkReorgDelta times computeReorgDelta for 1M lost nodes against a 2M-hash winner
// set at 50% and 99% overlap; subtrees are read from the memory blob store every iteration.
func BenchmarkReorgDelta(b *testing.B) {
	overlaps := []float64{0.5, 0.99}
	if testing.Short() {
		overlaps = overlaps[:1]
	}

	for _, overlap := range overlaps {
		// the benchmark function runs again for every b.N the framework tries: build once
		var (
			stp       *SubtreeProcessor
			d         *deltaBlocks
			winners   *SortedHashSet
			lostNodes int
		)

		b.Run(fmt.Sprintf("lost=1M/winners=2M/overlap=%.0f%%", overlap*100), func(b *testing.B) {
			if stp == nil {
				var store *blob_memory.Memory

				stp, store = newTxMapTestProcessor(b)
				d = newDeltaBlocks(b, store, deltaCase{
					lostBlocks: 1, winBlocks: 2, subtrees: 16, subtreeSize: 1 << 16,
					overlap: overlap, seed: 2269,
				})

				var err error

				winners, err = stp.buildUnionSet(context.Background(), d.winners)
				require.NoError(b, err)

				for _, st := range d.lostLeaves[0] {
					lostNodes += len(st)
				}
			}

			ctx := context.Background()
			workers := stp.settings.BlockAssembly.SubtreeProcessorConcurrentReads

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				delta, err := stp.computeReorgDelta(ctx, d.lost, winners, workers)
				if err != nil {
					b.Fatal(err)
				}

				if delta.Hits == 0 {
					b.Fatal("no hits")
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(lostNodes)*float64(b.N)/b.Elapsed().Seconds(), "nodes/s")
		})
	}
}
