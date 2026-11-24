package subtreeprocessor

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// recordingLaneKeeper keeps what the processor hands it, and the number of chained subtrees at
// the moment it was handed each subtree's bitmaps.
type recordingLaneKeeper struct {
	mu           sync.Mutex
	stp          *SubtreeProcessor
	bitmaps      map[chainhash.Hash]lane.LaneBitmaps
	unpositioned map[chainhash.Hash]int
	chainedAt    map[chainhash.Hash]int
}

func newRecordingLaneKeeper() *recordingLaneKeeper {
	return &recordingLaneKeeper{
		bitmaps:      map[chainhash.Hash]lane.LaneBitmaps{},
		unpositioned: map[chainhash.Hash]int{},
		chainedAt:    map[chainhash.Hash]int{},
	}
}

func (k *recordingLaneKeeper) KeepLaneBitmaps(bitmaps lane.SubtreeBitmaps, unpositioned int) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.bitmaps[bitmaps.SubtreeHash] = bitmaps.Lanes
	k.unpositioned[bitmaps.SubtreeHash] = unpositioned

	if k.stp != nil {
		k.chainedAt[bitmaps.SubtreeHash] = len(k.stp.chainedSubtrees)
	}
}

func (k *recordingLaneKeeper) HasLaneBitmaps(root chainhash.Hash) bool {
	k.mu.Lock()
	defer k.mu.Unlock()

	_, ok := k.bitmaps[root]

	return ok
}

// positions returns the packed positions kept for root, in lane then position order.
func (k *recordingLaneKeeper) positions(t *testing.T, root chainhash.Hash) []uint64 {
	t.Helper()

	k.mu.Lock()
	defer k.mu.Unlock()

	lanes, ok := k.bitmaps[root]
	require.True(t, ok, "bitmaps are kept for subtree %s", root)

	var out []uint64

	for l := 0; l <= lane.MaxLane; l++ {
		bm := lanes[uint8(l)] //nolint:gosec // l is at most MaxLane
		if bm == nil {
			continue
		}

		for _, p := range bm.ToArray() {
			out = append(out, lane.Pack(uint8(l), p)) //nolint:gosec // l is at most MaxLane
		}
	}

	return out
}

// keptLanePoolProcessor is lanePoolProcessor at shadow with a recording keeper.
func keptLanePoolProcessor(t *testing.T, disk bool) (*SubtreeProcessor, *recordingLaneKeeper) {
	t.Helper()

	keeper := newRecordingLaneKeeper()
	stp, _ := lanePoolProcessorWith(t, disk, true, WithLaneBitmapsKeeper(keeper))
	keeper.stp = stp

	return stp, keeper
}

func addPositioned(t *testing.T, stp *SubtreeProcessor, i int, position uint64) {
	t.Helper()

	require.NoError(t, stp.addNodeAt(subtreepkg.Node{Hash: lanePoolTxHash(i), Fee: 1, SizeInBytes: 1}, lanePoolInpoints(i), position, true))
}

// A subtree's lane bitmaps are kept when it is sealed, before it is chained, from the positions
// recorded as its members were added; a member without a position is counted, not kept.
func TestSealedSubtreeHasItsLaneBitmapsBeforeItIsChained(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)

			// 4-node subtrees: the coinbase placeholder and 3 txs, then 4 txs
			addPositioned(t, stp, 1, lane.Pack(3, 10))
			addPositioned(t, stp, 2, 0)
			addPositioned(t, stp, 3, lane.Pack(3, 12))

			for i := 4; i < 8; i++ {
				addPositioned(t, stp, i, lane.Pack(4, uint64(20+i))) //nolint:gosec // small test index
			}

			require.Len(t, stp.chainedSubtrees, 2)

			first, second := *stp.chainedSubtrees[0].RootHash(), *stp.chainedSubtrees[1].RootHash()

			require.Equal(t, []uint64{lane.Pack(3, 10), lane.Pack(3, 12)}, keeper.positions(t, first))
			require.Equal(t, 1, keeper.unpositioned[first])
			require.Equal(t, []uint64{lane.Pack(4, 24), lane.Pack(4, 25), lane.Pack(4, 26), lane.Pack(4, 27)}, keeper.positions(t, second))

			require.Equal(t, 0, keeper.chainedAt[first], "kept before the first subtree is chained")
			require.Equal(t, 1, keeper.chainedAt[second], "kept before the second subtree is chained")
		})
	}
}

// A subtree whose recorded positions miss members gets its lane bitmaps from the pool when it is
// sealed.
func TestSealBuildsFromThePoolWhenTheRecordMissesMembers(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)

			addPositioned(t, stp, 1, lane.Pack(5, 1))
			stp.laneFill = laneFill{} // the record loses the member just added

			addPositioned(t, stp, 2, lane.Pack(5, 2))
			addPositioned(t, stp, 3, lane.Pack(5, 3))

			require.Len(t, stp.chainedSubtrees, 1)
			require.Equal(t, []uint64{lane.Pack(5, 1), lane.Pack(5, 2), lane.Pack(5, 3)}, keeper.positions(t, *stp.chainedSubtrees[0].RootHash()))
		})
	}
}

// A tx removed from the open subtree leaves its position out of the subtree's bitmaps when it is
// sealed, with no pool read at the seal.
func TestRemovalFromTheOpenSubtreeLeavesItsPositionOut(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)

			addPositioned(t, stp, 1, lane.Pack(6, 1))
			addPositioned(t, stp, 2, lane.Pack(6, 2))

			require.NoError(t, stp.removeTxFromSubtrees(context.Background(), lanePoolTxHash(1)))
			require.True(t, stp.laneFill.matches(stp.currentSubtree.Load()), "the record follows the open subtree's copy")

			addPositioned(t, stp, 3, lane.Pack(6, 3))
			addPositioned(t, stp, 4, lane.Pack(6, 4))

			require.Len(t, stp.chainedSubtrees, 1)
			require.Equal(t, []uint64{lane.Pack(6, 2), lane.Pack(6, 3), lane.Pack(6, 4)}, keeper.positions(t, *stp.chainedSubtrees[0].RootHash()))
		})
	}
}

// A tx removed from a chained subtree re-chains the subtrees from there, and the re-chained
// subtrees' bitmaps leave its position out and keep everyone else's.
func TestRemovalFromAChainedSubtreeRebuildsItsBitmaps(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)

			for i := 1; i <= 7; i++ {
				addPositioned(t, stp, i, lane.Pack(7, uint64(i))) //nolint:gosec // small test index
			}

			require.Len(t, stp.chainedSubtrees, 2)

			require.NoError(t, stp.removeTxFromSubtrees(context.Background(), lanePoolTxHash(2)))

			var kept []uint64
			for _, st := range stp.chainedSubtrees {
				kept = append(kept, keeper.positions(t, *st.RootHash())...)
			}

			// the re-chain leaves 6 txs: the first subtree (placeholder and 3) is chained, and 3
			// stay in the open subtree, whose record covers them
			require.Equal(t, []uint64{lane.Pack(7, 1), lane.Pack(7, 3), lane.Pack(7, 4)}, kept)
			require.True(t, stp.laneFill.matches(stp.currentSubtree.Load()))

			addPositioned(t, stp, 8, lane.Pack(7, 8))
			require.Len(t, stp.chainedSubtrees, 2)
			require.Equal(t, []uint64{lane.Pack(7, 5), lane.Pack(7, 6), lane.Pack(7, 7), lane.Pack(7, 8)}, keeper.positions(t, *stp.chainedSubtrees[1].RootHash()))
		})
	}
}

// The parallel rebuild after a block keeps each full subtree's bitmaps, built from the positions
// the copy into the pool carried, before the subtree is chained, and leaves the open subtree with
// a record that covers the txs it took. The pool here holds the txs without positions, so a
// position read back from the pool would show as none.
func TestParallelRebuildKeepsEachSubtreesBitmaps(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)

			kept := make([]subtreepkg.Node, 0, 9)
			positions := make([]uint64, 0, 9)

			for i := 1; i <= 9; i++ {
				hash := lanePoolTxHash(100 + i)
				stp.currentTxMap.Set(hash, lanePoolInpoints(i))
				kept = append(kept, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
				positions = append(positions, lane.Pack(8, uint64(i))) //nolint:gosec // small test index
			}

			require.NoError(t, stp.parallelBuildRemainderSubtrees(context.Background(), kept, positions, true))
			require.Len(t, stp.chainedSubtrees, 2)

			first, second := *stp.chainedSubtrees[0].RootHash(), *stp.chainedSubtrees[1].RootHash()

			require.Equal(t, []uint64{lane.Pack(8, 1), lane.Pack(8, 2), lane.Pack(8, 3)}, keeper.positions(t, first))
			require.Equal(t, []uint64{lane.Pack(8, 4), lane.Pack(8, 5), lane.Pack(8, 6), lane.Pack(8, 7)}, keeper.positions(t, second))
			require.Equal(t, 0, keeper.chainedAt[first], "kept before the first subtree is chained")
			require.Equal(t, 1, keeper.chainedAt[second], "kept before the second subtree is chained")
			require.True(t, stp.laneFill.matches(stp.currentSubtree.Load()), "the open subtree's record covers the 2 txs it took")

			addPositioned(t, stp, 10, lane.Pack(8, 10))
			addPositioned(t, stp, 11, lane.Pack(8, 11))

			require.Len(t, stp.chainedSubtrees, 3)
			require.Equal(t, []uint64{lane.Pack(8, 8), lane.Pack(8, 9), lane.Pack(8, 10), lane.Pack(8, 11)}, keeper.positions(t, *stp.chainedSubtrees[2].RootHash()))
		})
	}
}

// A full chunk whose members are not all recorded, here the open subtree's own members, has its
// bitmaps built from the pool before it is chained and sent to storage.
func TestParallelRebuildKeepsAnUnrecordedChunkBeforeItIsChained(t *testing.T) {
	stp, keeper := keptLanePoolProcessor(t, false)

	// the open subtree holds one member its record does not cover
	stp.currentTxMap.SetAt(lanePoolTxHash(400), lanePoolInpoints(400), lane.Pack(6, 1))
	require.NoError(t, stp.addNodePreValidated(subtreepkg.Node{Hash: lanePoolTxHash(400), Fee: 1, SizeInBytes: 1}, lane.Pack(6, 1), true))
	stp.laneFill = laneFill{}

	kept := make([]subtreepkg.Node, 0, 2)
	positions := make([]uint64, 0, 2)

	for i := 1; i <= 2; i++ {
		hash := lanePoolTxHash(400 + i)
		stp.currentTxMap.SetAt(hash, lanePoolInpoints(400+i), lane.Pack(6, uint64(1+i))) //nolint:gosec // small test index
		kept = append(kept, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
		positions = append(positions, lane.Pack(6, uint64(1+i))) //nolint:gosec // small test index
	}

	require.NoError(t, stp.parallelBuildRemainderSubtrees(context.Background(), kept, positions, true))
	require.Len(t, stp.chainedSubtrees, 1)

	root := *stp.chainedSubtrees[0].RootHash()
	require.Equal(t, []uint64{lane.Pack(6, 1), lane.Pack(6, 2), lane.Pack(6, 3)}, keeper.positions(t, root))
	require.Equal(t, 0, keeper.chainedAt[root], "kept before it is chained")
}

// The copy between pool halves hands back each tx's lane position on every route it takes: the
// raw in-memory move, the raw disk move, and the decode-and-insert between map kinds.
func TestPoolCopyHandsBackEachTxsPosition(t *testing.T) {
	for _, tc := range []struct {
		name            string
		srcDisk, toDisk bool
	}{
		{"in-memory to in-memory", false, false},
		{"disk to disk", true, true},
		{"disk to in-memory", true, false},
		{"in-memory to disk", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stp, _ := lanePoolProcessor(t, tc.toDisk, true)

			var src TxInpointsMap = NewSplitTxInpointsMap(16)

			if tc.srcDisk {
				m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
				require.NoError(t, err)
				t.Cleanup(func() { _ = m.Close() })

				src = m
			}

			nodes := make([]subtreepkg.Node, 0, 5)
			want := make([]uint64, 0, 5)

			for i := 0; i < 5; i++ {
				hash := lanePoolTxHash(500 + i)

				position := lane.Pack(13, uint64(40+i)) //nolint:gosec // small test index
				if i == 2 {
					position = 0
				}

				src.SetAt(hash, lanePoolInpoints(i), position)
				nodes = append(nodes, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
				want = append(want, position)
			}

			wasInserted := make([]bool, len(nodes))
			positions := make([]uint64, len(nodes))

			require.NoError(t, stp.parallelGetAndSetIfNotExistsAt(nodes, src, 0, 2, wasInserted, positions))

			for i := range wasInserted {
				require.True(t, wasInserted[i])
			}

			require.Equal(t, want, positions)
		})
	}
}

// The own block's path adds the txs left in the open subtree with the positions the copy carried.
func TestOwnBlockPathRecordsTheCopiedPositions(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)
			stp.settings.BlockAssembly.ParallelSetIfNotExistsThreshold = 1

			var src TxInpointsMap = NewSplitTxInpointsMap(16)

			if disk {
				m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
				require.NoError(t, err)
				t.Cleanup(func() { _ = m.Close() })

				src = m
			}

			nodes := make([]subtreepkg.Node, 0, 3)

			for i := 1; i <= 3; i++ {
				hash := lanePoolTxHash(600 + i)
				src.SetAt(hash, lanePoolInpoints(i), lane.Pack(14, uint64(i))) //nolint:gosec // small test index
				nodes = append(nodes, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
			}

			block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}}
			require.NoError(t, stp.processOwnBlockSubtreeNodes(block, nodes, src, 0, nil, true))

			require.Len(t, stp.chainedSubtrees, 1)
			require.Equal(t, []uint64{lane.Pack(14, 1), lane.Pack(14, 2), lane.Pack(14, 3)}, keeper.positions(t, *stp.chainedSubtrees[0].RootHash()))
		})
	}
}

// A subtree a bulk rebuild chains has its bitmaps built from the pool before a mining candidate
// can list it.
func TestBulkRebuiltSubtreesGetBitmapsBeforeACandidateListsThem(t *testing.T) {
	stp, keeper := keptLanePoolProcessor(t, false)

	nodes := make([]subtreepkg.Node, 0, 7)

	for i := 1; i <= 7; i++ {
		hash := lanePoolTxHash(200 + i)
		stp.currentTxMap.SetAt(hash, lanePoolInpoints(i), lane.Pack(9, uint64(i))) //nolint:gosec // small test index
		nodes = append(nodes, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
	}

	require.NoError(t, stp.bulkBuildSubtrees(context.Background(), nodes, 4))
	require.Len(t, stp.chainedSubtrees, 2)
	require.False(t, keeper.HasLaneBitmaps(*stp.chainedSubtrees[0].RootHash()), "a bulk rebuild records no positions")

	stp.currentBlockHeader.Store(&model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}})
	stp.updatePrecomputedMiningData()

	require.Equal(t, []uint64{lane.Pack(9, 1), lane.Pack(9, 2), lane.Pack(9, 3)}, keeper.positions(t, *stp.chainedSubtrees[0].RootHash()))
	require.Equal(t, []uint64{lane.Pack(9, 4), lane.Pack(9, 5), lane.Pack(9, 6), lane.Pack(9, 7)}, keeper.positions(t, *stp.chainedSubtrees[1].RootHash()))
}

// A chained subtree whose members' positions the pool cannot give has no bitmaps, and is not
// tried again for the next candidate.
func TestASubtreeThePoolCannotGiveBitmapsForIsTriedOnce(t *testing.T) {
	stp, keeper := keptLanePoolProcessor(t, false)

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(lanePoolTxHash(300), 1, 1)) // not in the pool

	stp.ensureLaneBitmaps([]*subtreepkg.Subtree{st})
	require.False(t, keeper.HasLaneBitmaps(*st.RootHash()))
	require.Contains(t, stp.laneUnbuilt, *st.RootHash())

	// the pool now holds it, but the subtree is not built again while it stays chained
	stp.currentTxMap.SetAt(lanePoolTxHash(300), lanePoolInpoints(300), lane.Pack(1, 1))
	stp.ensureLaneBitmaps([]*subtreepkg.Subtree{st})
	require.False(t, keeper.HasLaneBitmaps(*st.RootHash()))

	// a subtree no longer chained leaves the record of unbuilt subtrees
	stp.ensureLaneBitmaps(nil)
	stp.ensureLaneBitmaps([]*subtreepkg.Subtree{})
	require.Contains(t, stp.laneUnbuilt, *st.RootHash(), "an empty list is not looked at")

	other, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, other.AddCoinbaseNode())
	require.NoError(t, other.AddNode(lanePoolTxHash(300), 1, 1))
	require.NoError(t, other.AddNode(lanePoolTxHash(301), 1, 1))
	stp.currentTxMap.SetAt(lanePoolTxHash(301), lanePoolInpoints(301), lane.Pack(1, 2))

	stp.ensureLaneBitmaps([]*subtreepkg.Subtree{other})
	require.NotContains(t, stp.laneUnbuilt, *st.RootHash())
	require.Equal(t, []uint64{lane.Pack(1, 1), lane.Pack(1, 2)}, keeper.positions(t, *other.RootHash()))
}

// A copy of the open subtree, which a mining candidate can list, has its bitmaps kept when it is
// made, and the open subtree's record is left as it was.
func TestIncompleteSubtreeCopyHasItsBitmaps(t *testing.T) {
	stp, keeper := keptLanePoolProcessor(t, false)

	addPositioned(t, stp, 1, lane.Pack(2, 1))
	addPositioned(t, stp, 2, lane.Pack(2, 2))

	copied, err := stp.createIncompleteSubtreeCopy()
	require.NoError(t, err)

	require.Equal(t, []uint64{lane.Pack(2, 1), lane.Pack(2, 2)}, keeper.positions(t, *copied.RootHash()))
	require.True(t, stp.laneFill.matches(stp.currentSubtree.Load()))
}

// A copy of a later open subtree leaves out its first member, which has no coinbase placeholder
// before it, so the copy's bitmaps hold only the members the copy has, not the open subtree's
// record.
func TestLaterIncompleteSubtreeCopyHasOnlyItsMembersBitmaps(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "in-memory map", true: "disk map"}[disk], func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)

			i := 0
			for len(stp.chainedSubtrees) == 0 {
				i++
				addPositioned(t, stp, i, lane.Pack(3, uint64(i))) //nolint:gosec // small test index
			}

			for j := 1; j <= 3; j++ {
				addPositioned(t, stp, i+j, lane.Pack(3, uint64(i+j))) //nolint:gosec // small test index
			}

			open := stp.currentSubtree.Load()
			require.NotEqual(t, subtreepkg.CoinbasePlaceholderHashValue, open.Nodes[0].Hash, "a later open subtree starts with a real tx")

			copied, err := stp.createIncompleteSubtreeCopy()
			require.NoError(t, err)

			require.Equal(t, []uint64{lane.Pack(3, uint64(i+2)), lane.Pack(3, uint64(i+3))}, keeper.positions(t, *copied.RootHash())) //nolint:gosec // small test index
		})
	}
}

// At off the processor records no position and keeps no bitmaps, even with a keeper given.
func TestOffRecordsNoLanePositions(t *testing.T) {
	keeper := newRecordingLaneKeeper()
	stp, _ := lanePoolProcessorWith(t, false, false, WithLaneBitmapsKeeper(keeper))

	require.Nil(t, stp.laneKeeper)

	for i := 1; i <= 7; i++ {
		addPositioned(t, stp, i, lane.Pack(1, uint64(i))) //nolint:gosec // small test index
	}

	require.Len(t, stp.chainedSubtrees, 2)
	require.Empty(t, keeper.bitmaps)
	require.Nil(t, stp.laneFill.lanes)
}

// BenchmarkSealLaneBitmaps measures building one 1M-member subtree's lane bitmaps from its
// recorded positions, 17 lanes taking turns in runs of 1,024 as 17 validators would, in arrival
// order and fully shuffled.
func BenchmarkSealLaneBitmaps(b *testing.B) {
	const members = 1 << 20

	positions := make([]uint64, members)
	next := make([]uint64, 17)

	for i := range positions {
		l := (i / 1024) % 17
		next[l]++
		positions[i] = lane.Pack(uint8(l), next[l]) //nolint:gosec // l < 17
	}

	r := rand.New(rand.NewSource(1))

	shuffled := append([]uint64(nil), positions...)
	r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	// concurrent validation reorders transactions locally: shuffled within windows of 1,000
	jittered := append([]uint64(nil), positions...)
	for i := 0; i < len(jittered); i += 1000 {
		w := jittered[i:min(i+1000, len(jittered))]
		r.Shuffle(len(w), func(a, b int) { w[a], w[b] = w[b], w[a] })
	}

	// a subtree holding one position in ten of its lanes' ranges, in arrival order
	sparse := make([]uint64, members)
	sparseNext := make([]uint64, 17)

	for i := range sparse {
		l := (i / 1024) % 17
		sparseNext[l] += 1 + uint64(r.Intn(19))        //nolint:gosec // small
		sparse[i] = lane.Pack(uint8(l), sparseNext[l]) //nolint:gosec // l < 17
	}

	for _, order := range []struct {
		name      string
		positions []uint64
	}{{"arrival", positions}, {"jittered", jittered}, {"shuffled", shuffled}, {"sparse", sparse}} {
		b.Run(order.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				fill := newLaneFill(nil)
				for _, p := range order.positions {
					fill.add(p)
				}

				b.StartTimer()

				_, _ = fill.bitmaps(chainhash.Hash{})
			}

			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/1e6, "ms/subtree")
		})
	}
}

// BenchmarkRecordLanePosition measures recording one member's position as it joins the open
// subtree.
func BenchmarkRecordLanePosition(b *testing.B) {
	stp := &SubtreeProcessor{laneKeeper: newRecordingLaneKeeper()}
	st := &subtreepkg.Subtree{}

	for i := 0; i < b.N; i++ {
		if i%(1<<20) == 0 {
			stp.laneFill = laneFill{}
		}

		stp.recordLanePosition(st, lane.Pack(uint8(i%17), uint64(i)+1)) //nolint:gosec // i%17 < 17
	}
}

// lookupFailingTxMap holds every tx but cannot read any position.
type lookupFailingTxMap struct {
	TxInpointsMap
}

func (lookupFailingTxMap) LookupPosition(chainhash.Hash) (uint64, bool, error) {
	return lane.Pack(4, 5), true, errors.NewStorageError("disk tx map: read failed")
}

// A pool position lookup that returns an error counts as not holding the tx, whatever position
// came with the error, so the subtree gets no bitmaps rather than ones with a wrong member.
func TestAPoolLookupThatFailsIsNotAPosition(t *testing.T) {
	stp, keeper := keptLanePoolProcessor(t, false)
	stp.currentTxMap = lookupFailingTxMap{TxInpointsMap: stp.currentTxMap}

	_, ok := stp.poolLanePosition(lanePoolTxHash(1))
	require.False(t, ok)

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(lanePoolTxHash(1), 1, 1))

	stp.keepLaneBitmapsFromPool(st)
	require.False(t, keeper.HasLaneBitmaps(*st.RootHash()))
}

// bitmapsFromPositions builds the same bitmaps as the per-lane record from positions in any
// order, counting the members without one.
func TestBitmapsFromPositionsMatchesTheRecord(t *testing.T) {
	positions := []uint64{lane.Pack(9, 7), 0, lane.Pack(2, 3), lane.Pack(9, 1), lane.Pack(2, 4), 0, lane.Pack(200, lane.MaxPosition)}

	fill := newLaneFill(nil)
	for _, p := range positions {
		fill.add(p)
	}

	root := chainhash.HashH([]byte("bitmaps-from-positions"))
	want, wantUnpositioned := fill.bitmaps(root)

	got, unpositioned := bitmapsFromPositions(root, append([]uint64(nil), positions...))

	require.Equal(t, wantUnpositioned, unpositioned)
	require.Equal(t, 2, unpositioned)
	require.Equal(t, root, got.SubtreeHash)
	require.Len(t, got.Lanes, len(want.Lanes))

	for l, bm := range want.Lanes {
		require.Equal(t, bm.ToArray(), got.Lanes[l].ToArray(), "lane %d", l)
	}
}

// startedLanePool starts stp's processing loop and returns a stop that waits for it to end, so the
// test can then read the processor's state.
func startedLanePool(t *testing.T, stp *SubtreeProcessor) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	stp.Start(ctx)

	stopped := false

	stop = func() {
		if stopped {
			return
		}

		stopped = true

		cancel()
		require.Eventually(t, stp.stopped.Load, 5*time.Second, time.Millisecond)
	}

	t.Cleanup(stop)

	return stop
}

// A mining candidate's subtrees, served through the processing loop, have their bitmaps kept
// before the candidate gets them, including subtrees a bulk rebuild chained.
func TestACandidatesSubtreesHaveTheirBitmapsWhenServed(t *testing.T) {
	stp, keeper := keptLanePoolProcessor(t, false)

	nodes := make([]subtreepkg.Node, 0, 7)

	for i := 1; i <= 7; i++ {
		hash := lanePoolTxHash(700 + i)
		stp.currentTxMap.SetAt(hash, lanePoolInpoints(i), lane.Pack(15, uint64(i))) //nolint:gosec // small test index
		nodes = append(nodes, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
	}

	require.NoError(t, stp.bulkBuildSubtrees(context.Background(), nodes, 4))
	require.False(t, keeper.HasLaneBitmaps(*stp.chainedSubtrees[0].RootHash()))

	startedLanePool(t, stp)

	subtrees := stp.GetCompletedSubtreesForMiningCandidate()
	require.Len(t, subtrees, 2)

	require.Equal(t, []uint64{lane.Pack(15, 1), lane.Pack(15, 2), lane.Pack(15, 3)}, keeper.positions(t, *subtrees[0].RootHash()))
	require.Equal(t, []uint64{lane.Pack(15, 4), lane.Pack(15, 5), lane.Pack(15, 6), lane.Pack(15, 7)}, keeper.positions(t, *subtrees[1].RootHash()))
}

// Transactions placed by the processing loop keep their positions in the pool, so a later re-chain
// after a conflict removal, which takes each position from the pool, rebuilds the subtrees'
// bitmaps with them.
func TestDequeuedTxsKeepTheirPositionsForALaterReChain(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)

			nodes := make([]subtreepkg.Node, 0, 7)
			inpoints := make([]*subtreepkg.TxInpoints, 0, 7)
			positions := make([]uint64, 0, 7)

			for i := 1; i <= 7; i++ {
				nodes = append(nodes, subtreepkg.Node{Hash: lanePoolTxHash(800 + i), Fee: 1, SizeInBytes: 1})
				inpoints = append(inpoints, lanePoolInpoints(800+i))
				positions = append(positions, lane.Pack(16, uint64(i))) //nolint:gosec // small test index
			}

			startedLanePool(t, stp)

			require.Equal(t, Admitted, stp.AddPositionedBatchBounded(nodes, inpoints, false, nil, positions))
			require.Eventually(t, func() bool { return stp.TxCount() >= 8 }, 5*time.Second, time.Millisecond, "the coinbase placeholder and 7 txs placed")

			// the processing loop removes the tx and re-chains the subtrees from the pool
			require.NoError(t, stp.Remove(context.Background(), lanePoolTxHash(802)))

			want := []uint64{lane.Pack(16, 1), lane.Pack(16, 3), lane.Pack(16, 4)}

			require.Eventually(t, func() bool {
				subtrees := stp.GetCompletedSubtreesForMiningCandidate()
				if len(subtrees) != 1 || !keeper.HasLaneBitmaps(*subtrees[0].RootHash()) {
					return false
				}

				return slices.Equal(want, keeper.positions(t, *subtrees[0].RootHash()))
			}, 5*time.Second, 5*time.Millisecond, "the re-chained subtree's bitmaps hold the pool's positions")
		})
	}
}

// The rebuild after a peer's block gives the remaining txs' subtrees the positions the copy
// carried out of the old pool half.
func TestRemainderRebuildCarriesThePositions(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)
			stp.settings.BlockAssembly.ParallelSetIfNotExistsThreshold = 1

			old := NewSplitTxInpointsMap(16)

			st, err := subtreepkg.NewTreeByLeafCount(4)
			require.NoError(t, err)
			require.NoError(t, st.AddCoinbaseNode())

			chained := []*subtreepkg.Subtree{st}

			mined := NewSortedHashSet(4)

			for i := 1; i <= 7; i++ {
				hash := lanePoolTxHash(900 + i)
				old.SetAt(hash, lanePoolInpoints(900+i), lane.Pack(17, uint64(i))) //nolint:gosec // small test index

				if st.IsComplete() {
					st, err = subtreepkg.NewTreeByLeafCount(4)
					require.NoError(t, err)

					chained = append(chained, st)
				}

				require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1}))

				if i%2 == 0 {
					require.NoError(t, mined.Put(hash))
				}
			}

			require.NoError(t, mined.Freeze(1))

			require.NoError(t, stp.processRemainderTxHashes(context.Background(), chained, mined, nil, nil, old, true))

			// 1, 3, 5 and 7 remain: the coinbase placeholder and 3 make the first subtree
			require.Len(t, stp.chainedSubtrees, 1)
			require.Equal(t, []uint64{lane.Pack(17, 1), lane.Pack(17, 3), lane.Pack(17, 5)}, keeper.positions(t, *stp.chainedSubtrees[0].RootHash()))
		})
	}
}

// laneBitmap and bitmapsFromPositions give the bitmaps a plain AddMany of each lane's positions
// gives, for positions in runs, with gaps and duplicates, in order and shuffled, across lanes.
func TestLaneBitmapBuildsMatchAddMany(t *testing.T) {
	r := rand.New(rand.NewSource(7))

	for round := 0; round < 50; round++ {
		var packed []uint64

		want := map[uint8]*roaring64.Bitmap{}
		next := map[uint8]uint64{}

		for i := 0; i < 2000; i++ {
			if r.Intn(20) == 0 {
				packed = append(packed, 0)
				continue
			}

			l := uint8(r.Intn(5)) //nolint:gosec // r.Intn(5) < 5
			step := uint64(1)

			switch r.Intn(10) {
			case 0:
				step = uint64(2 + r.Intn(300)) //nolint:gosec // small
			case 1:
				step = 0 // a duplicate
			}

			next[l] += step
			if next[l] == 0 {
				// some rounds run past 32 bits, where the bitset build does not apply
				next[l] = 1 + uint64(round%3/2)<<33 //nolint:gosec // round is small
			}

			packed = append(packed, lane.Pack(l, next[l]))

			if want[l] == nil {
				want[l] = roaring64.New()
			}

			want[l].Add(next[l])
		}

		if round%2 == 1 {
			r.Shuffle(len(packed), func(i, j int) { packed[i], packed[j] = packed[j], packed[i] })
		}

		root := chainhash.Hash{byte(round)}

		got, unpositioned := bitmapsFromPositions(root, append([]uint64(nil), packed...))

		fill := newLaneFill(nil)
		for _, p := range packed {
			fill.add(p)
		}

		fromFill, fillUnpositioned := fill.bitmaps(root)

		wantUnpositioned := 0
		for _, p := range packed {
			if !lane.Valid(p) {
				wantUnpositioned++
			}
		}

		require.Equal(t, wantUnpositioned, unpositioned)
		require.Equal(t, wantUnpositioned, fillUnpositioned)
		require.Len(t, got.Lanes, len(want))
		require.Len(t, fromFill.Lanes, len(want))

		for l, bm := range want {
			require.Equal(t, bm.ToArray(), got.Lanes[l].ToArray(), "round %d lane %d", round, l)
			require.Equal(t, bm.ToArray(), fromFill.Lanes[l].ToArray(), "round %d lane %d from the record", round, l)
		}
	}
}

// laneBitmap's range and bitset paths match AddMany for the positions production lanes hold:
// long ascending runs, as the dequeue loop fills a subtree, and dense positions far past the
// first container, including one lane whose lowest is not on a container boundary and one that
// ends at MaxUint32.
func TestLaneBitmapPathsMatchAddMany(t *testing.T) {
	r := rand.New(rand.NewSource(11))

	runs := func(starts []uint64, lengths []int) []uint64 {
		var positions []uint64

		for i, start := range starts {
			for j := 0; j < lengths[i]; j++ {
				positions = append(positions, start+uint64(j)) //nolint:gosec // small
			}
		}

		return positions
	}

	dense := func(lowest uint64, n int) []uint64 {
		positions := make([]uint64, 0, n)
		p := lowest

		for i := 0; i < n; i++ {
			positions = append(positions, p)
			p += uint64(1 + r.Intn(3)) //nolint:gosec // small
		}

		r.Shuffle(len(positions), func(i, j int) { positions[i], positions[j] = positions[j], positions[i] })

		return positions
	}

	cases := map[string][]uint64{
		// four runs, the last one position long: the range path, trailing run included
		"long runs": runs([]uint64{5_000_000, 5_000_900, 5_002_000, 5_003_000}, []int{700, 700, 699, 1}),
		// runs starting past the first container, off a boundary
		"long runs past 2^20": runs([]uint64{1<<20 + 12_345, 1<<20 + 20_000}, []int{1500, 1500}),
		// shuffled, short gaps: the bitset path with a base past the first container, off a boundary
		"dense past 2^20": dense(1<<20+12_345, 3000),
		// the bitset path on a container boundary
		"dense on a boundary": dense(7<<16, 3000),
		// the bitset path ending at MaxUint32
		"dense to MaxUint32": func() []uint64 {
			positions := make([]uint64, 0, 5000)
			for p := uint64(math.MaxUint32); len(positions) < 5000; p -= 2 {
				positions = append(positions, p)
			}

			r.Shuffle(len(positions), func(i, j int) { positions[i], positions[j] = positions[j], positions[i] })

			return positions
		}(),
	}

	for name, positions := range cases {
		t.Run(name, func(t *testing.T) {
			want := roaring64.New()
			want.AddMany(positions)

			require.Equal(t, want.ToArray(), laneBitmap(append([]uint64(nil), positions...)).ToArray())

			packed := make([]uint64, len(positions))
			for i, p := range positions {
				packed[i] = lane.Pack(4, p)
			}

			got, unpositioned := bitmapsFromPositions(chainhash.Hash{1}, append([]uint64(nil), packed...))
			require.Zero(t, unpositioned)
			require.Equal(t, want.ToArray(), got.Lanes[4].ToArray(), "bitmapsFromPositions")

			fill := newLaneFill(nil)
			for _, p := range packed {
				fill.add(p)
			}

			fromFill, _ := fill.bitmaps(chainhash.Hash{1})
			require.Equal(t, want.ToArray(), fromFill.Lanes[4].ToArray(), "the record")
		})
	}
}
