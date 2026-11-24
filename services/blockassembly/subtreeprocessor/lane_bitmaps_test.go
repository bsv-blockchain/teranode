package subtreeprocessor

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

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

// The parallel rebuild after a block keeps each full subtree's bitmaps from its workers' reads,
// and leaves the open subtree with a record that covers the txs it took.
func TestParallelRebuildKeepsEachSubtreesBitmaps(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			stp, keeper := keptLanePoolProcessor(t, disk)

			kept := make([]subtreepkg.Node, 0, 9)

			for i := 1; i <= 9; i++ {
				hash := lanePoolTxHash(100 + i)
				stp.currentTxMap.SetAt(hash, lanePoolInpoints(i), lane.Pack(8, uint64(i))) //nolint:gosec // small test index
				kept = append(kept, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
			}

			require.NoError(t, stp.parallelBuildRemainderSubtrees(context.Background(), kept, true))
			require.Len(t, stp.chainedSubtrees, 2)

			require.Equal(t, []uint64{lane.Pack(8, 1), lane.Pack(8, 2), lane.Pack(8, 3)}, keeper.positions(t, *stp.chainedSubtrees[0].RootHash()))
			require.Equal(t, []uint64{lane.Pack(8, 4), lane.Pack(8, 5), lane.Pack(8, 6), lane.Pack(8, 7)}, keeper.positions(t, *stp.chainedSubtrees[1].RootHash()))
			require.True(t, stp.laneFill.matches(stp.currentSubtree.Load()), "the open subtree's record covers the 2 txs it took")

			addPositioned(t, stp, 10, lane.Pack(8, 10))
			addPositioned(t, stp, 11, lane.Pack(8, 11))

			require.Len(t, stp.chainedSubtrees, 3)
			require.Equal(t, []uint64{lane.Pack(8, 8), lane.Pack(8, 9), lane.Pack(8, 10), lane.Pack(8, 11)}, keeper.positions(t, *stp.chainedSubtrees[2].RootHash()))
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

	shuffled := append([]uint64(nil), positions...)
	rand.New(rand.NewSource(1)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	for _, order := range []struct {
		name      string
		positions []uint64
	}{{"arrival", positions}, {"shuffled", shuffled}} {
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
