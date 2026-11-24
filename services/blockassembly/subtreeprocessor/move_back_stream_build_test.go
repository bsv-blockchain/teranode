package subtreeprocessor

import (
	"context"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// splitRuns cuts nodes into runs at the given positions.
func splitRuns(nodes []subtreepkg.Node, cuts ...int) [][]subtreepkg.Node {
	var runs [][]subtreepkg.Node

	prev := 0

	for _, cut := range cuts {
		runs = append(runs, nodes[prev:cut])
		prev = cut
	}

	return append(runs, nodes[prev:])
}

func requireSameSubtrees(t *testing.T, want, got *SubtreeProcessor) {
	t.Helper()

	require.Len(t, got.chainedSubtrees, len(want.chainedSubtrees))

	for i := range want.chainedSubtrees {
		require.Equal(t, want.chainedSubtrees[i].Nodes, got.chainedSubtrees[i].Nodes, "chained subtree %d", i)
		require.Equal(t, want.chainedSubtrees[i].RootHash(), got.chainedSubtrees[i].RootHash(), "root of chained subtree %d", i)
	}

	require.Equal(t, want.currentSubtree.Load().Nodes, got.currentSubtree.Load().Nodes, "the open subtree")
	require.Equal(t, want.chainedSubtreeCount.Load(), got.chainedSubtreeCount.Load())
	require.Equal(t, want.chainedSubtreesTotalSize.Load(), got.chainedSubtreesTotalSize.Load())
}

// streamBuildSubtrees reads its nodes from several slices in place and must leave exactly what
// bulkBuildSubtrees leaves for the same nodes in one slice: whole subtrees, a partial one, none, and
// the edge where the open subtree fills exactly.
func TestStreamBuildSubtrees_MatchesBulkBuild(t *testing.T) {
	// the open subtree of a new processor holds the coinbase placeholder, so it takes size-1 nodes; a
	// size of 1 is full from the start
	for _, size := range []int{1, 2, 64} {
		for _, n := range []int{0, 1, size - 2, size - 1, size, 2*size - 2, 2*size - 1, 2 * size, 5*size + 7, 6*size - 1} {
			if n < 0 {
				continue
			}

			for _, cuts := range [][]int{nil, {n / 2}, {n / 3, 2 * n / 3}, {0, n}} {
				for _, disk := range []bool{false, true} {
					var opts []Options
					if disk {
						opts = append(opts, WithTxMapDirs([]string{t.TempDir(), t.TempDir()}))
					}

					nodes := batchTestNodes(0, n)

					bulk, cleanupBulk := newAddNodesBenchProcessor(t, size, opts...)
					defer cleanupBulk()

					stream, cleanupStream := newAddNodesBenchProcessor(t, size, opts...)
					defer cleanupStream()

					if disk {
						setAll(bulk, nodes)
						setAll(stream, nodes)
					}

					require.NoError(t, bulk.bulkBuildSubtrees(context.Background(), nodes, size), "size %d, %d nodes", size, n)
					require.NoError(t, stream.streamBuildSubtrees(context.Background(), splitRuns(nodes, cuts...), size), "size %d, %d nodes", size, n)

					requireSameSubtrees(t, bulk, stream)

					if disk && n > size {
						require.Positive(t, requireIndexesMatchChain(t, stream))
					}
				}
			}
		}
	}
}

// The move-back build used to copy every node of the new assembly into one list before it built a
// subtree. With the subtrees mapped outside the heap, what the heap allocates while a move-back builds
// is that list, the subtrees' hashing and the block's own arrays, so the list shows in the difference
// between a move-back that builds it and one that does not.
func TestMoveBackBulkBuild_ReadsThePendingNodesInPlace(t *testing.T) {
	const (
		size    = 1024
		pending = 200_000
		lost    = 2_000
	)

	heapBytes := func(alreadyPending int) uint64 {
		stp, cleanup := newAddNodesBenchProcessor(t, size, WithMmapDir(t.TempDir()))
		defer cleanup()

		for _, node := range batchTestNodes(1_000_000, pending) {
			require.NoError(t, stp.addNode(node, &subtreepkg.TxInpoints{}, true))
		}

		block := &model.Block{Header: model.GenesisBlockHeader}
		nodes := append([]subtreepkg.Node{{Hash: subtreepkg.CoinbasePlaceholderHashValue}}, batchTestNodes(0, lost)...)

		// the lost block's first transactions are already pending: a child of one would be held behind it
		for i := 0; i < alreadyPending; i++ {
			nodes[1+i] = batchTestNodes(1_000_000+i, 1)[0]
		}

		inpoints := make([]subtreepkg.TxInpoints, len(nodes))

		chained, current := stp.chainedSubtrees, stp.currentSubtree.Load()

		var before, after runtime.MemStats

		runtime.GC()
		runtime.ReadMemStats(&before)

		_, _, err := stp.moveBackBlockBulkBuild(context.Background(), block, true, chained, current,
			[][]subtreepkg.Node{nodes}, [][]subtreepkg.TxInpoints{inpoints}, nil)
		require.NoError(t, err)

		runtime.ReadMemStats(&after)

		return after.TotalAlloc - before.TotalAlloc
	}

	const nodeBytes = 48

	list := uint64(pending * nodeBytes)

	// Both builds hash and index the same subtrees, so what separates them is the list: the held path
	// has to materialise it, the in-place path does not.
	inPlace, held := heapBytes(0), heapBytes(1)

	require.Greater(t, held, inPlace+list*7/10, "a transaction already pending holds its children, which needs the whole list; none pending reads in place (%d against %d bytes)", inPlace, held)
}

// A failed build installs nothing, so the caller's restore puts back the subtrees it started from, and
// retires what the build allocated so the mapped files are closed.
func TestStreamBuildSubtrees_FailureInstallsNothingAndRetiresWhatItBuilt(t *testing.T) {
	const size = 64

	stp, cleanup := newAddNodesBenchProcessor(t, size, WithMmapDir(t.TempDir()))
	defer cleanup()

	open := stp.currentSubtree.Load()
	openNodes := len(open.Nodes)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := stp.streamBuildSubtrees(ctx, splitRuns(batchTestNodes(0, 4*size), size), size)
	require.Error(t, err)

	require.Empty(t, stp.chainedSubtrees)
	require.Same(t, open, stp.currentSubtree.Load())
	require.Len(t, open.Nodes, openNodes, "the open subtree is untouched when the build never started")
	require.NotEmpty(t, stp.retiredSubtrees, "the subtrees it allocated are retired, to be closed")
}

// moveBackBuildBeforePR9 is moveBackBlockBulkBuild as it was before issue 2269 (PR 9), for a lost block
// none of whose transactions is pending: every node of the new assembly is copied into one list, which
// bulkBuildSubtrees then builds from.
func (stp *SubtreeProcessor) moveBackBuildBeforePR9(ctx context.Context, block []subtreepkg.Node, inpoints []subtreepkg.TxInpoints,
	previousChained []*subtreepkg.Subtree, previousCurrent *subtreepkg.Subtree, subtreeSize int) error {
	total := len(block) - 1

	for _, st := range previousChained {
		total += len(st.Nodes)
	}

	total += len(previousCurrent.Nodes)

	allNodes := make([]subtreepkg.Node, 0, total)
	flatHashes := make([]chainhash.Hash, 0, len(block)-1)
	flatInpoints := make([]*subtreepkg.TxInpoints, 0, len(block)-1)
	flatNodes := make([]subtreepkg.Node, 0, len(block)-1)

	for i := 1; i < len(block); i++ {
		flatHashes = append(flatHashes, block[i].Hash)
		flatInpoints = append(flatInpoints, &inpoints[i])
		flatNodes = append(flatNodes, block[i])
	}

	wasSet := make([]bool, len(flatHashes))
	stp.currentTxMap.(*SplitTxInpointsMap).ParallelBulkSetIfNotExists(flatHashes, flatInpoints, wasSet)

	allNodes = append(allNodes, flatNodes...)

	for _, st := range previousChained {
		for _, node := range st.Nodes {
			if !node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				allNodes = append(allNodes, node)
			}
		}
	}

	for _, node := range previousCurrent.Nodes {
		if !node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			allNodes = append(allNodes, node)
		}
	}

	newSubtree, err := stp.newSubtree(subtreeSize)
	if err != nil {
		return err
	}

	_ = newSubtree.AddCoinbaseNode()

	stp.retireSubtrees(previousCurrent)
	stp.currentSubtree.Store(newSubtree)
	stp.retireChainedSubtrees()

	return stp.bulkBuildSubtrees(ctx, allNodes, subtreeSize)
}

// BenchmarkReorgMoveBackBuild times the build of the move-back (issue 2269, PR 9) on a pending set and a
// lost block, three ways: before is the build as it was, copying every node into one list; stream is
// the build now, for a lost block with no transaction pending, which reads the nodes where they are;
// held is the build now for one that has a single, whose children are held behind their pending parents,
// which still needs the list and its per-node lookups. The subtrees are mapped outside the heap, so B/op
// is the list and the block's arrays.
func BenchmarkReorgMoveBackBuild(b *testing.B) {
	// the timed region is a few milliseconds behind a processor and a pending set built per iteration,
	// which the benchmark-compare job's -short run (-benchtime=1s, three times) would repeat for minutes
	if testing.Short() {
		b.Skip("builds a processor per iteration")
	}

	const size, pending, lost = 1 << 14, 1 << 20, 1 << 16

	for _, impl := range []string{"before", "stream", "held"} {
		b.Run("impl="+impl, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				stp, cleanup := newAddNodesBenchProcessor(b, size, WithMmapDir(b.TempDir()))

				for _, node := range batchTestNodes(1_000_000, pending) {
					if err := stp.addNode(node, &subtreepkg.TxInpoints{}, true); err != nil {
						b.Fatal(err)
					}
				}

				nodes := append([]subtreepkg.Node{{Hash: subtreepkg.CoinbasePlaceholderHashValue}}, batchTestNodes(0, lost)...)
				if impl == "held" {
					nodes[1] = batchTestNodes(1_000_000, 1)[0]
				}

				inpoints := make([]subtreepkg.TxInpoints, len(nodes))
				chained, current := stp.chainedSubtrees, stp.currentSubtree.Load()

				b.StartTimer()

				var err error

				if impl == "before" {
					err = stp.moveBackBuildBeforePR9(context.Background(), nodes, inpoints, chained, current, size)
				} else {
					_, _, err = stp.moveBackBlockBulkBuild(context.Background(), &model.Block{Header: model.GenesisBlockHeader}, true, chained, current,
						[][]subtreepkg.Node{nodes}, [][]subtreepkg.TxInpoints{inpoints}, nil)
				}

				b.StopTimer()

				if err != nil {
					b.Fatal(err)
				}

				cleanup()
			}
		})
	}
}
