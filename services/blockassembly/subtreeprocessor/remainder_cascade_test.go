package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// A subtree-resident descendant of a tx the block made conflicting must not
// survive the remainder pass. The pass used to filter by the block's tx map
// and immediate losers only, so a child drained into the subtrees before a
// catch-up (issue 2050) kept spending a conflicting parent. The cascade is
// transitive: a grandchild goes with its child.
func TestProcessRemainderTxHashes_DropsDescendantsOfConflictingTxs(t *testing.T) {
	for name, threshold := range map[string]int{"sequential_insert": 1 << 20, "parallel_insert": 1} {
		t.Run(name, func(t *testing.T) {
			stp := newTestProcessorNoStart(t)
			stp.settings.BlockAssembly.ParallelSetIfNotExistsThreshold = threshold

			parent := chainhash.HashH([]byte("cascade-conflicting-parent"))
			child := chainhash.HashH([]byte("cascade-child"))
			grandchild := chainhash.HashH([]byte("cascade-grandchild"))
			other := chainhash.HashH([]byte("cascade-unrelated"))

			st, err := subtreepkg.NewTreeByLeafCount(8)
			require.NoError(t, err)
			require.NoError(t, st.AddCoinbaseNode())

			sourceTxMap := NewSplitTxInpointsMap(16)
			for _, n := range []struct{ hash, parent chainhash.Hash }{
				{parent, chainhash.HashH([]byte("cascade-grandparent"))},
				{child, parent},
				{grandchild, child},
				{other, chainhash.HashH([]byte("cascade-unrelated-parent"))},
			} {
				require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: n.hash, Fee: 1, SizeInBytes: 250}))
				sourceTxMap.Set(n.hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{n.parent}})
			}

			transactionMap := NewSortedHashSet(16)
			require.NoError(t, transactionMap.Freeze(1))

			conflictingHashes := map[chainhash.Hash]struct{}{parent: {}}

			require.NoError(t, stp.processRemainderTxHashes(t.Context(), []*subtreepkg.Subtree{st}, transactionMap, nil, conflictingHashes, sourceTxMap, true))

			hashes := collectSubtreeHashes(stp)
			require.NotContains(t, hashes, parent, "a conflicting tx must not stay in the subtrees")
			require.NotContains(t, hashes, child, "a child of a conflicting tx must not stay in the subtrees")
			require.NotContains(t, hashes, grandchild, "a grandchild of a conflicting tx must not stay in the subtrees")
			require.Contains(t, hashes, other, "an unrelated tx must stay")

			require.Contains(t, conflictingHashes, child, "a dropped descendant must join the set, so the drain that follows rejects its queued children")
			require.Contains(t, conflictingHashes, grandchild, "a dropped descendant must join the set, so the drain that follows rejects its queued children")
		})
	}
}

// The pass works a window at a time. A child in a later window than its
// conflicting parent, and a grandchild later still, must still go.
func TestDropConflictingDescendants_CascadesAcrossWindows(t *testing.T) {
	const lookupChunk = 16 << 10 // one worker: one window

	parent := chainhash.HashH([]byte("window-conflicting-parent"))
	child := chainhash.HashH([]byte("window-child"))
	grandchild := chainhash.HashH([]byte("window-grandchild"))

	currentTxMap := NewSplitTxInpointsMap(16)
	nodes := make([]subtreepkg.Node, 0, 3*lookupChunk)

	add := func(hash, spends chainhash.Hash) {
		nodes = append(nodes, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
		currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{spends}})
	}

	var filler chainhash.Hash

	for len(nodes) < 3*lookupChunk {
		switch len(nodes) {
		case 0:
			add(parent, chainhash.HashH([]byte("window-grandparent")))
		case lookupChunk + 5:
			add(child, parent)
		case 2*lookupChunk + 5:
			add(grandchild, child)
		default:
			binary.LittleEndian.PutUint64(filler[:8], uint64(len(nodes)))
			add(filler, chainhash.HashH([]byte("window-filler-parent")))
		}
	}

	remainder := [][]subtreepkg.Node{nodes}
	conflictingHashes := map[chainhash.Hash]struct{}{parent: {}}

	require.NoError(t, dropConflictingDescendants(t.Context(), remainder, conflictingHashes, currentTxMap, 1))

	require.Len(t, remainder[0], 3*lookupChunk-3, "only the conflicting tx and its two descendants must go")

	for _, n := range remainder[0] {
		require.NotEqual(t, parent, n.Hash)
		require.NotEqual(t, child, n.Hash)
		require.NotEqual(t, grandchild, n.Hash)
	}
}

// The block's conflicting set must reach the remainder pass through
// processRemainderTransactionsAndDequeue, and what the pass adds to it must
// reach the drain that follows: a queued child of a subtree-resident
// descendant is rejected too.
func TestProcessRemainderTransactionsAndDequeue_AppliesConflictCascade(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	parent := chainhash.HashH([]byte("wiring-conflicting-parent"))
	child := chainhash.HashH([]byte("wiring-child"))
	queuedGrandchild := chainhash.HashH([]byte("wiring-queued-grandchild"))
	other := chainhash.HashH([]byte("wiring-unrelated"))

	chained, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)
	require.NoError(t, chained.AddCoinbaseNode())

	currentTxMap := NewSplitTxInpointsMap(16)
	for _, n := range []struct{ hash, parent chainhash.Hash }{
		{child, parent},
		{other, chainhash.HashH([]byte("wiring-unrelated-parent"))},
	} {
		require.NoError(t, chained.AddSubtreeNode(subtreepkg.Node{Hash: n.hash, Fee: 1, SizeInBytes: 250}))
		currentTxMap.Set(n.hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{n.parent}})
	}

	current, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)

	// a non-empty block tx map selects the foreign-block path
	transactionMap := NewSortedHashSet(16)
	require.NoError(t, transactionMap.Put(chainhash.HashH([]byte("wiring-mined-elsewhere"))))
	require.NoError(t, transactionMap.Freeze(1))

	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}
	stp.queue.enqueueBatch(
		[]subtreepkg.Node{{Hash: queuedGrandchild, Fee: 1, SizeInBytes: 250}},
		[]*subtreepkg.TxInpoints{{ParentTxHashes: []chainhash.Hash{child}}},
	)

	require.NoError(t, stp.processRemainderTransactionsAndDequeue(t.Context(), &RemainderTransactionParams{
		Block:             &model.Block{Header: blockHeader, CoinbaseTx: coinbaseTx},
		ChainedSubtrees:   []*subtreepkg.Subtree{chained},
		CurrentSubtree:    current,
		TransactionMap:    transactionMap,
		ConflictingHashes: map[chainhash.Hash]struct{}{parent: {}},
		CurrentTxMap:      currentTxMap,
		SkipNotification:  true,
	}))

	hashes := collectSubtreeHashes(stp)
	require.NotContains(t, hashes, child, "a subtree-resident child of a conflicting tx must go")
	require.NotContains(t, hashes, queuedGrandchild, "a queued child of a dropped descendant must be rejected by the drain")
	require.Contains(t, hashes, other)
}

// BenchmarkDropConflictingDescendants measures the cascade pass the remainder
// pass adds when a block has conflicts: one inpoints lookup per remainder
// node, then the sequential walk. Nothing matches, so every node is kept.
func BenchmarkDropConflictingDescendants(b *testing.B) {
	const (
		subtrees   = 8
		perSubtree = 1 << 20
	)

	currentTxMap := NewSplitTxInpointsMap(256)
	remainder := make([][]subtreepkg.Node, subtrees)

	var h chainhash.Hash

	for s := range remainder {
		remainder[s] = make([]subtreepkg.Node, perSubtree)

		for i := range remainder[s] {
			binary.LittleEndian.PutUint64(h[0:8], uint64(s)<<32|uint64(i))
			remainder[s][i] = subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 1}
			currentTxMap.Set(h, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{chainhash.HashH(h[:8])}})
		}
	}

	conflictingHashes := map[chainhash.Hash]struct{}{chainhash.HashH([]byte("bench-conflicting")): {}}
	workers := remainderWorkers(0)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// the pass compacts in place; nothing is dropped, so the input is unchanged
		require.NoError(b, dropConflictingDescendants(context.Background(), remainder, conflictingHashes, currentTxMap, workers))
	}

	b.ReportMetric(float64(subtrees*perSubtree)*float64(b.N)/b.Elapsed().Seconds(), "nodes/s")
}
