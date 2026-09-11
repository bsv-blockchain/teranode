package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// setupBatchRemovalProcessor builds a SubtreeProcessor with subtreeSize-sized
// subtrees, optionally backed by DiskTxMap (which is what maintains the
// currentTxMap.SubtreeIndex bookkeeping that locateTxInSubtrees uses for its
// O(1) shortcut).
//
// It returns the processor and a teardown function that stops it (closing the
// DiskTxMap/Badger store and the reader goroutine) and closes the UTXO store.
// teardown is safe to call more than once (the returned closure is guarded by
// a sync.Once), and is also registered via tb.Cleanup as a safety net for
// callers (e.g. the equivalence test below) that don't invoke it explicitly -
// callers that build many processors in a loop (e.g. the benchmark) MUST call
// it themselves at the end of each iteration, since tb.Cleanup only runs once
// at the very end of the whole test/benchmark function.
func setupBatchRemovalProcessor(tb testing.TB, subtreeSize int, useDiskMap bool) (*SubtreeProcessor, func()) {
	tb.Helper()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(tb, err)

	utxoStore, err := sql.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(tb), utxoStoreURL)
	require.NoError(tb, err)

	blobStore := memory.New()

	settings := test.CreateBaseTestSettings(tb)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	var opts []Options
	if useDiskMap {
		// Registering the temp dir before the teardown below means teardown
		// runs (via tb.Cleanup LIFO ordering) before the framework removes
		// this directory, so Badger is closed before its files disappear
		// underneath it.
		opts = append(opts, WithTxMapDirs([]string{tb.TempDir()}))
	}

	stp, err := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, blobStore, nil, utxoStore, newSubtreeChan, opts...)
	require.NoError(tb, err)

	if useDiskMap {
		require.NotNil(tb, stp.diskTxMap, "DiskTxMap must be active for the shortcut path")
	}

	var once sync.Once

	teardown := func() {
		once.Do(func() {
			stp.Stop(context.Background())
			close(newSubtreeChan)
			<-readerDone
			_ = utxoStore.Close(context.Background())
		})
	}

	tb.Cleanup(teardown)

	return stp, teardown
}

// populateChainedSubtrees adds numSubtrees*subtreeSize transactions directly,
// producing numSubtrees complete chained subtrees (plus a fresh, empty current
// subtree), and returns every hash in insertion order.
func populateChainedSubtrees(tb testing.TB, stp *SubtreeProcessor, numSubtrees, subtreeSize int) []chainhash.Hash {
	tb.Helper()

	hashes := make([]chainhash.Hash, 0, numSubtrees*subtreeSize)

	// The first subtree already carries the coinbase placeholder at index 0,
	// so it only has subtreeSize-1 free slots.
	for st := 0; st < numSubtrees; st++ {
		free := subtreeSize
		if st == 0 {
			free = subtreeSize - 1
		}

		for i := 0; i < free; i++ {
			h := chainhash.HashH([]byte(fmt.Sprintf("batchremoval_%d_%d", st, i)))
			hashes = append(hashes, h)

			node := &subtreepkg.Node{Hash: h, Fee: 1000, SizeInBytes: 250}
			inpoints := subtreepkg.NewTxInpoints()
			require.NoError(tb, stp.AddDirectly(node, &inpoints, true))
		}
	}

	require.Equal(tb, numSubtrees, len(stp.chainedSubtrees), "expected exactly numSubtrees complete chained subtrees")

	return hashes
}

// occurrencesOf counts how many times hash is present across the current
// subtree and every chained subtree (should be 0 or 1 in a well-formed
// processor).
func occurrencesOf(stp *SubtreeProcessor, h chainhash.Hash) int {
	count := 0
	if stp.currentSubtree.Load().NodeIndex(h) >= 0 {
		count++
	}

	for _, cs := range stp.chainedSubtrees {
		if cs.NodeIndex(h) >= 0 {
			count++
		}
	}

	return count
}

// BenchmarkRemoveTxsFromSubtrees_ManySubtrees exercises removeTxsFromSubtrees
// under worst-case deep-reorg conditions: many chained subtrees and many
// transactions to remove in a single batch call, with the hashes scattered
// across subtrees (including the last one), forcing a linear scan over every
// chained subtree per hash unless the currentTxMap shortcut is used.
//
// Both arms run on the same DiskTxMap backend; the only difference is whether
// locateTxInSubtrees is allowed to use the SubtreeIndex shortcut
// (disableSubtreeIndexShortcut), so the delta between them isolates the
// algorithmic change from backend cost - the in-memory map is not used here
// because it never populates SubtreeIndex, so it would only measure Badger
// vs. map overhead, not the shortcut itself.
func BenchmarkRemoveTxsFromSubtrees_ManySubtrees(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy benchmark; skipped in -short (CI passes -short)")
	}

	const (
		numSubtrees  = 2000
		subtreeSize  = 16
		removeStride = 4 // remove every 4th tx: ~25% of all transactions
	)

	b.Run("shortcut", func(b *testing.B) {
		benchmarkRemoveTxsFromSubtrees(b, numSubtrees, subtreeSize, removeStride, false)
	})

	b.Run("linear_scan_fallback", func(b *testing.B) {
		benchmarkRemoveTxsFromSubtrees(b, numSubtrees, subtreeSize, removeStride, true)
	})
}

func benchmarkRemoveTxsFromSubtrees(b *testing.B, numSubtrees, subtreeSize, removeStride int, disableShortcut bool) {
	b.ReportAllocs()

	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		stp, teardown := setupBatchRemovalProcessor(b, subtreeSize, true)
		stp.disableSubtreeIndexShortcut = disableShortcut

		allHashes := populateChainedSubtrees(b, stp, numSubtrees, subtreeSize)

		toRemove := make([]chainhash.Hash, 0, len(allHashes)/removeStride+1)
		for idx := 0; idx < len(allHashes); idx += removeStride {
			toRemove = append(toRemove, allHashes[idx])
		}

		b.StartTimer()

		if err := stp.removeTxsFromSubtrees(ctx, toRemove); err != nil {
			b.Fatalf("removeTxsFromSubtrees failed: %v", err)
		}

		b.StopTimer()

		teardown() // don't hold b.N Badger instances open at once
	}
}

// TestRemoveTxsFromSubtrees_DiskTxMapShortcutMatchesLinearScan proves that the
// currentTxMap-based shortcut used by removeTxsFromSubtrees (when DiskTxMap is
// active) produces exactly the same result as the linear-scan fallback: the
// same set of transactions removed, and the same subtree structure (including
// node order) left behind. Both processors share the same DiskTxMap backend;
// only disableSubtreeIndexShortcut differs, so the comparison isolates the
// shortcut's behavior rather than comparing two different tx-map backends.
func TestRemoveTxsFromSubtrees_DiskTxMapShortcutMatchesLinearScan(t *testing.T) {
	ctx := context.Background()

	const (
		numSubtrees  = 12
		subtreeSize  = 8
		removeStride = 3
	)

	buildAndRemove := func(disableShortcut bool) (*SubtreeProcessor, []chainhash.Hash, []chainhash.Hash) {
		stp, _ := setupBatchRemovalProcessor(t, subtreeSize, true)
		stp.disableSubtreeIndexShortcut = disableShortcut

		allHashes := populateChainedSubtrees(t, stp, numSubtrees, subtreeSize)

		var toRemove, expectedRemaining []chainhash.Hash
		for idx, h := range allHashes {
			if idx%removeStride == 0 {
				toRemove = append(toRemove, h)
			} else {
				expectedRemaining = append(expectedRemaining, h)
			}
		}

		require.NoError(t, stp.removeTxsFromSubtrees(ctx, toRemove))

		return stp, toRemove, expectedRemaining
	}

	stpShortcut, removedShortcut, remainingShortcut := buildAndRemove(false)
	stpScan, removedScan, remainingScan := buildAndRemove(true)

	// Same removed set produces the same absence, on both code paths.
	for _, h := range removedShortcut {
		require.Equal(t, 0, occurrencesOf(stpShortcut, h), "removed hash must be absent (shortcut path)")
	}

	for _, h := range removedScan {
		require.Equal(t, 0, occurrencesOf(stpScan, h), "removed hash must be absent (linear-scan path)")
	}

	_, existsShortcut := stpShortcut.currentTxMap.Get(removedShortcut[0])
	require.False(t, existsShortcut, "removed hash must be gone from currentTxMap (shortcut path)")

	// Same surviving set, present exactly once, on both code paths.
	require.Equal(t, len(remainingScan), len(remainingShortcut))

	for i, h := range remainingShortcut {
		require.Equal(t, 1, occurrencesOf(stpShortcut, h), "surviving hash must remain exactly once (shortcut path)")
		require.Equal(t, 1, occurrencesOf(stpScan, remainingScan[i]), "surviving hash must remain exactly once (linear-scan path)")
	}

	// Identical resulting subtree structure: same chained-subtree count, and
	// the exact same node sequence (order included) on both paths - not just
	// count/completeness, which wouldn't catch a lookup bug that removes the
	// wrong node.
	require.Equal(t, len(stpScan.chainedSubtrees), len(stpShortcut.chainedSubtrees), "chained subtree count must match")

	for i := range stpShortcut.chainedSubtrees {
		require.True(t, stpShortcut.chainedSubtrees[i].IsComplete(), "chained subtree %d must be complete (shortcut path)", i)
		require.True(t, stpScan.chainedSubtrees[i].IsComplete(), "chained subtree %d must be complete (linear-scan path)", i)
		require.Equal(t, stpScan.chainedSubtrees[i].Nodes, stpShortcut.chainedSubtrees[i].Nodes, "chained subtree %d node sequence must match", i)
	}

	require.Equal(t, stpScan.currentSubtree.Load().Nodes, stpShortcut.currentSubtree.Load().Nodes, "current subtree node sequence must match")

	requireCoinbasePlaceholderIntact(t, stpShortcut)
	requireCoinbasePlaceholderIntact(t, stpScan)
}

// TestRemoveTxsFromSubtrees_StaleSubtreeIndexFallsBackToScan corrupts
// currentTxMap.SubtreeIndex directly (via the exported UpdateSubtreeIndex) and
// proves that the shortcut in locateTxInSubtrees degrades safely: a stale
// in-range index, and an out-of-range index, both fall through to the linear
// scan rather than removing the wrong node or silently skipping the removal.
// This is the load-bearing safety argument for the whole optimization, so it
// needs its own coverage - the equivalence test above only ever exercises a
// correct SubtreeIndex.
func TestRemoveTxsFromSubtrees_StaleSubtreeIndexFallsBackToScan(t *testing.T) {
	ctx := context.Background()

	const (
		numSubtrees = 4
		subtreeSize = 8
	)

	stp, _ := setupBatchRemovalProcessor(t, subtreeSize, true)

	allHashes := populateChainedSubtrees(t, stp, numSubtrees, subtreeSize)

	// target1 actually lives in the last chained subtree; point its
	// SubtreeIndex at the wrong (but in-range) chained subtree. The range
	// check passes, so the hash check inside that subtree must miss and fall
	// through to the full scan.
	target1 := allHashes[len(allHashes)-1]
	require.NoError(t, stp.diskTxMap.UpdateSubtreeIndex(target1, 1)) // wrongly claims chainedSubtrees[0]
	require.NoError(t, stp.removeTxsFromSubtrees(ctx, []chainhash.Hash{target1}))
	require.Equal(t, 0, occurrencesOf(stp, target1), "linear-scan fallback must still remove a stale in-range-indexed hash")

	// target2 gets an out-of-range SubtreeIndex, guarding the bounds check
	// itself rather than the hash re-verification.
	target2 := allHashes[len(allHashes)-2]
	require.NoError(t, stp.diskTxMap.UpdateSubtreeIndex(target2, int16(len(stp.chainedSubtrees)+5)))
	require.NoError(t, stp.removeTxsFromSubtrees(ctx, []chainhash.Hash{target2}))
	require.Equal(t, 0, occurrencesOf(stp, target2), "linear-scan fallback must still remove an out-of-range-indexed hash")

	// A bad index must cost a wasted probe, not a wrong-node removal: every
	// other hash must still be present exactly once.
	removed := map[chainhash.Hash]bool{target1: true, target2: true}
	for _, h := range allHashes {
		if removed[h] {
			continue
		}

		require.Equal(t, 1, occurrencesOf(stp, h), "surviving hash %s must remain exactly once", h.String())
	}

	requireCoinbasePlaceholderIntact(t, stp)
}
