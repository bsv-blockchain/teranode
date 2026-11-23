package model

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestGetAndValidateSubtrees_ReloadsAfterNodesReleased reproduces the
// release-then-requeue poisoning behind the 2026-08-11 scale-2 freeze:
// blockvalidation's failure paths return a block's pooled node slices
// (ReleaseNodes sets Nodes=nil but leaves the *Subtree pointers in
// SubtreeSlices) and then requeue the same *Block for revalidation.
// GetAndValidateSubtrees must treat those gutted slices as not loaded and
// reload from the store — not early-exit "already loaded" and let Valid fail
// "first subtree has no nodes" on a perfectly valid block.
func TestGetAndValidateSubtrees_ReloadsAfterNodesReleased(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	txBytes := make([]byte, 32)
	_, _ = rand.Read(txBytes)
	txHash, err := chainhash.NewHash(txBytes)
	require.NoError(t, err)
	require.NoError(t, st.AddNode(*txHash, 1, 0))

	blobStore := blobmemory.New()
	storeSubtree(t, blobStore, st)

	b, err := NewBlock(newTestBlockHeader(t), newTestCoinbaseTx(t), []*chainhash.Hash{st.RootHash()}, 2, 123, 0, 0)
	require.NoError(t, err)

	ctx := context.Background()
	err = b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, blobStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)
	require.Len(t, b.SubtreeSlices, 1)
	require.Len(t, b.SubtreeSlices[0].Nodes, 2)

	// Simulate services/blockvalidation.releaseBlockNodes: the pooled Nodes
	// backing slice is taken back while the *Subtree pointer stays behind.
	_ = b.SubtreeSlices[0].ReleaseNodes()

	// Poison TransactionCount so the assertion below proves the full-load path
	// ran: only a real reload recomputes it (the incident's 4 failed
	// revalidations all showed the stale count of the early-exit path).
	b.TransactionCount = 999

	err = b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, blobStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)
	require.Len(t, b.SubtreeSlices, 1)
	require.NotNil(t, b.SubtreeSlices[0])
	require.Len(t, b.SubtreeSlices[0].Nodes, 2,
		"subtrees must be reloaded from the store after their nodes were released")
	require.Equal(t, uint64(2), b.TransactionCount,
		"a reload must recompute TransactionCount from the store, not keep the stale value")
}

// TestGetAndValidateSubtrees_SizeMismatchDoesNotDeadlock pins the error paths of
// the subtree-size check. GetAndValidateSubtrees holds b.subtreeSlicesMu for its
// whole body, and Block.String() takes that same mutex — sync.RWMutex is not
// reentrant, so formatting the error with b.String() hung the goroutine forever
// while still holding the block's mutex (which in turn blocks the
// lastValidatedBlocks eviction callback). A peer can reach this with a block
// whose non-final subtrees have unequal lengths.
func TestGetAndValidateSubtrees_SizeMismatchDoesNotDeadlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	blobStore := blobmemory.New()

	newSubtree := func(leafCount int, withCoinbase bool, label string) *subtreepkg.Subtree {
		st, err := subtreepkg.NewTreeByLeafCount(leafCount)
		require.NoError(t, err)

		if withCoinbase {
			require.NoError(t, st.AddCoinbaseNode())
		}

		for i := st.Length(); i < leafCount; i++ {
			require.NoError(t, st.AddNode(chainhash.HashH([]byte(label+string(rune('a'+i)))), 1, 0))
		}

		storeSubtree(t, blobStore, st)

		return st
	}

	// Lengths 2, 1, 2: the middle subtree is neither the first nor the last, so
	// it trips "subtree %d has length %d, expected %d".
	st0 := newSubtree(2, true, "zero")
	st1 := newSubtree(1, false, "one")
	st2 := newSubtree(2, false, "two")

	b, err := NewBlock(newTestBlockHeader(t), newTestCoinbaseTx(t), []*chainhash.Hash{
		st0.RootHash(), st1.RootHash(), st2.RootHash(),
	}, 5, 123, 0, 0)
	require.NoError(t, err)

	errCh := make(chan error, 1)

	go func() {
		errCh <- b.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, blobStore,
			tSettings.Block.GetAndValidateSubtreesConcurrency)
	}()

	select {
	case err = <-errCh:
		require.Error(t, err, "unequal non-final subtree lengths must be rejected")
		require.Contains(t, err.Error(), "expected",
			"the size-mismatch error must be the one returned")
	case <-time.After(30 * time.Second):
		t.Fatal("GetAndValidateSubtrees deadlocked formatting its error while holding subtreeSlicesMu")
	}
}

// TestGetAndValidateSubtrees_ReloadClosesSurvivingSubtrees pins the cleanup on
// the reload path. When only some entries are gutted, the survivors are still
// dropped by the SubtreeSlices reallocation — and an mmap-backed survivor
// dropped without Close leaks its mapping and its backing file, since nothing
// holds a reference once the slice header is replaced.
func TestGetAndValidateSubtrees_ReloadClosesSurvivingSubtrees(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	blobStore := blobmemory.New()

	// Two real subtrees of equal length so the block passes the size check.
	first, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(chainhash.HashH([]byte("survivor-a")), 1, 0))
	storeSubtree(t, blobStore, first)

	second, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, second.AddNode(chainhash.HashH([]byte("gutted-a")), 1, 0))
	require.NoError(t, second.AddNode(chainhash.HashH([]byte("gutted-b")), 1, 0))
	storeSubtree(t, blobStore, second)

	b, err := NewBlock(newTestBlockHeader(t), newTestCoinbaseTx(t), []*chainhash.Hash{first.RootHash(), second.RootHash()}, 4, 123, 0, 0)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, blobStore,
		tSettings.Block.GetAndValidateSubtreesConcurrency))

	// Swap entry 0 for an mmap-backed subtree so we can observe whether the
	// reload closes it, and gut entry 1 so the reload actually triggers.
	serialized, err := first.Serialize()
	require.NoError(t, err)

	mmapDir := t.TempDir()
	mmapSurvivor, err := subtreepkg.NewSubtreeFromReaderMmap(bytes.NewReader(serialized), mmapDir)
	require.NoError(t, err)
	require.True(t, mmapSurvivor.IsMmapBacked())

	b.SubtreeSlices[0] = mmapSurvivor
	_ = b.SubtreeSlices[1].ReleaseNodes()

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "sanity: the survivor's backing file exists before the reload")

	require.NoError(t, b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, blobStore,
		tSettings.Block.GetAndValidateSubtreesConcurrency))

	entries, err = os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries,
		"the reload must Close a surviving mmap-backed subtree before dropping it, not leak its mapping")

	// Close unmaps the region and removes the backing file, and never touches
	// Nodes — so a closed mmap subtree that keeps its slice hands a dangling
	// pointer into freed memory to anyone still holding it. That is a fault
	// rather than a clean panic. closeMmapSubtreesLocked's ReleaseNodes is the
	// whole of that fix, and until this assertion existed nothing in the tree
	// failed if it were removed: the check above only proves the file is gone.
	//
	// The closed subtree is still reachable through this local variable. The
	// nil-ness is evaluated here and only the bool is handed to testify, never
	// the slice: require.Nil formats a failure as "Expected nil, but got: %#v",
	// which walks the elements and so faults the test binary instead of failing
	// the assertion once the region is unmapped. Verified, not assumed — with
	// the ReleaseNodes call removed, require.Nil dies in fmt.(*pp).printValue
	// under assert.Nil rather than reporting anything. Same trap as
	// _ReloadDoesNotWriteIntoACallerOwnedArray's require.Same below.
	require.True(t, mmapSurvivor.Nodes == nil,
		"a closed mmap subtree must not keep a slice into the region Close just unmapped")
}

// TestReleaseSubtreeNodes_ClosesMmapAndNilsEntries pins the full release
// contract: heap-backed node slices are handed to the pool callback, mmap-backed
// slices are NOT (their backing is the mapped region — pooling it after munmap
// would be use-after-free), every subtree is Closed (removing the mmap backing
// file), and every entry is nil-ed under the block's subtree mutex.
func TestReleaseSubtreeNodes_ClosesMmapAndNilsEntries(t *testing.T) {
	heapSt, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, heapSt.AddCoinbaseNode())
	require.NoError(t, heapSt.AddNode(chainhash.HashH([]byte("tx-a")), 1, 0))

	serialized, err := heapSt.Serialize()
	require.NoError(t, err)

	mmapDir := t.TempDir()
	mmapSt, err := subtreepkg.NewSubtreeFromReaderMmap(bytes.NewReader(serialized), mmapDir)
	require.NoError(t, err)
	require.True(t, mmapSt.IsMmapBacked())

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "sanity: mmap backing file exists while the subtree is live")

	b := &Block{SubtreeSlices: []*subtreepkg.Subtree{heapSt, mmapSt, nil}}

	// Capture the backing arrays before the release nils them. Asserting the
	// COUNT of pooled slices is not enough: both subtrees carry two nodes, so
	// inverting the IsMmapBacked guard still pools exactly one slice — the mmap
	// region — and a count-only assertion waves through the precise
	// use-after-free the guard exists to prevent. Identity is the assertion
	// that has teeth.
	// Compared as plain integers, never dereferenced: by the time we assert,
	// Close has munmapped the mmap region, so reading through that pointer
	// would fault the test binary rather than fail it.
	heapNodesPtr := reflect.ValueOf(heapSt.Nodes).Pointer()
	mmapNodesPtr := reflect.ValueOf(mmapSt.Nodes).Pointer()
	require.NotEmpty(t, heapSt.Nodes, "sanity: the heap-backed subtree has nodes to pool")
	require.NotEmpty(t, mmapSt.Nodes, "sanity: the mmap-backed subtree has nodes")

	var pooled [][]subtreepkg.Node
	require.NoError(t, b.ReleaseSubtreeNodes(func(nodes []subtreepkg.Node) { pooled = append(pooled, nodes) }),
		"a clean release must report no Close errors")

	require.Len(t, pooled, 1, "only the heap-backed slice may be pooled")
	require.Equal(t, heapNodesPtr, reflect.ValueOf(pooled[0]).Pointer(),
		"the pooled slice must be the heap-backed subtree's own backing array")
	require.NotEqual(t, mmapNodesPtr, reflect.ValueOf(pooled[0]).Pointer(),
		"the mmap region must never reach the pool: Close munmaps it, and the pool would hand it out as a live Node slice")
	require.Nil(t, b.SubtreeSlices[0])
	require.Nil(t, b.SubtreeSlices[1])

	entries, err = os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries, "Close must unmap and remove the mmap backing file")
}

// TestGetAndValidateSubtrees_ReloadLeavesSharedHeapSubtreeUsable pins the
// ownership boundary on the reload path.
//
// The reload's release exists to stop a surviving mmap-backed subtree leaking
// its mapping and backing file when SubtreeSlices is reallocated over it. But it
// released every survivor, and a block does not exclusively own all of them.
// blockvalidation's quick_validate stores the same *Subtree pointer into both
// block.SubtreeSlices and the SubtreeWriteJob it queues on the shared write
// channel, and blockassembly aliases the processor's live job.Subtrees into a
// model.Block. Gutting one of those under a write worker means
// job.Subtree.Serialize() sees no nodes — writing a 0-leaf .subtree over a good
// one, or dereferencing a nil RootHash — and it is a data race on Nodes either
// way.
//
// Only mmap-backed subtrees need Closing, so the reload closes mmap survivors
// and leaves heap survivors alone. quick_validate's shared entry is always
// heap-backed (it builds it with AddNode, and its mmap-capable branch queues a
// job carrying no Subtree at all). blockassembly's job subtrees can be
// mmap-backed when blockassembly_subtreeMmapDir is set; they are safe only
// because that path takes GetAndValidateSubtrees' "already loaded" early exit
// and never reaches the release at all.
func TestGetAndValidateSubtrees_ReloadLeavesSharedHeapSubtreeUsable(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	blobStore := blobmemory.New()

	first, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(chainhash.HashH([]byte("shared-a")), 1, 0))
	storeSubtree(t, blobStore, first)

	second, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, second.AddNode(chainhash.HashH([]byte("gutted-a")), 1, 0))
	require.NoError(t, second.AddNode(chainhash.HashH([]byte("gutted-b")), 1, 0))
	storeSubtree(t, blobStore, second)

	b, err := NewBlock(newTestBlockHeader(t), newTestCoinbaseTx(t),
		[]*chainhash.Hash{first.RootHash(), second.RootHash()}, 4, 123, 0, 0)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, blobStore,
		tSettings.Block.GetAndValidateSubtreesConcurrency))

	// The pointer a queued SubtreeWriteJob would still be holding.
	shared := b.SubtreeSlices[0]
	require.False(t, shared.IsMmapBacked(), "sanity: this stands in for quick_validate's shared entry, which is always heap-backed")
	require.NotEmpty(t, shared.Nodes, "sanity: the shared subtree is loaded before the reload")

	// Gut entry 1 so the reload branch runs over the surviving entry 0.
	_ = b.SubtreeSlices[1].ReleaseNodes()

	require.NoError(t, b.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, blobStore,
		tSettings.Block.GetAndValidateSubtreesConcurrency))

	require.NotEmpty(t, shared.Nodes,
		"the reload must not strip the nodes of a subtree a write worker still holds")

	// The write worker's actual next move. This is what fails in production if
	// the reload gutted it: a 0-leaf file written over a good one.
	serialized, err := shared.Serialize()
	require.NoError(t, err, "a shared subtree must still serialize after an unrelated reload")

	roundTripped, err := subtreepkg.NewSubtreeFromBytes(serialized)
	require.NoError(t, err)
	require.Equal(t, 2, roundTripped.Length(),
		"the shared subtree must still serialize its real nodes, not an empty tree")
}

// TestTryReleaseSubtreeNodes_DoesNotBlockOnABusyBlock pins the non-blocking
// contract the cache-eviction path depends on.
//
// blockvalidation's lastValidatedBlocks eviction function runs inside
// expiringmap.clean(), which holds the map's own write lock for the whole sweep.
// Releasing a block's nodes needs the block's subtree mutex, and
// GetAndValidateSubtrees holds that mutex across store reads with retries and
// backoff. A blocking release therefore parks the cleaner under the map lock and
// every Get/Set/Delete on the cache queues behind it.
//
// So the release must be attemptable and declinable: the caller vetoes the
// eviction and the entry is retried on the next tick.
func TestTryReleaseSubtreeNodes_DoesNotBlockOnABusyBlock(t *testing.T) {
	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("busy-a")), 1, 0))

	b := &Block{SubtreeSlices: []*subtreepkg.Subtree{st}}

	// Stand in for a validation holding the mutex across store I/O.
	b.subtreeSlicesMu.Lock()

	type result struct {
		released bool
		err      error
	}

	done := make(chan result, 1)

	go func() {
		released, releaseErr := b.TryReleaseSubtreeNodes(nil)
		done <- result{released: released, err: releaseErr}
	}()

	select {
	case got := <-done:
		require.False(t, got.released, "a busy block must be declined, not released")
		require.NoError(t, got.err)
	case <-time.After(5 * time.Second):
		b.subtreeSlicesMu.Unlock()
		t.Fatal("TryReleaseSubtreeNodes blocked on a busy block - this is what stalls the whole lastValidatedBlocks cache")
	}

	require.NotEmpty(t, b.SubtreeSlices[0].Nodes, "a declined release must leave the block untouched")

	b.subtreeSlicesMu.Unlock()

	// Free now, so the same call must succeed and actually release.
	released, err := b.TryReleaseSubtreeNodes(nil)
	require.NoError(t, err)
	require.True(t, released, "an idle block must be released, or eviction would never reclaim anything")
	require.Nil(t, b.SubtreeSlices[0], "a successful release must nil the entry")
}

// TestGetAndValidateSubtrees_ReloadDoesNotWriteIntoACallerOwnedArray pins that
// the reload leaves the SubtreeSlices array itself alone.
//
// A block's SubtreeSlices can be the caller's own slice rather than one the
// block allocated: blockassembly builds a model.Block with
// SubtreeSlices: job.Subtrees and calls Valid on it, sharing the backing array
// with the live subtree processor. Writing nil into an element there reaches
// straight into the processor's state — the block replaces its own header a
// moment later, so nothing is gained by nil-ing first.
func TestGetAndValidateSubtrees_ReloadDoesNotWriteIntoACallerOwnedArray(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	blobStore := blobmemory.New()

	first, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(chainhash.HashH([]byte("owned-a")), 1, 0))
	storeSubtree(t, blobStore, first)

	second, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, second.AddNode(chainhash.HashH([]byte("owned-b0")), 1, 0))
	require.NoError(t, second.AddNode(chainhash.HashH([]byte("owned-b1")), 1, 0))
	storeSubtree(t, blobStore, second)

	serialized, err := first.Serialize()
	require.NoError(t, err)

	// An mmap-backed entry, because that is the only kind the reload's close
	// touches — so it is the entry that would be nil-ed in the caller's array.
	mmapSt, err := subtreepkg.NewSubtreeFromReaderMmap(bytes.NewReader(serialized), t.TempDir())
	require.NoError(t, err)
	require.True(t, mmapSt.IsMmapBacked())

	// The caller's array, shared with the block the way blockassembly shares
	// job.Subtrees.
	callerOwned := []*subtreepkg.Subtree{mmapSt, nil}

	b, err := NewBlock(newTestBlockHeader(t), newTestCoinbaseTx(t),
		[]*chainhash.Hash{first.RootHash(), second.RootHash()}, 4, 123, 0, 0)
	require.NoError(t, err)

	b.SubtreeSlices = callerOwned

	// Compared as a plain integer rather than with require.Same: the reload
	// Closes this subtree, so only the pointer identity of the array element is
	// under test here, not anything reachable through it.
	expected := reflect.ValueOf(mmapSt).Pointer()

	require.NoError(t, b.GetAndValidateSubtrees(context.Background(), ulogger.TestLogger{}, blobStore,
		tSettings.Block.GetAndValidateSubtreesConcurrency))

	require.Equal(t, expected, reflect.ValueOf(callerOwned[0]).Pointer(),
		"the reload must not write into an array it may not own - the block replaces its own header instead")
}
