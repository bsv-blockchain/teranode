package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/util/testutil/blobfault"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 3: the storage side adds a subtree's root to the stored set when its store
// ends with everything it has to write stored, and notes a store that failed for good.

func storedSetServer(t *testing.T) (*BlockAssembly, *subtreeprocessor.StoredSet) {
	t.Helper()

	server, _, _, _ := setup(t)
	server.storedSet = subtreeprocessor.NewStoredSet()

	return server, server.storedSet
}

func TestStoredSet_AStoredSubtreeAndItsMetaAddTheRoot(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 101)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, set.Has(*subtree.RootHash()), "the root is in the set when the result is posted")
	require.Equal(t, uint64(1), set.Added())
	require.False(t, set.Retrying(*subtree.RootHash()))
}

func TestStoredSet_ASubtreeStoredByARetryAddsTheRootAndTheRetryIsNoLongerInFlight(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 102)

	fault := blobfault.New(server.subtreeStore)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: 1})
	server.subtreeStore = fault

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, set.Has(*subtree.RootHash()))
	require.False(t, set.Retrying(*subtree.RootHash()), "the retry settled")
}

func TestStoredSet_ARetryThatIsExhaustedLeavesTheRootOutAndNotesTheFailure(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 103)

	fault := blobfault.New(server.subtreeStore)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtree, blobfault.Rule{FailTimes: blobfault.Always})
	server.subtreeStore = fault
	setForTest(t, &subtreeRetryLimit, 1)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.Error(t, result.Err)
	require.False(t, set.Has(*subtree.RootHash()))
	require.True(t, set.HasFailed(*subtree.RootHash()), "the processor sends it again from memory")
	require.False(t, set.Retrying(*subtree.RootHash()))
}

// A subtree whose file landed but whose meta did not: the second request for it finds the file and
// must build the meta, or the root never reaches the set and the prefix stops there for good.
func TestStoredSet_AnExistingSubtreeWithoutItsMetaGetsTheMetaBuiltAndIsStored(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 104)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	p := startStorePipeline(t, server)

	// no Result channel: the processor's re-enqueue does not ask for one
	errChan := make(chan error, 1)
	p.in <- subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: txMap, SkipNotification: true, ErrChan: errChan}
	require.NoError(t, <-errChan)

	require.True(t, set.Has(*subtree.RootHash()), "the meta was built")
	require.True(t, exists(t, server.subtreeStore, subtree, fileformat.FileTypeSubtreeMeta))
}

func TestStoredSet_AMetaThatFailedForGoodIsStoredByTheSecondRequest(t *testing.T) {
	server, set := storedSetServer(t)
	subtree, txMap := makeSubtree(t, 105)

	fault := blobfault.New(server.subtreeStore)
	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, blobfault.Rule{FailTimes: blobfault.Always})
	server.subtreeStore = fault
	setForTest(t, &subtreeRetryLimit, 1)

	p := startStorePipeline(t, server)

	first := awaitResult(t, p.store(subtree, txMap, true))
	require.Error(t, first.Err)
	require.False(t, set.Has(*subtree.RootHash()), "the meta is missing")
	require.True(t, set.HasFailed(*subtree.RootHash()))

	fault.OnSet(subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, blobfault.Rule{})

	second := awaitResult(t, p.store(subtree, txMap, true))
	require.True(t, second.OK(), "%+v", second)
	require.True(t, set.Has(*subtree.RootHash()))
	require.False(t, set.HasFailed(*subtree.RootHash()), "stored since")
}

func TestStoredSet_WithoutAMetaOnlyTheSubtreeIsNeeded(t *testing.T) {
	server, set := storedSetServer(t)
	server.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	subtree, txMap := makeSubtree(t, 106)

	p := startStorePipeline(t, server)
	result := awaitResult(t, p.store(subtree, txMap, true))

	require.True(t, result.OK(), "%+v", result)
	require.True(t, set.Has(*subtree.RootHash()))
}

// Every way a request settles must leave the retry count where it was, or the progress rule,
// which waits for no retry in flight, would stay off for good.
func TestStoredSet_EveryWayARequestSettlesBalancesTheRetryCount(t *testing.T) {
	root := chainhash.HashH([]byte("outcome"))
	boom := errors.NewStorageError("boom")

	for name, settle := range map[string]func(o *subtreeStoreOutcome){
		"stored":         func(o *subtreeStoreOutcome) { o.subtreeStored(false); o.metaStored() },
		"existed":        func(o *subtreeStoreOutcome) { o.subtreeStored(true); o.metaAbsent() },
		"retry stored":   func(o *subtreeStoreOutcome) { o.toRetry(); o.subtreeStored(false); o.metaStored() },
		"retry failed":   func(o *subtreeStoreOutcome) { o.toRetry(); o.fail(boom); o.metaStored() },
		"cancelled":      func(o *subtreeStoreOutcome) { o.toRetry(); o.cancel(boom); o.cancel(boom) },
		"aborted":        func(o *subtreeStoreOutcome) { o.abort(boom) },
		"both retrying":  func(o *subtreeStoreOutcome) { o.toRetry(); o.toRetry(); o.subtreeStored(false); o.metaStored() },
		"settled twice":  func(o *subtreeStoreOutcome) { o.subtreeStored(false); o.metaStored(); o.fail(boom) },
		"meta not built": func(o *subtreeStoreOutcome) { o.subtreeStored(false); o.fail(boom) },
	} {
		t.Run(name, func(t *testing.T) {
			set := subtreeprocessor.NewStoredSet()
			o := newSubtreeStoreOutcome(subtreeprocessor.NewSubtreeRequest{}, true, nil)
			o.track(set, root)

			settle(o)

			require.False(t, set.Retrying(root))
		})
	}
}
