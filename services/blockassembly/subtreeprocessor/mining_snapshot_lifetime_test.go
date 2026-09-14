package subtreeprocessor

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func TestMiningSnapshotSurvivesRetiredMmapSubtrees(t *testing.T) {
	dir := t.TempDir()
	st, err := subtreepkg.NewTreeByLeafCountMmap(2, dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	require.NoError(t, st.AddCoinbaseNode())
	txID := chainhash.HashH([]byte("mining job before same-tip recovery"))
	require.NoError(t, st.AddNode(txID, 17, 123))

	stp := &SubtreeProcessor{chainedSubtrees: []*subtreepkg.Subtree{st}}
	stp.currentBlockHeader.Store(&model.BlockHeader{})
	stp.updatePrecomputedMiningData()
	snapshot := stp.GetPrecomputedMiningData()
	require.NotNil(t, snapshot)
	t.Cleanup(snapshot.Lease.Release)
	require.Same(t, st, snapshot.Subtrees[0], "mining snapshots must share completed subtree storage")

	stp.closeChainedSubtrees()

	// Inspect the backing file before touching Nodes: reading an already unmapped
	// slice would terminate the test process, rather than produce a useful failure.
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1, "reset must retain mmap storage referenced by an issued mining snapshot")
	require.Equal(t, txID, snapshot.Subtrees[0].Nodes[1].Hash)
}

func TestMiningSnapshotReleasesOnlyAfterLastReader(t *testing.T) {
	dir := t.TempDir()
	st, err := subtreepkg.NewTreeByLeafCountMmap(2, dir)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	stp := &SubtreeProcessor{chainedSubtrees: []*subtreepkg.Subtree{st}}
	stp.currentBlockHeader.Store(&model.BlockHeader{})
	stp.updatePrecomputedMiningData()
	job := stp.GetPrecomputedMiningData()
	request, ok := job.Lease.Retain()
	require.True(t, ok)
	stp.closeChainedSubtrees()
	require.Nil(t, stp.GetPrecomputedMiningData(), "retired data must no longer be available to new jobs")
	job.Lease.Release()
	job.Lease.Release()
	_, ok = job.Lease.Retain()
	require.False(t, ok, "an evicted job cannot acquire new readers")
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1, "in-flight submission still owns storage after job eviction")
	require.Equal(t, subtreepkg.CoinbasePlaceholderHashValue, job.Subtrees[0].Nodes[0].Hash)
	request.Release()
	files, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files, "last reader must release the retired mapping")
}

func TestQueuedSubtreeStorageSurvivesReset(t *testing.T) {
	dir := t.TempDir()
	st, err := subtreepkg.NewTreeByLeafCountMmap(2, dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	require.NoError(t, st.AddCoinbaseNode())
	stp := &SubtreeProcessor{chainedSubtrees: []*subtreepkg.Subtree{st}, newSubtreeChan: make(chan NewSubtreeRequest, 1)}
	require.NoError(t, stp.sendNewSubtree(t.Context(), NewSubtreeRequest{Subtree: st}))
	stp.closeChainedSubtrees()
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1, "queued asynchronous storage still needs the old mmap nodes")
	owned, retained := (<-stp.newSubtreeChan).TakeStorageOwnership()
	require.True(t, retained)
	require.Equal(t, subtreepkg.CoinbasePlaceholderHashValue, owned.Subtree.Nodes[0].Hash)
	owned.Release()
	files, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestSubtreeStorageCancellationReleasesQueuedMapping(t *testing.T) {
	dir := t.TempDir()
	st, err := subtreepkg.NewTreeByLeafCountMmap(2, dir)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	stp := &SubtreeProcessor{chainedSubtrees: []*subtreepkg.Subtree{st}, newSubtreeChan: make(chan NewSubtreeRequest, 1)}
	require.NoError(t, stp.sendNewSubtree(ctx, NewSubtreeRequest{Subtree: st}))
	stp.closeChainedSubtrees()
	cancel()
	require.Eventually(t, func() bool { files, err := os.ReadDir(dir); return err == nil && len(files) == 0 }, time.Second, time.Millisecond)
	_, retained := (<-stp.newSubtreeChan).TakeStorageOwnership()
	require.False(t, retained, "a cancelled queued mapping cannot be read by the listener")
}

func TestSubtreeStorageOwnershipSurvivesQueueCancellation(t *testing.T) {
	dir := t.TempDir()
	st, err := subtreepkg.NewTreeByLeafCountMmap(2, dir)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	ctx, cancel := context.WithCancel(t.Context())
	stp := &SubtreeProcessor{chainedSubtrees: []*subtreepkg.Subtree{st}, newSubtreeChan: make(chan NewSubtreeRequest, 1)}
	stp.processorCtx.Store(&ctx)
	require.NoError(t, stp.sendNewSubtree(ctx, NewSubtreeRequest{Subtree: st}))
	owned, retained := (<-stp.newSubtreeChan).TakeStorageOwnership()
	require.True(t, retained)
	stp.closeChainedSubtrees()
	cancel()
	require.Equal(t, subtreepkg.CoinbasePlaceholderHashValue, owned.Subtree.Nodes[0].Hash)
	owned.Release()
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestFailedSubtreeStorageEnqueueReleasesMapping(t *testing.T) {
	dir := t.TempDir()
	st, err := subtreepkg.NewTreeByLeafCountMmap(2, dir)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	stp := &SubtreeProcessor{chainedSubtrees: []*subtreepkg.Subtree{st}, newSubtreeChan: make(chan NewSubtreeRequest)}
	require.ErrorIs(t, stp.sendNewSubtree(ctx, NewSubtreeRequest{Subtree: st}), context.Canceled)
	stp.closeChainedSubtrees()
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestRecoveryWaitsForStorageReaders(t *testing.T) {
	for _, useMmap := range []bool{false, true} {
		name := "heap"
		if useMmap {
			name = "mmap"
		}
		t.Run(name, func(t *testing.T) {
			stp := &SubtreeProcessor{newSubtreeChan: make(chan NewSubtreeRequest, 1)}
			if useMmap {
				stp.mmapDir = t.TempDir()
			}
			st, err := stp.newSubtree(2)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			require.NoError(t, stp.sendNewSubtree(t.Context(), NewSubtreeRequest{Subtree: st}))
			request, retained := (<-stp.newSubtreeChan).TakeStorageOwnership()
			require.True(t, retained)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, stp.waitForSubtreeStorage(ctx), context.DeadlineExceeded, "recovery cannot clear a map with outstanding metadata readers")
			request.Release()
			require.NoError(t, stp.waitForSubtreeStorage(t.Context()))
		})
	}
}

func TestStorageCancellationRacesOwnershipTransfer(t *testing.T) {
	directory := t.TempDir()
	for range 100 {
		st, err := subtreepkg.NewTreeByLeafCountMmap(2, directory)
		require.NoError(t, err)
		stp := &SubtreeProcessor{chainedSubtrees: []*subtreepkg.Subtree{st}, newSubtreeChan: make(chan NewSubtreeRequest, 1)}
		ctx, cancel := context.WithCancel(t.Context())
		stp.processorCtx.Store(&ctx)
		require.NoError(t, stp.sendNewSubtree(ctx, NewSubtreeRequest{Subtree: st}))
		queued := <-stp.newSubtreeChan
		stp.closeChainedSubtrees()
		cancelled := make(chan struct{})
		go func() { cancel(); close(cancelled) }()
		owned, retained := queued.TakeStorageOwnership()
		if retained {
			files, err := os.ReadDir(directory)
			require.NoError(t, err)
			require.Len(t, files, 1, "cancellation cannot unmap a request acquired by storage")
			owned.Release()
		}
		<-cancelled
		waitCtx, stop := context.WithTimeout(t.Context(), time.Second)
		err = stp.waitForSubtreeStorage(waitCtx)
		stop()
		require.NoError(t, err)
		files, err := os.ReadDir(directory)
		require.NoError(t, err)
		require.Empty(t, files, "either cancellation or storage must release the mapping exactly once")
	}
}

func TestCompletedRecoveryKeepsQueuedSubtreeStorageAlive(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.newSubtreeChan = make(chan NewSubtreeRequest, 2)
	stp.mmapDir = t.TempDir()
	stp.SetCurrentItemsPerFile(2)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })
	node := &subtreepkg.Node{Hash: chainhash.HashH([]byte("storage after recovery returns")), Fee: 1, SizeInBytes: 100}
	require.NoError(t, stp.RecoverUnmined(t.Context(), prevBlockHeader, nil,
		func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
			return []*utxostore.UnminedTransaction{{Node: node, TxInpoints: &subtreepkg.TxInpoints{}}}, nil
		}))
	// Deliberately start storage after RecoverUnmined has returned and cancelled
	// its per-call context. Accepted announcements belong to processor lifetime.
	queued := <-stp.newSubtreeChan
	require.True(t, queued.stopLeaseRelease(), "successful recovery must not cancel a queued subtree's storage lifetime")
	owned, retained := queued.TakeStorageOwnership()
	require.True(t, retained)
	require.Equal(t, node.Hash, owned.Subtree.Nodes[1].Hash)
	owned.Release()
	owned.ErrChan <- nil
	require.NoError(t, stp.waitForSubtreeStorage(t.Context()))
}
