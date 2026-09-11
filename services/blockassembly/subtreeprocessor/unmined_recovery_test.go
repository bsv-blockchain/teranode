package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func recoveryRow(name string) *utxostore.UnminedTransaction {
	return &utxostore.UnminedTransaction{Node: &subtreepkg.Node{Hash: chainhash.HashH([]byte(name)), Fee: 1, SizeInBytes: 100}, TxInpoints: &subtreepkg.TxInpoints{}}
}

func TestUnminedRecoverySelectionFailurePreservesAssemblyAndQueue(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	old, queued := recoveryRow("assembled"), recoveryRow("queued")
	require.NoError(t, stp.AddDirectly(old.Node, old.TxInpoints, true))
	stp.queue.enqueueBatch([]subtreepkg.Node{*queued.Node}, []*subtreepkg.TxInpoints{queued.TxInpoints})
	before := stp.currentSubtree.Load()
	want := errors.NewProcessingError("read unavailable")
	err := stp.recoverUnmined(t.Context(), prevBlockHeader, []chainhash.Hash{queued.Hash}, func(_ context.Context, hashes []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		require.ElementsMatch(t, []chainhash.Hash{old.Hash, queued.Hash}, hashes)
		require.True(t, accepted(old.Hash))
		require.True(t, accepted(queued.Hash))
		return nil, want
	})
	require.ErrorIs(t, err, want)
	require.Same(t, before, stp.currentSubtree.Load())
	require.Equal(t, int64(1), stp.queue.length())
	require.Contains(t, collectSubtreeHashes(stp), old.Hash)
	require.False(t, stp.recoveryPending.Load())
}

func TestUnminedRecoveryPreservesArrivalsAfterSnapshotExactlyOnce(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.clock, stp.queue.clock = fixedClock{t: now}, fixedClock{t: now}
	parent, child, mined := recoveryRow("parent"), recoveryRow("child"), recoveryRow("mined")
	child.TxInpoints.ParentTxHashes = []chainhash.Hash{parent.Hash}
	stp.queue.enqueueBatch([]subtreepkg.Node{*mined.Node}, []*subtreepkg.TxInpoints{mined.TxInpoints})
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, []chainhash.Hash{parent.Hash}, func(_ context.Context, hashes []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		require.ElementsMatch(t, []chainhash.Hash{parent.Hash, mined.Hash}, hashes)
		require.True(t, accepted(mined.Hash))
		require.False(t, accepted(parent.Hash))
		stp.queue.enqueueBatch([]subtreepkg.Node{*parent.Node, *child.Node}, []*subtreepkg.TxInpoints{parent.TxInpoints, child.TxInpoints})
		return []*utxostore.UnminedTransaction{parent}, nil
	}))
	require.Equal(t, int64(2), stp.queue.length())
	stp.clock = fixedClock{t: now.Add(time.Millisecond)}
	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))
	require.Equal(t, []chainhash.Hash{*subtreepkg.CoinbasePlaceholderHash, parent.Hash, child.Hash}, collectSubtreeHashes(stp))
	require.Zero(t, stp.queue.length())
}

func TestUnminedRecoveryFailedRebuildClosesMiningAndRetainsAdmission(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	parent, queued := recoveryRow("accepted-locked-parent"), recoveryRow("queued-child")
	parent.Locked = true
	require.NoError(t, stp.AddDirectly(parent.Node, parent.TxInpoints, true))
	stp.queue.enqueueBatch([]subtreepkg.Node{*queued.Node}, []*subtreepkg.TxInpoints{queued.TxInpoints})
	// A one-leaf subtree fills with its coinbase placeholder. Adding the
	// selected transaction then fails AFTER the live map/tree were cleared.
	stp.currentItemsPerFile.Store(1)
	prepare := func(_ context.Context, hashes []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		require.Contains(t, hashes, parent.Hash)
		require.True(t, accepted(parent.Hash), "a retry must retain pre-failure admission proof for locked records")
		return []*utxostore.UnminedTransaction{parent, queued}, nil
	}
	require.Error(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, prepare))
	require.True(t, stp.recoveryPending.Load())
	require.Nil(t, stp.GetPrecomputedMiningData())
	require.Nil(t, stp.GetIncompleteSubtreeMiningData(t.Context()))
	require.Equal(t, int64(1), stp.queue.length())
	stp.currentItemsPerFile.Store(32)
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, prepare))
	require.False(t, stp.recoveryPending.Load())
	require.Empty(t, stp.recoveryAccepted)
	require.Zero(t, stp.queue.length())
	require.Equal(t, []chainhash.Hash{*subtreepkg.CoinbasePlaceholderHash, parent.Hash, queued.Hash}, collectSubtreeHashes(stp))
}

func TestUnminedRecoveryRejectsChangedTipAndCancelledRequest(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })
	prepare := func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return nil, errors.NewProcessingError("selection must not run")
	}
	require.ErrorContains(t, stp.RecoverUnmined(t.Context(), blockHeader, nil, prepare), "tip changed")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, stp.RecoverUnmined(ctx, prevBlockHeader, nil, prepare), context.Canceled)
}

func TestUnminedRecoveryCancelsBlockedSubtreeStorage(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.currentItemsPerFile.Store(2)
	// Completing the rebuilt subtree must honour the recovery context even
	// when the storage listener is stalled, while the processor remains alive.
	stp.newSubtreeChan = make(chan NewSubtreeRequest)
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- stp.RecoverUnmined(ctx, prevBlockHeader, nil, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
			return []*utxostore.UnminedTransaction{recoveryRow("fills-subtree")}, nil
		})
	}()
	require.Eventually(t, func() bool { return stp.chainedSubtreeCount.Load() == 1 }, 5*time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("request cancellation did not unblock recovery storage send")
	}
	require.True(t, stp.recoveryPending.Load())
	require.Nil(t, stp.GetPrecomputedMiningData())
}

func TestUnminedRecoveryPendingRejectsBlockMovementAndLegacyReset(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.recoveryPending.Store(true)
	stp.recoveryAccepted = map[chainhash.Hash]struct{}{chainhash.HashH([]byte("prior-admission")): {}}
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })
	require.ErrorContains(t, stp.Reorg(nil, nil), "read-only repair")
	require.ErrorContains(t, stp.MoveForwardBlock(nil), "read-only repair")
	called := false
	before := stp.currentSubtree.Load()
	require.ErrorContains(t, stp.Reset(prevBlockHeader, nil, nil, false, func() error { called = true; return nil }).Err, "read-only repair")
	require.False(t, called, "legacy startup loader must not run while recovery is incomplete")
	require.True(t, stp.RecoveryPending())
	require.Same(t, before, stp.currentSubtree.Load())
	require.NoError(t, stp.RecoverUnmined(t.Context(), prevBlockHeader, nil, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return nil, nil
	}))
	require.False(t, stp.RecoveryPending())
}

func TestUnminedRecoveryRemainsPendingUntilSnapshotPublication(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.recoveryPending.Store(true)
	stp.precomputedMiningData.Store(&PrecomputedMiningData{PreviousHeader: blockHeader})
	// A concurrent snapshot acquisition can own this mutex at the instant a
	// rebuild completes. Pending must stay true until the replacement snapshot
	// can actually be published, not merely until publication is requested.
	stp.miningSnapshots.mu.Lock()
	started, finished := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		stp.publishRecoveredMiningData()
		close(finished)
	}()
	<-started
	remainedPending := true
	deadline := time.After(30 * time.Millisecond)
wait:
	for {
		select {
		case <-deadline:
			break wait
		default:
			if !stp.RecoveryPending() {
				remainedPending = false
				break wait
			}
			time.Sleep(time.Millisecond)
		}
	}
	stp.miningSnapshots.mu.Unlock()
	<-finished
	require.True(t, remainedPending, "recovery opened mining before replacement snapshot could be published")
	require.False(t, stp.RecoveryPending())
	snapshot := stp.GetPrecomputedMiningData()
	require.NotNil(t, snapshot)
	defer snapshot.Lease.Release()
	require.Equal(t, prevBlockHeader.Hash(), snapshot.PreviousHeader.Hash())
}

func TestUnminedRecoveryPreservesOutstandingRemovalsAndDescendants(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	removed, child, grandchild, good := recoveryRow("removed"), recoveryRow("removed-child"), recoveryRow("removed-grandchild"), recoveryRow("good")
	child.TxInpoints.ParentTxHashes = []chainhash.Hash{removed.Hash}
	grandchild.TxInpoints.ParentTxHashes = []chainhash.Hash{child.Hash}
	require.NoError(t, stp.removeMap.Put(removed.Hash, 1))
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return []*utxostore.UnminedTransaction{removed, child, grandchild, good}, nil
	}))
	require.Equal(t, []chainhash.Hash{*subtreepkg.CoinbasePlaceholderHash, good.Hash}, collectSubtreeHashes(stp))
	require.True(t, stp.removeMap.Exists(removed.Hash), "recovery must retain outstanding removal for delayed feeds")
}

func TestUnminedRecoveryDoesNotResampleAdaptiveSubtreeSizing(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.currentItemsPerFile.Store(2)
	stp.subtreesInBlock = 7
	ringBefore := stp.subtreeNodeCounts
	ringBefore.Value = 19
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return []*utxostore.UnminedTransaction{recoveryRow("replayed")}, nil
	}))
	require.Equal(t, 7, stp.subtreesInBlock)
	require.Same(t, ringBefore, stp.subtreeNodeCounts)
	require.Equal(t, 19, ringBefore.Value)
	require.Equal(t, int32(1), stp.chainedSubtreeCount.Load(), "structural counters must still describe the rebuilt template")
}
