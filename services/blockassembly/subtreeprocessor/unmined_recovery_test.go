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

func TestUnminedRecoveryHealthyPassKeepsTemplate(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	assembled := recoveryRow("healthy-assembled")
	require.NoError(t, stp.AddDirectly(assembled.Node, assembled.TxInpoints, false))
	notifications := make(chan NewSubtreeRequest, 1)
	stp.newSubtreeChan = notifications
	before := stp.currentSubtree.Load()
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, []chainhash.Hash{assembled.Hash}, func(_ context.Context, _ []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		require.True(t, accepted(assembled.Hash))
		return []*utxostore.UnminedTransaction{assembled}, nil
	}))
	require.Same(t, before, stp.currentSubtree.Load())
	require.Zero(t, stp.queue.length())
	require.Empty(t, notifications, "healthy pass must not write or announce another subtree")
	require.False(t, stp.RecoveryPending())
	require.Equal(t, []chainhash.Hash{*subtreepkg.CoinbasePlaceholderHash, assembled.Hash}, collectSubtreeHashes(stp))
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
		require.Equal(t, []chainhash.Hash{queued.Hash}, hashes)
		require.True(t, accepted(old.Hash))
		require.True(t, accepted(queued.Hash))
		return nil, want
	})
	require.ErrorIs(t, err, want)
	require.Same(t, before, stp.currentSubtree.Load())
	require.Equal(t, int64(1), stp.queue.length())
	require.False(t, stp.RecoveryPending())
}

func TestUnminedRecoveryEnqueuesMissingParentFirstAndAnnouncesCompletedSubtree(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.currentItemsPerFile.Store(2)
	initial, err := stp.newSubtree(2)
	require.NoError(t, err)
	require.NoError(t, initial.AddCoinbaseNode())
	stp.currentSubtree.Store(initial)
	notifications := make(chan NewSubtreeRequest, 2)
	stp.newSubtreeChan = notifications
	parent, child := recoveryRow("missing-parent"), recoveryRow("missing-child")
	child.TxInpoints.ParentTxHashes = []chainhash.Hash{parent.Hash}
	before := stp.currentSubtree.Load()
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, []chainhash.Hash{parent.Hash, child.Hash}, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return []*utxostore.UnminedTransaction{parent, child}, nil
	}))
	require.Same(t, before, stp.currentSubtree.Load(), "selection must not replace announced roots")
	require.Equal(t, int64(2), stp.queue.length())
	stp.clock = fixedClock{t: time.Now().Add(time.Second)}
	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, false))
	require.Equal(t, []chainhash.Hash{*subtreepkg.CoinbasePlaceholderHash, parent.Hash, child.Hash}, collectSubtreeHashes(stp))
	select {
	case req := <-notifications:
		require.False(t, req.SkipNotification, "ordinary dequeue must announce a newly completed subtree")
		req.ErrChan <- nil
		req.Release()
	case <-time.After(time.Second):
		t.Fatal("completed repair subtree was not sent for storage")
	}
}

func TestUnminedRecoveryKeepsArrivalsAfterSnapshotOnce(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	parent, child := recoveryRow("late-parent"), recoveryRow("late-child")
	child.TxInpoints.ParentTxHashes = []chainhash.Hash{parent.Hash}
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, []chainhash.Hash{parent.Hash}, func(_ context.Context, _ []chainhash.Hash, _ func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		stp.queue.enqueueBatch([]subtreepkg.Node{*parent.Node, *child.Node}, []*subtreepkg.TxInpoints{parent.TxInpoints, child.TxInpoints})
		return []*utxostore.UnminedTransaction{parent}, nil
	}))
	require.Equal(t, int64(2), stp.queue.length(), "recovery must not duplicate a feed arriving during selection")
	stp.clock = fixedClock{t: time.Now().Add(time.Second)}
	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, false))
	require.Equal(t, []chainhash.Hash{*subtreepkg.CoinbasePlaceholderHash, parent.Hash, child.Hash}, collectSubtreeHashes(stp))
}

func TestUnminedRecoveryQueueFullRetriesWithoutLatching(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.queue.maxItems = 1
	first, second := recoveryRow("first"), recoveryRow("second")
	selected := []*utxostore.UnminedTransaction{first, second}
	err := stp.recoverUnmined(t.Context(), prevBlockHeader, nil, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return selected, nil
	})
	require.ErrorContains(t, err, "queue full")
	require.False(t, stp.RecoveryPending())
	require.Zero(t, stp.currentTxMap.Length())
	require.Equal(t, int64(1), stp.queue.length())
	stp.clock = fixedClock{t: time.Now().Add(time.Second)}
	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, false))
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return selected, nil
	}))
	require.Equal(t, int64(1), stp.queue.length())
	require.False(t, stp.RecoveryPending())
}

func TestUnminedRecoveryHasCapWhenNormalQueueUnbounded(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	require.Zero(t, stp.queue.maxItems)
	stp.queue.queueLength.Store(maxRecoveryQueuedItems)
	row := recoveryRow("bounded-repair")
	err := stp.recoverUnmined(t.Context(), prevBlockHeader, nil,
		func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
			return []*utxostore.UnminedTransaction{row}, nil
		})
	require.ErrorContains(t, err, "queue full")
	require.Equal(t, maxRecoveryQueuedItems, stp.queue.length())
	require.False(t, stp.RecoveryPending())
}

func TestUnminedRecoveryCursorRetainsDequeuedPrefix(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	row := recoveryRow("cursor-retains-dequeued")
	stp.queue.enqueueBatch([]subtreepkg.Node{*row.Node}, []*subtreepkg.TxInpoints{row.TxInpoints})
	snapshot, err := stp.captureUnminedRecovery(t.Context(), prevBlockHeader, nil)
	require.NoError(t, err)
	_, found := stp.queue.dequeueBatch(0)
	require.True(t, found)
	queued, err := snapshot.queueHashes(t.Context())
	require.NoError(t, err)
	require.Contains(t, queued, row.Hash)
}

func TestUnminedRecoveryCursorHashesSurviveNormalDequeueFiltering(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	row := recoveryRow("filtered-cursor")
	nodes := make([]subtreepkg.Node, 4096)
	inpoints := make([]*subtreepkg.TxInpoints, len(nodes))
	for i := range nodes {
		nodes[i], inpoints[i] = *row.Node, row.TxInpoints
	}
	stp.queue.enqueueBatch(nodes, inpoints)
	snapshot, err := stp.captureUnminedRecovery(t.Context(), prevBlockHeader, nil)
	require.NoError(t, err)
	require.NoError(t, stp.removeMap.Put(row.Hash, 1))
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })
	require.Eventually(t, func() bool { return stp.queue.length() == 0 && !stp.removeMap.Exists(row.Hash) },
		5*time.Second, time.Millisecond)
	queued, err := snapshot.queueHashes(t.Context())
	require.NoError(t, err)
	require.Contains(t, queued, row.Hash)
	require.NotContains(t, queued, chainhash.Hash{}, "dequeue must not mutate hashes held by a recovery cursor")
}

func TestUnminedRecoveryAbortsWhenQueuedParentRejectedAfterSnapshot(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	parent, child := recoveryRow("rejected-queued-parent"), recoveryRow("orphaned-child")
	child.TxInpoints.ParentTxHashes = []chainhash.Hash{parent.Hash}
	stp.queue.enqueueBatch([]subtreepkg.Node{*parent.Node}, []*subtreepkg.TxInpoints{parent.TxInpoints})
	snapshot, err := stp.captureUnminedRecovery(t.Context(), prevBlockHeader, nil)
	require.NoError(t, err)
	queued, err := snapshot.queueHashes(t.Context())
	require.NoError(t, err)
	require.Contains(t, queued, parent.Hash)
	require.NoError(t, stp.removeMap.Put(parent.Hash, 1))
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })
	require.Eventually(t, func() bool { return stp.queue.length() == 0 && !stp.removeMap.Exists(parent.Hash) },
		5*time.Second, time.Millisecond)
	result := stp.runRecoveryOnDispatcher(t.Context(), func() error {
		return stp.commitUnminedRecovery(t.Context(), prevBlockHeader, snapshot.epoch,
			[]*utxostore.UnminedTransaction{parent, child}, queued, make(map[chainhash.Hash]struct{}))
	})
	require.ErrorContains(t, result, "queued responsibility changed", "consumed removal must invalidate stale parent proof")
	require.Zero(t, stp.queue.length(), "child must not enter queue without its rejected parent")
}

func TestUnminedRecoveryRejectsLostLockedAdmission(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	row := recoveryRow("locked-unwound-during-selection")
	row.Locked = true
	stp.queue.enqueueBatch([]subtreepkg.Node{*row.Node}, []*subtreepkg.TxInpoints{row.TxInpoints})
	err := stp.recoverUnmined(t.Context(), prevBlockHeader, []chainhash.Hash{row.Hash},
		func(_ context.Context, _ []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
			require.True(t, accepted(row.Hash))
			_, found := stp.queue.dequeueBatch(0) // validator unwinds before commit
			require.True(t, found)
			return []*utxostore.UnminedTransaction{row}, nil
		})
	require.ErrorContains(t, err, "lost accepted proof")
	require.Zero(t, stp.queue.length())
	require.False(t, stp.currentTxMap.Exists(row.Hash))
}

func TestUnminedRecoveryRejectsChangedTipAndCancellation(t *testing.T) {
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

func TestUnminedRecoverySelectionLeavesDispatcherResponsive(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })
	selectionStarted, finishSelection := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- stp.RecoverUnmined(t.Context(), prevBlockHeader, nil,
			func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
				close(selectionStarted)
				<-finishSelection
				return nil, nil
			})
	}()
	<-selectionStarted
	dispatcherAnswered := make(chan error, 1)
	go func() { dispatcherAnswered <- stp.runRecoveryOnDispatcher(t.Context(), func() error { return nil }) }()
	select {
	case err := <-dispatcherAnswered:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("selection blocked subtree processor dispatcher")
	}
	close(finishSelection)
	require.NoError(t, <-result)
}

func TestUnminedRecoverySameTipResetInvalidatesSelection(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })
	row := recoveryRow("same-tip-stale")
	err := stp.RecoverUnmined(t.Context(), prevBlockHeader, []chainhash.Hash{row.Hash},
		func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
			response := stp.Reset(prevBlockHeader, nil, nil, false, nil)
			require.NoError(t, response.Err)
			return []*utxostore.UnminedTransaction{row}, nil
		})
	require.ErrorContains(t, err, "assembly state or queued responsibility changed")
	require.Zero(t, stp.queue.length())
}

func TestUnminedRecoveryAfterPartialRemovalAndNormalDequeue(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.SetCurrentItemsPerFile(8)
	initial, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)
	require.NoError(t, initial.AddCoinbaseNode())
	stp.SetCurrentSubtree(initial)
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	removed, survivor := recoveryRow("partial-removed"), recoveryRow("partial-survivor")
	stp.queue.enqueueBatch([]subtreepkg.Node{*removed.Node, *survivor.Node}, []*subtreepkg.TxInpoints{removed.TxInpoints, survivor.TxInpoints})
	require.Eventually(t, func() bool {
		ready := false
		_ = stp.runRecoveryOnDispatcher(t.Context(), func() error {
			ready = stp.currentSubtree.Load().Length() == 3
			return nil
		})
		return ready
	}, time.Second, time.Millisecond)
	require.NoError(t, stp.runRecoveryOnDispatcher(t.Context(), func() error {
		return stp.removeTxFromSubtrees(t.Context(), removed.Hash)
	}))

	rows := []*utxostore.UnminedTransaction{survivor}
	nodes := make([]subtreepkg.Node, 0, 6)
	inpoints := make([]*subtreepkg.TxInpoints, 0, 6)
	for i := range 6 {
		row := recoveryRow(string(rune('a' + i)))
		rows = append(rows, row)
		nodes = append(nodes, *row.Node)
		inpoints = append(inpoints, row.TxInpoints)
	}
	stp.queue.enqueueBatch(nodes, inpoints)
	require.Eventually(t, func() bool {
		ready := false
		_ = stp.runRecoveryOnDispatcher(t.Context(), func() error {
			ready = stp.queue.length() == 0 && len(stp.chainedSubtrees) == 1
			return nil
		})
		return ready
	}, time.Second, time.Millisecond, "normal dequeue must finish the original eight-leaf subtree")
	require.NoError(t, stp.RecoverUnmined(t.Context(), prevBlockHeader, nil,
		func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
			return rows, nil
		}))
	require.Zero(t, stp.queue.length(), "repair must recognize every assembled row")
	require.NoError(t, stp.runRecoveryOnDispatcher(t.Context(), func() error {
		for _, row := range rows {
			_, present := stp.currentTxMap.Get(row.Hash)
			require.True(t, present, "normal dequeue must keep an assembled-map entry for %s", row.Hash)
			require.GreaterOrEqual(t, stp.chainedSubtrees[0].NodeIndex(row.Hash), 0)
		}
		return nil
	}))
}

func TestUnminedRecoveryWhenInitialSubtreeSizeIsOne(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	stp.SetCurrentItemsPerFile(1)
	initial, err := subtreepkg.NewTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, initial.AddCoinbaseNode())
	stp.SetCurrentSubtree(initial)
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	row := recoveryRow("size-one-queued")
	stp.queue.enqueueBatch([]subtreepkg.Node{*row.Node}, []*subtreepkg.TxInpoints{row.TxInpoints})
	require.Eventually(t, func() bool {
		ready := false
		_ = stp.runRecoveryOnDispatcher(t.Context(), func() error {
			ready = stp.queue.length() == 0 && len(stp.chainedSubtrees) == 2
			return nil
		})
		return ready
	}, time.Second, time.Millisecond, "a full coinbase-only subtree must rotate before queue admission")
	require.NoError(t, stp.RecoverUnmined(t.Context(), prevBlockHeader, nil,
		func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
			return []*utxostore.UnminedTransaction{row}, nil
		}))
	require.Zero(t, stp.queue.length(), "assembled transaction must not be requeued by repair")
	require.NoError(t, stp.runRecoveryOnDispatcher(t.Context(), func() error {
		require.Equal(t, row.Hash, stp.chainedSubtrees[1].Nodes[0].Hash)
		return nil
	}))
}

func TestUnminedRecoveryQueuedIngressDuringDispatcherResetReload(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	// Reset drains queue entries stamped through its final anchor. A future
	// stamp keeps this producer arrival for the normal dequeue after reset.
	stp.queue.clock = fixedClock{t: time.Now().Add(time.Hour)}
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	direct, queued := recoveryRow("reset-direct"), recoveryRow("reset-queued")
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan ResetResponse, 1)
	go func() {
		done <- stp.Reset(prevBlockHeader, nil, nil, false, func() error {
			close(entered)
			<-release
			return stp.AddDirectly(direct.Node, direct.TxInpoints, true)
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reset did not enter dispatcher-owned reload")
	}
	stp.queue.enqueueBatch([]subtreepkg.Node{*queued.Node}, []*subtreepkg.TxInpoints{queued.TxInpoints})
	require.Equal(t, int64(1), stp.queue.length(), "producer may enqueue while dispatcher owns reload")
	close(release)
	select {
	case result := <-done:
		require.NoError(t, result.Err)
	case <-time.After(time.Second):
		t.Fatal("reset did not finish")
	}
	require.Eventually(t, func() bool {
		ready := false
		_ = stp.runRecoveryOnDispatcher(t.Context(), func() error {
			ready = stp.queue.length() == 0 && stp.currentSubtree.Load().NodeIndex(direct.Hash) >= 0 && stp.currentSubtree.Load().NodeIndex(queued.Hash) >= 0
			return nil
		})
		return ready
	}, time.Second, time.Millisecond, "queued ingress must drain after reset-owned direct reload")
}

func TestUnminedRecoveryPreservesOutstandingRemovalsAndDescendants(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.SetCurrentBlockHeader(prevBlockHeader)
	removed, child, good := recoveryRow("removed"), recoveryRow("removed-child"), recoveryRow("good")
	child.TxInpoints.ParentTxHashes = []chainhash.Hash{removed.Hash}
	require.NoError(t, stp.removeMap.Put(removed.Hash, 1))
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return []*utxostore.UnminedTransaction{removed, child, good}, nil
	}))
	require.Equal(t, int64(1), stp.queue.length())
	stp.clock = fixedClock{t: time.Now().Add(time.Second)}
	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, false))
	require.Equal(t, []chainhash.Hash{*subtreepkg.CoinbasePlaceholderHash, good.Hash}, collectSubtreeHashes(stp))
	require.True(t, stp.removeMap.Exists(removed.Hash))
}
