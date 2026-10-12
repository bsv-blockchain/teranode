package blockassembly

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// resetTestMock wires a mock subtree processor for the heartbeat reset tests.
// onReset runs inside every Reset call, where the real subtree processor's
// reset would run its reload, and returns that reset's response; it can also
// raise a new request, exactly as the reload would. waitResults, if given, are
// the results of the first WaitForPendingBlocks calls in order, the first
// being Start's own (every later call succeeds): a failure there ends
// BlockAssembler.reset before it reaches the subtree processor, so before the
// rotation.
func resetTestMock(onReset func(m *subtreeprocessor.MockSubtreeProcessor) subtreeprocessor.ResetResponse,
	waitResults ...error) (*subtreeprocessor.MockSubtreeProcessor, *atomic.Int32) {
	resets := &atomic.Int32{}
	m := &subtreeprocessor.MockSubtreeProcessor{}

	m.On("Start", mock.Anything).Return()

	for _, waitErr := range waitResults {
		m.On("WaitForPendingBlocks", mock.Anything).Return(waitErr).Once()
	}

	m.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	m.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func() subtreeprocessor.ResetResponse {
			resets.Add(1)

			if onReset != nil {
				return onReset(m)
			}

			return subtreeprocessor.ResetResponse{Rotated: true}
		})
	m.On("GetCurrentBlockHeader").Return(model.GenesisBlockHeader)
	m.On("InitCurrentBlockHeader", mock.Anything).Return()
	m.On("FlushDiskTxMapForLoad", mock.Anything, mock.Anything).Return(nil)

	return m, resets
}

func initDegradedGauge(t *testing.T) {
	t.Helper()

	initPrometheusMetrics()
	prometheusBlockAssemblyDiskTxMapDegraded.Set(0)
	t.Cleanup(func() { prometheusBlockAssemblyDiskTxMapDegraded.Set(0) })
}

func startWithMock(t *testing.T, m *subtreeprocessor.MockSubtreeProcessor, before ...func(items *baTestItems)) *baTestItems {
	t.Helper()

	initDegradedGauge(t)

	items := setupBlockAssemblyTest(t)
	items.blockAssembler.heartbeatInterval = 10 * time.Millisecond
	items.blockAssembler.storageResetMinInterval = time.Nanosecond // tests that pace resets set their own
	injectMockStp(t, items, m)

	for _, fn := range before {
		fn(items)
	}

	require.NoError(t, items.blockAssembler.Start(t.Context()))

	return items
}

func degradedGauge() float64 {
	return testutil.ToFloat64(prometheusBlockAssemblyDiskTxMapDegraded)
}

// A pending post-commit storage request makes BlockAssembler reset, with no
// other trigger involved; a reset that completes clean leaves the node healthy.
func TestBlockAssembler_HeartbeatActsOnDiskTxMapResetRequest(t *testing.T) {
	m, resets := resetTestMock(nil)
	m.ResetRequested.Store(true)

	startWithMock(t, m)

	require.Eventually(t, func() bool { return resets.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return resets.Load() > 1 }, 200*time.Millisecond, 10*time.Millisecond,
		"the request is take-once: one reset per request")
	require.Zero(t, degradedGauge())
}

// A failed drain after a block (bsv-blockchain/teranode#1881) lost the txs it
// had taken off the queue. That is not a storage fault, so the reset it asks
// for must run even on a node whose disk tx map is degraded, where
// storage-triggered resets are suppressed.
func TestBlockAssembler_HeartbeatResetsOnDrainFailureEvenWhenDegraded(t *testing.T) {
	m, resets := resetTestMock(nil)
	m.DrainResetRequested.Store(true)

	startWithMock(t, m, func(items *baTestItems) {
		items.blockAssembler.diskTxMapDegraded = true
	})

	require.Eventually(t, func() bool { return resets.Load() == 1 }, 2*time.Second, 5*time.Millisecond,
		"a drain failure must reset block assembly even while the disk tx map is degraded")
	require.Never(t, func() bool { return resets.Load() > 1 }, 200*time.Millisecond, 10*time.Millisecond,
		"the request is take-once: one reset per request")
}

// A persistent fault: the storage-triggered reset's own reload hits a storage
// error again. There must be exactly one such reset, after which the node is
// degraded and stops auto-resetting instead of looping full reloads.
func TestBlockAssembler_PersistentDiskTxMapFault_OneResetThenDegraded(t *testing.T) {
	m, resets := resetTestMock(func(m *subtreeprocessor.MockSubtreeProcessor) subtreeprocessor.ResetResponse {
		m.ResetRequested.Store(true) // the reload left a phantom again
		return subtreeprocessor.ResetResponse{Rotated: true, StorageFailed: true}
	})
	m.ResetRequested.Store(true)

	startWithMock(t, m)

	require.Eventually(t, func() bool { return degradedGauge() == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return resets.Load() > 1 }, 300*time.Millisecond, 10*time.Millisecond,
		"no further storage-triggered resets while degraded")
	require.False(t, m.ResetRequested.Load(), "requests raised while degraded are consumed, not left to pile up")
}

// A phantom left by a manual reset's reload is still escalated: the request it
// raises is picked up by the heartbeat and causes a storage-triggered reset.
func TestBlockAssembler_PhantomFromManualResetIsEscalated(t *testing.T) {
	var calls atomic.Int32

	m, resets := resetTestMock(func(m *subtreeprocessor.MockSubtreeProcessor) subtreeprocessor.ResetResponse {
		if calls.Add(1) > 1 {
			return subtreeprocessor.ResetResponse{Rotated: true}
		}

		m.ResetRequested.Store(true)

		return subtreeprocessor.ResetResponse{Rotated: true, StorageFailed: true}
	})

	items := startWithMock(t, m)
	items.blockAssembler.Reset(false)

	require.Eventually(t, func() bool { return resets.Load() == 2 }, 2*time.Second, 5*time.Millisecond,
		"manual reset, then the storage-triggered one it requested")
	require.Never(t, func() bool { return resets.Load() > 2 }, 200*time.Millisecond, 10*time.Millisecond)
	require.Zero(t, degradedGauge(), "the storage-triggered reset completed clean")
}

// A storage-triggered reset that fails before reaching the subtree processor
// (WaitForPendingBlocks here) says nothing about the disk: it must not be
// judged by the storage outcome of the reset before it, and since the
// rotation never ran, the phantom is still there, so it is retried.
func TestBlockAssembler_PreRotationFailureIsRetriedNotDegraded(t *testing.T) {
	var calls atomic.Int32

	m, resets := resetTestMock(func(m *subtreeprocessor.MockSubtreeProcessor) subtreeprocessor.ResetResponse {
		if calls.Add(1) == 1 {
			// The manual reset's reload leaves a phantom.
			m.ResetRequested.Store(true)
			return subtreeprocessor.ResetResponse{Rotated: true, StorageFailed: true}
		}

		return subtreeprocessor.ResetResponse{Rotated: true}
	}, nil, nil, errors.NewServiceError("block validation unavailable")) // Start, the manual reset, the storage-triggered reset

	items := startWithMock(t, m)
	items.blockAssembler.Reset(false)

	require.Eventually(t, func() bool { return resets.Load() == 2 }, 2*time.Second, 5*time.Millisecond,
		"manual reset, then the retry of the storage-triggered reset that failed before the rotation")
	require.Never(t, func() bool { return resets.Load() > 2 }, 200*time.Millisecond, 10*time.Millisecond)
	require.Zero(t, degradedGauge(), "a failure before the rotation is not a storage failure")
}

// A storage-triggered reset that fails after the rotation for a non-storage
// reason is not retried: the rotation discarded the phantom it was for.
func TestBlockAssembler_PostRotationFailureIsNotRetried(t *testing.T) {
	m, resets := resetTestMock(func(*subtreeprocessor.MockSubtreeProcessor) subtreeprocessor.ResetResponse {
		return subtreeprocessor.ResetResponse{Err: errors.NewServiceError("utxo store unavailable"), Rotated: true}
	})
	m.ResetRequested.Store(true)

	startWithMock(t, m)

	require.Eventually(t, func() bool { return resets.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return resets.Load() > 1 }, 200*time.Millisecond, 10*time.Millisecond)
	require.Zero(t, degradedGauge())
}

// A storage-triggered request queued behind another reset is drained, not run;
// the reset that ran stands in for it, so that reset is judged as
// storage-triggered.
func TestBlockAssembler_DrainedStorageTriggeredRequestCounts(t *testing.T) {
	m, resets := resetTestMock(func(*subtreeprocessor.MockSubtreeProcessor) subtreeprocessor.ResetResponse {
		return subtreeprocessor.ResetResponse{Err: errors.NewProcessingError("tx map rotation failed"), StorageFailed: true}
	})

	startWithMock(t, m, func(items *baTestItems) {
		items.blockAssembler.resetCh <- resetRequest{ErrCh: make(chan error, 1)}
		items.blockAssembler.resetCh <- resetRequest{ErrCh: make(chan error, 1), StorageTriggered: true}
	})

	require.Eventually(t, func() bool { return degradedGauge() == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), resets.Load(), "the queued storage-triggered request is drained, not run")
}

// The reorg paths that fall back to a reset go through the same outcome
// handling: a clean fallback reset clears degraded.
func TestBlockAssembler_ReorgFallbackResetClearsDegraded(t *testing.T) {
	t.Run("large reorg", func(t *testing.T) {
		initDegradedGauge(t)

		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)

		a1 := buildChain(genesis, 1, 610)[0]
		b1 := buildChain(genesis, 1, 710)[0]
		require.NoError(t, items.addBlock(t.Context(), a1))
		require.NoError(t, items.addBlock(t.Context(), b1))

		m, resets := resetTestMock(nil)
		injectMockStp(t, items, m)

		b := items.blockAssembler
		b.diskTxMapDegraded = true
		prometheusBlockAssemblyDiskTxMapDegraded.Set(1)
		b.setBestBlockHeader(a1, 1001) // arms the large-reorg guard

		err := b.handleReorg(t.Context(), b1, 1)
		require.True(t, errors.Is(err, errors.ErrBlockAssemblyReset), "got: %v", err)

		require.Equal(t, int32(1), resets.Load())
		require.False(t, b.diskTxMapDegraded)
		require.Zero(t, degradedGauge())
	})

	t.Run("failed reorg", func(t *testing.T) {
		initDegradedGauge(t)

		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)

		a1 := buildChain(genesis, 1, 620)[0]
		fork := buildChain(genesis, 2, 720)
		require.NoError(t, items.addBlock(t.Context(), a1))
		addChain(t, items, fork) // longer, so it becomes best

		m, resets := resetTestMock(nil)
		m.On("Reorg", mock.Anything, mock.Anything).Return(errors.NewProcessingError("reorg failed"))
		injectMockStp(t, items, m)

		b := items.blockAssembler
		b.diskTxMapDegraded = true
		prometheusBlockAssemblyDiskTxMapDegraded.Set(1)
		b.setBestBlockHeader(a1, 1)

		err := b.handleReorg(t.Context(), fork[1], 2)
		require.True(t, errors.Is(err, errors.ErrBlockAssemblyReset), "got: %v", err)

		require.Equal(t, int32(1), resets.Load())
		require.False(t, b.diskTxMapDegraded)
		require.Zero(t, degradedGauge())
	})
}

// onResetDone decides the degraded state, and whether to retry, from how a
// reset ended.
func TestBlockAssembler_OnResetDone(t *testing.T) {
	initDegradedGauge(t)

	storageErr := errors.NewStorageError("rotation failed")
	otherErr := errors.NewServiceError("blockchain unavailable")

	for _, tc := range []struct {
		name             string
		storageTriggered bool
		resp             subtreeprocessor.ResetResponse
		resetErr         error
		degradedBefore   bool
		degradedAfter    bool
		retryBefore      bool
		retry            bool
	}{
		{name: "storage-triggered reset fails on storage", storageTriggered: true, resp: subtreeprocessor.ResetResponse{Rotated: true, StorageFailed: true}, degradedAfter: true},
		{name: "storage-triggered rotation failure", storageTriggered: true, resp: subtreeprocessor.ResetResponse{StorageFailed: true}, resetErr: storageErr, degradedAfter: true},
		{name: "storage-triggered reset clean", storageTriggered: true, resp: subtreeprocessor.ResetResponse{Rotated: true}},
		{name: "clean storage-triggered reset clears degraded", storageTriggered: true, resp: subtreeprocessor.ResetResponse{Rotated: true}, degradedBefore: true},
		{name: "clean manual or reorg reset clears degraded", resp: subtreeprocessor.ResetResponse{Rotated: true}, degradedBefore: true},
		{name: "manual reset leaving a phantom does not degrade", resp: subtreeprocessor.ResetResponse{Rotated: true, StorageFailed: true}},
		{name: "storage-triggered failure before the rotation is retried", storageTriggered: true, resetErr: otherErr, retry: true},
		{name: "storage-triggered failure after the rotation is not retried", storageTriggered: true, resp: subtreeprocessor.ResetResponse{Rotated: true}, resetErr: otherErr},
		{name: "manual failure before the rotation is not retried", resetErr: otherErr},
		{name: "non-storage failure keeps degraded", resetErr: otherErr, degradedBefore: true, degradedAfter: true},
		{name: "a clean reset cancels a pending retry", resp: subtreeprocessor.ResetResponse{Rotated: true}, retryBefore: true},
		{name: "a failure after the rotation cancels a pending retry", resp: subtreeprocessor.ResetResponse{Rotated: true}, resetErr: otherErr, retryBefore: true},
		{name: "a failure before the rotation keeps a pending retry", resetErr: otherErr, retryBefore: true, retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := setupBlockAssemblyTest(t)
			b := items.blockAssembler
			b.diskTxMapDegraded = tc.degradedBefore
			b.diskTxMapResetPending = tc.retryBefore
			prometheusBlockAssemblyDiskTxMapDegraded.Set(map[bool]float64{false: 0, true: 1}[tc.degradedBefore])

			b.onResetDone(tc.storageTriggered, tc.resp, tc.resetErr)

			require.Equal(t, tc.degradedAfter, b.diskTxMapDegraded)
			require.Equal(t, map[bool]float64{false: 0, true: 1}[tc.degradedAfter], degradedGauge())
			require.Equal(t, tc.retry, b.diskTxMapResetPending)
		})
	}
}

// A fault that misses each storage-triggered reset's reload but hits the next
// operation makes every such reset end clean and raise a new request at once.
// Storage-triggered resets are paced: the next starts no sooner than
// storageResetMinInterval after the previous one finished, and a request
// arriving sooner is kept, not dropped.
func TestBlockAssembler_StorageTriggeredResetsArePaced(t *testing.T) {
	intermittent := func(m *subtreeprocessor.MockSubtreeProcessor) subtreeprocessor.ResetResponse {
		m.ResetRequested.Store(true) // the fault hits again right after the reset
		return subtreeprocessor.ResetResponse{Rotated: true}
	}

	t.Run("a request within the interval waits", func(t *testing.T) {
		m, resets := resetTestMock(intermittent)
		m.ResetRequested.Store(true)

		startWithMock(t, m, func(items *baTestItems) {
			items.blockAssembler.storageResetMinInterval = time.Hour
		})

		require.Eventually(t, func() bool { return resets.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
		require.Never(t, func() bool { return resets.Load() > 1 }, 300*time.Millisecond, 10*time.Millisecond,
			"no second storage-triggered reset within the interval")
		require.Zero(t, degradedGauge(), "a paced reset is not a degraded node")
	})

	t.Run("the waiting request runs once the interval has passed", func(t *testing.T) {
		m, resets := resetTestMock(intermittent)
		m.ResetRequested.Store(true)

		startWithMock(t, m, func(items *baTestItems) {
			items.blockAssembler.storageResetMinInterval = 300 * time.Millisecond
		})

		require.Eventually(t, func() bool { return resets.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
		require.Never(t, func() bool { return resets.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond)
		require.Eventually(t, func() bool { return resets.Load() >= 2 }, 3*time.Second, 5*time.Millisecond,
			"the request raised within the interval was kept and runs after it")
	})
}
