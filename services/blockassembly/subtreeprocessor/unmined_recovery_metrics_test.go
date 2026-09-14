package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func recoveryPendingGauge(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "teranode_subtreeprocessor_recovery_pending_seconds" {
			require.Len(t, family.Metric, 1)
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatal("recovery pending duration gauge is not registered")
	return 0
}

func TestUnminedRecoveryPendingMetricPreservesFirstFailureUntilPublish(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	t.Cleanup(func() { stp.Stop(context.Background()) })
	// Scrapes run outside the processor goroutine, including while pending is
	// entered or cleared. Exercise those reads concurrently with real recovery.
	scrapeCtx, cancelScrapes := context.WithCancel(t.Context())
	scrapeDone := make(chan error, 1)
	go func() {
		for scrapeCtx.Err() == nil {
			if _, err := prometheus.DefaultGatherer.Gather(); err != nil {
				scrapeDone <- err
				return
			}
		}
		scrapeDone <- nil
	}()
	t.Cleanup(func() {
		cancelScrapes()
		require.NoError(t, <-scrapeDone)
	})
	stp.SetCurrentBlockHeader(prevBlockHeader)
	row := recoveryRow("pending-metric")
	prepare := func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return []*utxostore.UnminedTransaction{row}, nil
	}
	// A one-leaf tree fails during reconstruction, after live state is cleared.
	stp.currentItemsPerFile.Store(1)
	firstFailure := time.Now().Add(-time.Minute)
	stp.clock = fixedClock{t: firstFailure}
	require.Error(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, prepare))
	require.True(t, stp.RecoveryPending())
	require.InDelta(t, time.Since(firstFailure).Seconds(), recoveryPendingGauge(t), 1)

	stp.clock = fixedClock{t: firstFailure.Add(30 * time.Second)}
	require.Error(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, prepare))
	require.InDelta(t, time.Since(firstFailure).Seconds(), recoveryPendingGauge(t), 1,
		"a failed retry must not restart the pending timer")

	stp.currentItemsPerFile.Store(32)
	require.NoError(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, prepare))
	require.False(t, stp.RecoveryPending())
	require.Zero(t, recoveryPendingGauge(t))

	// A later outage starts a fresh timer, then shutdown removes its sample.
	stp.currentItemsPerFile.Store(1)
	require.Error(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, prepare))
	require.InDelta(t, time.Since(firstFailure.Add(30*time.Second)).Seconds(), recoveryPendingGauge(t), 1)
	stp.Stop(context.Background())
	require.Zero(t, recoveryPendingGauge(t))
	require.True(t, stp.RecoveryPending(), "metric cleanup must not change the safety gate")
}

func TestUnminedRecoveryPendingMetricStopPreservesOtherProcessorSample(t *testing.T) {
	first, second := newTestProcessorNoStart(t), newTestProcessorNoStart(t)
	t.Cleanup(func() { first.Stop(context.Background()); second.Stop(context.Background()) })
	row := recoveryRow("pending-metric-replacement")
	prepare := func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error) {
		return []*utxostore.UnminedTransaction{row}, nil
	}
	secondFailure := time.Now().Add(-30 * time.Second)
	for i, stp := range []*SubtreeProcessor{first, second} {
		stp.SetCurrentBlockHeader(prevBlockHeader)
		stp.currentItemsPerFile.Store(1)
		stp.clock = fixedClock{t: secondFailure.Add(-time.Duration(1-i) * time.Minute)}
		require.Error(t, stp.recoverUnmined(t.Context(), prevBlockHeader, nil, prepare))
	}
	first.Stop(context.Background())
	require.InDelta(t, time.Since(secondFailure).Seconds(), recoveryPendingGauge(t), 1)
	second.Stop(context.Background())
	require.Zero(t, recoveryPendingGauge(t))
}
