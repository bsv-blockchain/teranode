package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestDownloadWaste_PublishAddsOnlyTheIncrease pins the one property that makes
// the waste counters real Prometheus counters rather than copies of a gauge:
// each publish adds what changed since the last one. The atomics are the
// source of truth for the 30-second log line too, and received is bumped per
// read chunk through a pointer, so the counters are fed from those atomics at
// the report tick instead of at every site.
func TestDownloadWaste_PublishAddsOnlyTheIncrease(t *testing.T) {
	initPrometheusMetrics()

	var w downloadWaste

	receivedBase := testutil.ToFloat64(prometheusLegacyNetsyncDownloadReceivedBytes)
	localBase := testutil.ToFloat64(prometheusLegacyNetsyncDownloadLocalFaultDrained)

	w.received.Store(100)
	w.localFaultDrained.Store(3)
	w.publish()

	require.Equal(t, receivedBase+100, testutil.ToFloat64(prometheusLegacyNetsyncDownloadReceivedBytes))
	require.Equal(t, localBase+3, testutil.ToFloat64(prometheusLegacyNetsyncDownloadLocalFaultDrained))

	w.received.Store(150)
	w.localFaultDrained.Store(5)
	w.publish()

	require.Equal(t, receivedBase+150, testutil.ToFloat64(prometheusLegacyNetsyncDownloadReceivedBytes),
		"the second publish must add the 50 that arrived since the first, not the 150 total again")
	require.Equal(t, localBase+5, testutil.ToFloat64(prometheusLegacyNetsyncDownloadLocalFaultDrained))
}

// TestPublishDownloadMetrics_ReportsOwedCachedAndHeldAhead drives the report
// tick's publish over the real park harness: a parked block on a real file
// store, a real download ledger and the header cache the harness filled. The
// gauges are set to a sentinel first, so a publish that skipped one leaves the
// sentinel behind and fails here.
func TestPublishDownloadMetrics_ReportsOwedCachedAndHeldAhead(t *testing.T) {
	h := newParkWiringHarness(t, true)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "block 2 arrives before block 1 and must be parked")

	owedBefore := h.sm.blockDownloads.Len()

	for i := byte(1); i <= 3; i++ {
		require.True(t, h.sm.blockDownloads.Add(h.peer, chainhash.Hash{0xd0, i}))
	}

	localBefore := testutil.ToFloat64(prometheusLegacyNetsyncDownloadLocalFaultDrained)
	localNow := h.sm.waste.localFaultDrained.Add(2)

	for _, g := range []interface{ Set(float64) }{
		prometheusLegacyNetsyncBlocksOwed, prometheusLegacyNetsyncHeaderCacheHeights, prometheusLegacyNetsyncBytesAhead,
	} {
		g.Set(-1)
	}

	h.sm.publishDownloadMetrics()

	require.Equal(t, float64(owedBefore+3), testutil.ToFloat64(prometheusLegacyNetsyncBlocksOwed),
		"the three blocks just asked for are owed on top of what the harness already asked")

	require.Positive(t, h.sm.headerCache.Len(), "sanity: the harness fills the header cache")
	require.Equal(t, float64(h.sm.headerCache.Len()), testutil.ToFloat64(prometheusLegacyNetsyncHeaderCacheHeights))

	ahead := h.sm.bytesAhead(h.sm.blockSizeTracker.largestRecentSize())
	require.Positive(t, ahead, "sanity: the parked block carries its wire size")
	require.Equal(t, float64(ahead), testutil.ToFloat64(prometheusLegacyNetsyncBytesAhead))

	require.Equal(t, float64(parkBackstopBytes), testutil.ToFloat64(prometheusLegacyNetsyncParkBackstopBytes))

	require.Equal(t, localBefore+float64(localNow), testutil.ToFloat64(prometheusLegacyNetsyncDownloadLocalFaultDrained),
		"the same tick publishes the waste counters")
}

// TestApplyParkDisposition_CountsByName follows two parked blocks through the
// real drain and commit: the parent commits, the child's first commit fails on a
// store that is not answering and is kept, and the child's second attempt
// commits. Every one of those outcomes goes through applyParkDisposition, and
// the counter must say so under the row's name.
func TestApplyParkDisposition_CountsByName(t *testing.T) {
	h := newParkWiringHarness(t, true)

	committed := prometheusLegacyNetsyncParkDispositions.WithLabelValues(parkDispositionCommitted.name)
	retryLater := prometheusLegacyNetsyncParkDispositions.WithLabelValues(parkDispositionRetryLater.name)

	committedBefore := testutil.ToFloat64(committed)
	retryBefore := testutil.ToFloat64(retryLater)

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	h.validation.failOnce(child, errors.NewStorageError("the store is not answering"))

	require.NoError(t, h.deliver(t, 1))
	require.NoError(t, h.deliver(t, 0))

	h.requireCommitted(t, parent)
	require.Equal(t, 1, h.sm.blockPark.Len(), "the child must be kept after a store fault")
	require.Equal(t, committedBefore+1, testutil.ToFloat64(committed), "the parent's commit is one committed disposition")
	require.Equal(t, retryBefore+1, testutil.ToFloat64(retryLater), "the child's store fault is one retry-later disposition")

	h.sm.drainParkedDescendants(parent)

	require.Equal(t, 0, h.sm.blockPark.Len())
	require.Equal(t, committedBefore+2, testutil.ToFloat64(committed), "the child's second attempt commits")
	require.Equal(t, retryBefore+1, testutil.ToFloat64(retryLater))
}

// TestParkDispositionNames_AreUniqueAndSet pins the label set of
// park_dispositions_total: one non-empty, distinct name per table row, and every
// row in parkDispositionRows so each is pre-initialised at zero.
func TestParkDispositionNames_AreUniqueAndSet(t *testing.T) {
	seen := make(map[string]string, len(parkDispositionRows))

	for _, d := range parkDispositionRows {
		require.NotEmpty(t, d.name, "the row %q has no metric name", d.reason)

		other, dup := seen[d.name]
		require.False(t, dup, "the rows %q and %q share the metric name %q", other, d.reason, d.name)

		seen[d.name] = d.reason
	}

	for _, d := range []parkDisposition{
		parkDispositionCommitted, parkDispositionRetryLater, parkDispositionParentGone, parkDispositionParentNotMinedYet,
		parkDispositionBlobUnusable, parkDispositionRecordCorrupt, parkDispositionFilesGone, parkDispositionLocalUtxoFault,
		parkDispositionAbandoned, parkDispositionParentInvalid, parkDispositionBlockInvalid, parkDispositionPolicyDeclined,
		parkDispositionBlockRejected,
	} {
		_, listed := seen[d.name]
		require.True(t, listed, "the row %q is missing from parkDispositionRows", d.reason)
	}
}
