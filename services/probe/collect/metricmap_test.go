package collect

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/services/propagation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/metered"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func join(sets ...MetricSet) MetricSet {
	out := MetricSet{}

	for _, s := range sets {
		for name, family := range s {
			out[name] = family
		}
	}

	return out
}

func labelledCounterSet(name string, values ...float64) MetricSet {
	family := &dto.MetricFamily{Name: proto.String(name), Type: dto.MetricType_COUNTER.Enum()}

	for i, v := range values {
		family.Metric = append(family.Metric, &dto.Metric{
			Label:   []*dto.LabelPair{{Name: proto.String("source"), Value: proto.String(string(rune('a' + i)))}},
			Counter: &dto.Counter{Value: proto.Float64(v)},
		})
	}

	return MetricSet{name: family}
}

func testSettings() *settings.Settings {
	cfg := &settings.Settings{}
	cfg.SubtreeValidation.TxMetaCacheMaxMB = 2048
	cfg.BlockAssembly.MaxQueueItems = 100_000
	cfg.BlockAssembly.SendBatchSize = 100
	cfg.UtxoStore.SpendBatcherSize = 100

	return cfg
}

func applyTwo(t *testing.T, first, second MetricSet, cfg *settings.Settings) snapshot.Snapshot {
	t.Helper()

	return applyGap(t, first, second, 2*time.Second, cfg)
}

func applyOverWindow(t *testing.T, first, second MetricSet, cfg *settings.Settings) snapshot.Snapshot {
	t.Helper()

	return applyGap(t, first, second, cacheRateWindow, cfg)
}

func applyGap(t *testing.T, first, second MetricSet, gap time.Duration, cfg *settings.Settings) snapshot.Snapshot {
	t.Helper()

	r := NewRates()
	start := time.Unix(1000, 0)
	r.Observe(start, first)
	r.Observe(start.Add(gap), second)

	s := snapshot.NewSnapshot("node")
	Apply(&s, second, r, cfg)

	return s
}

func requireOK(t *testing.T, want float64, got snapshot.Reading) {
	t.Helper()

	require.Equal(t, snapshot.StatusOK, got.Status)
	require.InDelta(t, want, got.Value, 1e-9)
}

func requireNoData(t *testing.T, got snapshot.Reading) {
	t.Helper()

	require.Equal(t, snapshot.StatusNoData, got.Status)
	require.Zero(t, got.Value)
	require.NotEmpty(t, got.Note)
}

func cacheSet(hits, misses float64) MetricSet {
	return join(gaugeSet(metricCacheHits, hits), gaugeSet(metricCacheMisses, misses))
}

func TestCacheHitRate(t *testing.T) {
	s := applyOverWindow(t, cacheSet(1000, 500), cacheSet(1900, 600), testSettings())

	requireOK(t, 0.9, s.Cache.HitRate)
	requireOK(t, 10, s.Cache.MissesPerSec)
	requireOK(t, 100, s.Flow(snapshot.FlowLookups).Rate)
}

func TestCacheHitRateNoLookups(t *testing.T) {
	s := applyOverWindow(t, cacheSet(1000, 500), cacheSet(1000, 500), testSettings())

	require.Equal(t, snapshot.StatusNoData, s.Cache.HitRate.Status)
	require.Equal(t, "no data", s.Cache.HitRate.Note)
	require.Zero(t, s.Cache.HitRate.Value)
	requireOK(t, 0, s.Cache.MissesPerSec)
	requireOK(t, 0, s.Flow(snapshot.FlowLookups).Rate)
}

func TestCacheHitRateFirstSampleAndResetGiveNoData(t *testing.T) {
	r := NewRates()
	r.Observe(time.Unix(1000, 0), cacheSet(1000, 500))

	s := snapshot.NewSnapshot("node")
	Apply(&s, cacheSet(1000, 500), r, testSettings())

	require.Equal(t, snapshot.StatusNoData, s.Cache.HitRate.Status)
	require.Equal(t, "no data", s.Cache.HitRate.Note)

	s = applyOverWindow(t, cacheSet(1000, 500), cacheSet(10, 1), testSettings())

	require.Equal(t, snapshot.StatusNoData, s.Cache.HitRate.Status)
	require.Equal(t, "no data", s.Cache.HitRate.Note)
	requireNoData(t, s.Cache.MissesPerSec)
	requireNoData(t, s.Flow(snapshot.FlowLookups).Rate)
}

func publishedCacheSet(at time.Duration) MetricSet {
	published := float64(int(at.Seconds()) / 5 * 5)

	return join(
		gaugeSet(metricCacheHits, 900*published),
		gaugeSet(metricCacheMisses, 100*published),
		gaugeSet(metricCacheEvictions, 50*published),
		gaugeSet(metricCacheInsertions, 20*published),
	)
}

func TestCacheRatesStableAcrossPublishCadence(t *testing.T) {
	r := NewRates()
	start := time.Unix(1000, 0)

	for at := time.Duration(0); at <= 60*time.Second; at += 2 * time.Second {
		set := publishedCacheSet(at)
		r.Observe(start.Add(at), set)

		s := snapshot.NewSnapshot("node")
		Apply(&s, set, r, testSettings())

		if at < cacheRateWindow {
			require.Equal(t, snapshot.StatusNoData, s.Cache.HitRate.Status, at)
			requireNoData(t, s.Cache.MissesPerSec)
			requireNoData(t, s.Cache.EvictionsPerSec)
			requireNoData(t, s.Cache.InsertionsPerSec)
			requireNoData(t, s.Flow(snapshot.FlowLookups).Rate)

			continue
		}

		requireOK(t, 0.9, s.Cache.HitRate)
		requireOK(t, 100, s.Cache.MissesPerSec)
		requireOK(t, 50, s.Cache.EvictionsPerSec)
		requireOK(t, 20, s.Cache.InsertionsPerSec)
		requireOK(t, 1000, s.Flow(snapshot.FlowLookups).Rate)
	}
}

func TestCacheFillUsesSettingCapacity(t *testing.T) {
	set := join(
		gaugeSet(metricCacheValidEntries, 1_500_000),
		gaugeSet(metricCacheCurrentGen, 600_000),
		gaugeSet(metricCachePreviousGen, 900_000),
		gaugeSet(metricCacheEvictions, 100),
	)
	next := join(
		gaugeSet(metricCacheValidEntries, 1_500_000),
		gaugeSet(metricCacheCurrentGen, 600_000),
		gaugeSet(metricCachePreviousGen, 900_000),
		gaugeSet(metricCacheEvictions, 2100),
	)

	s := applyOverWindow(t, set, next, testSettings())

	requireOK(t, 2, s.Cache.CapacityGB)
	requireOK(t, 1.5, s.Cache.EntriesMillions)
	requireOK(t, 0.4, s.Cache.CurrentGenFrac)
	requireOK(t, 0.6, s.Cache.PreviousGenFrac)
	requireOK(t, 200, s.Cache.EvictionsPerSec)

	// An older node exports no bytes gauges: fill and size stay nodata.
	requireNoData(t, s.Cache.Fill)
	requireNoData(t, s.Cache.SizeGB)

	cfg := testSettings()
	cfg.SubtreeValidation.TxMetaCacheMaxMB = 0
	s = applyOverWindow(t, set, next, cfg)

	requireNoData(t, s.Cache.CapacityGB)
}

func cacheBytesSet(bytes, capacity float64) MetricSet {
	return join(
		gaugeSet(metricCacheValidEntries, 2_500_000),
		gaugeSet(metricCacheBytes, bytes),
		gaugeSet(metricCacheCapacityBytes, capacity),
	)
}

func TestCacheFillFromBytesGauge(t *testing.T) {
	set := cacheBytesSet(7.4e9, 8e9)

	s := applyTwo(t, set, set, testSettings())

	requireOK(t, 0.925, s.Cache.Fill)
	requireOK(t, 7.4, s.Cache.SizeGB)
	requireOK(t, 8, s.Cache.CapacityGB)
	requireOK(t, 2.5, s.Cache.EntriesMillions)
}

func TestCacheFillNoDataWhenBytesGaugeAbsent(t *testing.T) {
	set := gaugeSet(metricCacheValidEntries, 2_500_000)

	s := applyTwo(t, set, set, testSettings())

	requireNoData(t, s.Cache.Fill)
	requireNoData(t, s.Cache.SizeGB)
	requireOK(t, 2, s.Cache.CapacityGB)
	requireOK(t, 2.5, s.Cache.EntriesMillions)

	set = join(set, gaugeSet(metricCacheBytes, 7.4e9))
	s = applyTwo(t, set, set, testSettings())

	requireNoData(t, s.Cache.Fill)
	requireOK(t, 7.4, s.Cache.SizeGB)
	requireOK(t, 2, s.Cache.CapacityGB)

	set = cacheBytesSet(7.4e9, 0)
	s = applyTwo(t, set, set, testSettings())

	requireNoData(t, s.Cache.Fill)
	requireOK(t, 7.4, s.Cache.SizeGB)
}

func TestCacheFillUsesCapacityGaugeNotSetting(t *testing.T) {
	cfg := testSettings()
	cfg.SubtreeValidation.TxMetaCacheMaxMB = 1

	set := cacheBytesSet(4e9, 8e9)

	s := applyTwo(t, set, set, cfg)

	requireOK(t, 0.5, s.Cache.Fill)
	requireOK(t, 4, s.Cache.SizeGB)
	requireOK(t, 8, s.Cache.CapacityGB)
}

func TestCacheGenerationSplitEmptyCacheIsNoData(t *testing.T) {
	set := join(
		gaugeSet(metricCacheValidEntries, 0),
		gaugeSet(metricCacheCurrentGen, 0),
		gaugeSet(metricCachePreviousGen, 0),
	)

	s := applyTwo(t, set, set, testSettings())

	requireOK(t, 0, s.Cache.EntriesMillions)
	requireNoData(t, s.Cache.CurrentGenFrac)
	requireNoData(t, s.Cache.PreviousGenFrac)
}

func TestIngressReadings(t *testing.T) {
	first := join(
		histSet(metricPropagationTx, 1000, 1, []float64{1}, []uint64{1000}),
		counterSet(metricPropagationInvalid, 10),
		histSet(metricSubtreeIntake, 0, 0, []float64{1}, []uint64{0}),
		gaugeSet(metricPeers, 38),
		gaugeSet(metricSubtreeWaiting, 7),
	)
	second := join(
		histSet(metricPropagationTx, 9200, 9, []float64{1}, []uint64{9200}),
		counterSet(metricPropagationInvalid, 34),
		histSet(metricSubtreeIntake, 420, 3, []float64{1}, []uint64{420}),
		gaugeSet(metricPeers, 38),
		gaugeSet(metricSubtreeWaiting, 5),
	)

	s := applyTwo(t, first, second, testSettings())

	requireOK(t, 4100, s.Ingress.PropagationTxPerSec)
	requireOK(t, 210, s.Ingress.SubtreeIntakePerSec)
	requireOK(t, 38, s.Ingress.Peers)
	requireOK(t, 12, s.Ingress.RejectedPerSec)
	requireOK(t, 5, s.Ingress.SubtreeQueue)
	requireNoData(t, s.Ingress.SubtreeQueueOldestSec)
	requireOK(t, 4100, s.Flow(snapshot.FlowTxInput).Rate)
}

func TestValidatorReadings(t *testing.T) {
	uppers := []float64{0.1, 0.5, 1}
	first := join(
		histSet(metricValidatorTx, 30, 5, uppers, []uint64{10, 20, 30}),
		counterSet(metricValidatorInvalid, 3),
		histSet(metricSpendBatchSize, 10, 500, []float64{1000}, []uint64{10}),
	)
	second := join(
		histSet(metricValidatorTx, 130, 50, uppers, []uint64{60, 110, 130}),
		counterSet(metricValidatorInvalid, 3),
		histSet(metricSpendBatchSize, 20, 1280, []float64{1000}, []uint64{20}),
	)

	s := applyTwo(t, first, second, testSettings())

	requireOK(t, 50, s.Validator.ValidatedPerSec)
	require.Equal(t, "includes block-validation checks and rejected transactions", s.Validator.ValidatedPerSec.Note)
	require.Equal(t, s.Validator.ValidatedPerSec.Note, s.Flow(snapshot.FlowValidated).Rate.Note)
	requireOK(t, 0.95, s.Validator.LatencyP99Sec)
	requireOK(t, 0, s.Validator.ErrorsPerSec)
	requireOK(t, 0.78, s.Validator.SpendBatchFill)
	requireNoData(t, s.Validator.QueueFill)
	requireOK(t, 50, s.Flow(snapshot.FlowValidated).Rate)

	cfg := testSettings()
	cfg.UtxoStore.SpendBatcherSize = 0
	s = applyTwo(t, first, second, cfg)

	requireNoData(t, s.Validator.SpendBatchFill)
}

func assemblySet(state, queued float64) MetricSet {
	return join(
		gaugeSet(metricAssemblyState, state),
		gaugeSet(metricAssemblyTipLag, 3),
		gaugeSet(metricAssemblyHeadAge, 42),
		gaugeSet(metricAssemblyQueued, queued),
		gaugeSet(metricAssemblyTxCount, 1_250_000),
	)
}

func TestAssemblyReadings(t *testing.T) {
	s := applyTwo(t, assemblySet(5, 17_000), assemblySet(5, 17_000), testSettings())

	requireOK(t, 1, s.Assembly.Paused)
	require.Equal(t, "reorging", s.Assembly.Paused.Note)
	requireOK(t, 3, s.Assembly.TipLagBlocks)
	requireOK(t, 42, s.Assembly.QueueHeadAgeSec)
	requireOK(t, 1_250_000, s.Assembly.Transactions)
	requireOK(t, 0.17, s.Assembly.IngestQueueFill)
	requireNoData(t, s.Assembly.SubtreeFill)

	s = applyTwo(t, assemblySet(1, 0), assemblySet(1, 0), testSettings())

	requireOK(t, 0, s.Assembly.Paused)
	require.Equal(t, "running", s.Assembly.Paused.Note)
	requireOK(t, 0, s.Assembly.IngestQueueFill)
}

func TestAssemblyIngestQueueCapacity(t *testing.T) {
	cfg := testSettings()
	cfg.BlockAssembly.MaxQueueItems = 0
	s := applyTwo(t, assemblySet(1, 500), assemblySet(1, 500), cfg)

	require.Equal(t, snapshot.StatusNoData, s.Assembly.IngestQueueFill.Status)
	require.Equal(t, "unbounded", s.Assembly.IngestQueueFill.Note)

	cfg.BlockAssembly.MaxQueueItems = 10
	cfg.BlockAssembly.SendBatchSize = 100
	s = applyTwo(t, assemblySet(1, 3200), assemblySet(1, 3200), cfg)

	requireOK(t, 0.5, s.Assembly.IngestQueueFill)
}

func TestChainReadings(t *testing.T) {
	set := join(
		gaugeSet(metricForkLongestDepth, 3),
		labelledCounterSet(metricSubscriberPendingFull, 1, 1),
		labelledCounterSet(metricSubscriberSendErrors, 2),
	)

	r := NewRates()
	r.Observe(time.Unix(1000, 0), set)

	s := snapshot.NewSnapshot("node")
	s.Chain.PersisterBehind = snapshot.Reading{Value: 4, Status: snapshot.StatusOK}
	Apply(&s, set, r, testSettings())

	requireOK(t, 3, s.Chain.ForkDepth)
	requireOK(t, 4, s.Chain.SubscribersDropped)
	requireOK(t, 4, s.Chain.PersisterBehind)
}

func aerospikeCounters(base map[string]float64, perSec map[string]float64, seconds float64) MetricSet {
	sets := make([]MetricSet, 0, len(base))
	for name, v := range base {
		sets = append(sets, counterSet(name, v+perSec[name]*seconds))
	}

	return join(sets...)
}

func TestAerospikeReadsWritesAndStoreFlows(t *testing.T) {
	base := map[string]float64{
		"teranode_aerospike_txmeta_get":               100,
		"teranode_aerospike_txmeta_get_multi_n":       200,
		"teranode_aerospike_utxo_get":                 300,
		metricAerospikeCreate:                         10,
		metricAerospikeSpend:                          20,
		"teranode_aerospike_txmeta_set_mined_batch_n": 30,
		"teranode_aerospike_utxo_reset":               40,
		"teranode_aerospike_utxo_delete":              50,
	}
	perSec := map[string]float64{
		"teranode_aerospike_txmeta_get":               10,
		"teranode_aerospike_txmeta_get_multi_n":       20,
		"teranode_aerospike_utxo_get":                 30,
		metricAerospikeCreate:                         1,
		metricAerospikeSpend:                          2,
		"teranode_aerospike_txmeta_set_mined_batch_n": 3,
		"teranode_aerospike_utxo_reset":               4,
		"teranode_aerospike_utxo_delete":              5,
	}

	require.ElementsMatch(t, append(append([]string{}, aerospikeReadCounters...), aerospikeWriteCounters...), keysOf(base))

	first := join(aerospikeCounters(base, perSec, 0), histSet(metricBlockchainAddBlock, 4, 1, []float64{1}, []uint64{4}))
	second := join(aerospikeCounters(base, perSec, 2), histSet(metricBlockchainAddBlock, 32, 2, []float64{1}, []uint64{32}))

	s := snapshot.NewSnapshot("node")
	s.Stores.Aerospike.Configured = true
	s.Stores.Aerospike.SizeGB = snapshot.Reading{Value: 1840, Status: snapshot.StatusOK}
	s.Stores.Aerospike.DiskFill = snapshot.Reading{Value: 0.71, Status: snapshot.StatusOK}

	r := NewRates()
	start := time.Unix(1000, 0)
	r.Observe(start, first)
	r.Observe(start.Add(2*time.Second), second)
	Apply(&s, second, r, testSettings())

	requireOK(t, 60, s.Stores.Aerospike.ReadsPerSec)
	requireOK(t, 15, s.Stores.Aerospike.WritesPerSec)
	requireNoData(t, s.Stores.Aerospike.ReadP99Sec)
	require.True(t, s.Stores.Aerospike.Configured)
	requireOK(t, 1840, s.Stores.Aerospike.SizeGB)
	requireOK(t, 0.71, s.Stores.Aerospike.DiskFill)
	requireOK(t, 3, s.Flow(snapshot.FlowUTXOOps).Rate)
	requireOK(t, 14, s.Flow(snapshot.FlowBlockWrites).Rate)
}

func TestAerospikeReadsWithoutWritesLeaveWritesNoData(t *testing.T) {
	first := join(counterSet("teranode_aerospike_utxo_get", 10))
	second := join(counterSet("teranode_aerospike_utxo_get", 30))

	s := snapshot.NewSnapshot("node")

	r := NewRates()
	start := time.Unix(1000, 0)
	r.Observe(start, first)
	r.Observe(start.Add(2*time.Second), second)
	Apply(&s, second, r, testSettings())

	requireOK(t, 10, s.Stores.Aerospike.ReadsPerSec)
	requireNoData(t, s.Stores.Aerospike.WritesPerSec)
}

func keysOf(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

func TestMissingMetricsGiveNoData(t *testing.T) {
	r := NewRates()
	r.Observe(time.Unix(1000, 0), MetricSet{})
	r.Observe(time.Unix(1002, 0), MetricSet{})

	s := snapshot.NewSnapshot("node")
	Apply(&s, MetricSet{}, r, testSettings())

	for _, key := range []string{
		"ingress.propagationTxPerSec", "ingress.subtreeIntakePerSec", "ingress.peers", "ingress.rejectedPerSec",
		"ingress.subtreeQueue", "ingress.subtreeQueueOldestSec",
		"validator.validatedPerSec", "validator.latencyP99Sec", "validator.queueFill", "validator.spendBatchFill",
		"validator.errorsPerSec",
		"cache.hitRate", "cache.missesPerSec", "cache.evictionsPerSec", "cache.fill", "cache.currentGenFrac",
		"cache.previousGenFrac", "cache.entriesMillions", "cache.sizeGB",
		"assembly.paused", "assembly.tipLagBlocks", "assembly.queueHeadAgeSec", "assembly.transactions", "assembly.ingestQueueFill",
		"assembly.subtreeFill",
		"chain.forkDepth", "chain.subscribersDropped",
		"stores.aerospike.readP99Sec", "stores.aerospike.readsPerSec", "stores.aerospike.writesPerSec",
	} {
		reading, found := s.Lookup(key)
		require.True(t, found, key)
		require.Equal(t, snapshot.StatusNoData, reading.Status, key)
	}

	requireOK(t, 2, s.Cache.CapacityGB)

	for _, id := range []string{
		snapshot.FlowTxInput, snapshot.FlowLookups, snapshot.FlowValidated, snapshot.FlowUTXOOps, snapshot.FlowBlockWrites,
	} {
		require.Equal(t, snapshot.StatusNoData, s.Flow(id).Rate.Status, id)
	}
}

func TestApplyKeepsNormal(t *testing.T) {
	normal := 0.96
	s := snapshot.NewSnapshot("node")
	s.Cache.HitRate.Normal = &normal

	r := NewRates()
	r.Observe(time.Unix(1000, 0), cacheSet(1000, 500))
	r.Observe(time.Unix(1010, 0), cacheSet(1900, 600))
	Apply(&s, cacheSet(1900, 600), r, testSettings())

	require.NotNil(t, s.Cache.HitRate.Normal)
	require.InDelta(t, 0.96, *s.Cache.HitRate.Normal, 1e-9)
}

type sequenceGatherer struct {
	sets []MetricSet
	err  error
	n    int
}

func (g *sequenceGatherer) Gather() ([]*dto.MetricFamily, error) {
	set := g.sets[g.n]
	g.n++

	families := make([]*dto.MetricFamily, 0, len(set))
	for _, family := range set {
		families = append(families, family)
	}

	return families, g.err
}

func steppingClock(step time.Duration) func() time.Time {
	now := time.Unix(1000, 0)

	return func() time.Time {
		now = now.Add(step)
		return now
	}
}

func TestMetricsCollectorReturnsPatch(t *testing.T) {
	g := &sequenceGatherer{sets: []MetricSet{cacheSet(1000, 500), cacheSet(1900, 600)}}
	c := NewMetricsCollector(newLocalSource(g), NewRates(), testSettings())
	c.now = steppingClock(cacheRateWindow)

	require.Equal(t, "metrics", c.Name())

	_, err := c.Collect(context.Background())
	require.NoError(t, err)

	patch, err := c.Collect(context.Background())
	require.NoError(t, err)

	s := snapshot.NewSnapshot("node")
	patch(&s)

	requireOK(t, 0.9, s.Cache.HitRate)
}

func TestMetricsCollectorPatchKeepsItsInterval(t *testing.T) {
	g := &sequenceGatherer{sets: []MetricSet{cacheSet(1000, 500), cacheSet(1900, 600), cacheSet(1900, 600)}}
	c := NewMetricsCollector(newLocalSource(g), NewRates(), testSettings())
	c.now = steppingClock(cacheRateWindow)

	_, err := c.Collect(context.Background())
	require.NoError(t, err)

	patch, err := c.Collect(context.Background())
	require.NoError(t, err)

	_, err = c.Collect(context.Background())
	require.NoError(t, err)

	s := snapshot.NewSnapshot("node")
	patch(&s)

	requireOK(t, 0.9, s.Cache.HitRate)
}

func TestMetricsCollectorTotalGatherFailureReturnsNoPatch(t *testing.T) {
	gatherErr := errors.NewServiceError("all targets down")
	g := &sequenceGatherer{sets: []MetricSet{{}}, err: gatherErr}
	c := NewMetricsCollector(newLocalSource(g), NewRates(), testSettings())

	patch, err := c.Collect(context.Background())
	require.ErrorIs(t, err, gatherErr)
	require.Nil(t, patch)
}

func counterTarget(t *testing.T, values []float64, failOn int) *httptest.Server {
	t.Helper()

	calls := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := calls
		calls++

		if call == failOn {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		_, _ = fmt.Fprintf(w, "# TYPE %s counter\n%s %g\n", metricPropagationInvalid, metricPropagationInvalid, values[call])
	}))
	t.Cleanup(srv.Close)

	return srv
}

func TestPartialGatherDoesNotObserveAndNoSpikeAfterRecovery(t *testing.T) {
	a := counterTarget(t, []float64{100, 300, 500}, -1)
	b := counterTarget(t, []float64{1000, 0, 1400}, 1)

	c := NewMetricsCollector(NewHTTPSource([]string{a.URL, b.URL}, 5*time.Second), NewRates(), testSettings())
	clock := time.Unix(1000, 0)
	c.now = func() time.Time { return clock }

	_, err := c.Collect(context.Background())
	require.NoError(t, err)

	clock = clock.Add(2 * time.Second)

	patch, err := c.Collect(context.Background())
	require.Error(t, err)
	require.Nil(t, patch)

	clock = clock.Add(2 * time.Second)

	patch, err = c.Collect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, patch)

	s := snapshot.NewSnapshot("node")
	patch(&s)

	// A rose 400 and B rose 400 between the first and third gathers, 4 s apart.
	requireOK(t, 200, s.Ingress.RejectedPerSec)
}

func TestMetricsCollectorCancelledContext(t *testing.T) {
	g := &sequenceGatherer{sets: []MetricSet{gaugeSet(metricPeers, 12)}}
	c := NewMetricsCollector(newLocalSource(g), NewRates(), testSettings())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	patch, err := c.Collect(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, patch)
}

func TestMetricNamesExist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	validator.NewServer(logger, tSettings, nil, nil, nil, nil, nil, nil, nil)
	propagation.New(logger, tSettings, nil, nil, nil, nil, nil)
	blockassembly.New(logger, tSettings, nil, nil, nil, nil)
	blockvalidation.New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
	subtreevalidation.InitPrometheusMetrics()
	aerospike.InitPrometheusMetrics()

	p2pSettings := settings.NewSettings()
	p2pSettings.P2P.Port = 0

	_, err := p2p.NewServer(ctx, logger, p2pSettings, nil, nil, nil, nil, nil, nil, nil, nil)
	require.Error(t, err)

	storeURL, err := url.Parse("sqlitememory:///probe_metric_names")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(logger, storeURL, tSettings)
	require.NoError(t, err)

	_, err = blockchain.New(ctx, logger, tSettings, store, nil)
	require.NoError(t, err)

	_, err = txmetacache.NewTxMetaCache(ctx, tSettings, logger, nil, txmetacache.Unallocated)
	require.NoError(t, err)

	metered.New("probe_metric_names", blobmemory.New())

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	gathered := make(map[string]bool, len(families))
	for _, family := range families {
		gathered[family.GetName()] = true
	}

	require.NotEmpty(t, MetricNames)

	for _, name := range MetricNames {
		require.True(t, gathered[name] || nameTaken(name), "metric %s is not registered", name)
	}
}

func nameTaken(name string) bool {
	probe := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: "probe"})

	if err := prometheus.DefaultRegisterer.Register(probe); err != nil {
		return true
	}

	prometheus.DefaultRegisterer.Unregister(probe)

	return false
}

func lanesSet(skipped, validated, missing, found, blockValidated, candidates, solutions float64) MetricSet {
	return join(
		counterSet(metricSubtreeSkipped, skipped),
		histSet(metricSubtreeValidated, uint64(validated), 1, []float64{1}, []uint64{uint64(validated)}),
		histSet(metricBlessMissing, uint64(missing), 1, []float64{1}, []uint64{uint64(missing)}),
		histSet(metricBlockFound, uint64(found), 1, []float64{1}, []uint64{uint64(found)}),
		histSet(metricBlockValidated, uint64(blockValidated), 1, []float64{1}, []uint64{uint64(blockValidated)}),
		counterSet(metricMiningCandidates, candidates),
		histSet(metricMiningSolutions, uint64(solutions), 1, []float64{1}, []uint64{uint64(solutions)}),
	)
}

func TestLanesReadings(t *testing.T) {
	s := applyTwo(t, lanesSet(10, 100, 900, 5, 4, 20, 2), lanesSet(30, 140, 1500, 6, 5, 24, 3), testSettings())

	requireOK(t, 10, s.Lanes.Subtrees.AlreadyKnownPerSec)
	requireOK(t, 20, s.Lanes.Subtrees.CompletedPerSec)
	requireOK(t, 300, s.Lanes.Subtrees.MissingTxsPerSec)
	requireOK(t, 1500.0/140.0, s.Lanes.Subtrees.TxsFetchedAvg)
	requireOK(t, 6, s.Lanes.Blocks.AnnouncedTotal)
	requireOK(t, 5, s.Lanes.Blocks.ValidatedTotal)
	requireOK(t, 2, s.Lanes.Mining.CandidatesPerSec)
	requireOK(t, 3, s.Lanes.Mining.SolutionsTotal)
}

func labelled(set MetricSet, store string) MetricSet {
	out := MetricSet{}

	for name, family := range set {
		copied := &dto.MetricFamily{Name: family.Name, Type: family.Type}

		for _, metric := range family.Metric {
			clone := proto.Clone(metric).(*dto.Metric)
			clone.Label = append(clone.Label, &dto.LabelPair{Name: proto.String("store"), Value: proto.String(store)})
			copied.Metric = append(copied.Metric, clone)
		}

		out[name] = copied
	}

	return out
}

func mergeStores(sets ...MetricSet) MetricSet {
	out := MetricSet{}

	for _, set := range sets {
		for name, family := range set {
			if existing, found := out[name]; found {
				existing.Metric = append(existing.Metric, family.Metric...)
				continue
			}

			out[name] = family
		}
	}

	return out
}

func blobSet(store string, reads, writes, errs float64, readCount, writeCount uint64, readCums, writeCums []uint64) MetricSet {
	uppers := []float64{0.01, 0.1}

	return labelled(join(
		counterSet(metricBlobReads, reads),
		counterSet(metricBlobWrites, writes),
		counterSet(metricBlobErrors, errs),
		histSet(metricBlobReadDuration, readCount, 1, uppers, readCums),
		histSet(metricBlobWriteDuration, writeCount, 1, uppers, writeCums),
	), store)
}

func TestBlobStoreReadingsAreReportedPerStore(t *testing.T) {
	first := mergeStores(
		blobSet("subtree", 100, 50, 0, 0, 0, []uint64{0, 0}, []uint64{0, 0}),
		blobSet("block", 10, 5, 0, 0, 0, []uint64{0, 0}, []uint64{0, 0}),
	)
	second := mergeStores(
		blobSet("subtree", 300, 70, 4, 100, 20, []uint64{90, 100}, []uint64{20, 20}),
		blobSet("block", 10, 5, 0, 0, 0, []uint64{0, 0}, []uint64{0, 0}),
	)

	s := applyTwo(t, first, second, testSettings())

	requireOK(t, 100, s.Blobs.Subtree.ReadsPerSec)
	requireOK(t, 10, s.Blobs.Subtree.WritesPerSec)
	requireOK(t, 2, s.Blobs.Subtree.ErrorsPerSec)
	requireOK(t, 0.091, s.Blobs.Subtree.ReadP99Sec)
	requireOK(t, 0.01*0.99, s.Blobs.Subtree.WriteP99Sec)

	requireOK(t, 0, s.Blobs.Block.ReadsPerSec)
	requireOK(t, 0, s.Blobs.Block.WritesPerSec)
	requireOK(t, 0, s.Blobs.Block.ErrorsPerSec)
	requireNoData(t, s.Blobs.Block.ReadP99Sec)
	requireNoData(t, s.Blobs.Block.WriteP99Sec)
}

func TestBlobStoreReadingsNeedTheStoreLabel(t *testing.T) {
	both := mergeStores(blobSet("tx", 100, 50, 0, 0, 0, []uint64{0, 0}, []uint64{0, 0}))
	more := mergeStores(blobSet("tx", 300, 70, 0, 0, 0, []uint64{0, 0}, []uint64{0, 0}))

	s := applyTwo(t, both, more, testSettings())

	requireNoData(t, s.Blobs.Subtree.ReadsPerSec)
	requireNoData(t, s.Blobs.Block.WritesPerSec)
}

func TestBlobStoreReadingsMissingMetricsGiveNoData(t *testing.T) {
	s := applyTwo(t, MetricSet{}, MetricSet{}, testSettings())

	for _, v := range []snapshot.BlobStoreView{s.Blobs.Subtree, s.Blobs.Block} {
		requireNoData(t, v.ReadsPerSec)
		requireNoData(t, v.WritesPerSec)
		requireNoData(t, v.ErrorsPerSec)
		requireNoData(t, v.ReadP99Sec)
		requireNoData(t, v.WriteP99Sec)
	}
}

func newLanesSet(existing, missing, outstanding, candidateTime float64) MetricSet {
	return join(
		counterSet(metricValidatorExisting, existing),
		counterSet(metricBlockMissingSubtrees, missing),
		gaugeSet(metricBlockMissingOutstanding, outstanding),
		gaugeSet(metricMiningCandidateTime, candidateTime),
	)
}

func TestLanesSkippedSubtreeAndCandidateReadings(t *testing.T) {
	s := applyTwo(t, newLanesSet(100, 4, 3, 995), newLanesSet(700, 10, 2, 1001), testSettings())

	requireOK(t, 300, s.Lanes.Tx.AlreadySeenPerSec)
	requireOK(t, 3, s.Lanes.Blocks.MissingSubtreesPerSec)
	requireOK(t, 2, s.Lanes.Blocks.MissingSubtrees)
	requireOK(t, 1, s.Lanes.Mining.CandidateAgeSec)
}

func TestLanesSkippedSubtreeAndCandidateIdle(t *testing.T) {
	idle := newLanesSet(700, 10, 0, 998)
	s := applyTwo(t, idle, idle, testSettings())

	requireOK(t, 0, s.Lanes.Tx.AlreadySeenPerSec)
	requireOK(t, 0, s.Lanes.Blocks.MissingSubtreesPerSec)
	requireOK(t, 0, s.Lanes.Blocks.MissingSubtrees)
	requireOK(t, 4, s.Lanes.Mining.CandidateAgeSec)
}

func TestLanesNoCandidateServedYetGivesNoAge(t *testing.T) {
	never := newLanesSet(0, 0, 0, 0)
	s := applyTwo(t, never, never, testSettings())

	requireNoData(t, s.Lanes.Mining.CandidateAgeSec)
	requireOK(t, 0, s.Lanes.Tx.AlreadySeenPerSec)
}

func TestLanesIdleNodeIsStatic(t *testing.T) {
	idle := lanesSet(30, 140, 1500, 6, 5, 24, 3)
	s := applyTwo(t, idle, idle, testSettings())

	requireOK(t, 0, s.Lanes.Subtrees.AlreadyKnownPerSec)
	requireOK(t, 0, s.Lanes.Subtrees.CompletedPerSec)
	requireOK(t, 0, s.Lanes.Subtrees.MissingTxsPerSec)
	requireOK(t, 0, s.Lanes.Mining.CandidatesPerSec)
	requireOK(t, 6, s.Lanes.Blocks.AnnouncedTotal)
}

func TestLanesNoSubtreesCompletedGivesNoAverage(t *testing.T) {
	zero := lanesSet(0, 0, 0, 0, 0, 0, 0)
	s := applyTwo(t, zero, zero, testSettings())

	requireNoData(t, s.Lanes.Subtrees.TxsFetchedAvg)
	requireOK(t, 0, s.Lanes.Blocks.AnnouncedTotal)
}

func TestLanesMissingMetricsGiveNoData(t *testing.T) {
	s := applyTwo(t, MetricSet{}, MetricSet{}, testSettings())

	requireNoData(t, s.Lanes.Subtrees.CompletedPerSec)
	requireNoData(t, s.Lanes.Subtrees.TxsFetchedAvg)
	requireNoData(t, s.Lanes.Blocks.AnnouncedTotal)
	requireNoData(t, s.Lanes.Mining.SolutionsTotal)
	requireNoData(t, s.Lanes.Tx.AlreadySeenPerSec)
	requireNoData(t, s.Lanes.Blocks.MissingSubtrees)
	requireNoData(t, s.Lanes.Blocks.MissingSubtreesPerSec)
	requireNoData(t, s.Lanes.Mining.CandidateAgeSec)
}
