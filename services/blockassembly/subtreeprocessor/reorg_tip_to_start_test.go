package subtreeprocessor

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 0b: the time from block assembly seeing the new tip to the processor starting
// the fork reorg is reported, queue wait included, when the request carries when the tip was seen.

var summaryCount = regexp.MustCompile(`^(\w+)=(-?\d+)$`)

type countValue struct {
	name  string
	value int
}

// summaryCounts returns the counts of a summary line, in the order they were recorded. A phase
// time ends in a unit, so it never matches.
func summaryCounts(summary string) []countValue {
	var out []countValue

	for _, field := range strings.Fields(summary) {
		if m := summaryCount.FindStringSubmatch(field); m != nil {
			v, _ := strconv.Atoi(m[2])
			out = append(out, countValue{m[1], v})
		}
	}

	return out
}

// countsNamed returns every value recorded under name, in order.
func countsNamed(summary, name string) []int {
	var out []int

	for _, c := range summaryCounts(summary) {
		if c.name == name {
			out = append(out, c.value)
		}
	}

	return out
}

func tipToStartSampleCount(t *testing.T) uint64 {
	t.Helper()

	initPrometheusMetrics()

	m := &dto.Metric{}
	require.NoError(t, prometheusSubtreeProcessorReorgTipToStart.(prometheus.Histogram).Write(m))

	return m.GetHistogram().GetSampleCount()
}

func TestReorgBlocks_ReportsTipToStartWhenTheRequestCarriesIt(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger
	sc.stp.Start(t.Context())
	before := tipToStartSampleCount(t)

	require.NoError(t, sc.stp.Reorg([]*model.Block{sc.lost}, sc.forward, WithTipSeenAt(time.Now().Add(-5*time.Second))))

	got := countsNamed(logger.summary("reorgBlocks"), "tip_to_start_ms")
	require.Len(t, got, 1)
	require.GreaterOrEqual(t, got[0], 5000)
	require.Less(t, got[0], 15000)
	require.Equal(t, "tip_to_start_ms", summaryCounts(logger.summary("reorgBlocks"))[0].name, "the first count of the summary")
	require.Equal(t, before+1, tipToStartSampleCount(t))
}

// The processor is busy with an earlier request while the reorg waits: that wait is part of the time.
func TestReorgBlocks_TipToStartIncludesTheProcessorQueueWait(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger
	sc.stp.Start(t.Context())

	// The loop answers this request on an unbuffered channel nobody reads, so it stays busy
	// until the test reads it.
	busy := make(chan []chainhash.Hash)
	sc.stp.getTransactionHashesChan <- busy

	seen := time.Now()
	done := make(chan error, 1)

	go func() { done <- sc.stp.Reorg([]*model.Block{sc.lost}, sc.forward, WithTipSeenAt(seen)) }()

	time.Sleep(400 * time.Millisecond)
	<-busy
	require.NoError(t, <-done)

	got := countsNamed(logger.summary("reorgBlocks"), "tip_to_start_ms")
	require.Len(t, got, 1)
	require.GreaterOrEqual(t, got[0], 400)
}

func TestReorgBlocks_ReportsNoTipToStartWithoutATipTime(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger
	before := tipToStartSampleCount(t)

	require.NoError(t, sc.run(t))

	require.NotEmpty(t, logger.summary("reorgBlocks"))
	require.Empty(t, countsNamed(logger.summary("reorgBlocks"), "tip_to_start_ms"))
	require.Equal(t, before, tipToStartSampleCount(t))
}

// A request's tip time belongs to that request only: the processor drops it when the request is
// done, so the next one cannot report it.
func TestReorgBlocks_TipToStartIsDroppedWhenTheRequestIsDone(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	sc.stp.Start(t.Context())

	require.NoError(t, sc.stp.Reorg([]*model.Block{sc.lost}, sc.forward, WithTipSeenAt(time.Now())))

	require.True(t, sc.stp.reorgTipSeenAt.IsZero())
}
