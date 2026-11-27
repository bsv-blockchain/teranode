package subtreeprocessor

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 0: every phase of a fork reorg and of a reset gets a wall time, as a log line
// and a histogram observation, so the scaling run shows where a long reorg spends its time.

// phaseLogger records Infof and Debugf lines; everything else is dropped as by TestLogger.
type phaseLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *phaseLogger) Infof(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *phaseLogger) Debugf(format string, args ...interface{}) {
	l.Infof(format, args...)
}

var phaseLine = regexp.MustCompile(`^\[(\w+)\] phase (\w+) took `)

// phases returns the phases op logged, in order.
func (l *phaseLogger) phases(op string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []string

	for _, line := range l.lines {
		if m := phaseLine.FindStringSubmatch(line); m != nil && m[1] == op {
			out = append(out, m[2])
		}
	}

	return out
}

// summary returns op's summary line.
func (l *phaseLogger) summary(op string) string {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range l.lines {
		if strings.HasPrefix(line, "["+op+"] phases ") {
			return line
		}
	}

	return ""
}

// steppingClock advances by step on every Now.
type steppingClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(c.step)

	return c.now
}

func phaseSampleCount(t *testing.T, op, phase string) uint64 {
	t.Helper()

	initPrometheusMetrics()

	m := &dto.Metric{}
	require.NoError(t, prometheusSubtreeProcessorReorgPhase.WithLabelValues(op, phase).(prometheus.Histogram).Write(m))

	return m.GetHistogram().GetSampleCount()
}

func TestPhaseTimer_LogsAndObservesEachPhase(t *testing.T) {
	logger := &phaseLogger{}
	before := phaseSampleCount(t, "unittest", "alpha")

	p := newPhaseTimer("unittest", logger, &steppingClock{step: time.Second})
	p.count("pending", 42)
	p.done("alpha")
	p.done("beta")
	p.finish(false)

	require.Equal(t, []string{"alpha", "beta"}, logger.phases("unittest"), "a success records no tail phase")
	require.Contains(t, logger.lines, "[unittest] phase alpha took 1s")
	require.Equal(t, "[unittest] phases total=2s pending=42 alpha=1s beta=1s", logger.summary("unittest"))
	require.Equal(t, before+1, phaseSampleCount(t, "unittest", "alpha"))
}

func TestPhaseTimer_FailureRecordsTheTimeAfterTheLastPhase(t *testing.T) {
	logger := &phaseLogger{}

	p := newPhaseTimer("unittest", logger, &steppingClock{step: time.Second})
	p.done("alpha")
	p.finish(true)

	require.Equal(t, []string{"alpha", "after_last_phase"}, logger.phases("unittest"),
		"on a failure the time after the last phase is the phase that failed")
	require.Equal(t, "[unittest] phases total=2s alpha=1s after_last_phase=1s", logger.summary("unittest"))
}

func TestPhaseTimer_NilIsANoOp(t *testing.T) {
	var p *phaseTimer

	require.NotPanics(t, func() {
		p.count("x", 1)
		p.done("x")
		p.finish(true)
	})
}

// forwardStepPhases is a forward step over a block of other miners' transactions with a remainder
// at or above the parallel threshold.
var forwardStepPhases = []string{
	"forward_set_build", "forward_conflicts", "forward_reset_state",
	"remainder_flag_copy", "remainder_map_insert", "remainder_build",
	"forward_drain", "forward_commit", "forward_mark_sets", "forward_snapshot",
}

func TestReorgBlocks_LogsEachPhaseOnceInOrder(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.pending = 30_000 // the remainder of each step is above the parallel threshold
	sc := newReorgScenario(t, cfg)

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.run(t))

	want := []string{"wait_pending_blocks", "upfront_reads", "head_drain", "move_back", "move_back_finish"}
	for range sc.forward {
		want = append(want, forwardStepPhases...)
	}

	want = append(want, "mark_sets", "marks", "store_wait", "processed_at", "finalize")

	require.Equal(t, want, logger.phases("reorgBlocks"))
}

func TestReorgBlocks_SmallRemainderLogsTheSequentialPhase(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.run(t))

	phases := logger.phases("reorgBlocks")
	require.Contains(t, phases, "remainder_add_nodes")
	require.NotContains(t, phases, "remainder_map_insert")
}

func TestReorgBlocks_SummaryCarriesTheCounts(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.run(t))

	summary := logger.summary("reorgBlocks")
	require.NotEmpty(t, summary)

	for _, want := range []string{
		" pending=2000 ",
		" tx_map=2000 ",
		" queue=0 ",
		fmt.Sprintf(" mark_true=%d ", len(sc.winnerNotLost())),
		fmt.Sprintf(" mark_false=%d ", len(sc.rawD1())),
	} {
		require.Contains(t, summary, want)
	}
}

// A forward step reports its counts in a fixed order: remainder, hits, last_hit_position, then
// drain_queue, and d2 once the step's block is committed. The reorg's own counts come first and
// mark_true and mark_false after the last step.
func TestReorgBlocks_SummaryCountsHaveAStableOrder(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.run(t))

	var names []string
	for _, c := range summaryCounts(logger.summary("reorgBlocks")) {
		names = append(names, c.name)
	}

	want := []string{"pending", "tx_map", "queue"}
	for range sc.forward {
		want = append(want, "remainder", "hits", "last_hit_position", "drain_queue", "d2")
	}

	want = append(want, "mark_true", "mark_false")

	require.Equal(t, want, names)
}

// hits counts the nodes a step's block mined that block assembly held, the nodes the lost block
// moved back included: a transaction both blocks mined is flagged as in the block like a pending
// one. Each transaction is in one block of the chain, so no step counts a transaction twice and
// the steps sum to the pending hits plus the lost transactions the winners also mined.
func TestReorgBlocks_HitsSumToWhatTheWinnersMined(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.hitsAtFront = true
	sc := newReorgScenario(t, cfg)

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.run(t))

	hits := countsNamed(logger.summary("reorgBlocks"), "hits")
	require.Len(t, hits, len(sc.forward))

	lostInWinners := len(sc.lostOrder()) - len(sc.rawD1())
	require.NotZero(t, lostInWinners)
	require.Equal(t, len(sc.hits())+lostInWinners, sum(hits))
}

// last_hit_position is the largest index, in the order block assembly holds the nodes (the
// coinbase placeholder is index 0), of a node the step's block mined. With the winners taking the
// oldest transactions every hit sits before the pending transactions it left, so the last one is
// well before the end; with hits spread through the pending set it is near the end.
func TestReorgBlocks_LastHitPositionTracksWhereTheHitsAre(t *testing.T) {
	// positions returns each step's last hit position and the nodes it held (kept plus flagged).
	positions := func(front bool) (last, held []int) {
		cfg := smallScenarioConfig()
		cfg.hitsAtFront = front
		sc := newReorgScenario(t, cfg)

		logger := &phaseLogger{}
		sc.stp.logger = logger

		require.NoError(t, sc.run(t))

		summary := logger.summary("reorgBlocks")

		held = countsNamed(summary, "remainder")
		for i, h := range countsNamed(summary, "hits") {
			held[i] += h
		}

		return countsNamed(summary, "last_hit_position"), held
	}

	frontLast, frontHeld := positions(true)
	scatteredLast, scatteredHeld := positions(false)

	for i := range scatteredLast {
		require.Greater(t, scatteredLast[i], scatteredHeld[i]*9/10, "step %d, scattered hits", i)
	}

	for i := range frontLast {
		require.GreaterOrEqual(t, frontLast[i], 0, "step %d, hits at the front", i)
		require.Less(t, frontLast[i], frontHeld[i]*3/4, "step %d, hits at the front", i)
	}
}

// A step that flags nothing reports -1.
func TestReorgBlocks_LastHitPositionIsMinusOneWithoutHits(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.pendingInWinners = 0
	cfg.lostInWinners = 0
	sc := newReorgScenario(t, cfg)

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.run(t))

	summary := logger.summary("reorgBlocks")
	require.Equal(t, []int{0, 0}, countsNamed(summary, "hits"))
	require.Equal(t, []int{-1, -1}, countsNamed(summary, "last_hit_position"))
}

// d2 is what a forward step adds to the longest-chain marks: its block's transactions the lost
// block did not have. The steps sum to D2.
func TestReorgBlocks_D2PerStepSumsToWinnerNotLost(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.run(t))

	d2 := countsNamed(logger.summary("reorgBlocks"), "d2")
	require.Len(t, d2, len(sc.forward))
	require.Equal(t, len(sc.winnerNotLost()), sum(d2))
	require.NotZero(t, d2[0])
	require.NotZero(t, d2[1])
}

func sum(values []int) int {
	total := 0
	for _, v := range values {
		total += v
	}

	return total
}

func TestReset_LogsEachPhaseOnceInOrder(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.stp.reset(sc.genesis, []*model.Block{sc.lost}, sc.forward, false, func() error { return nil }))

	require.Equal(t, []string{
		"upfront_reads", "mark_assembly", "clear_and_retire", "move_back", "move_forward", "post_process", "queue_discard",
	}, logger.phases("reset"))
}

func TestMoveForwardBlock_OutsideAReorgLogsNoPhases(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	logger := &phaseLogger{}
	sc.stp.logger = logger

	// Move back to genesis first so the first forward block is a plain move forward.
	require.NoError(t, sc.stp.reorgBlocks(t.Context(), []*model.Block{sc.lost}, []*model.Block{}))

	logger.mu.Lock()
	logger.lines = nil
	logger.mu.Unlock()

	_, _, err := sc.stp.moveForwardBlock(t.Context(), sc.forward[0], true, map[chainhash.Hash]struct{}{}, false, false)
	require.NoError(t, err)

	require.Empty(t, slices.Concat(logger.phases("reorgBlocks"), logger.phases("reset"), logger.phases("moveForwardBlock")))
}

// A catch-up (nothing to move back) is not a fork reorg: it must not be recorded as one.
func TestReorgBlocks_CatchUpLogsNoPhases(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	// Back to genesis, then catch up onto the winners.
	require.NoError(t, sc.stp.reorgBlocks(t.Context(), []*model.Block{sc.lost}, []*model.Block{}))

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.stp.reorgBlocks(t.Context(), []*model.Block{}, sc.forward))
	require.Equal(t, sc.forward[len(sc.forward)-1].Header.Hash(), sc.stp.GetCurrentBlockHeader().Hash())

	require.Empty(t, logger.phases("reorgBlocks"))
	require.Empty(t, logger.summary("reorgBlocks"))
}

// A fork reorg that fails part way records the phase it failed in as the time after its last
// completed phase, and still logs its summary.
func TestReorgBlocks_FailedReorgRecordsTheFailingPhase(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	// The second forward block has a subtree that cannot be opened.
	sc.stp.subtreeStore = openFailingStore{Memory: sc.blobStore, key: *sc.forward[1].Subtrees[0]}

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.Error(t, sc.run(t))

	phases := logger.phases("reorgBlocks")
	// The subtrees of every winning block are opened before anything moves, so the reads fail.
	require.Equal(t, []string{"wait_pending_blocks", "after_last_phase"}, phases)
	require.NotEmpty(t, logger.summary("reorgBlocks"))
}

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

// A reset that fails records the phase it failed in, as a failed fork reorg does.
func TestReset_FailedResetRecordsTheFailingPhase(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	sc.stp.subtreeStore = openFailingStore{Memory: sc.blobStore, key: *sc.forward[0].Subtrees[0]}

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.Error(t, sc.stp.reset(sc.genesis, []*model.Block{sc.lost}, sc.forward, false, func() error { return nil }))

	// The moveForward subtree check is part of the reads, which never complete.
	require.Equal(t, []string{"after_last_phase"}, logger.phases("reset"))
	require.Contains(t, logger.summary("reset"), " pending=2000 ")
}

// The summary's counts are what a slow reorg is read against, so each one is pinned.
func TestReorgBlocks_SummaryCarriesTheQueueAndRemainderCounts(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	// Nothing dequeues while the processor is not started, so the queue holds what intake sent.
	stop := sc.startIntake(100_000)
	require.Eventually(t, func() bool { return sc.stp.QueueLength() > 0 }, 10*time.Second, 5*time.Millisecond)

	sent := stop()

	logger := &phaseLogger{}
	sc.stp.logger = logger

	require.NoError(t, sc.run(t))

	summary := logger.summary("reorgBlocks")
	require.Contains(t, summary, fmt.Sprintf(" queue=%d ", sent))
	// The head drain took the queue, so a forward step drains only what arrived since.
	require.Equal(t, len(sc.forward), strings.Count(summary, " drain_queue="), "one drain count per forward step")
	require.Equal(t, len(sc.forward), strings.Count(summary, " remainder="), "one remainder count per forward step")
	require.Regexp(t, ` remainder=[1-9]\d* `, summary)
}

// BenchmarkReorgPath times a fork reorg of one block back and two forward through reorgBlocks, the
// production call site, on the scenario shape of issue 2269: the lost block about twice the pending
// set and almost entirely in the winners, the winners taking the oldest pending transactions. It is
// the baseline every reorg change of that issue is measured against.
//
// The name matches the subtreeprocessor-unit filter of the benchmark-compare workflow, which runs
// with -short: under -short only the 256K pending set runs, so the job stays inside its budget. The
// larger sets run without -short. The 125M and larger sizes are the reorgscale suite.
//
// Each iteration builds a fresh scenario outside the timer, because a reorg consumes its input.
func BenchmarkReorgPath(b *testing.B) {
	sizes := []int{256 << 10}
	if !testing.Short() {
		sizes = append(sizes, 1<<20)
	}

	for _, pending := range sizes {
		for _, hits := range []float64{0.3, 0.9} {
			for _, disk := range []bool{false, true} {
				cfg := reorgPathBenchConfig(pending, hits, disk)
				name := fmt.Sprintf("pending=%dK/hits=%.0f%%/map=%s", pending>>10, hits*100, map[bool]string{false: "memory", true: "disk"}[disk])

				b.Run(name, func(b *testing.B) {
					benchmarkReorgPath(b, cfg, slowReorgPath)
				})
			}
		}
	}
}

func reorgPathBenchConfig(pending int, hits float64, disk bool) reorgScenarioConfig {
	return reorgScenarioConfig{
		seed:             uint64(pending) ^ uint64(hits*100),
		pending:          pending,
		lost:             2 * pending,
		lostInWinners:    0.995,
		pendingInWinners: hits,
		hitsAtFront:      true,
		winnerOnly:       pending / 1000,
		forwardBlocks:    2,
		subtreeSize:      1 << 14,
		diskTxMap:        disk,
		// sqlite marks of synthetic hashes would be most of the time and log mid-line, which
		// breaks benchstat's parsing; the marks are measured on their own against Aerospike.
		skipStoreMarks: true,
	}
}

func benchmarkReorgPath(b *testing.B, cfg reorgScenarioConfig, path reorgPath) {
	b.Helper()
	b.ReportAllocs()

	var total time.Duration

	phases := make(map[string]float64)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sc := newReorgScenario(b, cfg)
		before := reorgPhaseSeconds("reorgBlocks")
		b.StartTimer()

		start := time.Now()
		err := path(b, sc)
		total += time.Since(start)

		b.StopTimer()

		// b.Cleanup runs once per round, so without this every iteration's processor, tx map and
		// listener would stay alive during the next timed reorg.
		sc.release()

		if err != nil {
			b.Fatal(err)
		}

		for phase, seconds := range reorgPhaseSeconds("reorgBlocks") {
			phases[phase] += seconds - before[phase]
		}
	}

	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/float64(cfg.pending), "ns/pending_tx")

	for _, phase := range slices.Sorted(maps.Keys(phases)) {
		b.ReportMetric(phases[phase]*1000/float64(b.N), phase+"_ms")
	}
}

// reorgPhaseSeconds is the total time reorg_phase_seconds has recorded for each phase of op.
func reorgPhaseSeconds(op string) map[string]float64 {
	initPrometheusMetrics()

	ch := make(chan prometheus.Metric, 64)

	go func() {
		prometheusSubtreeProcessorReorgPhase.Collect(ch)
		close(ch)
	}()

	out := make(map[string]float64)

	for metric := range ch {
		m := &dto.Metric{}
		if err := metric.Write(m); err != nil {
			continue
		}

		var labelOp, phase string

		for _, l := range m.GetLabel() {
			switch l.GetName() {
			case "op":
				labelOp = l.GetValue()
			case "phase":
				phase = l.GetValue()
			}
		}

		if labelOp == op {
			out[phase] = m.GetHistogram().GetSampleSum()
		}
	}

	return out
}
