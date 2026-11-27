package subtreeprocessor

import (
	"fmt"
	"regexp"
	"slices"
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
