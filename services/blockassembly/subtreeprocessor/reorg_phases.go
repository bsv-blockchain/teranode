package subtreeprocessor

import (
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// phaseTimer gives each phase of a long block-assembly operation (a fork reorg, a reset) its wall
// time: one log line per phase as it ends, one observation of
// teranode_subtreeprocessor_reorg_phase_seconds{op,phase}, and one summary line at the end with the
// counts and every phase. A phase that repeats, such as each forward step of a reorg, is logged and
// observed each time.
//
// While such an operation runs, the processor holds its timer in stp.phases, so the steps it calls
// (moveForwardBlock, the remainder pass) report their sub-phases without new parameters. Outside one
// stp.phases is nil, and every method of a nil timer does nothing. Only the processor goroutine
// touches it.
type phaseTimer struct {
	op          string
	logger      ulogger.Logger
	clock       clock
	start, last time.Time
	counts      []phaseCount
	timings     []phaseTiming
}

type phaseCount struct {
	name  string
	value int
}

type phaseTiming struct {
	name     string
	duration time.Duration
}

func newPhaseTimer(op string, logger ulogger.Logger, c clock) *phaseTimer {
	initPrometheusMetrics()

	now := c.Now()

	return &phaseTimer{op: op, logger: logger, clock: c, start: now, last: now}
}

// count records a size for the summary line.
func (p *phaseTimer) count(name string, value int) {
	if p == nil {
		return
	}

	p.counts = append(p.counts, phaseCount{name, value})
}

// done ends the phase that started when the previous phase ended (or the timer started).
func (p *phaseTimer) done(phase string) {
	if p == nil {
		return
	}

	now := p.clock.Now()
	d := now.Sub(p.last)
	p.last = now
	p.timings = append(p.timings, phaseTiming{phase, d})

	p.logger.Infof("[%s] phase %s took %s", p.op, phase, d)
	prometheusSubtreeProcessorReorgPhase.WithLabelValues(p.op, phase).Observe(d.Seconds())
}

// finish records the time since the last phase as after_last_phase, which is the phase that failed
// when the operation returns an error part way, then logs the summary line: the total, the counts,
// and every phase in order.
func (p *phaseTimer) finish() {
	if p == nil {
		return
	}

	p.done("after_last_phase")

	var b strings.Builder

	fmt.Fprintf(&b, "[%s] phases total=%s", p.op, p.last.Sub(p.start))

	for _, c := range p.counts {
		fmt.Fprintf(&b, " %s=%d", c.name, c.value)
	}

	for _, t := range p.timings {
		fmt.Fprintf(&b, " %s=%s", t.name, t.duration)
	}

	p.logger.Infof("%s", b.String())
}

// startPhases installs a timer for op as stp.phases and returns the function that logs its summary
// and removes it.
func (stp *SubtreeProcessor) startPhases(op string) (finish func()) {
	stp.phases = newPhaseTimer(op, stp.logger, stp.clock)

	return func() {
		stp.phases.finish()
		stp.phases = nil
	}
}
