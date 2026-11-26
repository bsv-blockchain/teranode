package probe

import (
	"strconv"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	faultFlush       = "flush"
	faultPublish     = "publish"
	faultClose       = "close"
	faultHub         = "hub"
	faultClientWrite = "client-write"

	groupPipeline   = "pipeline"
	groupHub        = "websocket hub"
	groupCollectors = "collectors"

	hubLogEvery = time.Second
)

type stopSignal struct {
	failed chan struct{}
	once   sync.Once

	mu     sync.Mutex
	reason string
}

func newStopSignal() *stopSignal {
	return &stopSignal{failed: make(chan struct{})}
}

func (s *stopSignal) fail(reason string) {
	s.once.Do(func() {
		s.mu.Lock()
		s.reason = reason
		s.mu.Unlock()

		close(s.failed)
	})
}

func (s *stopSignal) stopped() bool {
	select {
	case <-s.failed:
		return true
	default:
		return false
	}
}

func (s *stopSignal) Reason() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.reason
}

type faultCounter struct {
	group  string
	logger ulogger.Logger
	signal *stopSignal

	now func() time.Time

	mu         sync.Mutex
	streak     int
	haveSeen   bool
	newestSeq  uint64
	haveFailed bool
	failedSeq  uint64
	resetSeq   uint64
	resetPrev  int
	haveReset  bool
	haveLogged bool
	loggedSeq  uint64
	lastLog    time.Time
	suppressed int
}

func newFaultCounter(group string, logger ulogger.Logger, signal *stopSignal) *faultCounter {
	return &faultCounter{group: group, logger: logger, signal: signal, now: time.Now}
}

func (f *faultCounter) Panicked(component string, value any) {
	f.logger.Errorf("[Probe] %s panicked: %s", component, oneLine(value))

	f.mu.Lock()
	f.streak++
	streak := f.streak
	f.mu.Unlock()

	f.check(streak)
}

func (f *faultCounter) check(streak int) {
	if streak >= maxConsecutivePanics {
		f.signal.fail(probeStoppedMessage + " in the " + f.group)
	}
}

func (f *faultCounter) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.streak = 0
}

func (f *faultCounter) observeLocked(seq uint64) bool {
	if f.haveSeen && seq < f.newestSeq {
		return false
	}

	f.haveSeen = true
	f.newestSeq = seq

	return true
}

func (f *faultCounter) logSuffixLocked(seq uint64) (string, bool) {
	now := f.now()

	if f.haveLogged && (seq == f.loggedSeq || now.Sub(f.lastLog) < hubLogEvery) {
		f.suppressed++

		return "", false
	}

	suffix := ""
	if f.suppressed > 0 {
		suffix = "; suppressed " + strconv.Itoa(f.suppressed) + " earlier hub panic lines"
	}

	f.haveLogged = true
	f.loggedSeq = seq
	f.lastLog = now
	f.suppressed = 0

	return suffix, true
}

func (f *faultCounter) failedAt(seq uint64, component string, value any) {
	f.mu.Lock()

	suffix, logIt := f.logSuffixLocked(seq)
	counted := false
	streak := 0

	if f.observeLocked(seq) && (!f.haveFailed || seq != f.failedSeq) {
		f.haveFailed = true
		f.failedSeq = seq

		if f.haveReset && f.resetSeq == seq {
			f.streak = f.resetPrev + 1
			f.haveReset = false
		} else {
			f.streak++
		}

		counted = true
		streak = f.streak
	}

	f.mu.Unlock()

	if logIt {
		f.logger.Errorf("[Probe] %s panicked: %s%s", component, oneLine(value), suffix)
	}

	if counted {
		f.check(streak)
	}
}

func (f *faultCounter) succeededAt(seq uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.observeLocked(seq) {
		return
	}

	if f.haveFailed && f.failedSeq == seq {
		return
	}

	if f.haveReset && f.resetSeq == seq {
		return
	}

	f.resetPrev = f.streak
	f.resetSeq = seq
	f.haveReset = true
	f.streak = 0
}

func (f *faultCounter) fatal() guard.Recorder {
	return guard.RecorderFunc(func(component string, value any) {
		f.logger.Errorf("[Probe] %s panicked: %s", component, oneLine(value))
		f.signal.fail("probe stopped after a panic ended the " + component + " loop")
	})
}

type faults struct {
	signal     *stopSignal
	pipeline   *faultCounter
	hub        *faultCounter
	collectors *faultCounter
}

func (f *faults) setClock(now func() time.Time) {
	for _, c := range []*faultCounter{f.pipeline, f.hub, f.collectors} {
		c.now = now
	}
}

func newFaults(logger ulogger.Logger) *faults {
	signal := newStopSignal()

	return &faults{
		signal:     signal,
		pipeline:   newFaultCounter(groupPipeline, logger, signal),
		hub:        newFaultCounter(groupHub, logger, signal),
		collectors: newFaultCounter(groupCollectors, logger, signal),
	}
}

func (s *Server) setFaultHook(hook func(point string)) {
	s.hook.Store(&hook)
}

func (s *Server) fault(point string) {
	if h := s.hook.Load(); h != nil && *h != nil {
		(*h)(point)
	}
}
