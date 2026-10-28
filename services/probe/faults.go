package probe

import (
	"sync"

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

	mu         sync.Mutex
	streak     int
	haveFailed bool
	failedSeq  uint64
}

func newFaultCounter(group string, logger ulogger.Logger, signal *stopSignal) *faultCounter {
	return &faultCounter{group: group, logger: logger, signal: signal}
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

func (f *faultCounter) failedAt(seq uint64, component string, value any) {
	f.logger.Errorf("[Probe] %s panicked: %s", component, oneLine(value))

	f.mu.Lock()

	if f.haveFailed && seq <= f.failedSeq {
		f.mu.Unlock()

		return
	}

	f.haveFailed = true
	f.failedSeq = seq
	f.streak++
	streak := f.streak
	f.mu.Unlock()

	f.check(streak)
}

func (f *faultCounter) succeededAt(seq uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.haveFailed || seq > f.failedSeq {
		f.streak = 0
	}
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
