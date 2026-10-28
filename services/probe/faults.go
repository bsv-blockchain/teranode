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
)

type faultTracker struct {
	logger ulogger.Logger

	mu     sync.Mutex
	streak int

	failed chan struct{}
	once   sync.Once
}

func newFaultTracker(logger ulogger.Logger) *faultTracker {
	return &faultTracker{logger: logger, failed: make(chan struct{})}
}

func (f *faultTracker) Panicked(component string, value any) {
	f.logger.Errorf("[Probe] %s panicked: %s", component, oneLine(value))

	f.mu.Lock()
	f.streak++
	streak := f.streak
	f.mu.Unlock()

	if streak >= maxConsecutivePanics {
		f.fail()
	}
}

func (f *faultTracker) fatal() guard.Recorder {
	return guard.RecorderFunc(func(component string, value any) {
		f.Panicked(component, value)
		f.fail()
	})
}

func (f *faultTracker) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.streak = 0
}

func (f *faultTracker) fail() {
	f.once.Do(func() { close(f.failed) })
}

func (f *faultTracker) hasFailed() bool {
	select {
	case <-f.failed:
		return true
	default:
		return false
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
