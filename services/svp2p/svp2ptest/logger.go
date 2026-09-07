package svp2ptest

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// RecordingLogger keeps every formatted Warnf and Infof line so a test can
// assert on a decision the production code already logs, without adding any
// new production surface. It is the same technique protocol/manager_test.go
// uses for the self-connection disconnect reason.
type RecordingLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *RecordingLogger) record(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *RecordingLogger) Warnf(format string, args ...interface{}) { l.record(format, args...) }

func (l *RecordingLogger) Infof(format string, args ...interface{}) { l.record(format, args...) }

func (l *RecordingLogger) Errorf(format string, args ...interface{}) { l.record(format, args...) }

func (l *RecordingLogger) Debugf(format string, args ...interface{}) { l.record(format, args...) }

// dump prints what the node logged, so a failing leg is diagnosable from the
// test output alone.
func (l *RecordingLogger) Dump(t *testing.T) {
	t.Helper()

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range l.lines {
		t.Log(line)
	}
}

func (l *RecordingLogger) Contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}

	return false
}

// ---------------------------------------------------------------------------
// Chain fixture: a run of regtest coinbase-only blocks the real pipeline
// accepts (valid proof of work under the regtest limit, BIP34 coinbase, merkle
// root = the coinbase txid, which is what a single-transaction block's root is).
// ---------------------------------------------------------------------------

// Lines returns a copy of every line recorded so far.
func (l *RecordingLogger) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]string, len(l.lines))
	copy(out, l.lines)

	return out
}

// Matching returns the recorded lines that contain substr.
func (l *RecordingLogger) Matching(substr string) []string {
	var out []string

	for _, line := range l.Lines() {
		if strings.Contains(line, substr) {
			out = append(out, line)
		}
	}

	return out
}
