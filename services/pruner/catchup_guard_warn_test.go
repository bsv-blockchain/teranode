// Package pruner — the skipDuringCatchup guard's stop-switch visibility
// (reconciliation notes 54, open question 33).
package pruner

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type warnCapture struct {
	mu     sync.Mutex
	warns  []string
	debugs []string
}

func (c *warnCapture) LogLevel() int      { return 0 }
func (c *warnCapture) SetLogLevel(string) {}
func (c *warnCapture) Debugf(f string, a ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.debugs = append(c.debugs, fmt.Sprintf(f, a...))
}
func (c *warnCapture) Infof(string, ...interface{})                 {}
func (c *warnCapture) Errorf(string, ...interface{})                {}
func (c *warnCapture) Fatalf(string, ...interface{})                {}
func (c *warnCapture) New(string, ...ulogger.Option) ulogger.Logger { return c }
func (c *warnCapture) Duplicate(...ulogger.Option) ulogger.Logger   { return c }
func (c *warnCapture) WithTraceContext(context.Context) ulogger.Logger {
	return c
}
func (c *warnCapture) Warnf(f string, a ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.warns = append(c.warns, fmt.Sprintf(f, a...))
}

func (c *warnCapture) joinedWarns() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.warns, "\n")
}

func (c *warnCapture) countWarns() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.warns)
}

func TestWarnCatchupGuardStopped(t *testing.T) {
	cap := &warnCapture{}
	s := &Server{logger: cap}

	idle := blockchain.FSMStateIDLE
	require.Equal(t, "IDLE", idle.String())

	// IDLE is the trap state: it must warn, and name the operator-RUN cause.
	s.warnCatchupGuardStopped(&idle)
	require.Contains(t, cap.joinedWarns(), "IDLE")
	require.Contains(t, cap.joinedWarns(), "RUN command")

	// Repeat IDLE inside the throttle window stays quiet.
	before := cap.countWarns()
	s.warnCatchupGuardStopped(&idle)
	require.Equal(t, before, cap.countWarns())

	// Forcing the clock back re-arms the periodic reminder.
	s.lastGuardWarnedAt = time.Now().Add(-11 * time.Minute)
	s.warnCatchupGuardStopped(&idle)
	require.Equal(t, before+1, cap.countWarns())

	// A genuine catching-up state gets a debug line and, within an hour of the
	// same state, no repeat warning storm.
	cap2 := &warnCapture{}
	s2 := &Server{logger: cap2}
	catching := blockchain.FSMStateCATCHINGBLOCKS
	s2.warnCatchupGuardStopped(&catching)
	require.Contains(t, strings.Join(cap2.debugs, "\n"), "CATCHINGBLOCKS")
	s2.warnCatchupGuardStopped(&catching)
	require.Equal(t, 1, cap2.countWarns(), "lingering catchup warns once an hour, not every skip")

	// Unknown state names itself.
	cap3 := &warnCapture{}
	s3 := &Server{logger: cap3}
	s3.warnCatchupGuardStopped(nil)
	require.Contains(t, cap3.joinedWarns(), "unknown(nil)")
}
