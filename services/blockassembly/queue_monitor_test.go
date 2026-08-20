package blockassembly

// Tests for the shared block-assembly queue monitor that backs the
// blockassembly_max_queued_transactions gate in propagation (ingress shed) and
// the validator (pre-validation shed).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestQueueMonitorTracksQueueDepth drives the poller against a mocked block
// assembly client: the cached verdict follows the reported queue depth, a
// couple of poll errors keep the last verdict (no flapping on a blip), and
// persistent failure fails OPEN so a crash-looping block assembly cannot leave
// every ingest path shedding on a stale verdict.
func TestQueueMonitorTracksQueueDepth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baMock := NewMock()
	baMock.On("GetQueueLength", mock.Anything).Return(int64(101), nil).Once()
	baMock.On("GetQueueLength", mock.Anything).Return(int64(0), errors.NewServiceError("block assembly unavailable"))

	m := NewQueueMonitor(ctx, ulogger.TestLogger{}, baMock, 100)
	require.NotNil(t, m)

	require.Eventually(t, m.Overloaded, 5*time.Second, 50*time.Millisecond, "monitor must flag the queue as overloaded")

	// A single failed poll keeps the last verdict (no flapping on a blip)…
	time.Sleep(2 * QueueMonitorInterval)
	require.True(t, m.Overloaded(), "early poll errors must keep the last verdict")

	// …but persistent failure crosses queueMonitorFailOpenAfter and clears it.
	require.Eventually(t, func() bool {
		return !m.Overloaded()
	}, (queueMonitorFailOpenAfter+3)*QueueMonitorInterval, 100*time.Millisecond, "persistent poll failure must fail open")
}

// TestQueueMonitorClearsWhenQueueDrains verifies the verdict flips back once
// the queue is under the limit again.
func TestQueueMonitorClearsWhenQueueDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baMock := NewMock()
	baMock.On("GetQueueLength", mock.Anything).Return(int64(500), nil).Once()
	baMock.On("GetQueueLength", mock.Anything).Return(int64(7), nil)

	m := NewQueueMonitor(ctx, ulogger.TestLogger{}, baMock, 100)
	require.NotNil(t, m)

	require.Eventually(t, m.Overloaded, 5*time.Second, 50*time.Millisecond, "queue over the limit must set the verdict")

	require.Eventually(t, func() bool {
		return !m.Overloaded()
	}, 5*time.Second, 50*time.Millisecond, "a drained queue must clear the verdict")
}

// TestQueueMonitorDisabled pins the nil-monitor contract: with the limit unset
// (or no client) no poller runs and the nil-safe verdict is always false, so
// callers can consult it unconditionally.
func TestQueueMonitorDisabled(t *testing.T) {
	ctx := context.Background()

	baMock := NewMock()

	require.Nil(t, NewQueueMonitor(ctx, ulogger.TestLogger{}, baMock, 0), "limit 0 disables the monitor")
	require.Nil(t, NewQueueMonitor(ctx, ulogger.TestLogger{}, baMock, -1), "a negative limit disables the monitor")
	require.Nil(t, NewQueueMonitor(ctx, ulogger.TestLogger{}, nil, 100), "no client disables the monitor")

	var disabled *QueueMonitor

	require.False(t, disabled.Overloaded(), "a disabled monitor never sheds")

	baMock.AssertNotCalled(t, "GetQueueLength", mock.Anything)
}

// TestQueueMonitorStopsWithContext verifies the poller exits with its context
// rather than running for the process lifetime.
func TestQueueMonitorStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Count polls in an atomic rather than reading the mock's call log: the
	// monitor goroutine is writing that log concurrently.
	var polls atomic.Int64

	baMock := NewMock()
	baMock.On("GetQueueLength", mock.Anything).
		Run(func(mock.Arguments) { polls.Add(1) }).
		Return(int64(1), nil)

	m := NewQueueMonitor(ctx, ulogger.TestLogger{}, baMock, 100)
	require.NotNil(t, m)

	require.Eventually(t, func() bool {
		return polls.Load() > 0
	}, 5*time.Second, 50*time.Millisecond, "monitor must poll while running")

	cancel()

	time.Sleep(2 * QueueMonitorInterval)

	before := polls.Load()

	time.Sleep(2 * QueueMonitorInterval)

	require.Equal(t, before, polls.Load(), "a cancelled monitor must stop polling")
}
