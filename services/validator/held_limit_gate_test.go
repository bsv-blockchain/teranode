package validator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/stretchr/testify/require"
)

type countingReader struct {
	inner *fakeReader
	reads atomic.Int64
}

func (c *countingReader) GetBlockAssemblyQueueStats(ctx context.Context) (blockassembly.QueueStats, error) {
	c.reads.Add(1)

	return c.inner.GetBlockAssemblyQueueStats(ctx)
}

func newPollingGate(t *testing.T, reader queueStatsReader) *heldLimitGate {
	t.Helper()

	gate := newHeldLimitGate(t.Context(), testBackpressureConfig(), reader)
	t.Cleanup(gate.stop)

	return gate
}

func TestHeldLimitGate_PollFlipsAtLimitFromTheStats(t *testing.T) {
	reader := &fakeReader{}
	reader.setHeld(10, 100, 0)

	gate := newPollingGate(t, reader)

	require.False(t, gate.atLimit(), "the first call starts the poll loop and has no reading yet")

	reader.setHeld(100, 100, 0)

	require.Eventually(t, gate.atLimit, 2*time.Second, time.Millisecond, "a held count equal to the limit is at the limit")

	held, limit := gate.held()
	require.Equal(t, int64(100), held)
	require.Equal(t, int64(100), limit)

	reader.setHeld(99, 100, 0)

	require.Eventually(t, func() bool { return !gate.atLimit() }, 2*time.Second, time.Millisecond, "a block connected and the count fell below the limit")
}

func TestHeldLimitGate_NoReportedLimitIsNeverAtLimit(t *testing.T) {
	reader := &fakeReader{}
	reader.setHeld(1_000_000, 0, 0)

	gate := newPollingGate(t, reader)

	require.False(t, gate.atLimit())
	time.Sleep(20 * time.Millisecond)
	require.False(t, gate.atLimit(), "a reported limit of 0 means block assembly applies none")
}

func TestHeldLimitGate_FailsOpenWhenTheReadFails(t *testing.T) {
	reader := &fakeReader{}
	reader.setHeld(100, 100, 0)

	gate := newPollingGate(t, reader)

	require.False(t, gate.atLimit())
	require.Eventually(t, gate.atLimit, 2*time.Second, time.Millisecond)

	reader.set(0, errors.NewServiceError("block assembly unreachable"))

	require.Eventually(t, func() bool { return !gate.atLimit() }, 2*time.Second, time.Millisecond, "a dark signal must never shed")
}

func TestHeldLimitGate_AFailedReadKeepsThePollCadenceWhileTheReadingIsFresh(t *testing.T) {
	reader := &fakeReader{}
	reader.set(0, errors.NewServiceError("block assembly busy"))

	gate := newPollingGate(t, reader)
	gate.once.Do(func() {}) // the test drives poll itself, so no background poller runs

	base := time.Unix(9000, 0)
	now := base
	gate.now = func() time.Time { return now }

	gate.observe(blockassembly.QueueStats{HeldCount: 100, MaxHeld: 100})

	now = base.Add(3 * gate.interval)
	require.Equal(t, gate.interval, gate.poll(t.Context()), "one failed read does not idle the poll for ten seconds")
	require.True(t, gate.atLimit(), "the reading is still fresh")

	now = base.Add(time.Duration(heldGateFreshnessPolls+1) * gate.interval)
	require.Equal(t, heldGateIdleInterval, gate.poll(t.Context()), "with no reading inside the window the poll idles")
	require.False(t, gate.atLimit(), "a dark signal never sheds")
}

func TestHeldLimitGate_AReadingGoesStale(t *testing.T) {
	gate := newPollingGate(t, &fakeReader{})

	base := time.Unix(9000, 0)
	now := base
	gate.now = func() time.Time { return now }

	gate.observe(blockassembly.QueueStats{HeldCount: 100, MaxHeld: 100})
	require.True(t, gate.atLimit())

	now = base.Add(time.Duration(heldGateFreshnessPolls)*gate.interval + time.Nanosecond)
	require.False(t, gate.atLimit(), "a reading older than the freshness window is not trusted")
}

func TestHeldLimitGate_AnObservationSavesTheOwnRead(t *testing.T) {
	inner := &fakeReader{}
	inner.setHeld(0, 100, 0)

	reader := &countingReader{inner: inner}
	gate := newPollingGate(t, reader)
	gate.interval = time.Hour

	gate.observe(blockassembly.QueueStats{HeldCount: 100, MaxHeld: 100})

	require.Equal(t, time.Hour, gate.poll(t.Context()), "a fresh observation covers the interval")
	require.Equal(t, int64(0), reader.reads.Load(), "the stats are read once, by the observer, not twice")
	require.True(t, gate.atLimit())
}

func TestHeldLimitGate_NilGateIsNeverAtLimit(t *testing.T) {
	var gate *heldLimitGate

	require.False(t, gate.atLimit())

	gate.observe(blockassembly.QueueStats{HeldCount: 1, MaxHeld: 1})
	gate.stop()
}

func TestHeldLimitGate_BackpressureControllerFeedsTheGate(t *testing.T) {
	reader := &fakeReader{}
	reader.setHeld(100, 100, 0)

	gate := newPollingGate(t, &countingReader{inner: &fakeReader{}})

	c := newTestController(reader, &fakeConsumer{})
	c.onStats = gate.observe

	c.tick(t.Context())

	require.True(t, gate.atLimit(), "a reading the controller took reaches the gate")
}
