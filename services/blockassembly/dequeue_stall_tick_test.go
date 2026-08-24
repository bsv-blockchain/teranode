package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestSampleBlockAssemblerMetrics_DrivesTheStallSignal drives the metrics
// updater's tick directly, with the subtree processor mocked so all four
// sampled inputs are controllable.
//
// The tick is otherwise unreachable: the updater goroutine waits on a
// hard-coded time.After(5 * time.Second), so covering a two-minute repeat
// cadence through it would mean a two-minute test. Calling the method with an
// explicit now instead makes the whole sequence deterministic and instant.
//
// The server is assembled by hand rather than through setupServer because that
// helper runs Init, which starts the real updater goroutine - it would race
// this test for the mock and consume its expectations on its own schedule.
//
// What this adds over the pure observeDequeueStall table is the wiring: that
// staleness really is derived from LastDequeueTime rather than a fresh clock
// read, that depth comes from QueueLength, and that the state one tick returns
// is what the next tick consumes.
func TestSampleBlockAssemblerMetrics_DrivesTheStallSignal(t *testing.T) {
	// The gauges the tick publishes are created behind a sync.Once that New
	// normally drives; without this they are nil and the first Set panics.
	initPrometheusMetrics()

	stp := &subtreeprocessor.MockSubtreeProcessor{}
	// Without this an unmatched expectation panics in place instead of failing
	// the test, and because the real updater runs this tick in a detached
	// goroutine that panic takes the whole package binary down with it.
	stp.Test(t)

	// The tick's whole product is its log lines, and which line it picks is a
	// decision with its own logic - startup versus a wedged consumer - so the
	// choice is asserted rather than merely reached.
	logger := newCapturingLogger()

	ba := &BlockAssembly{
		logger:         logger,
		blockAssembler: &BlockAssembler{subtreeProcessor: stp},
	}

	// Sampled every tick, but plays no part in the stall decision.
	stp.On("TxCount").Return(uint64(42))
	stp.On("SubtreeCount").Return(7)

	// lastDequeue is the instant the consumer was last seen in its dequeue
	// branch. It stays put for the first three ticks - which is exactly what a
	// parked consumer looks like, since nothing else stamps it - and only moves
	// when the consumer resumes.
	lastDequeue := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	stp.On("LastDequeueTime").Return(lastDequeue).Times(4)
	stp.On("LastDequeueTime").Return(lastDequeue.Add(7 * time.Minute)).Once()
	// Tick 6 reads a timestamp fractionally ahead of its own now - see there.
	stp.On("LastDequeueTime").Return(lastDequeue.Add(8*time.Minute + time.Second)).Once()

	// The consumer is running throughout: this sequence is a wedge, not startup.
	stp.On("ConsumerStarted").Return(true)
	// ...and it is still there, which is what makes this a wedge rather than a
	// consumer that died and will never come back.
	stp.On("ConsumerExited").Return(false)
	// The branch holding the loop. The warning names it so the operator does
	// not have to go and find it, which is the whole reason it is sampled.
	stp.On("GetCurrentRunningState").Return(subtreeprocessor.StateMoveForwardBlock)

	// Deep queue for the first two ticks; the blocking handler drains it from
	// inside its own branch before the third.
	stp.On("QueueLength").Return(int64(10_000)).Twice()
	stp.On("QueueLength").Return(int64(0)).Times(4)

	var state dequeueStallState

	// Tick 1, within the threshold: quiet.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(10*time.Second))
	require.False(t, state.stalled, "a deep queue with a live consumer is normal, not a stall")
	require.Equal(t, float64(10), testutil.ToFloat64(prometheusBlockAssemblerDequeueStalenessSeconds),
		"the gauge is the artifact an operator reads, so it must carry the real gap between now and the last dequeue")
	require.Equal(t, float64(10_000), testutil.ToFloat64(prometheusBlockAssemblerQueuedTransactions),
		"depth must come from QueueLength, so the two can be read against each other")

	// Tick 2, past the threshold with work queued: the stall begins, backdated
	// to when the consumer actually stopped rather than to this tick.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(35*time.Second))
	require.True(t, state.stalled)
	require.Equal(t, lastDequeue, state.stalledSince,
		"staleness must be derived from LastDequeueTime, so backdating lands on the last dequeue exactly")
	require.False(t, state.beforeConsumerStarted, "a running consumer that stops dequeuing is a wedge, not startup")
	require.Equal(t, float64(35), testutil.ToFloat64(prometheusBlockAssemblerDequeueStalenessSeconds))
	require.True(t, logger.sawWarn("intake is growing unbounded"),
		"the rising edge of a genuine wedge warns immediately")
	require.True(t, logger.sawWarn(`state "moveForwardBlock"`),
		"the warning must name the select branch holding the loop, or it sends the operator looking for what it already knows")

	// Tick 3, queue drained from inside the blocking handler. The consumer has
	// not moved, so this is still the same stall.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(65*time.Second))
	require.True(t, state.stalled, "an empty queue must not be mistaken for a recovered consumer")
	require.Equal(t, lastDequeue, state.stalledSince, "the incident must keep its original start instant")
	require.Equal(t, float64(65), testutil.ToFloat64(prometheusBlockAssemblerDequeueStalenessSeconds),
		"the gauge must keep climbing while the consumer is parked, even with the queue drained from inside the blocking branch")

	// Tick 4, once the repeat cadence has elapsed since the rising edge: the
	// warning repeats, still on an empty queue, because the consumer is still
	// parked. A stall that goes quiet after one line would be worse than no
	// signal - it reads as resolved.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(35*time.Second+dequeueStallWarnRepeat))
	require.True(t, state.stalled)
	require.Equal(t, lastDequeue.Add(35*time.Second+dequeueStallWarnRepeat), state.lastWarn,
		"the repeat must re-arm the cadence, so warnings keep coming at a fixed rate rather than once")

	// Tick 5, the consumer resumes: it stamps on every loop iteration, so
	// staleness collapses even though the queue is still empty.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(7*time.Minute))
	require.False(t, state.stalled, "a fresh dequeue timestamp is the only thing that ends a stall")
	require.True(t, logger.sawInfo("consumer recovered"),
		"a wedge that recovers must be reported as a recovery, not as a consumer that finally started")

	// Tick 6: now is sampled in the updater before the timestamp is read, so a
	// consumer that stamps in that window yields a negative gap. The gauge must
	// floor at zero - a dashboard claiming the consumer last ran in the future
	// is nonsense, and "it just ran" is the honest reading.
	state = ba.sampleBlockAssemblerMetrics(state, lastDequeue.Add(8*time.Minute))
	require.False(t, state.stalled)
	require.Equal(t, float64(0), testutil.ToFloat64(prometheusBlockAssemblerDequeueStalenessSeconds),
		"a negative gap must publish as zero, never as a negative staleness")

	require.True(t, logger.sawWarn("still stalled"),
		"a stall that goes quiet after one line reads as resolved, so the repeat cadence must warn too")
	require.False(t, logger.sawInfo("nothing is at risk"),
		"the drain at tick 3 empties the queue mid-incident, and severity must not follow it down - the latch is what keeps this a warning")

	stp.AssertExpectations(t)
}

// TestSampleBlockAssemblerMetrics_StartupIsNotReportedAsUnboundedGrowth pins the
// classification, which is the difference between a signal an operator trusts
// and one they learn to ignore.
//
// The updater goroutine starts in BlockAssembly.Init and gRPC ingest comes up in
// BlockAssembly.Start, but BlockAssembler.Start only reaches
// subtreeProcessor.Start after loadUnminedTransactions, which takes minutes on a
// busy node while AddTx is already enqueueing (BlockAssembler.go says so at the
// DrainQueue call). So a non-empty queue with a long-stale dequeue timestamp is
// the ordinary startup path on every restart. Reporting that as "intake is
// growing unbounded" at warning level, on every restart, would erode trust in
// the one signal this whole change exists to add.
//
// It is still reported, at info: a loadUnminedTransactions that never returns
// looks exactly like this, and suppressing the window outright would make that
// failure silent.
//
// The assertions that catch a regression here are the negative ones: drop the
// consumerStarted branch and this test sees the unbounded-growth warning, while
// every assertion in the sibling test still passes.
func TestSampleBlockAssemblerMetrics_StartupIsNotReportedAsUnboundedGrowth(t *testing.T) {
	initPrometheusMetrics()

	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.Test(t)

	// The tick's whole product is its log lines, and which line it picks is a
	// decision with its own logic - startup versus a wedged consumer - so the
	// choice is asserted rather than merely reached.
	logger := newCapturingLogger()

	ba := &BlockAssembly{
		logger:         logger,
		blockAssembler: &BlockAssembler{subtreeProcessor: stp},
	}

	stp.On("TxCount").Return(uint64(0))
	stp.On("SubtreeCount").Return(0)
	stp.On("QueueLength").Return(int64(120_000))

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// The constructor seeded the timestamp at start, and nothing has moved it
	// since, because the consumer does not exist yet.
	stp.On("LastDequeueTime").Return(start).Twice()
	stp.On("ConsumerStarted").Return(false).Twice()

	var state dequeueStallState

	// Tick 1, well past the threshold: reported, but as startup.
	state = ba.sampleBlockAssemblerMetrics(state, start.Add(45*time.Second))
	require.True(t, state.stalled, "the condition is real and must be tracked, so its repeat cadence and end are reported")
	require.True(t, state.beforeConsumerStarted, "the cause must be latched, because by the closing edge the consumer has started either way")
	require.False(t, logger.sawWarn("intake is growing unbounded"),
		"routine startup must not be reported as unbounded growth")
	require.True(t, logger.sawInfo("consumer has not started yet"))
	require.False(t, logger.sawInfo("queued for"),
		"on this path the duration is time since the subtree processor was constructed, not how long anything has been queued - saying otherwise overstates by minutes on a node that starts ingesting late in the reload")
	require.True(t, logger.sawInfo("subtree processor was created 45s ago"),
		"the line must say what the duration actually measures")

	// Tick 2, once the repeat cadence has elapsed: still startup, still info,
	// and it names the reload as the thing to suspect if it persists.
	state = ba.sampleBlockAssemblerMetrics(state, start.Add(45*time.Second+dequeueStallWarnRepeat))
	require.True(t, state.stalled)
	require.False(t, logger.sawWarn("still stalled"), "the repeat must stay at info while the consumer simply has not started")
	require.True(t, logger.sawInfo("still has not started"))

	// Tick 3: the consumer starts. SubtreeProcessor.Start re-seeds the
	// timestamp, so staleness collapses and the gap closes - and it must be
	// reported as the consumer arriving, not as a wedge that recovered, because
	// nothing was ever wedged.
	stp.On("LastDequeueTime").Return(start.Add(5 * time.Minute)).Once()
	stp.On("ConsumerStarted").Return(true).Once()

	state = ba.sampleBlockAssemblerMetrics(state, start.Add(5*time.Minute+time.Second))
	require.False(t, state.stalled)
	require.True(t, logger.sawInfo("consumer started after"),
		"the consumer arriving is not a recovery - nothing was ever wedged")
	require.False(t, logger.sawInfo("consumer recovered"),
		"reporting startup as a recovery would imply an incident that never happened")

	stp.AssertExpectations(t)
}

// TestSampleBlockAssemblerMetrics_ReadsConsumerStartedBeforeStaleness pins the
// order the tick reads its two lifecycle inputs in, which is load-bearing and
// silently reversible.
//
// SubtreeProcessor.Start stores the re-seeded dequeue timestamp and then sets
// consumerStarted, in that order. Go's atomics are sequentially consistent, so
// a reader that loads the flag first and sees true is guaranteed to see the
// re-seed on the following load. Load them the other way round and one
// interleaving lies: the pre-Start timestamp paired with the post-Start flag,
// which classifies a routine restart as a wedged consumer and warns "intake is
// growing unbounded" - the false alarm the whole classification exists to
// prevent.
//
// That interleaving is a two-instruction window that happens at most once per
// process, so it cannot be provoked from a test. The order itself can be, and
// it is the only thing a regression would change: swap the two reads back and
// this fails, while every behavioural test in this file still passes.
func TestSampleBlockAssemblerMetrics_ReadsConsumerStartedBeforeStaleness(t *testing.T) {
	initPrometheusMetrics()

	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.Test(t)

	ba := &BlockAssembly{
		logger:         newCapturingLogger(),
		blockAssembler: &BlockAssembler{subtreeProcessor: stp},
	}

	stp.On("TxCount").Return(uint64(0))
	stp.On("SubtreeCount").Return(0)
	stp.On("QueueLength").Return(int64(0))
	stp.On("LastDequeueTime").Return(time.Now())
	stp.On("ConsumerStarted").Return(true)

	ba.sampleBlockAssemblerMetrics(dequeueStallState{}, time.Now())

	consumerStarted, lastDequeue := -1, -1

	for i, call := range stp.Calls {
		switch call.Method {
		case "ConsumerStarted":
			if consumerStarted == -1 {
				consumerStarted = i
			}
		case "LastDequeueTime":
			if lastDequeue == -1 {
				lastDequeue = i
			}
		}
	}

	require.NotEqual(t, -1, consumerStarted, "the tick must sample ConsumerStarted, or it cannot tell startup from a wedge at all")
	require.NotEqual(t, -1, lastDequeue, "the tick must sample LastDequeueTime, or there is no staleness to report")
	require.Less(t, consumerStarted, lastDequeue,
		"ConsumerStarted must be read before LastDequeueTime, so a true reading guarantees the re-seeded timestamp is visible and a restart is never reported as a wedge")
}

// TestSampleBlockAssemblerMetrics_DepartedConsumerIsNotReportedAsAWedge pins
// the third lifecycle case, which the wedge message actively misdirects on.
//
// A panic in the dequeue branch is not covered by runHandlerWithRecover, so it
// unwinds to the goroutine's deferred close: the consumer is gone, the service
// stays up, and nothing will ever drain the queue again for the lifetime of the
// process. Staleness climbs exactly as it does for a wedge, so without this
// classification the operator is told to go and find which select branch is
// occupying a loop that no longer exists - the one thing that is not happening.
//
// The assertion that regresses is the negative one: delete the ConsumerExited
// arm and this sees the unbounded-growth warning instead, while every other
// test in this file still passes.
func TestSampleBlockAssemblerMetrics_DepartedConsumerIsNotReportedAsAWedge(t *testing.T) {
	initPrometheusMetrics()

	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.Test(t)

	logger := newCapturingLogger()

	ba := &BlockAssembly{
		logger:         logger,
		blockAssembler: &BlockAssembler{subtreeProcessor: stp},
	}

	stp.On("TxCount").Return(uint64(0))
	stp.On("SubtreeCount").Return(0)
	stp.On("QueueLength").Return(int64(80_000))

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// The consumer ran, then died. The timestamp froze where it fell.
	stp.On("LastDequeueTime").Return(start)
	stp.On("ConsumerStarted").Return(true)
	stp.On("ConsumerExited").Return(true)

	var state dequeueStallState

	state = ba.sampleBlockAssemblerMetrics(state, start.Add(45*time.Second))

	require.True(t, state.stalled)
	require.True(t, logger.sawError("consumer has exited and will not restart"),
		"a consumer that will never come back is a dead service, not a slow one, and the level has to say so")
	require.False(t, logger.sawWarn("intake is growing unbounded"),
		"the wedge message sends the operator after the select branch holding the loop, and here there is no loop")

	stp.AssertExpectations(t)
}

// TestSampleBlockAssemblerMetrics_SeveritySplitsOnWorkNotOnStaleness pins the
// answer to the two things that pull in opposite directions here.
//
// The incident has to open on staleness alone, because depth is the
// untrustworthy half: the handlers that block the consumer drain the queue from
// inside the branch that is blocking it, so a tick landing on zero would
// otherwise decline to open an incident that is already minutes old. But
// opening on staleness alone means a large moveForwardBlock on a quiet node
// trips the threshold legitimately, and warning "intake is growing unbounded"
// at an operator on a routine block is how a new signal gets ignored.
//
// So the incident opens either way and the level follows whether work has ever
// stacked up behind the consumer during it - latched, not sampled, so the same
// mid-incident drain cannot walk it back down.
func TestSampleBlockAssemblerMetrics_SeveritySplitsOnWorkNotOnStaleness(t *testing.T) {
	initPrometheusMetrics()

	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.Test(t)

	logger := newCapturingLogger()

	ba := &BlockAssembly{
		logger:         logger,
		blockAssembler: &BlockAssembler{subtreeProcessor: stp},
	}

	stp.On("TxCount").Return(uint64(0))
	stp.On("SubtreeCount").Return(0)
	stp.On("ConsumerStarted").Return(true)
	stp.On("ConsumerExited").Return(false)
	stp.On("GetCurrentRunningState").Return(subtreeprocessor.StateMoveForwardBlock)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stp.On("LastDequeueTime").Return(start)

	// Tick 1: a long block move with nothing queued behind it. Reported, so a
	// handler holding the loop is never silent, but nothing is at risk.
	stp.On("QueueLength").Return(int64(0)).Once()

	var state dequeueStallState

	state = ba.sampleBlockAssemblerMetrics(state, start.Add(45*time.Second))
	require.True(t, state.stalled, "the incident opens on staleness alone - depth must not decide whether there is anything to report")
	require.False(t, state.sawQueuedWork)
	require.False(t, logger.sawWarn("intake is growing unbounded"),
		"a long handler on an empty queue is not unbounded growth, and warning as though it were is how the signal gets ignored")
	require.True(t, logger.sawInfo("nothing is at risk"))
	require.True(t, logger.sawInfo(`state "moveForwardBlock"`),
		"even the quiet line names the branch, since that is the whole diagnostic")

	// Tick 2: the same handler is still holding the loop, and now transactions
	// are stacking up behind it. That is issue #1429, and it must escalate.
	stp.On("QueueLength").Return(int64(50_000)).Once()

	state = ba.sampleBlockAssemblerMetrics(state, start.Add(45*time.Second+dequeueStallWarnRepeat))
	require.True(t, state.sawQueuedWork, "work arriving mid-incident must latch, or the escalation is lost on the next drain")
	require.True(t, logger.sawWarn("still stalled"),
		"once work is stacking up behind a consumer that is not dequeuing, the incident is the one this signal exists for")

	stp.AssertExpectations(t)
}
