package blockassembly

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stallBase is an arbitrary fixed instant. Every case below is expressed as an
// offset from it, so the table never touches the wall clock and the
// arithmetic assertions are exact rather than approximate.
var stallBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestObserveDequeueStall_Transitions table-tests the whole stall state
// machine that used to live inline in the metrics-updater goroutine, where
// nothing could reach it: it was an anonymous closure driven by a hard-coded
// time.After(5 * time.Second), so exercising a two-minute repeat cadence
// meant a two-minute test.
//
// The case that matters most is drainedMidStallMustNotReportRecovery. The
// original form keyed both edges on stalledNow, which is
// `queueLength > 0 && staleness > threshold`, so queueLength == 0 alone
// satisfied the exit - and the queue is drained from inside the very handlers that
// block the consumer (reorgBlocks and moveForwardBlock both call
// SubtreeProcessor.dequeueDuringBlockMovement, which does not stamp
// lastDequeueMillis). A long moveForwardBlock would therefore empty its own
// queue partway through and the next tick would log "consumer recovered"
// with the consumer still parked and the gauge still climbing. Worse, it
// cleared the stalled flag, discarding the repeat cadence, so one incident
// reported as a series of short flapping fragments and anything grepping for
// "recovered" got a false all-clear.
//
// Depth has since left the entry condition too, for the same reason it left
// the exit: the tick that lands just after that in-branch drain reads zero in
// the middle of the failure this exists for. It survives only as the input to
// the sawQueuedWork latch, which picks how loudly the caller reports.
func TestObserveDequeueStall_Transitions(t *testing.T) {
	stalledState := func(sinceOffset, warnOffset time.Duration) dequeueStallState {
		return dequeueStallState{
			stalled:      true,
			stalledSince: stallBase.Add(sinceOffset),
			lastWarn:     stallBase.Add(warnOffset),
		}
	}

	tests := []struct {
		name  string
		state dequeueStallState
		now   time.Time
		// queueLength never gates entry or exit. It only feeds the
		// sawQueuedWork latch, which decides how loudly the caller reports.
		queueLength int64
		staleness   time.Duration
		wantEvent   dequeueStallEvent
		wantStalled bool
		// wantSawQueuedWork is only checked when wantStalled is true.
		wantSawQueuedWork bool
		// beforeConsumerStarted drives the consumerStarted input. Zero value
		// means the consumer had started, which is the case for every wedge.
		beforeConsumerStarted bool
		// wantFor is only meaningful for dequeueStallEnded.
		wantFor time.Duration
		// wantSince is only checked when wantStalled is true.
		wantSince time.Time
	}{
		{
			name:        "quietQueueIsNotAStall",
			now:         stallBase,
			queueLength: 0,
			staleness:   time.Second,
			wantEvent:   dequeueStallNone,
		},
		{
			name: "deepQueueWithLiveConsumerIsNotAStall",
			// The whole point of the signal: at Teranode's target rates a
			// deep queue is normal and self-correcting. Only a queue the
			// consumer is not touching is wrong.
			now:         stallBase,
			queueLength: 500_000,
			staleness:   time.Second,
			wantEvent:   dequeueStallNone,
		},
		{
			// Depth no longer gates entry. It cannot: the handlers that block
			// the consumer drain the queue from inside the branch that blocks
			// it, so a zero reading is as likely to mean "the wedge is draining
			// its own backlog" as "there is nothing to drain". The caller
			// reports this one at info precisely because nothing is
			// accumulating - see BlockAssembly.reportDequeueStall.
			name:        "staleConsumerWithEmptyQueueStillBeginsAStall",
			now:         stallBase,
			staleness:   5 * time.Minute,
			wantEvent:   dequeueStallBegan,
			wantStalled: true,
			wantSince:   stallBase.Add(-5 * time.Minute),
		},
		{
			name:        "atThresholdExactlyIsNotYetAStall",
			now:         stallBase,
			queueLength: 1,
			staleness:   dequeueStallThreshold,
			wantEvent:   dequeueStallNone,
		},
		{
			name:              "risingEdgeBackdatesToWhenTheConsumerStopped",
			now:               stallBase.Add(time.Hour),
			queueLength:       10_000,
			staleness:         dequeueStallThreshold + time.Second,
			wantEvent:         dequeueStallBegan,
			wantStalled:       true,
			wantSawQueuedWork: true,
			// Not `now`: the stall began when the consumer stopped, which is
			// at least the threshold plus up to one tick earlier.
			wantSince: stallBase.Add(time.Hour).Add(-(dequeueStallThreshold + time.Second)),
		},
		{
			name:        "repeatIsSuppressedBeforeTheCadenceElapses",
			state:       stalledState(0, 0),
			now:         stallBase.Add(dequeueStallWarnRepeat - time.Second),
			staleness:   5 * time.Minute,
			wantEvent:   dequeueStallNone,
			wantStalled: true,
			wantSince:   stallBase,
		},
		{
			name:        "repeatFiresOnceTheCadenceElapses",
			state:       stalledState(0, 0),
			now:         stallBase.Add(dequeueStallWarnRepeat),
			staleness:   5 * time.Minute,
			wantEvent:   dequeueStallContinues,
			wantStalled: true,
			wantSince:   stallBase,
		},
		{
			name: "drainedMidStallMustNotReportRecovery",
			// moveForwardBlock entered at stallBase with 10k queued; its in-branch
			// drain empties the queue while the handler grinds on through
			// processCoinbaseUtxos/finalizeBlockProcessing for minutes. The
			// consumer has not moved, so this is still one stall.
			state:       stalledState(0, 0),
			now:         stallBase.Add(65 * time.Second),
			staleness:   65 * time.Second,
			wantEvent:   dequeueStallNone,
			wantStalled: true,
			wantSince:   stallBase,
		},
		{
			name: "drainedMidStallKeepsTheOriginalStartInstant",
			// The follow-on damage from the old behaviour: clearing the flag
			// discarded the cadence, so the next non-empty tick re-fired the
			// rising edge and reset stalledSince, chopping one long incident
			// into fragments. stalledSince must survive the drain untouched.
			state:       stalledState(0, 0),
			now:         stallBase.Add(dequeueStallWarnRepeat + time.Minute),
			staleness:   dequeueStallWarnRepeat + time.Minute,
			wantEvent:   dequeueStallContinues,
			wantStalled: true,
			wantSince:   stallBase,
		},
		{
			name: "recoveryRequiresTheConsumerToActuallyMove",
			// Only a fresh stamp ends the stall. The consumer stamps on every
			// loop iteration, including empty ones, so a low staleness really
			// does mean it is running.
			state:       stalledState(0, 0),
			now:         stallBase.Add(10 * time.Minute),
			staleness:   time.Second,
			wantEvent:   dequeueStallEnded,
			wantStalled: false,
			// The consumer resumed one second before this tick saw it, so the
			// incident ran to stallBase+10m-1s, not to the tick instant.
			wantFor: 10*time.Minute - time.Second,
		},
		{
			name: "recoveryWithWorkStillQueuedStillCounts",
			// The consumer resumed and is working through a backlog. That is
			// the healthy deep-queue case, not a stall.
			state:       stalledState(0, 0),
			now:         stallBase.Add(3 * time.Minute),
			staleness:   time.Second,
			wantEvent:   dequeueStallEnded,
			wantStalled: false,
			wantFor:     3*time.Minute - time.Second,
		},
		{
			// The startup window is still reported - a queue nobody is draining
			// matters either way, and a loadUnminedTransactions that never
			// returns looks exactly like this - but it is latched as startup so
			// it can be reported as such rather than as unbounded growth.
			name:                  "startupGapIsReportedButMarkedAsStartup",
			now:                   stallBase.Add(45 * time.Second),
			queueLength:           10_000,
			staleness:             45 * time.Second,
			beforeConsumerStarted: true,
			wantEvent:             dequeueStallBegan,
			wantStalled:           true,
			wantSawQueuedWork:     true,
			wantSince:             stallBase,
		},
		{
			name:        "recoveryAtThresholdExactly",
			state:       stalledState(0, 0),
			now:         stallBase.Add(time.Minute),
			staleness:   dequeueStallThreshold,
			wantEvent:   dequeueStallEnded,
			wantStalled: false,
			// The worst case for the correction: recovery is only detectable
			// once staleness has fallen to the threshold, so a full threshold's
			// worth of running time would otherwise be counted as stalled.
			wantFor: time.Minute - dequeueStallThreshold,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, event, stalledFor := observeDequeueStall(tt.state, tt.now, tt.queueLength, tt.staleness, !tt.beforeConsumerStarted)

			require.Equal(t, tt.wantEvent, event, "event")
			require.Equal(t, tt.wantStalled, got.stalled, "stalled flag")

			if tt.wantStalled {
				require.Equal(t, tt.wantSince, got.stalledSince,
					"stalledSince must only ever be set on the rising edge, so one incident reports as one incident")
				require.Equal(t, tt.beforeConsumerStarted, got.beforeConsumerStarted,
					"the cause must be latched on the rising edge, since by the closing edge the consumer has necessarily started and the live reading can no longer tell startup from a wedge")
				require.Equal(t, tt.wantSawQueuedWork, got.sawQueuedWork,
					"severity must follow whether work has queued at any point in the incident, not this tick's depth")
			}

			if tt.wantEvent == dequeueStallEnded {
				require.Equal(t, tt.wantFor, stalledFor, "reported stall duration")
			}
		})
	}
}

// TestObserveDequeueStall_IncidentIsReportedAsOneIncident walks a whole
// move-forward-block incident tick by tick, at the real 5-second cadence the
// updater uses, and asserts the operator sees exactly one beginning, one end,
// and an end-to-end duration covering the entire stall.
//
// This is the regression the transition table proves in pieces; driving the
// sequence catches state that is individually plausible but does not compose
// - in particular a mid-stall drain resetting stalledSince, which no single
// transition assertion can see. The same drain is now also the case that would
// silently downgrade the incident to info if severity were read per tick
// instead of latched, so this asserts the latch survives it.
func TestObserveDequeueStall_IncidentIsReportedAsOneIncident(t *testing.T) {
	const tick = 5 * time.Second

	// The consumer parks at stallBase. The queue is drained from inside the
	// blocking handler at stallBase+60s, and the consumer only resumes at
	// stallBase+7m. Staleness climbs throughout, because nothing stamps
	// lastDequeueMillis until the consumer reaches its dequeue branch again.
	const (
		drainAt  = time.Minute
		resumeAt = 7 * time.Minute
	)

	var (
		state      dequeueStallState
		began      int
		ended      int
		continued  int
		reportedAs time.Duration
	)

	for elapsed := time.Duration(0); elapsed <= resumeAt+tick; elapsed += tick {
		var (
			queueLength = int64(10_000)
			staleness   = elapsed
		)

		if elapsed >= drainAt {
			queueLength = 0
		}

		if elapsed >= resumeAt {
			// The consumer is back in its dequeue branch, stamping on every
			// iteration, so staleness collapses to under one tick.
			staleness = time.Second
		}

		var (
			event      dequeueStallEvent
			stalledFor time.Duration
		)

		state, event, stalledFor = observeDequeueStall(state, stallBase.Add(elapsed), queueLength, staleness, true)

		if state.stalled {
			require.True(t, state.sawQueuedWork,
				"the incident opened with 10k queued, so it must stay a warning even on the ticks after the blocking handler drained its own queue")
		}

		switch event {
		case dequeueStallBegan:
			began++
		case dequeueStallContinues:
			continued++
		case dequeueStallEnded:
			ended++
			reportedAs = stalledFor
		case dequeueStallNone:
		}
	}

	require.Equal(t, 1, began, "one incident must report one beginning, not one per drain/refill cycle")
	require.Equal(t, 1, ended, "one incident must report exactly one recovery, and only when the consumer truly resumed")
	require.Equal(t, 3, continued,
		"a 7-minute stall must repeat on the 2-minute cadence throughout, including while the queue sits at zero")

	// Both ends are dated from the consumer, not from the tick that noticed.
	// The stall opens on the first tick where staleness exceeds the threshold -
	// stallBase+35s - and backdates by that staleness to stallBase+0s. Recovery
	// is seen at stallBase+7m with a second of staleness still on the clock, so
	// the consumer resumed at stallBase+7m-1s. The whole incident, not a
	// fragment, and not padded at either end.
	require.Equal(t, resumeAt-time.Second, reportedAs,
		"the reported duration must cover the whole incident, from when the consumer stopped to when it resumed")
	require.False(t, state.stalled)
}

// TestObserveDequeueStall_StartupThatBecomesAWedgeIsReportedAsAWedge covers the
// one sequence where the startup classification would otherwise outlive its
// truth.
//
// BlockAssembler.Start loads unmined transactions with gRPC ingest already
// accepting, so by the time the consumer starts there is a backlog waiting for
// it. A consumer that dives into that backlog and jams - in the subtree store,
// say - has started, so the incident is a wedge from that moment on, and the
// repeat warnings say so because they read the flag live. The closing line reads
// the carried value instead, and without clearing it would credit the startup
// sequence for the whole thing: "consumer started after 12m and is now
// dequeuing", for an incident that was a wedge for eleven of those minutes.
//
// The clearing is safe precisely because an ordinary startup gap cannot reach
// it: SubtreeProcessor.Start re-seeds the dequeue timestamp in the same breath
// as setting the flag, so the first tick to see the flag true also sees low
// staleness and ends the incident. Both halves are asserted here - the ordinary
// startup gap still closes as startup, and this one does not.
func TestObserveDequeueStall_StartupThatBecomesAWedgeIsReportedAsAWedge(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("startup that gives way to a wedge closes as a recovery", func(t *testing.T) {
		var state dequeueStallState

		// The reload is still running: queue filling, consumer not yet started.
		state, event, _ := observeDequeueStall(state, base, 10_000, time.Minute, false)
		require.Equal(t, dequeueStallBegan, event)
		require.True(t, state.beforeConsumerStarted, "a gap that opens before the consumer exists is startup")

		// The consumer has started, and is still not dequeuing. It cannot be
		// startup any more.
		state, _, _ = observeDequeueStall(state, base.Add(time.Minute), 10_000, 2*time.Minute, true)
		require.False(t, state.beforeConsumerStarted,
			"once the consumer exists and still is not dequeuing, the incident is a wedge and must stop being reported as startup")

		// It recovers. The closing edge must call this a recovery.
		state, event, stalledFor := observeDequeueStall(state, base.Add(10*time.Minute), 10_000, time.Second, true)
		require.Equal(t, dequeueStallEnded, event)
		require.False(t, state.stalled)
		require.Positive(t, stalledFor)
	})

	t.Run("a reload that hangs for many ticks still closes as startup", func(t *testing.T) {
		// The guard on the clearing arm is what this pins. A hanging
		// loadUnminedTransactions is the one failure mode of the pre-start
		// window, and it is the only way an incident racks up repeat ticks with
		// the consumer still absent. Clearing unconditionally rather than only
		// when the consumer has started would reclassify exactly this case, so a
		// reload that eventually completed would close with "recovered, was
		// stalled for 20m" - blaming a consumer that had not existed for any of
		// it.
		var state dequeueStallState

		state, event, _ := observeDequeueStall(state, base, 10_000, time.Minute, false)
		require.Equal(t, dequeueStallBegan, event)

		for tick := 1; tick <= 40; tick++ {
			elapsed := time.Duration(tick) * 30 * time.Second
			state, _, _ = observeDequeueStall(state, base.Add(elapsed), 10_000, time.Minute+elapsed, false)
			require.True(t, state.beforeConsumerStarted,
				"a stall the consumer has still not started into stays startup, however long the reload hangs")
		}

		// The reload finally completes and the consumer starts, re-seeding the
		// timestamp.
		captured := state.beforeConsumerStarted

		state, event, _ = observeDequeueStall(state, base.Add(21*time.Minute), 10_000, time.Second, true)
		require.Equal(t, dequeueStallEnded, event)
		require.True(t, captured,
			"the reported cause must be the reload, not a consumer that was absent for the whole incident")
	})

	t.Run("an ordinary startup gap still closes as startup", func(t *testing.T) {
		var state dequeueStallState

		state, event, _ := observeDequeueStall(state, base, 10_000, 5*time.Minute, false)
		require.Equal(t, dequeueStallBegan, event)
		require.True(t, state.beforeConsumerStarted)

		// The consumer starts, which re-seeds the timestamp, so the very tick
		// that first sees the flag true also sees low staleness. The clearing
		// arm is unreachable on this path, which is why it is safe.
		captured := state.beforeConsumerStarted

		state, event, _ = observeDequeueStall(state, base.Add(5*time.Minute), 10_000, time.Second, true)
		require.Equal(t, dequeueStallEnded, event)
		require.True(t, captured,
			"the value the caller reports with must still say startup, or every restart closes as a recovery from an incident that never happened")
		require.False(t, state.stalled)
	})
}
