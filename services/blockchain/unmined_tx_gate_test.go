package blockchain

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// gateInState returns a gate whose client reports state on every read.
func gateInState(t *testing.T, state FSMStateType, route string) *UnminedTxGate {
	t.Helper()

	client := &Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

	return NewUnminedTxGate(ulogger.TestLogger{}, client, nil, route)
}

// gateSettings returns settings whose network has one checkpoint, at height
// 100, and whose UTXO store URL has the given scheme.
func gateSettings(t *testing.T, scheme string) *settings.Settings {
	t.Helper()

	return &settings.Settings{
		ChainCfgParams: &chaincfg.Params{Checkpoints: []chaincfg.Checkpoint{{Height: 100}}},
		UtxoStore:      settings.UtxoStoreSettings{UtxoStore: &url.URL{Scheme: scheme, Host: "localhost"}},
	}
}

// fsmRead is what a test client returns from GetFSMCurrentState.
func fsmRead(client *Mock, state *FSMStateType) {
	if state == nil {
		client.On("GetFSMCurrentState", mock.Anything).Return((*FSMStateType)(nil), errors.NewServiceError("blockchain down"))
		return
	}

	client.On("GetFSMCurrentState", mock.Anything).Return(state, nil)
}

// TestUnminedTxGate_RefusesOnlyWhileCatchingBlocks pins the rule from issue
// 2003: CATCHINGBLOCKS refuses, RUNNING accepts, and IDLE accepts because a
// cached IDLE is what a healthy node at the tip reads while the blockchain
// service restarts.
func TestUnminedTxGate_RefusesOnlyWhileCatchingBlocks(t *testing.T) {
	for _, tt := range []struct {
		state   FSMStateType
		refuses bool
	}{
		{FSMStateCATCHINGBLOCKS, true},
		{FSMStateRUNNING, false},
		{FSMStateIDLE, false},
	} {
		t.Run(tt.state.String(), func(t *testing.T) {
			gate := gateInState(t, tt.state, "test_state_"+tt.state.String())
			before := gate.RefusedTotal()

			require.Equal(t, tt.refuses, gate.Refuses(context.Background(), &chainhash.Hash{}))

			want := before
			if tt.refuses {
				want++
			}

			require.InDelta(t, want, gate.RefusedTotal(), 0)
		})
	}
}

// TestUnminedTxGate_StateReadFailureAccepts covers the other unknown: a failed
// read says nothing about a sync, so it must not drop the transaction.
func TestUnminedTxGate_StateReadFailureAccepts(t *testing.T) {
	client := &Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return((*FSMStateType)(nil), errors.NewServiceError("blockchain down"))

	gate := NewUnminedTxGate(ulogger.TestLogger{}, client, nil, "test_read_failure")

	require.False(t, gate.Refuses(context.Background(), nil))
	require.InDelta(t, 0.0, gate.RefusedTotal(), 0)
}

// TestUnminedTxGate_NilAccepts keeps services built without a blockchain
// client, and struct-literal servers in tests, working unchanged.
func TestUnminedTxGate_NilAccepts(t *testing.T) {
	var nilGate *UnminedTxGate
	require.False(t, nilGate.Refuses(context.Background(), nil))
	require.InDelta(t, 0.0, nilGate.RefusedTotal(), 0)

	noClient := NewUnminedTxGate(ulogger.TestLogger{}, nil, gateSettings(t, "utxoset"), "test_no_client")
	require.False(t, noClient.Refuses(context.Background(), nil))
}

// TestUnminedTxGate_LogsOncePerInterval checks the log limit: a burst of
// refusals produces one line, and the next line reports the refusals that
// were counted but not logged.
func TestUnminedTxGate_LogsOncePerInterval(t *testing.T) {
	gate := gateInState(t, FSMStateCATCHINGBLOCKS, "test_log_interval")

	for i := 0; i < 5; i++ {
		require.True(t, gate.Refuses(context.Background(), nil))
	}

	// The first refusal logged and reset the pending count; four are pending.
	require.Equal(t, uint64(4), gate.sinceLog.Load())

	// Open the interval again: the next refusal logs all five pending.
	gate.lastLogNanos.Store(0)
	require.True(t, gate.Refuses(context.Background(), nil))
	require.Equal(t, uint64(0), gate.sinceLog.Load())
}

// On a netting store the gate refuses below the highest checkpoint in every FSM
// state, including before and after a failed height read, which closes the IDLE
// window. A store that does not net never reads the height.
func TestUnminedTxGate_NettingStoreRefusesBelowHighestCheckpoint(t *testing.T) {
	idle, running, catching := FSMStateIDLE, FSMStateRUNNING, FSMStateCATCHINGBLOCKS

	for _, tt := range []struct {
		name      string
		scheme    string
		state     *FSMStateType // nil: the state read fails
		height    int
		heightErr bool
		// refusesAfterRead is the answer once the first height read is done.
		// Before it, a netting store always refuses.
		refusesAfterRead bool
		wantReached      bool
	}{
		{"idle_below", "utxoset", &idle, 50, false, true, false},
		{"running_below", "utxoset", &running, 50, false, true, false},
		{"state_read_fails_below", "utxoset", nil, 50, false, true, false},
		{"height_read_fails", "utxoset", &idle, 0, true, true, false},
		{"idle_at_checkpoint", "utxoset", &idle, 100, false, false, true},
		{"idle_above", "utxoset", &idle, 150, false, false, true},
		// Reads go on while the FSM syncs, so the gate knows when it stops.
		{"catching_above", "utxoset", &catching, 150, false, true, true},
		{"not_netting_idle", "postgres", &idle, 0, false, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &Mock{}
			t.Cleanup(func() { client.AssertExpectations(t) })
			fsmRead(client, tt.state)

			// A store that does not net registers no height read: the strict
			// mock fails the test if the gate makes one.
			var deadlineLeft time.Duration

			netting := tt.scheme == "utxoset"
			if netting {
				var err error
				if tt.heightErr {
					err = errors.NewServiceError("blockchain down")
				}

				client.On("GetBestHeightAndTime", mock.Anything).Run(func(args mock.Arguments) {
					if deadline, ok := args.Get(0).(context.Context).Deadline(); ok {
						deadlineLeft = time.Until(deadline)
					}
				}).Return(tt.height, 0, err).Once()
			}

			logger := mocklogger.NewTestLogger()
			gate := NewUnminedTxGate(logger, client, gateSettings(t, tt.scheme), "test_checkpoint_"+tt.name)
			before := gate.RefusedTotal()

			require.Equal(t, netting, gate.Refuses(context.Background(), &chainhash.Hash{}), "before the first read")
			waitForHeightRead(t, gate)
			require.Equal(t, tt.refusesAfterRead, gate.Refuses(context.Background(), &chainhash.Hash{}), "after the first read")
			require.Equal(t, tt.wantReached, gate.reached.Load())

			want := before
			for _, refused := range []bool{netting, tt.refusesAfterRead} {
				if refused {
					want++
				}
			}

			require.InDelta(t, want, gate.RefusedTotal(), 0)

			if netting {
				require.Greater(t, deadlineLeft, bestHeightReadTimeout/2, "the read carries its own timeout")
				require.LessOrEqual(t, deadlineLeft, bestHeightReadTimeout)
			}

			wantWarn := 0
			if tt.heightErr {
				wantWarn = 1
			}

			logger.AssertNumberOfCalls(t, "Warnf", wantWarn)

			if tt.heightErr {
				gate.warnReadFailure(errors.NewServiceError("blockchain down"))
				logger.AssertNumberOfCalls(t, "Warnf", 1) // inside the interval: no second line

				gate.lastWarnNanos.Store(gate.sinceStart() - int64(unminedTxRefusedLogInterval))
				gate.warnReadFailure(errors.NewServiceError("blockchain down"))
				logger.AssertNumberOfCalls(t, "Warnf", 2)
			}
		})
	}
}

// One read runs at a time and the next is scheduled no sooner than the interval
// after the last was; once a read reaches the checkpoint the gate accepts and never reads
// again.
func TestUnminedTxGate_HeightReadIntervalAndLatch(t *testing.T) {
	idle := FSMStateIDLE
	client := &Mock{}
	t.Cleanup(func() { client.AssertExpectations(t) })
	fsmRead(client, &idle)

	release := make(chan struct{})
	client.On("GetBestHeightAndTime", mock.Anything).Run(func(mock.Arguments) { <-release }).Return(50, 0, nil).Once()
	client.On("GetBestHeightAndTime", mock.Anything).Return(150, 0, nil).Once()

	gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_checkpoint_interval")
	ctx := context.Background()

	require.True(t, gate.Refuses(ctx, nil), "unknown refuses and starts read 1")

	gate.heightReadNanos.Store(0)
	require.True(t, gate.Refuses(ctx, nil), "read 1 in flight: no second read")
	close(release)
	waitForHeightRead(t, gate)

	gate.heightReadNanos.Store(gate.sinceStart() - int64(bestHeightReadInterval) + int64(200*time.Millisecond))
	require.True(t, gate.Refuses(ctx, nil), "inside the interval: no read")
	client.AssertNumberOfCalls(t, "GetBestHeightAndTime", 1)

	gate.heightReadNanos.Store(gate.sinceStart() - int64(bestHeightReadInterval) - int64(time.Millisecond))
	require.True(t, gate.Refuses(ctx, nil), "interval passed: read 2 starts, still below until it lands")
	waitForHeightRead(t, gate)
	require.False(t, gate.Refuses(ctx, nil), "read 2 returned 150")

	gate.heightReadNanos.Store(0)
	require.False(t, gate.Refuses(ctx, nil), "reached is kept without reading")
	client.AssertNumberOfCalls(t, "GetBestHeightAndTime", 2)
}

// The interval and timeout are fixed, not tuning: a longer interval or timeout
// keeps a starting node refusing for longer, and no timeout lets a silent
// blockchain service hold the read slot for ever.
func TestUnminedTxGate_HeightReadTimeoutFreesTheSlot(t *testing.T) {
	require.Equal(t, time.Second, bestHeightReadInterval)
	require.Equal(t, 2*time.Second, bestHeightReadTimeout)

	idle := FSMStateIDLE
	client := &Mock{}
	t.Cleanup(func() { client.AssertExpectations(t) })
	fsmRead(client, &idle)

	// The client answers only when the read's own deadline ends it.
	client.On("GetBestHeightAndTime", mock.Anything).Run(func(args mock.Arguments) {
		<-args.Get(0).(context.Context).Done()
	}).Return(0, 0, errors.NewServiceError("deadline")).Once()

	gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_checkpoint_timeout")

	start := time.Now()
	require.True(t, gate.Refuses(context.Background(), nil))
	require.Eventually(t, func() bool { return !gate.heightReading.Load() }, 4*time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, time.Since(start), 1500*time.Millisecond, "the slot is held until the timeout")
}

// A caller that checked before another caller's read landed must start no read
// once it holds the slot: not after the checkpoint was reached, and not inside
// the interval. The strict mock fails the test on any read.
func TestUnminedTxGate_ScheduleRechecksInsideTheSlot(t *testing.T) {
	gate := NewUnminedTxGate(ulogger.TestLogger{}, &Mock{}, gateSettings(t, "utxoset"), "test_checkpoint_recheck")

	gate.reached.Store(true)
	gate.scheduleHeightRead()
	require.False(t, gate.heightReading.Load(), "reached: slot released, no read")

	gate.reached.Store(false)
	gate.heightReadNanos.Store(gate.sinceStart())
	gate.scheduleHeightRead()
	require.False(t, gate.heightReading.Load(), "inside the interval: slot released, no read")
}

// RefusesAfterCheckpointAnswer refuses while syncing and accepts once reached,
// and refuses on a below answer only from a read scheduled after it began, so a
// stale or in-flight read cannot drop a message the intake already acknowledged.
func TestUnminedTxGate_RefusesAfterCheckpointAnswer(t *testing.T) {
	ctx := context.Background()
	running, catching := FSMStateRUNNING, FSMStateCATCHINGBLOCKS

	decide := func(t *testing.T, gate *UnminedTxGate, wantRefused bool) {
		t.Helper()

		decideCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()

		refused, err := gate.RefusesAfterCheckpointAnswer(decideCtx, nil)
		require.NoError(t, err)
		require.Equal(t, wantRefused, refused)
	}

	t.Run("rule_off_and_nil", func(t *testing.T) {
		off := &Mock{}
		fsmRead(off, &running)
		decide(t, NewUnminedTxGate(ulogger.TestLogger{}, off, gateSettings(t, "postgres"), "test_decide_off"), false)
		off.AssertNotCalled(t, "GetBestHeightAndTime", mock.Anything)

		var nilGate *UnminedTxGate
		decide(t, nilGate, false)
	})

	t.Run("syncing_refuses_without_waiting", func(t *testing.T) {
		// Rule off, checkpoint reached, or below it: a syncing FSM refuses at
		// once and counts it, even while the height cannot be read.
		for _, tt := range []struct {
			name, scheme string
			reached      bool
		}{{"off", "postgres", false}, {"reached", "utxoset", true}, {"below", "utxoset", false}} {
			syncing := &Mock{}
			fsmRead(syncing, &catching)
			syncing.On("GetBestHeightAndTime", mock.Anything).Return(0, 0, errors.NewServiceError("blockchain down")).Maybe()

			gate := NewUnminedTxGate(ulogger.TestLogger{}, syncing, gateSettings(t, tt.scheme), "test_decide_syncing_"+tt.name)
			gate.reached.Store(tt.reached)
			before := gate.RefusedTotal()

			decide(t, gate, true)
			require.InDelta(t, before+1, gate.RefusedTotal(), 0, tt.name)
			waitForHeightRead(t, gate)
		}
	})

	t.Run("state_read_failure_waits_for_the_height", func(t *testing.T) {
		client := &Mock{}
		fsmRead(client, nil)
		client.On("GetBestHeightAndTime", mock.Anything).Return(50, 0, nil).Once()
		decide(t, NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_decide_state_fails"), true)
		client.AssertExpectations(t)
	})

	t.Run("fresh_read_decides", func(t *testing.T) {
		for _, tt := range []struct {
			height      int
			wantRefused bool
		}{{50, true}, {150, false}} {
			client := &Mock{}
			fsmRead(client, &running)
			client.On("GetBestHeightAndTime", mock.Anything).Return(tt.height, 0, nil).Once()
			// A later read would answer the other way: only the first, fresh one may decide.
			client.On("GetBestHeightAndTime", mock.Anything).Return(200-tt.height, 0, nil).Maybe()

			gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_decide_fresh")
			decide(t, gate, tt.wantRefused)

			if !tt.wantRefused {
				// Reached: later messages decide with no further read.
				decide(t, gate, false)
				client.AssertNumberOfCalls(t, "GetBestHeightAndTime", 1)
			}
		}
	})

	t.Run("in_flight_read_reaching_the_checkpoint_accepts", func(t *testing.T) {
		client := &Mock{}
		fsmRead(client, &running)

		release := make(chan struct{})
		client.On("GetBestHeightAndTime", mock.Anything).Run(func(mock.Arguments) { <-release }).Return(150, 0, nil).Once()

		gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_decide_in_flight")
		require.True(t, gate.Refuses(ctx, nil), "starts the read")

		go func() {
			time.Sleep(200 * time.Millisecond)
			close(release)
		}()

		decide(t, gate, false)
		client.AssertNumberOfCalls(t, "GetBestHeightAndTime", 1)
	})

	t.Run("in_flight_below_read_is_not_fresh", func(t *testing.T) {
		client := &Mock{}
		fsmRead(client, &running)

		release := make(chan struct{})
		client.On("GetBestHeightAndTime", mock.Anything).Run(func(mock.Arguments) { <-release }).Return(50, 0, nil).Once()
		client.On("GetBestHeightAndTime", mock.Anything).Return(150, 0, nil).Once()

		gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_decide_in_flight_below")
		require.True(t, gate.Refuses(ctx, nil), "starts the read")

		go func() {
			time.Sleep(200 * time.Millisecond)
			close(release)
		}()

		decide(t, gate, false)
		client.AssertNumberOfCalls(t, "GetBestHeightAndTime", 2)
	})

	t.Run("stale_below_answer_reads_again", func(t *testing.T) {
		client := &Mock{}
		fsmRead(client, &running)
		client.On("GetBestHeightAndTime", mock.Anything).Return(50, 0, nil).Once()
		client.On("GetBestHeightAndTime", mock.Anything).Return(150, 0, nil).Once()

		gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_decide_stale")
		decide(t, gate, true)

		gate.heightReadNanos.Store(0)
		decide(t, gate, false)
		client.AssertExpectations(t)
	})

	t.Run("unreadable_height_returns_ctx_error", func(t *testing.T) {
		client := &Mock{}
		fsmRead(client, &running)
		client.On("GetBestHeightAndTime", mock.Anything).Return(0, 0, errors.NewServiceError("blockchain down"))

		gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_decide_fail")
		failCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()

		refused, err := gate.RefusesAfterCheckpointAnswer(failCtx, nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, refused)
		waitForHeightRead(t, gate)
	})
}

// A waiting decision stalls only when a height read fails: onStall runs once
// however many stall and onRecover once when the last returns. Slow reads that
// succeed, even one in flight when the decision began, stall nothing.
func TestUnminedTxGate_StallHooks(t *testing.T) {
	running := FSMStateRUNNING

	var stalls, recovers atomic.Int32

	hooks := func(gate *UnminedTxGate) {
		gate.SetStallHooks(func() { stalls.Add(1) }, func() { recovers.Add(1) })
	}

	slow := &Mock{}
	fsmRead(slow, &running)
	slowRead := func(mock.Arguments) { time.Sleep(1200 * time.Millisecond) }
	slow.On("GetBestHeightAndTime", mock.Anything).Run(slowRead).Return(50, 0, nil).Once()
	slow.On("GetBestHeightAndTime", mock.Anything).Run(slowRead).Return(150, 0, nil).Once()

	gate := NewUnminedTxGate(ulogger.TestLogger{}, slow, gateSettings(t, "utxoset"), "test_stall_slow")
	hooks(gate)
	gate.readFailNanos.Store(1) // a read that failed before the decision began does not count
	require.True(t, gate.Refuses(context.Background(), nil), "starts the first slow read")

	refused, err := gate.RefusesAfterCheckpointAnswer(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, refused, "judged by the second read, scheduled after the decision began")
	require.Zero(t, stalls.Load()+recovers.Load(), "reads that succeed stall nothing, however slow")

	catching := FSMStateCATCHINGBLOCKS
	down := &Mock{}
	down.On("GetFSMCurrentState", mock.Anything).Return(&catching, nil).Once()
	fsmRead(down, &running)
	down.On("GetBestHeightAndTime", mock.Anything).Return(0, 0, errors.NewServiceError("blockchain down"))

	gate = NewUnminedTxGate(ulogger.TestLogger{}, down, gateSettings(t, "utxoset"), "test_stall_down")
	hooks(gate)

	refused, err = gate.RefusesAfterCheckpointAnswer(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, refused, "refused while syncing, before any read")
	require.Zero(t, gate.stalled, "a decision that never stalled ends no stall")

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	for range 2 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, _ = gate.RefusesAfterCheckpointAnswer(ctx, nil)
		}()
	}

	require.Eventually(t, func() bool { return stalls.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), stalls.Load(), "two stalled waits, one onStall")
	require.Zero(t, recovers.Load())

	cancel()
	wg.Wait()
	require.Equal(t, int32(1), recovers.Load(), "one onRecover, after the last wait returns")
	waitForHeightRead(t, gate)
}

// Refuses judges the checkpoint once per call: a read that lands while the call
// is still deciding cannot turn its refusal into an accept.
func TestUnminedTxGate_AnswerFixedForTheCall(t *testing.T) {
	running := FSMStateRUNNING
	client := &Mock{}
	t.Cleanup(func() { client.AssertExpectations(t) })
	client.On("GetBestHeightAndTime", mock.Anything).Return(150, 0, nil).Once()

	gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_checkpoint_fixed")

	// The state read returns only after the read this call scheduled has landed.
	client.On("GetFSMCurrentState", mock.Anything).Run(func(mock.Arguments) {
		require.Eventually(t, gate.reached.Load, 2*time.Second, time.Millisecond)
	}).Return(&running, nil)

	require.True(t, gate.Refuses(context.Background(), nil), "judged before the read landed")
	require.False(t, gate.Refuses(context.Background(), nil), "the next call sees it")
}

// A height read that reaches the checkpoint between the decision's two loads
// must not make it refuse: freshness is loaded first, reached second.
func TestUnminedTxGate_ReadLandingInsideTheDecisionAccepts(t *testing.T) {
	running := FSMStateRUNNING
	client := &Mock{}
	t.Cleanup(func() { client.AssertExpectations(t) })
	client.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_decision_loads")

	betweenDecisionLoads = func() {
		gate.reached.Store(true)
		gate.readOKNanos.Store(gate.sinceStart())
	}
	t.Cleanup(func() { betweenDecisionLoads = nil })

	refused, err := gate.RefusesAfterCheckpointAnswer(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, refused)
}

// waitForHeightRead waits until the gate's background height read has finished.
func waitForHeightRead(t *testing.T, gate *UnminedTxGate) {
	t.Helper()

	require.Eventually(t, func() bool { return !gate.heightReading.Load() }, 5*time.Second, time.Millisecond)
}

// The settings argument decides the rule, so each intake must pass its own.
func TestUnminedTxGate_CheckpointHeight(t *testing.T) {
	require.Equal(t, uint32(100), NewUnminedTxGate(ulogger.TestLogger{}, &Mock{}, gateSettings(t, "utxoset"), "test_height_on").CheckpointHeight())
	require.Zero(t, NewUnminedTxGate(ulogger.TestLogger{}, &Mock{}, gateSettings(t, "postgres"), "test_height_off").CheckpointHeight())
	require.Zero(t, NewUnminedTxGate(ulogger.TestLogger{}, &Mock{}, nil, "test_height_nil").CheckpointHeight())

	// Read times are measured from the gate's start on the monotonic clock,
	// not as wall-clock unix time.
	require.Less(t, NewUnminedTxGate(ulogger.TestLogger{}, nil, nil, "test_since_start").sinceStart(), int64(time.Hour))

	var nilGate *UnminedTxGate
	require.Zero(t, nilGate.CheckpointHeight())
	require.Empty(t, nilGate.Route())
	require.Equal(t, "test_height_on", NewUnminedTxGate(ulogger.TestLogger{}, nil, nil, "test_height_on").Route())
}
