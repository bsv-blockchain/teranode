package blockchain

import (
	"context"
	"net/url"
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
	idle, running := FSMStateIDLE, FSMStateRUNNING

	for _, tt := range []struct {
		name      string
		scheme    string
		state     *FSMStateType // nil: the state read fails
		height    int
		heightErr bool
		// refusesAfterRead is the answer once the first height read is done.
		// Before it, a netting store always refuses.
		refusesAfterRead bool
	}{
		{"idle_below", "utxoset", &idle, 50, false, true},
		{"running_below", "utxoset", &running, 50, false, true},
		{"state_read_fails_below", "utxoset", nil, 50, false, true},
		{"height_read_fails", "utxoset", &idle, 0, true, true},
		{"idle_at_checkpoint", "utxoset", &idle, 100, false, false},
		{"idle_above", "utxoset", &idle, 150, false, false},
		{"not_netting_idle", "postgres", &idle, 0, false, false},
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

				gate.lastWarnNanos.Store(time.Now().Add(-unminedTxRefusedLogInterval).UnixNano())
				gate.warnReadFailure(errors.NewServiceError("blockchain down"))
				logger.AssertNumberOfCalls(t, "Warnf", 2)
			}
		})
	}
}

// One read runs at a time and the next starts no sooner than the interval after
// the last; once a read reaches the checkpoint the gate accepts and never reads
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

	gate.heightReadNanos.Store(time.Now().Add(-bestHeightReadInterval + 200*time.Millisecond).UnixNano())
	require.True(t, gate.Refuses(ctx, nil), "inside the interval: no read")
	client.AssertNumberOfCalls(t, "GetBestHeightAndTime", 1)

	gate.heightReadNanos.Store(time.Now().Add(-bestHeightReadInterval - time.Millisecond).UnixNano())
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

	var nilGate *UnminedTxGate
	require.Zero(t, nilGate.CheckpointHeight())
	require.Empty(t, nilGate.Route())
	require.Equal(t, "test_height_on", NewUnminedTxGate(ulogger.TestLogger{}, nil, nil, "test_height_on").Route())
}
