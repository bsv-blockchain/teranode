package blockchain

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
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
// state and while the height is unknown, which closes the IDLE window. A store
// that does not net never reads the height.
func TestUnminedTxGate_NettingStoreRefusesBelowHighestCheckpoint(t *testing.T) {
	idle, running := FSMStateIDLE, FSMStateRUNNING

	for _, tt := range []struct {
		name      string
		scheme    string
		state     *FSMStateType // nil: the state read fails
		height    uint32
		heightErr bool
		refuses   bool
	}{
		{"idle_below", "utxoset", &idle, 50, false, true},
		{"running_below", "utxoset", &running, 50, false, true},
		{"state_read_fails_below", "utxoset", nil, 50, false, true},
		{"height_read_fails_never_known", "utxoset", &idle, 0, true, true},
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
			if tt.scheme == "utxoset" {
				var err error
				if tt.heightErr {
					err = errors.NewServiceError("blockchain down")
				}

				client.On("GetBestHeightAndTime", mock.Anything).Return(int(tt.height), 0, err).Once()
			}

			gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, tt.scheme), "test_checkpoint_"+tt.name)
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

// The height is read at most once per interval, and a failed read keeps the last
// answer, so a node at the tip keeps accepting through a blockchain restart. A
// later read below the checkpoint refuses again.
func TestUnminedTxGate_CheckpointAnswerOutlivesFailedReads(t *testing.T) {
	idle := FSMStateIDLE
	client := &Mock{}
	t.Cleanup(func() { client.AssertExpectations(t) })
	fsmRead(client, &idle)

	client.On("GetBestHeightAndTime", mock.Anything).Return(150, 0, nil).Once()
	client.On("GetBestHeightAndTime", mock.Anything).Return(0, 0, errors.NewServiceError("blockchain down")).Once()
	client.On("GetBestHeightAndTime", mock.Anything).Return(50, 0, nil).Once()

	gate := NewUnminedTxGate(ulogger.TestLogger{}, client, gateSettings(t, "utxoset"), "test_checkpoint_cache")
	ctx := context.Background()

	require.False(t, gate.Refuses(ctx, nil), "read 150: reached")
	require.False(t, gate.Refuses(ctx, nil), "inside the interval: no read, still reached")
	client.AssertNumberOfCalls(t, "GetBestHeightAndTime", 1)

	gate.heightReadNanos.Store(0)
	require.False(t, gate.Refuses(ctx, nil), "failed read keeps reached")

	gate.heightReadNanos.Store(0)
	require.True(t, gate.Refuses(ctx, nil), "read 50: below again")
}
