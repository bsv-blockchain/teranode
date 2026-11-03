package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// gateInState returns a gate whose client reports state on every read.
func gateInState(t *testing.T, state FSMStateType, route string) *UnminedTxGate {
	t.Helper()

	client := &Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

	return NewUnminedTxGate(ulogger.TestLogger{}, client, route)
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

	gate := NewUnminedTxGate(ulogger.TestLogger{}, client, "test_read_failure")

	require.False(t, gate.Refuses(context.Background(), nil))
	require.InDelta(t, 0.0, gate.RefusedTotal(), 0)
}

// TestUnminedTxGate_NilAccepts keeps services built without a blockchain
// client, and struct-literal servers in tests, working unchanged.
func TestUnminedTxGate_NilAccepts(t *testing.T) {
	var nilGate *UnminedTxGate
	require.False(t, nilGate.Refuses(context.Background(), nil))
	require.InDelta(t, 0.0, nilGate.RefusedTotal(), 0)

	noClient := NewUnminedTxGate(ulogger.TestLogger{}, nil, "test_no_client")
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
