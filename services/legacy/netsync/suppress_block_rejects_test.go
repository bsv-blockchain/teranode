package netsync

import (
	"testing"

	teranodeblockchain "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

func TestSuppressBlockRejects_OnlyRunningRejects(t *testing.T) {
	for _, tt := range []struct {
		state    teranodeblockchain.FSMStateType
		suppress bool
	}{
		{teranodeblockchain.FSMStateRUNNING, false},
		{teranodeblockchain.FSMStateCATCHINGBLOCKS, true},
		{teranodeblockchain.FSMStateIDLE, true},
	} {
		t.Run(tt.state.String(), func(t *testing.T) {
			state := tt.state
			require.Equal(t, tt.suppress, suppressBlockRejects(&state))
		})
	}

	require.True(t, suppressBlockRejects(nil), "an unknown state must fail safe")
}
