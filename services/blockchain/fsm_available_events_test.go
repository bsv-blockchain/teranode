package blockchain

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
)

func TestAvailableEventsForState(t *testing.T) {
	tests := []struct {
		name   string
		state  string
		expect []string
	}{
		{
			name:  "IDLE",
			state: blockchain_api.FSMStateType_IDLE.String(),
			expect: []string{
				blockchain_api.FSMEventType_RUN.String(),
				blockchain_api.FSMEventType_LEGACYSYNC.String(),
			},
		},
		{
			name:  "RUNNING",
			state: blockchain_api.FSMStateType_RUNNING.String(),
			expect: []string{
				blockchain_api.FSMEventType_CATCHUPBLOCKS.String(),
				blockchain_api.FSMEventType_STOP.String(),
			},
		},
		{
			name:  "LEGACYSYNCING",
			state: blockchain_api.FSMStateType_LEGACYSYNCING.String(),
			expect: []string{
				blockchain_api.FSMEventType_RUN.String(),
				blockchain_api.FSMEventType_STOP.String(),
			},
		},
		{
			name:  "CATCHINGBLOCKS",
			state: blockchain_api.FSMStateType_CATCHINGBLOCKS.String(),
			expect: []string{
				blockchain_api.FSMEventType_RUN.String(),
			},
		},
		{
			name:   "unknown state",
			state:  "UNKNOWN",
			expect: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AvailableEventsForState(tt.state)
			require.NotNil(t, got)
			require.Equal(t, tt.expect, got)
		})
	}
}
