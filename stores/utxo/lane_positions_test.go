package utxo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type fixedLanePosition uint64

func (p fixedLanePosition) Next(context.Context) (uint64, error) { return uint64(p), nil }

// Rejecting invalid explicit/source positions must not turn them into fresh allocations or
// successful zero-position writes. Disabled stores retain their existing behavior.
func TestLanePositionsRequireValidPositionsWhenEnabled(t *testing.T) {
	for _, tc := range []struct {
		name       string
		enabled    bool
		explicit   uint64
		fromSource uint64
		want       uint64
		wantErr    bool
	}{
		{"disabled without position", false, 0, 0, 0, false},
		{"disabled explicit unchanged", false, 1 << 56, 0, 1 << 56, false},
		{"valid lane zero from source", true, 0, 1, 1, false},
		{"valid explicit skips source", true, 1, 0, 1, false},
		{"valid signed position", true, 0, 0xffffffffffffffff, 0xffffffffffffffff, false},
		{"explicit lane with zero position", true, 1 << 56, 1, 0, true},
		{"source returns no position", true, 0, 0, 0, true},
		{"source returns lane with zero position", true, 0, 1 << 56, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var positions LanePositions
			if tc.enabled {
				positions.SetLanePositionSource(fixedLanePosition(tc.fromSource))
			}
			p, err := positions.Take(context.Background(), &CreateOptions{LanePosition: tc.explicit})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, p)
		})
	}
}

func TestLanePositionsRejectInvalidBulkSourcePositions(t *testing.T) {
	for _, invalid := range []uint64{0, 1 << 56} {
		var positions LanePositions
		positions.SetLanePositionSource(fixedLanePosition(invalid))
		got, err := positions.TakeLanePositions(context.Background(), 2)
		require.Error(t, err)
		require.Nil(t, got, "no partial position list may escape on invalid source data")
	}
}
