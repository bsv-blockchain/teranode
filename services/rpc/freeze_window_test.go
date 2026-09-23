package rpc

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestFreezeWindowFromRPC pins how the freeze command's optional heights map to the
// stored window: omission is the legacy unqualified freeze, and only an explicit range
// gets SV Node's interval semantics — so an explicit 0, 0 is an empty interval, not
// "at every height" (issue #1422).
func TestFreezeWindowFromRPC(t *testing.T) {
	ip := func(i int) *int { return &i }

	for _, tc := range []struct {
		name        string
		start, stop *int
		wantFrom    uint32
		wantUntil   uint32
		wantErr     bool
	}{
		{name: "both omitted is the unqualified freeze", wantFrom: 0, wantUntil: 0},
		{name: "an explicit zero range is an empty interval", start: ip(0), stop: ip(0), wantFrom: utxo.FreezeWindowNever, wantUntil: utxo.FreezeWindowNever},
		{name: "stop at start is an empty interval", start: ip(100), stop: ip(100), wantFrom: utxo.FreezeWindowNever, wantUntil: utxo.FreezeWindowNever},
		{name: "stop below start is an empty interval", start: ip(100), stop: ip(50), wantFrom: utxo.FreezeWindowNever, wantUntil: utxo.FreezeWindowNever},
		{name: "a window", start: ip(100), stop: ip(200), wantFrom: 100, wantUntil: 200},
		{name: "a start alone has no end", start: ip(100), wantFrom: 100, wantUntil: 0},
		{name: "a stop alone starts at genesis", stop: ip(200), wantFrom: 0, wantUntil: 200},
		{name: "a negative bound is rejected", start: ip(-1), stop: ip(5), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from, until, err := freezeWindowFromRPC(tc.start, tc.stop)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantFrom, from)
			require.Equal(t, tc.wantUntil, until)
		})
	}
}
