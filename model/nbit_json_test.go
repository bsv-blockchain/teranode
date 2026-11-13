package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// NBit must survive a Marshal -> Unmarshal round-trip. The old UnmarshalJSON had
// a value receiver (a silent no-op) and passed the quoted JSON straight to the
// hex decoder (which rejects the '"'), so this failed on both counts.
func TestNBit_JSONRoundTrip(t *testing.T) {
	orig, err := NewNBitFromString("1e0cbb05")
	require.NoError(t, err)

	data, err := json.Marshal(orig)
	require.NoError(t, err)

	var got NBit
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, orig.String(), got.String())
	require.Equal(t, *orig, got)
}

// UnmarshalJSON must decode the JSON string value (quotes stripped) into the
// receiver, so a direct call on a quoted hex string populates the NBit.
func TestNBit_UnmarshalJSON_direct(t *testing.T) {
	var nb NBit
	require.NoError(t, nb.UnmarshalJSON([]byte(`"1e0cbb05"`)))
	require.Equal(t, "1e0cbb05", nb.String())
}

// A hex value that is not exactly 4 bytes must be rejected rather than leaving a
// partially written NBit, matching NewNBitFromSlice.
func TestNBit_UnmarshalJSON_rejectsWrongLength(t *testing.T) {
	var nb NBit
	require.Error(t, nb.UnmarshalJSON([]byte(`"1e0cbb"`)))
}
