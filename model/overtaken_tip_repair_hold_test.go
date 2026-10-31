package model

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestOvertakenTipRepairHoldRoundTrip(t *testing.T) {
	held := []HeldBlock{
		{Hash: chainhash.HashH([]byte("a")), ID: 7, Height: 100},
		{Hash: chainhash.HashH([]byte("b")), ID: 9, Height: 98},
	}

	decoded, err := DecodeOvertakenTipRepairHold(EncodeOvertakenTipRepairHold(held))
	require.NoError(t, err)
	require.Equal(t, held, decoded)
}

// No hold is still a non-empty record: over gRPC empty bytes arrive as nil, and the SQL
// state table refuses a NULL.
func TestOvertakenTipRepairHoldEmpty(t *testing.T) {
	encoded := EncodeOvertakenTipRepairHold(nil)
	require.NotEmpty(t, encoded, "no hold still encodes to bytes")

	decoded, err := DecodeOvertakenTipRepairHold(encoded)
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func TestOvertakenTipRepairHoldRejectsUnknownVersion(t *testing.T) {
	encoded := EncodeOvertakenTipRepairHold(nil)
	encoded[0]++

	_, err := DecodeOvertakenTipRepairHold(encoded)
	require.Error(t, err)

	_, err = DecodeOvertakenTipRepairHold(nil)
	require.Error(t, err, "a record without a version is not a hold this code wrote")
}

func TestOvertakenTipRepairHoldRejectsTruncated(t *testing.T) {
	encoded := EncodeOvertakenTipRepairHold([]HeldBlock{{Hash: chainhash.HashH([]byte("a")), ID: 7, Height: 100}})

	_, err := DecodeOvertakenTipRepairHold(encoded[:len(encoded)-1])
	require.Error(t, err)
}
