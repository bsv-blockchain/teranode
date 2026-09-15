package replayrecovery

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestProofBoundaryValidatesIdentityAndBranchShape(t *testing.T) {
	target := chainhash.DoubleHashH([]byte("branch target"))
	header := make([]byte, 80)
	copy(header[36:68], target[:])
	for _, id := range []string{"short", strings.Repeat("gg", 32), strings.ToUpper(target.String())} {
		_, err := VerifyInclusion(id, Inclusion{Header: header})
		require.Error(t, err)
	}
	for _, inclusion := range []Inclusion{
		{Header: header[:79]},
		{Header: header, MerkleBranch: make([]string, 33)},
		{Header: header, MerkleBranch: []string{"not a sibling hash"}},
		{Header: header, Index: 1}, // A leaf-only proof cannot claim another position.
	} {
		_, err := VerifyInclusion(target.String(), inclusion)
		require.Error(t, err)
	}
}

func TestProofBoundaryRejectsTruncatedTransactions(t *testing.T) {
	raw, err := hex.DecodeString("0100000001" + strings.Repeat("00", 32) + "ffffffff0100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	for length := 0; length < len(raw); length++ {
		tx, err := ReadBoundedTransaction(bytes.NewReader(raw[:length]))
		require.Error(t, err, "accepted truncation at byte %d", length)
		require.Nil(t, tx)
	}
	// Oversized unlocking-script length, bad extended marker, and a truncated
	// extended input count must fail before any raw transaction can authorize work.
	oversized := append([]byte(nil), raw...)
	oversized[41] = byte(len(raw))
	invalidExtended := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	missingExtendedCount := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 239}
	for _, data := range [][]byte{oversized, invalidExtended, missingExtendedCount} {
		tx, err := ReadBoundedTransaction(bytes.NewReader(data))
		require.Error(t, err)
		require.Nil(t, tx)
	}
	tx, err := ReadBoundedTransaction(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, raw, tx.Bytes())
}
