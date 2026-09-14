package replayrecovery

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundedStreamConsumesExactlyOne(t *testing.T) {
	wire, err := hex.DecodeString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	r := bytes.NewReader(append(append([]byte{}, wire...), wire...))
	tx, err := ReadBoundedTransactionStream(r, 1024)
	require.NoError(t, err)
	require.Equal(t, wire, tx.Bytes())
	require.Equal(t, len(wire), r.Len())
	_, err = ReadBoundedTransactionStream(r, 20)
	require.Error(t, err)
}
func TestBoundedStreamRejectsHugeCounts(t *testing.T) {
	for _, wire := range [][]byte{{1, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 127}, {1, 0, 0, 0, 1, 0}} {
		_, err := ReadBoundedTransactionStream(bytes.NewReader(wire), 1<<20)
		require.Error(t, err)
	}
}

func TestBoundedStreamRejectsEveryTruncationIncludingExtendedInputs(t *testing.T) {
	wire, err := hex.DecodeString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0100ffffffff010100000000000000015100000000")
	require.NoError(t, err)
	// Extended encoding carries the original input amount and locking script.
	extended := append([]byte{}, wire[:4]...)
	extended = append(extended, 0, 0, 0, 0, 0, 239)
	extended = append(extended, wire[4:47]...)
	extended = append(extended, 1, 0, 0, 0, 0, 0, 0, 0, 1, 81)
	extended = append(extended, wire[47:]...)
	for _, data := range [][]byte{wire, extended} {
		complete, err := ReadBoundedTransactionStream(bytes.NewReader(data), 1024)
		require.NoError(t, err)
		require.Equal(t, wire, complete.Bytes())
		for end := 0; end < len(data); end++ {
			tx, err := ReadBoundedTransactionStream(bytes.NewReader(data[:end]), 1024)
			require.Error(t, err, "accepted truncated transaction at %d/%d", end, len(data))
			require.Nil(t, tx)
		}
	}
}

func TestBoundedStreamRejectsAmbiguousLengthsAndInvalidLimits(t *testing.T) {
	for _, count := range [][]byte{{253, 1, 0}, {254, 1, 0, 0, 0}, {255, 1, 0, 0, 0, 0, 0, 0, 0}} {
		wire := append([]byte{1, 0, 0, 0}, count...)
		tx, err := ReadBoundedTransactionStream(bytes.NewReader(wire), 1024)
		require.ErrorContains(t, err, "noncanonical varint")
		require.Nil(t, tx)
	}
	for _, limit := range []int64{-1, 0, 9, 256<<20 + 1} {
		reader := bytes.NewReader([]byte{1, 0, 0, 0})
		_, err := ReadBoundedTransactionStream(reader, limit)
		require.ErrorContains(t, err, "invalid transaction byte limit")
		require.Equal(t, 4, reader.Len(), "invalid limits must fail before reading archive bytes")
	}
	_, err := ReadBoundedTransactionStream(bytes.NewReader([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 1}), 1024)
	require.ErrorContains(t, err, "invalid extended marker")
}
