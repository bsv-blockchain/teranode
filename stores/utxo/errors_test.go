// Package utxo provides UTXO (Unspent Transaction Output) management for the BSV Blockchain Teranode implementation.
package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrTxNotFound(t *testing.T) {
	err := errors.NewTxNotFoundError("tx not found")
	require.Error(t, err)

	assert.True(t, errors.Is(err, errors.ErrTxNotFound))
}

func TestErrSpent(t *testing.T) {
	txID := chainhash.HashH([]byte("test"))
	vOut := uint32(0)
	utxoHash := chainhash.HashH([]byte("utxo"))
	spendingData := spend.NewSpendingData(&chainhash.Hash{}, 1)

	err := errors.NewUtxoSpentError(txID, vOut, utxoHash, spendingData)
	require.NotNil(t, err)

	err = errors.NewProcessingError("processing error 1", err)
	err = errors.NewProcessingError("processing error 2", err)
	err = errors.NewProcessingError("processing error 3", err)
	err = errors.NewProcessingError("processing error 4", err)

	var uErr *errors.Error
	ok := errors.As(err, &uErr)
	require.True(t, ok)

	var usErr *errors.UtxoSpentErrData
	ok = errors.AsData(uErr, &usErr)
	require.True(t, ok)

	assert.Equal(t, txID.String(), usErr.Hash.String())
	assert.Equal(t, vOut, usErr.Vout)
	assert.Equal(t, spendingData.TxID.String(), usErr.SpendingData.TxID.String())
	assert.Equal(t, spendingData.Vin, usErr.SpendingData.Vin)
}

func TestNewErrLockTime_ZeroLockTime(t *testing.T) {
	err := NewErrLockTime(0, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ErrLockTime")
	assert.True(t, errors.Is(err, errors.ErrTxLockTime))
}

func TestNewErrLockTime_TimestampBased(t *testing.T) {
	// Test with a value < 500,000,000 (treated as timestamp in this implementation)
	// Using a small timestamp value: 100000 = 1970-01-02T03:46:40Z
	lockTime := uint32(100000)
	blockHeight := uint32(100)

	err := NewErrLockTime(lockTime, blockHeight)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "utxo is locked until")
	assert.Contains(t, err.Error(), "1970-01-02T03:46:40Z")
	assert.True(t, errors.Is(err, errors.ErrTxLockTime))
}

func TestNewErrLockTime_BlockHeightBased(t *testing.T) {
	// Test with a value >= 500,000,000 (treated as block height in this implementation)
	lockTime := uint32(800000000)
	blockHeight := uint32(750000000)

	err := NewErrLockTime(lockTime, blockHeight)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "utxo is locked until block")
	assert.Contains(t, err.Error(), "800000000")
	assert.Contains(t, err.Error(), "750000000")
	assert.True(t, errors.Is(err, errors.ErrTxLockTime))
}

func TestNewErrLockTime_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		lockTime    uint32
		blockHeight uint32
		contains    string
	}{
		{
			name:        "locktime exactly at threshold minus 1",
			lockTime:    499999999,
			blockHeight: 100,
			contains:    "utxo is locked until",
		},
		{
			name:        "locktime exactly at threshold",
			lockTime:    500000000,
			blockHeight: 100,
			contains:    "utxo is locked until block 500000000",
		},
		{
			name:        "locktime at 1",
			lockTime:    1,
			blockHeight: 0,
			contains:    "1970-01-01T00:00:01Z",
		},
		{
			name:        "very high block height",
			lockTime:    4000000000,
			blockHeight: 3999999999,
			contains:    "utxo is locked until block 4000000000",
		},
		{
			name:        "max uint32 locktime",
			lockTime:    ^uint32(0),
			blockHeight: 1000000,
			contains:    "utxo is locked until block",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewErrLockTime(tt.lockTime, tt.blockHeight)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.contains)
			assert.True(t, errors.Is(err, errors.ErrTxLockTime))
		})
	}
}

func TestNewErrLockTime_WithOptionalErrors(t *testing.T) {
	// Test that optional errors parameter doesn't cause issues
	// (even though the current implementation doesn't use them)
	optionalErr := errors.NewProcessingError("optional error")

	err := NewErrLockTime(0, 100, optionalErr)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrTxLockTime))
}

func TestNewErrLockTime_TimestampFormatting(t *testing.T) {
	// Test various values < 500,000,000 to ensure ISO 8601 formatting is correct
	// Note: values < 500,000,000 are treated as timestamps in this implementation
	tests := []struct {
		name      string
		timestamp uint32
		expected  string
	}{
		{
			name:      "value 1",
			timestamp: 1,
			expected:  "1970-01-01T00:00:01Z",
		},
		{
			name:      "early timestamp",
			timestamp: 86400,
			expected:  "1970-01-02T00:00:00Z",
		},
		{
			name:      "year 2000 equivalent",
			timestamp: 100000000,
			expected:  "1973-03-03T09:46:40Z",
		},
		{
			name:      "near threshold",
			timestamp: 499999999,
			expected:  "1985-11-05T00:53:19Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewErrLockTime(tt.timestamp, 100)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expected)
		})
	}
}

func TestConstants(t *testing.T) {
	// Verify constants are set correctly
	assert.Equal(t, "2006-01-02T15:04:05Z", isoFormat)
	assert.Equal(t, "nil", nilString)
}
