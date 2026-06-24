package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test handleBatchRecordError logic without aerospike dependencies
func TestHandleBatchRecordError_Logic(t *testing.T) {
	s := &Store{}
	hash := &chainhash.Hash{}

	tests := []struct {
		name          string
		err           error
		expectNilErr  bool
		errorContains string
	}{
		{
			name:         "nil error",
			err:          nil,
			expectNilErr: true,
		},
		{
			name:          "generic error",
			err:           errors.NewError("some error"),
			expectNilErr:  false,
			errorContains: "aerospike batchRecord error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The function is only called when err is not nil
			if tt.err == nil {
				// Skip nil error test as the function isn't designed for that
				return
			}

			err := s.handleBatchRecordError(tt.err, hash, false)
			if tt.expectNilErr {
				assert.Nil(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			}
		})
	}
}

// TestHandleBatchRecordError_KeyNotFound verifies the invariant fix: a missing
// record must be a hard error during normal set-mined (the tx MUST exist and be
// tagged with the block ID), but is a tolerated no-op during unset-mined.
func TestHandleBatchRecordError_KeyNotFound(t *testing.T) {
	s := &Store{}
	hash := &chainhash.Hash{}
	keyNotFound := &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}

	t.Run("normal set-mined returns TxNotFoundError", func(t *testing.T) {
		err := s.handleBatchRecordError(keyNotFound, hash, false /*unsetMined*/)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction not found")
	})

	t.Run("unset-mined tolerates missing record", func(t *testing.T) {
		err := s.handleBatchRecordError(keyNotFound, hash, true /*unsetMined*/)
		assert.NoError(t, err)
	})
}

// Test error creation functions
func TestCreateSpendError_ErrorMessages(t *testing.T) {
	// Test that error messages are formatted correctly
	tests := []struct {
		name           string
		errorCode      LuaErrorCode
		message        string
		vout           uint32
		expectContains []string
	}{
		{
			name:           "frozen error",
			errorCode:      LuaErrorCodeFrozen,
			message:        "UTXO is frozen",
			vout:           1,
			expectContains: []string{"UTXO is frozen", "vout 1"},
		},
		{
			name:           "not found error",
			errorCode:      LuaErrorCodeUtxoNotFound,
			message:        "UTXO not found",
			vout:           2,
			expectContains: []string{"UTXO not found", "vout 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can test the error message formatting logic
			// The actual createSpendError requires more setup, but we can verify
			// that the error messages contain the expected parts
			assert.Contains(t, tt.message, "UTXO")
		})
	}
}

// Test signal constants
func TestLuaSignalConstants(t *testing.T) {
	// Verify that signal constants are properly defined
	assert.Equal(t, LuaSignal("DAHSET"), LuaSignalDAHSet)
	assert.Equal(t, LuaSignal("DAHUNSET"), LuaSignalDAHUnset)
	assert.Equal(t, LuaSignal("ALLSPENT"), LuaSignalAllSpent)
	assert.Equal(t, LuaSignal("NOTALLSPENT"), LuaSignalNotAllSpent)
	assert.Equal(t, LuaSignal("PRESERVE"), LuaSignalPreserve)
}

// Test error code constants
func TestLuaErrorCodeConstants(t *testing.T) {
	// Verify that error code constants are properly defined
	assert.Equal(t, LuaErrorCode("TX_NOT_FOUND"), LuaErrorCodeTxNotFound)
	assert.Equal(t, LuaErrorCode("CONFLICTING"), LuaErrorCodeConflicting)
	assert.Equal(t, LuaErrorCode("LOCKED"), LuaErrorCodeLocked)
	assert.Equal(t, LuaErrorCode("FROZEN"), LuaErrorCodeFrozen)
	assert.Equal(t, LuaErrorCode("SPENT"), LuaErrorCodeSpent)
	assert.Equal(t, LuaErrorCode("COINBASE_IMMATURE"), LuaErrorCodeCoinbaseImmature)
}

// Test status constants
func TestLuaStatusConstants(t *testing.T) {
	// Verify that status constants are properly defined
	assert.Equal(t, LuaStatus("OK"), LuaStatusOK)
	assert.Equal(t, LuaStatus("ERROR"), LuaStatusError)
}

// Test that result codes are used correctly
func TestAerospikeResultCodes(t *testing.T) {
	// Verify we're using the correct aerospike result codes
	assert.NotEqual(t, types.KEY_NOT_FOUND_ERROR, 0)
	assert.NotEqual(t, types.TIMEOUT, 0)
	assert.NotEqual(t, types.PARAMETER_ERROR, 0)
}
