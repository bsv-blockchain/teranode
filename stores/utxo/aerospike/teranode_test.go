package aerospike

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLuaMapResponse(t *testing.T) {
	s := &Store{}

	tests := []struct {
		name           string
		response       interface{}
		expectError    bool
		errorContains  string
		validateResult func(t *testing.T, result *LuaMapResponse)
	}{
		{
			name: "valid map response with all fields",
			response: map[interface{}]interface{}{
				"status":     "OK",
				"errorCode":  "TX_NOT_FOUND",
				"message":    "Transaction not found",
				"signal":     "DAHSET",
				"blockIDs":   []interface{}{100, 101, 102},
				"childCount": 5,
				"errors": map[interface{}]interface{}{
					0: map[interface{}]interface{}{
						"errorCode":    "SPENT",
						"message":      "UTXO already spent",
						"spendingData": "deadbeef",
					},
					2: map[interface{}]interface{}{
						"errorCode": "FROZEN",
						"message":   "UTXO is frozen",
					},
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, result *LuaMapResponse) {
				assert.Equal(t, LuaStatusOK, result.Status)
				assert.Equal(t, LuaErrorCodeTxNotFound, result.ErrorCode)
				assert.Equal(t, "Transaction not found", result.Message)
				assert.Equal(t, LuaSignalDAHSet, result.Signal)
				assert.Equal(t, []int{100, 101, 102}, result.BlockIDs)
				assert.Equal(t, 5, result.ChildCount)

				assert.Len(t, result.Errors, 2)
				assert.Equal(t, LuaErrorCodeSpent, result.Errors[0].ErrorCode)
				assert.Equal(t, "UTXO already spent", result.Errors[0].Message)
				assert.Equal(t, "deadbeef", result.Errors[0].SpendingData)

				assert.Equal(t, LuaErrorCodeFrozen, result.Errors[2].ErrorCode)
				assert.Equal(t, "UTXO is frozen", result.Errors[2].Message)
				assert.Empty(t, result.Errors[2].SpendingData)
			},
		},
		{
			name: "minimal response with only status",
			response: map[interface{}]interface{}{
				"status": "ERROR",
			},
			expectError: false,
			validateResult: func(t *testing.T, result *LuaMapResponse) {
				assert.Equal(t, LuaStatusError, result.Status)
				assert.Empty(t, result.ErrorCode)
				assert.Empty(t, result.Message)
				assert.Empty(t, result.Signal)
				assert.Nil(t, result.BlockIDs)
				assert.Nil(t, result.Errors)
				assert.Zero(t, result.ChildCount)
			},
		},
		{
			name:          "invalid response type - not a map",
			response:      "not a map",
			expectError:   true,
			errorContains: "expected map response but got string",
		},
		{
			name:          "invalid response type - nil",
			response:      nil,
			expectError:   true,
			errorContains: "expected map response but got <nil>",
		},
		{
			name: "missing status field",
			response: map[interface{}]interface{}{
				"message": "No status",
			},
			expectError:   true,
			errorContains: "missing or invalid status in response",
		},
		{
			name: "invalid status type",
			response: map[interface{}]interface{}{
				"status": 123, // Not a string
			},
			expectError:   true,
			errorContains: "missing or invalid status in response",
		},
		{
			name: "invalid blockIDs type",
			response: map[interface{}]interface{}{
				"status":   "OK",
				"blockIDs": "not an array",
			},
			expectError:   true,
			errorContains: "invalid blockIDs type: string",
		},
		{
			name: "invalid blockID value in array",
			response: map[interface{}]interface{}{
				"status":   "OK",
				"blockIDs": []interface{}{100, "not an int", 102},
			},
			expectError:   true,
			errorContains: "invalid blockID at index 1",
		},
		{
			name: "invalid errors type",
			response: map[interface{}]interface{}{
				"status": "ERROR",
				"errors": "not a map",
			},
			expectError:   true,
			errorContains: "invalid errors type: string",
		},
		{
			name: "invalid error offset type",
			response: map[interface{}]interface{}{
				"status": "ERROR",
				"errors": map[interface{}]interface{}{
					"not_an_int": map[interface{}]interface{}{
						"errorCode": "SPENT",
						"message":   "Error",
					},
				},
			},
			expectError:   true,
			errorContains: "invalid error offset type: string",
		},
		{
			name: "invalid error object type",
			response: map[interface{}]interface{}{
				"status": "ERROR",
				"errors": map[interface{}]interface{}{
					0: "not a map",
				},
			},
			expectError:   true,
			errorContains: "invalid error object type: string",
		},
		{
			name: "missing errorCode in error object",
			response: map[interface{}]interface{}{
				"status": "ERROR",
				"errors": map[interface{}]interface{}{
					0: map[interface{}]interface{}{
						"message": "Error without code",
					},
				},
			},
			expectError:   true,
			errorContains: "invalid errorCode type in error object",
		},
		{
			name: "missing message in error object",
			response: map[interface{}]interface{}{
				"status": "ERROR",
				"errors": map[interface{}]interface{}{
					0: map[interface{}]interface{}{
						"errorCode": "SPENT",
					},
				},
			},
			expectError:   true,
			errorContains: "invalid message type in error object",
		},
		{
			name: "valid childCount",
			response: map[interface{}]interface{}{
				"status":     "OK",
				"childCount": 42,
			},
			expectError: false,
			validateResult: func(t *testing.T, result *LuaMapResponse) {
				assert.Equal(t, 42, result.ChildCount)
			},
		},
		{
			name: "invalid childCount type - ignored",
			response: map[interface{}]interface{}{
				"status":     "OK",
				"childCount": "not a number",
			},
			expectError: false,
			validateResult: func(t *testing.T, result *LuaMapResponse) {
				assert.Zero(t, result.ChildCount)
			},
		},
		{
			name: "empty blockIDs array",
			response: map[interface{}]interface{}{
				"status":   "OK",
				"blockIDs": []interface{}{},
			},
			expectError: false,
			validateResult: func(t *testing.T, result *LuaMapResponse) {
				assert.NotNil(t, result.BlockIDs)
				assert.Len(t, result.BlockIDs, 0)
			},
		},
		{
			name: "all optional fields present",
			response: map[interface{}]interface{}{
				"status":    "OK",
				"errorCode": "LOCKED",
				"message":   "Transaction is locked",
				"signal":    "PRESERVE",
			},
			expectError: false,
			validateResult: func(t *testing.T, result *LuaMapResponse) {
				assert.Equal(t, LuaStatusOK, result.Status)
				assert.Equal(t, LuaErrorCodeLocked, result.ErrorCode)
				assert.Equal(t, "Transaction is locked", result.Message)
				assert.Equal(t, LuaSignalPreserve, result.Signal)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := s.ParseLuaMapResponse(tt.response)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				if tt.validateResult != nil {
					tt.validateResult(t, result)
				}
			}
		})
	}
}

func TestParseLuaMapResponse_EdgeCases(t *testing.T) {
	s := &Store{}

	t.Run("large error map", func(t *testing.T) {
		// Create a large errors map
		errors := make(map[interface{}]interface{})
		for i := 0; i < 100; i++ {
			errors[i] = map[interface{}]interface{}{
				"errorCode": "SPENT",
				"message":   "UTXO already spent",
			}
		}

		response := map[interface{}]interface{}{
			"status": "ERROR",
			"errors": errors,
		}

		result, err := s.ParseLuaMapResponse(response)
		require.NoError(t, err)
		assert.Len(t, result.Errors, 100)

		// Verify all errors were parsed correctly
		for i := 0; i < 100; i++ {
			assert.Equal(t, LuaErrorCodeSpent, result.Errors[i].ErrorCode)
			assert.Equal(t, "UTXO already spent", result.Errors[i].Message)
		}
	})

	t.Run("special characters in strings", func(t *testing.T) {
		response := map[interface{}]interface{}{
			"status":  "ERROR",
			"message": "Error with special chars: \n\t\"quotes\" and 'apostrophes'",
			"errors": map[interface{}]interface{}{
				0: map[interface{}]interface{}{
					"errorCode":    "SPENT",
					"message":      "Unicode: 你好世界 🌍",
					"spendingData": "00112233445566778899aabbccddeeff",
				},
			},
		}

		result, err := s.ParseLuaMapResponse(response)
		require.NoError(t, err)
		assert.Contains(t, result.Message, "special chars")
		assert.Contains(t, result.Errors[0].Message, "你好世界")
	})

	t.Run("mixed type keys in errors map", func(t *testing.T) {
		// This should fail as we expect int keys
		response := map[interface{}]interface{}{
			"status": "ERROR",
			"errors": map[interface{}]interface{}{
				0:     map[interface{}]interface{}{"errorCode": "SPENT", "message": "msg1"},
				"key": map[interface{}]interface{}{"errorCode": "FROZEN", "message": "msg2"},
			},
		}

		_, err := s.ParseLuaMapResponse(response)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid error offset type: string")
	})
}
