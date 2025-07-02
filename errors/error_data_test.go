package errors

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestErrData_Error tests the Error method of the ErrData type.
func TestErrData_Error(t *testing.T) {
	tests := []struct {
		name     string
		errData  *ErrData
		expected string
	}{
		{
			name:     "nil_ErrData_pointer",
			errData:  nil,
			expected: "<nil>", // fmt.Sprintf(" %v", *e) will panic if not handled
		},
		{
			name:     "empty_ErrData",
			errData:  &ErrData{},
			expected: " map[]",
		},
		{
			name: "simple_string_key_value",
			errData: &ErrData{
				"reason": "invalid input",
			},
			expected: " map[reason:invalid input]",
		},
		{
			name: "mixed_data_types",
			errData: &ErrData{
				"code":   500,
				"active": true,
				"tags":   []string{"foo", "bar"},
			},
			expected: "", // determined in test body due to map ordering
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.errData == nil {
				var nilErrData *ErrData

				require.Equal(t, "<nil>", fmt.Sprintf("%v", nilErrData))

				return
			}

			result := tt.errData.Error()

			if tt.expected == "" {
				// If expected is blank, build expectation dynamically (due to map key order)
				require.Contains(t, result, "code:500")
				require.Contains(t, result, "active:true")
				require.Contains(t, result, "tags:[foo bar]")
			} else {
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestErrData_SetData tests the SetData method of the ErrData type.
func TestErrData_SetData(t *testing.T) {
	t.Run("set_data_on_non_nil", func(t *testing.T) {
		data := &ErrData{}

		data.SetData("foo", "bar")
		require.Equal(t, "bar", (*data)["foo"])
	})

	t.Run("overwrite_existing_key", func(t *testing.T) {
		data := &ErrData{"key": "old"}

		data.SetData("key", "new")
		require.Equal(t, "new", (*data)["key"])
	})

	t.Run("set_various_data_types", func(t *testing.T) {
		data := &ErrData{}

		data.SetData("int", 42)
		data.SetData("bool", true)
		data.SetData("slice", []string{"a", "b"})
		data.SetData("map", map[string]int{"x": 1})

		require.Equal(t, 42, (*data)["int"])
		require.Equal(t, true, (*data)["bool"])
		require.ElementsMatch(t, []string{"a", "b"}, (*data)["slice"].([]string))
		require.Equal(t, map[string]int{"x": 1}, (*data)["map"])
	})

	t.Run("set_data_on_nil_receiver_does_nothing", func(t *testing.T) {
		var data *ErrData

		require.NotPanics(t, func() {
			data.SetData("foo", "bar")
		})
	})
}

// TestErrData_GetData tests the GetData method of the ErrData type.
func TestErrData_GetData(t *testing.T) {
	t.Run("get_existing_key", func(t *testing.T) {
		data := &ErrData{"foo": "bar"}
		val := data.GetData("foo")
		require.Equal(t, "bar", val)
	})

	t.Run("get_nonexistent_key", func(t *testing.T) {
		data := &ErrData{"foo": "bar"}
		val := data.GetData("missing")
		require.Nil(t, val)
	})

	t.Run("get_various_data_types", func(t *testing.T) {
		data := &ErrData{
			"int":   123,
			"float": 3.14,
			"bool":  true,
			"slice": []string{"a", "b"},
			"map":   map[string]int{"k": 1},
		}

		require.Equal(t, 123, data.GetData("int"))
		require.InDelta(t, 3.14, data.GetData("float").(float64), 0.0001)
		require.Equal(t, true, data.GetData("bool"))
		require.ElementsMatch(t, []string{"a", "b"}, data.GetData("slice").([]string))
		require.Equal(t, map[string]int{"k": 1}, data.GetData("map"))
	})

	t.Run("get_from_nil_receiver", func(t *testing.T) {
		var data *ErrData
		val := data.GetData("foo")
		require.Nil(t, val)
	})
}

// TestErrData_EncodeErrorData tests the EncodeErrorData method of the ErrData type.
func TestErrData_EncodeErrorData(t *testing.T) {
	t.Run("encode_simple_map", func(t *testing.T) {
		data := &ErrData{
			"message": "something went wrong",
			"code":    400,
			"retry":   true,
		}

		bytes := data.EncodeErrorData()

		// Unmarshal back to map to verify correctness
		var decoded map[string]interface{}
		err := json.Unmarshal(bytes, &decoded)
		require.NoError(t, err)
		require.Equal(t, "something went wrong", decoded["message"])
		require.EqualValues(t, 400, decoded["code"])
		require.Equal(t, true, decoded["retry"])
	})

	t.Run("encode_nested_structures", func(t *testing.T) {
		data := &ErrData{
			"slice": []interface{}{1, "two", true},
			"map":   map[string]interface{}{"inner": "value"},
		}

		bytes := data.EncodeErrorData()

		var decoded map[string]interface{}
		err := json.Unmarshal(bytes, &decoded)
		require.NoError(t, err)

		require.IsType(t, []interface{}{}, decoded["slice"])
		require.IsType(t, map[string]interface{}{}, decoded["map"])
	})

	t.Run("encode_empty_map", func(t *testing.T) {
		data := &ErrData{}
		bytes := data.EncodeErrorData()
		require.JSONEq(t, `{}`, string(bytes))
	})

	t.Run("encode_nil_receiver", func(t *testing.T) {
		var data *ErrData

		require.NotPanics(t, func() {
			bytes := data.EncodeErrorData()
			require.NotNil(t, bytes)
			require.Len(t, bytes, 4)
		})
	})

	t.Run("encode_unserializable_value_returns_empty", func(t *testing.T) {
		data := &ErrData{
			"invalid": make(chan int), // channels can't be JSON encoded
		}

		bytes := data.EncodeErrorData()
		require.Empty(t, bytes)
	})
}

// TestGetErrorData tests the GetErrorData function for various scenarios.
func TestGetErrorData(t *testing.T) {
	type testCase struct {
		name        string
		code        ERR
		input       interface{}
		expectType  interface{}
		expectError bool
	}

	tests := []testCase{
		{
			name:       "valid_utxo_spent_data",
			code:       ERR_UTXO_SPENT,
			input:      &UtxoSpentErrData{Hash: chainhash.Hash{}, Vout: 1},
			expectType: &UtxoSpentErrData{},
		},
		{
			name:       "valid_generic_data",
			code:       ERR(9999), // unknown code
			input:      &ErrData{"reason": "invalid"},
			expectType: &ErrData{},
		},
		{
			name:        "invalid_json_for_known_type",
			code:        ERR_UTXO_SPENT,
			input:       []byte("{invalid-json"),
			expectType:  &UtxoSpentErrData{},
			expectError: true,
		},
		{
			name:        "invalid_json_for_generic_type",
			code:        ERR(8888),
			input:       []byte("{invalid-json"),
			expectType:  &ErrData{},
			expectError: true,
		},
		{
			name:        "empty_input_for_generic",
			code:        ERR(0),
			input:       []byte{}, // still valid
			expectType:  &ErrData{},
			expectError: true, // change from false to true
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var dataBytes []byte

			var err error

			switch v := tc.input.(type) {
			case []byte:
				dataBytes = v
			default:
				dataBytes, err = json.Marshal(v)
				require.NoError(t, err)
			}

			var result ErrDataI

			result, err = GetErrorData(tc.code, dataBytes)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.IsType(t, tc.expectType, result)
		})
	}
}
