package errors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestERR_Enum tests the Enum method of the ERR type.
func TestERR_Enum(t *testing.T) {
	t.Run("Enum returns pointer to same value", func(t *testing.T) {
		code := ERR_TX_NOT_FOUND
		ptr := code.Enum()
		require.NotNil(t, ptr)
		require.Equal(t, ERR_TX_NOT_FOUND, *ptr)
	})
}

// TestERR_String tests the String method of the ERR type.
func TestERR_String(t *testing.T) {
	t.Run("String returns correct name", func(t *testing.T) {
		tests := map[ERR]string{
			ERR_UNKNOWN:              "UNKNOWN",
			ERR_INVALID_ARGUMENT:     "INVALID_ARGUMENT",
			ERR_BLOCK_NOT_FOUND:      "BLOCK_NOT_FOUND",
			ERR_TX_POLICY:            "TX_POLICY",
			ERR_UTXO_SPENT:           "UTXO_SPENT",
			ERR_STATE_INITIALIZATION: "STATE_INITIALIZATION",
		}
		for code, expected := range tests {
			t.Run(expected, func(t *testing.T) {
				actual := code.String()
				require.Equal(t, expected, actual)
			})
		}
	})
}

// TestERR_Descriptor tests the Descriptor method of the ERR type.
func TestERR_Descriptor(t *testing.T) {
	t.Run("returns non-nil descriptor", func(t *testing.T) {
		d := ERR_UNKNOWN.Descriptor()
		require.NotNil(t, d)
		require.Contains(t, d.FullName(), "errors.ERR")
	})
}

// TestERR_Type tests the Type method of the ERR type.
func TestERR_Type(t *testing.T) {
	t.Run("returns valid EnumType", func(t *testing.T) {
		typ := ERR_UNKNOWN.Type()
		require.NotNil(t, typ)
		require.Equal(t, "ERR", string(typ.Descriptor().Name()))
	})
}

// TestERR_Number tests the Number method of the ERR type.
func TestERR_Number(t *testing.T) {
	t.Run("returns correct EnumNumber", func(t *testing.T) {
		code := ERR_TX_CONFLICTING
		num := code.Number()
		require.Equal(t, protoreflect.EnumNumber(36), num)
	})
}

// TestERR_EnumDescriptor tests the EnumDescriptor method of the ERR type.
func TestERR_EnumDescriptor(t *testing.T) {
	t.Run("returns valid descriptor and index", func(t *testing.T) {
		bytes, indexes := ERR_UNKNOWN.EnumDescriptor()
		require.NotNil(t, bytes)
		require.Greater(t, len(bytes), 0)
		require.Equal(t, []int{0}, indexes)
	})
}

// TestTError_Reset tests the Reset method of the TError type.
func TestTError_Reset(t *testing.T) {
	t.Run("resets all fields to zero values", func(t *testing.T) {
		err := &TError{
			Code:         ERR_UTXO_SPENT,
			Message:      "utxo already spent",
			Data:         []byte(`{"key":"value"}`),
			WrappedError: &TError{Message: "inner error"},
			File:         "error.go",
			Line:         42,
			Function:     "TestFunction",
		}

		err.Reset()

		require.Equal(t, ERR_UNKNOWN, err.Code)
		require.Empty(t, err.Message)
		require.Nil(t, err.Data)
		require.Nil(t, err.WrappedError)
		require.Empty(t, err.File)
		require.Zero(t, err.Line)
		require.Empty(t, err.Function)
	})
}

// TestTError_String tests the String method of the TError type.
func TestTError_String(t *testing.T) {
	t.Run("returns string representation without panic", func(t *testing.T) {
		err := &TError{
			Code:     ERR_UTXO_SPENT,
			Message:  "example error",
			Function: "TestFunction",
			Line:     123,
		}

		str := err.String()
		require.NotEmpty(t, str)
		require.Contains(t, str, "example error")
	})
}

// TestTError_ProtoMessage tests the ProtoMessage interface implementation of TError.
func TestTError_ProtoMessage(t *testing.T) {
	t.Run("satisfies proto.Message interface", func(t *testing.T) {
		var _ protoreflect.ProtoMessage = &TError{}
	})
}

// TestTError_ProtoReflect tests the ProtoReflect method of the TError type.
func TestTError_ProtoReflect(t *testing.T) {
	t.Run("returns non-nil protoreflect.Message", func(t *testing.T) {
		err := &TError{}
		msg := err.ProtoReflect()
		require.NotNil(t, msg)
		require.Equal(t, msg.Descriptor().FullName(), protoreflect.FullName("errors.TError"))
	})

	t.Run("does not panic when TError is nil", func(t *testing.T) {
		var err *TError
		msg := err.ProtoReflect()
		require.NotNil(t, msg)
	})
}

// TestTError_Descriptor tests the Descriptor method of the TError type.
func TestTError_Descriptor(t *testing.T) {
	t.Run("returns expected descriptor slice", func(t *testing.T) {
		var terr *TError
		descriptor, indexes := terr.Descriptor()
		require.NotNil(t, descriptor)
		require.NotEmpty(t, descriptor)
		require.Equal(t, []int{0}, indexes)
	})
}

// TestTError_GetCode tests the GetCode method of the TError type.
func TestTError_GetCode(t *testing.T) {
	t.Run("returns set code when TError is not nil", func(t *testing.T) {
		terr := &TError{Code: ERR_UTXO_SPENT}
		code := terr.GetCode()
		require.Equal(t, ERR_UTXO_SPENT, code)
	})

	t.Run("returns ERR_UNKNOWN when TError is nil", func(t *testing.T) {
		var terr *TError
		code := terr.GetCode()
		require.Equal(t, ERR_UNKNOWN, code)
	})
}

// TestTError_GetMessage tests the GetMessage method of the TError type.
func TestTError_GetMessage(t *testing.T) {
	t.Run("returns message when TError is not nil", func(t *testing.T) {
		terr := &TError{Message: "custom error"}
		msg := terr.GetMessage()
		require.Equal(t, "custom error", msg)
	})

	t.Run("returns empty string when TError is nil", func(t *testing.T) {
		var terr *TError
		msg := terr.GetMessage()
		require.Equal(t, "", msg)
	})
}

// TestTError_GetData tests the GetData method of the TError type.
func TestTError_GetData(t *testing.T) {
	t.Run("returns data when TError is not nil", func(t *testing.T) {
		terr := &TError{Data: []byte("sample data")}
		data := terr.GetData()
		require.NotNil(t, data)
		require.Equal(t, []byte("sample data"), data)
	})

	t.Run("returns nil when TError is nil", func(t *testing.T) {
		var terr *TError
		data := terr.GetData()
		require.Nil(t, data)
	})
}

// TestTError_GetWrappedError tests the GetWrappedError method of the TError type.
func TestTError_GetWrappedError(t *testing.T) {
	t.Run("returns wrapped error when TError is not nil", func(t *testing.T) {
		wrapped := &TError{Message: "wrapped"}
		terr := &TError{WrappedError: wrapped}
		result := terr.GetWrappedError()
		require.NotNil(t, result)
		require.Equal(t, "wrapped", result.Message)
	})

	t.Run("returns nil when TError is nil", func(t *testing.T) {
		var terr *TError
		result := terr.GetWrappedError()
		require.Nil(t, result)
	})
}

// TestTError_GetFile tests the GetFile method of the TError type.
func TestTError_GetFile(t *testing.T) {
	t.Run("returns file when TError is not nil", func(t *testing.T) {
		terr := &TError{File: "main.go"}
		file := terr.GetFile()
		require.Equal(t, "main.go", file)
	})

	t.Run("returns empty string when TError is nil", func(t *testing.T) {
		var terr *TError
		file := terr.GetFile()
		require.Equal(t, "", file)
	})
}

// TestTError_GetLine tests the GetLine method of the TError type.
func TestTError_GetLine(t *testing.T) {
	t.Run("returns line number when TError is not nil", func(t *testing.T) {
		terr := &TError{Line: 123}
		line := terr.GetLine()
		require.Equal(t, int32(123), line)
	})

	t.Run("returns 0 when TError is nil", func(t *testing.T) {
		var terr *TError
		line := terr.GetLine()
		require.Equal(t, int32(0), line)
	})
}

// TestTError_GetFunction tests the GetFunction method of the TError type.
func TestTError_GetFunction(t *testing.T) {
	t.Run("returns function name when TError is not nil", func(t *testing.T) {
		terr := &TError{Function: "myFunction"}
		fn := terr.GetFunction()
		require.Equal(t, "myFunction", fn)
	})

	t.Run("returns empty string when TError is nil", func(t *testing.T) {
		var terr *TError
		fn := terr.GetFunction()
		require.Equal(t, "", fn)
	})
}
