package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func spend() error {
	return New(ERR_INVALID_ARGUMENT, "Spend failed")
}

func validate() error {
	if err := spend(); err != nil {
		return New(ERR_INVALID_ARGUMENT, "Validate failed", err)
	}

	return nil
}

func processTransaction() error {
	if err := validate(); err != nil {
		return New(ERR_INVALID_ARGUMENT, "ProcessTransaction failed", err)
	}

	return nil
}

func Propagate() error {
	if err := processTransaction(); err != nil {
		return New(ERR_INVALID_ARGUMENT, "Propagate failed", err)
	}

	return nil
}

func TestStackTraceInformation(t *testing.T) {
	t.Run("stack information is preserved through WrapGRPC and UnwrapGRPC", func(t *testing.T) {
		// Create a function that returns an error with stack information
		createError := func() *Error {
			return New(ERR_INVALID_ARGUMENT, "test error")
		}

		// Get the error with stack information
		err := createError()
		require.NotNil(t, err)

		// Verify initial stack information is present
		require.NotEmpty(t, err.file)
		require.NotZero(t, err.line)
		require.NotEmpty(t, err.function)

		// Wrap the error with gRPC
		grpcErr := WrapGRPC(err)
		require.NotNil(t, grpcErr)

		// Unwrap the gRPC error
		unwrappedErr := UnwrapGRPC(grpcErr)
		require.NotNil(t, unwrappedErr)

		// Verify stack information is preserved
		assert.Equal(t, err.file, unwrappedErr.file, "File information should be preserved")
		assert.Equal(t, err.line, unwrappedErr.line, "Line information should be preserved")
		assert.Equal(t, err.function, unwrappedErr.function, "Function information should be preserved")

		// Test nested error stack information
		nestedErr := New(ERR_INVALID_ARGUMENT, "wrapper error", err)
		require.NotNil(t, nestedErr)

		// Verify nested error has its own stack information
		assert.NotEqual(t, err.file, nestedErr.file, "Nested error should have different file information")
		assert.NotEqual(t, err.line, nestedErr.line, "Nested error should have different line information")
		assert.NotEqual(t, err.function, nestedErr.function, "Nested error should have different function information")

		// Wrap and unwrap nested error
		grpcNestedErr := WrapGRPC(nestedErr)
		require.NotNil(t, grpcNestedErr)

		unwrappedNestedErr := UnwrapGRPC(grpcNestedErr)
		require.NotNil(t, unwrappedNestedErr)

		// Verify nested error stack information is preserved
		assert.Equal(t, nestedErr.file, unwrappedNestedErr.file, "Nested error file information should be preserved")
		assert.Equal(t, nestedErr.line, unwrappedNestedErr.line, "Nested error line information should be preserved")
		assert.Equal(t, nestedErr.function, unwrappedNestedErr.function, "Nested error function information should be preserved")

		// Verify wrapped error's stack information is also preserved
		wrappedOriginal := unwrappedNestedErr.wrappedErr.(*Error)
		assert.Equal(t, err.file, wrappedOriginal.file, "Original error file information should be preserved in nested error")
		assert.Equal(t, err.line, wrappedOriginal.line, "Original error line information should be preserved in nested error")
		assert.Equal(t, err.function, wrappedOriginal.function, "Original error function information should be preserved in nested error")
	})

	t.Run("stack information is preserved with data", func(t *testing.T) {
		// Create an error with data
		err := New(ERR_INVALID_ARGUMENT, "test error with data")
		err.SetData("key", "value")

		// Wrap and unwrap
		grpcErr := WrapGRPC(err)
		unwrappedErr := UnwrapGRPC(grpcErr)

		// Verify stack information and data are preserved
		assert.Equal(t, err.file, unwrappedErr.file)
		assert.Equal(t, err.line, unwrappedErr.line)
		assert.Equal(t, err.function, unwrappedErr.function)
		assert.Equal(t, "value", unwrappedErr.GetData("key"))
	})
}
