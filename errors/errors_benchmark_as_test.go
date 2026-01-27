package errors

import (
	"errors"
	"fmt"
	"testing"
)

// BenchmarkErrorAs benchmarks the As() method with various chain depths
func BenchmarkErrorAs(b *testing.B) {
	// Test target types
	var targetError *Error
	var targetStdError error

	benchCases := []struct {
		name  string
		depth int
	}{
		{"shallow_chain_5", 5},
		{"moderate_chain_20", 20},
		{"deep_chain_50", 50},
		{"very_deep_chain_100", 100},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			// Create a chain of errors
			chain := createErrorChain(bc.depth)

			b.Run("target_not_in_chain", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// This will walk the entire chain
					_ = chain.As(&targetStdError)
				}
			})

			b.Run("target_is_Error", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Fast path - should be immediate
					_ = chain.As(&targetError)
				}
			})

			b.Run("mixed_chain_with_std_error", func(b *testing.B) {
				// Create a chain with standard errors mixed in
				mixedChain := createMixedErrorChain(bc.depth)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = mixedChain.As(&targetStdError)
				}
			})
		})
	}
}

// BenchmarkErrorAsComparison compares our As with stdlib errors.As
func BenchmarkErrorAsComparison(b *testing.B) {
	depth := 50
	chain := createErrorChain(depth)

	// Convert to stdlib chain for comparison
	stdChain := convertToStdlibChain(chain)

	var targetError *Error
	var targetWrappedError *wrappedError

	b.Run("custom_Error_As", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = chain.As(&targetError)
		}
	})

	b.Run("stdlib_errors_As", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = errors.As(stdChain, &targetWrappedError)
		}
	})
}

// Helper to create a mixed chain with standard errors
func createMixedErrorChain(depth int) *Error {
	if depth <= 0 {
		return nil
	}

	baseErr := NewError(fmt.Sprintf("error at depth %d", depth))
	currentErr := baseErr

	for i := depth - 1; i > 0; i-- {
		if i%3 == 0 {
			// Every third error is a standard error
			stdErr := fmt.Errorf("standard error at depth %d", i)
			currentErr.SetWrappedErr(stdErr)
			// Can't continue the chain with standard error, so return what we have
			return baseErr
		}
		wrappedErr := NewProcessingError(fmt.Sprintf("error at depth %d", i))
		currentErr.SetWrappedErr(wrappedErr)
		currentErr = wrappedErr
	}

	return baseErr
}

// wrappedError is a simple error wrapper for benchmarking comparisons
type wrappedError struct {
	msg string
	err error
}

func (w *wrappedError) Error() string {
	return w.msg
}

func (w *wrappedError) Unwrap() error {
	return w.err
}

// Helper to convert our error chain to stdlib wrapped errors for comparison
func convertToStdlibChain(e *Error) error {
	if e == nil {
		return nil
	}

	we := &wrappedError{
		msg: e.message,
	}

	if e.wrappedErr != nil {
		if customErr, ok := e.wrappedErr.(*Error); ok {
			we.err = convertToStdlibChain(customErr)
		} else {
			we.err = e.wrappedErr
		}
	}

	return we
}
