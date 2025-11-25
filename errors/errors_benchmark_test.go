package errors

import (
	"fmt"
	"testing"
)

// BenchmarkErrorFormatting benchmarks the Error() method with various chain depths
// This simulates the heap profile finding where error formatting consumed 568 GB (67% of allocations)
func BenchmarkErrorFormatting(b *testing.B) {
	b.Run("shallow_chain_depth_3", func(b *testing.B) {
		err := createErrorChain(3)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})

	b.Run("medium_chain_depth_10", func(b *testing.B) {
		err := createErrorChain(10)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})

	b.Run("deep_chain_depth_20", func(b *testing.B) {
		err := createErrorChain(20)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})

	b.Run("very_deep_chain_depth_50", func(b *testing.B) {
		err := createErrorChain(50)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})
}

// BenchmarkErrorFormattingWithData benchmarks errors with data (common in UTXO operations)
func BenchmarkErrorFormattingWithData(b *testing.B) {
	b.Run("single_error_with_data", func(b *testing.B) {
		err := NewTxInvalidError("utxo already spent")
		err.SetData("txid", "test_txid")
		err.SetData("spending_txid", "test_spending_txid")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})

	b.Run("chained_errors_with_data", func(b *testing.B) {
		err1 := NewTxInvalidError("utxo 1 already spent")
		err1.SetData("txid", "test_txid_1")
		err2 := NewTxInvalidError("utxo 2 already spent")
		err2.SetData("txid", "test_txid_2")
		err3 := NewTxInvalidError("utxo 3 already spent")
		err3.SetData("txid", "test_txid_3")
		err2.SetWrappedErr(err3)
		err1.SetWrappedErr(err2)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err1.Error()
		}
	})
}

// BenchmarkContains benchmarks the contains() method which had 50.9M calls in the heap profile
func BenchmarkContains(b *testing.B) {
	b.Run("shallow_chain_3_vs_3", func(b *testing.B) {
		e1 := createErrorChain(3)
		e2 := createErrorChain(3)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = e1.contains(e2)
		}
	})

	b.Run("medium_chain_10_vs_10", func(b *testing.B) {
		e1 := createErrorChain(10)
		e2 := createErrorChain(10)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = e1.contains(e2)
		}
	})

	b.Run("deep_chain_20_vs_20", func(b *testing.B) {
		e1 := createErrorChain(20)
		e2 := createErrorChain(20)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = e1.contains(e2)
		}
	})

	b.Run("asymmetric_chain_5_vs_20", func(b *testing.B) {
		e1 := createErrorChain(5)
		e2 := createErrorChain(20)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = e1.contains(e2)
		}
	})
}

// BenchmarkSetWrappedErr benchmarks SetWrappedErr which calls contains() twice
func BenchmarkSetWrappedErr(b *testing.B) {
	b.Run("add_to_shallow_chain", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			e1 := createErrorChain(3)
			e2 := New(ERR_PROCESSING, "new error")
			b.StartTimer()
			e1.SetWrappedErr(e2)
		}
	})

	b.Run("add_to_medium_chain", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			e1 := createErrorChain(10)
			e2 := New(ERR_PROCESSING, "new error")
			b.StartTimer()
			e1.SetWrappedErr(e2)
		}
	})

	b.Run("add_to_deep_chain", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			e1 := createErrorChain(20)
			e2 := New(ERR_PROCESSING, "new error")
			b.StartTimer()
			e1.SetWrappedErr(e2)
		}
	})
}

// BenchmarkErrorJoin simulates the Join operations that contributed to allocations
func BenchmarkErrorJoin(b *testing.B) {
	b.Run("join_3_errors", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			e1 := NewError("error 1")
			e2 := NewProcessingError("error 2")
			e3 := NewServiceError("error 3")
			b.StartTimer()
			_ = Join(e1, e2, e3)
		}
	})

	b.Run("join_10_errors", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			errs := make([]error, 10)
			for j := 0; j < 10; j++ {
				errs[j] = NewError(fmt.Sprintf("error %d", j))
			}
			b.StartTimer()
			_ = Join(errs...)
		}
	})
}

// BenchmarkRealWorldScenario simulates a typical error flow in UTXO operations
// This mirrors what happens during SetMinedMulti operations in the heap profile
func BenchmarkRealWorldScenario(b *testing.B) {
	b.Run("utxo_validation_error_logging", func(b *testing.B) {
		// Simulate a chain of errors like what happens in UTXO validation:
		// Storage error -> Processing error -> Service error
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			storageErr := NewStorageError("failed to read from Aerospike")
			processingErr := NewProcessingError("failed to process batch", storageErr)
			serviceErr := NewServiceError("SetMinedMulti failed", processingErr)
			b.StartTimer()

			// This simulates logging the error (calls Error() method)
			_ = serviceErr.Error()

			// This simulates checking if it's a specific error type (calls contains indirectly)
			_ = serviceErr.Is(storageErr)
		}
	})

	b.Run("batch_processing_with_many_errors", func(b *testing.B) {
		// Simulate processing 100 transactions with errors (typical batch size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			errors := make([]*Error, 100)
			for j := 0; j < 100; j++ {
				errors[j] = NewTxInvalidError(fmt.Sprintf("utxo %d already spent", j))
			}
			b.StartTimer()

			// Format all errors (simulates logging)
			for _, err := range errors {
				_ = err.Error()
			}
		}
	})
}

// BenchmarkErrorCreation benchmarks the New() function
func BenchmarkErrorCreation(b *testing.B) {
	b.Run("create_simple_error", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewError("test error")
		}
	})

	b.Run("create_error_with_wrapped", func(b *testing.B) {
		wrappedErr := NewProcessingError("wrapped error")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewError("test error", wrappedErr)
		}
	})

	b.Run("create_error_with_formatting", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewError("test error with params: %s, %d, %v", "string", 123, true)
		}
	})
}

// Helper function to create an error chain of specified depth
func createErrorChain(depth int) *Error {
	if depth <= 0 {
		return nil
	}

	baseErr := NewError(fmt.Sprintf("error at depth %d", depth))
	currentErr := baseErr

	for i := depth - 1; i > 0; i-- {
		wrappedErr := NewProcessingError(fmt.Sprintf("error at depth %d", i))
		currentErr.SetWrappedErr(wrappedErr)
		currentErr = wrappedErr
	}

	return baseErr
}
