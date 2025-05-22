// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

// Options represents a function type for configuring the SubtreeProcessor.
// This type implements the functional options pattern, allowing for flexible and
// extensible configuration of the SubtreeProcessor with optional parameters.
// Multiple options can be composed together to customize processor behavior.
type Options func(*SubtreeProcessor)

// WithBatcherSize creates an option to set the batcher size for the SubtreeProcessor.
// This determines how many transactions will be processed in a single batch.
//
// Parameters:
//   - size: The desired size for the transaction batcher
//
// Returns:
//   - Options: A configuration function that sets the batcher size
func WithBatcherSize(size int) Options {
	return func(sp *SubtreeProcessor) {
		sp.batcher = NewTxIDAndFeeBatch(size)
	}
}
