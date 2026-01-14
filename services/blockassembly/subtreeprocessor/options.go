// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

// Options represents a function type for configuring the SubtreeProcessor.
// This type implements the functional options pattern, allowing for flexible and
// extensible configuration of the SubtreeProcessor with optional parameters.
// Multiple options can be composed together to customize processor behavior.
type Options func(*SubtreeProcessor)
