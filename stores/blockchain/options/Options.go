// Package options provides configuration options for blockchain store operations.
// It implements the functional options pattern for customizing block storage behavior.
//
// This package follows the functional options pattern, which allows for flexible and
// extensible configuration of blockchain storage operations without breaking API
// compatibility when new options are added. Options are implemented as functions that
// modify a configuration struct, allowing for a clean and expressive API.
//
// The primary use case is configuring block storage operations with flags that control
// how blocks are processed, such as whether mining status or subtree data should be
// explicitly set. This pattern allows for optional parameters without requiring
// numerous method overloads or complex parameter structs.
package options

// StoreBlockOptions defines the configuration parameters for storing blocks.
// It controls metadata flags that affect how blocks are processed and stored.
type StoreBlockOptions struct {
	// MinedSet indicates whether the mined status flag is explicitly set for the block
	MinedSet    bool
	// SubtreesSet indicates whether the subtrees data is explicitly set for the block
	SubtreesSet bool
}

// StoreBlockOption is a function type that modifies StoreBlockOptions.
// It implements the functional options pattern for configuring block storage.
type StoreBlockOption func(*StoreBlockOptions)

// WithMinedSet creates an option that sets the MinedSet flag.
// This option controls whether a block's mined status is explicitly recorded.
//
// Parameters:
//   - b: Boolean value to set for MinedSet flag
//
// Returns:
//   - StoreBlockOption: Function that applies the configuration
func WithMinedSet(b bool) StoreBlockOption {
	return func(opts *StoreBlockOptions) {
		opts.MinedSet = b
	}
}

// WithSubtreesSet creates an option that sets the SubtreesSet flag.
// This option controls whether a block's subtree data is explicitly recorded.
//
// Parameters:
//   - b: Boolean value to set for SubtreesSet flag
//
// Returns:
//   - StoreBlockOption: Function that applies the configuration
func WithSubtreesSet(b bool) StoreBlockOption {
	return func(opts *StoreBlockOptions) {
		opts.SubtreesSet = b
	}
}
