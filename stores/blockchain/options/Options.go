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
	MinedSet bool
	// SubtreesSet indicates whether the subtrees data is explicitly set for the block
	SubtreesSet bool
	// Invalid indicates whether the block is marked as invalid
	Invalid bool
	// ID is an optional identifier for the block, instead of incrementing the last known block ID
	ID uint64
}

// StoreBlockOption is a function type that modifies StoreBlockOptions.
// It implements the functional options pattern for configuring block storage.
type StoreBlockOption func(*StoreBlockOptions)

// ProcessStoreBlockOptions creates a StoreBlockOptions instance with default values and applies the provided options.
func ProcessStoreBlockOptions(opts ...StoreBlockOption) *StoreBlockOptions {
	options := &StoreBlockOptions{
		MinedSet:    false,
		SubtreesSet: false,
		Invalid:     false,
	}

	for _, o := range opts {
		if o != nil {
			o(options)
		}
	}

	return options
}

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

// WithInvalid creates an option that sets the Invalid flag.
// This option marks a block as invalid when creating it.
//
// Parameters:
//   - b: Boolean value to set for Invalid flag
//
// Returns:
//   - StoreBlockOption: Function that applies the configuration
func WithInvalid(b bool) StoreBlockOption {
	return func(opts *StoreBlockOptions) {
		opts.Invalid = b
	}
}

// WithID creates an option that sets the ID field.
// This option specifies the unique identifier for a block.
//
// Parameters:
//   - id: Integer value to set for ID field
//
// Returns:
//   - StoreBlockOption: Function that applies the configuration
func WithID(id uint64) StoreBlockOption {
	return func(opts *StoreBlockOptions) {
		opts.ID = id
	}
}
