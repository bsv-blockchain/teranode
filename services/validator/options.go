/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements option patterns for both general validation options and
transaction validator-specific options, providing flexible configuration for
validation operations.
*/
package validator

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// ParentTxMetadata holds metadata about a parent transaction needed for validation
// This allows the validator to skip UTXO store lookups for in-block parents
type ParentTxMetadata struct {
	BlockHeight uint32 // The block height where this transaction was mined
}

// Options defines the configuration options for validation operations
type Options struct {
	// SkipUtxoCreation determines whether UTXO creation should be skipped
	// When true, the validator won't create new UTXOs for transaction outputs
	SkipUtxoCreation bool

	// AddTXToBlockAssembly determines whether transactions should be added to block assembly
	// When true, validated transactions are forwarded to the block assembly process
	AddTXToBlockAssembly bool

	// SkipPolicyChecks determines whether policy checks should be skipped
	// this is done when validating transaction from a block that has been mined
	SkipPolicyChecks bool

	// CreateConflicting determines whether to allow conflicting transactions
	// this is done when validating transaction from a block that has been mined
	CreateConflicting bool

	// IgnoreConflicting determines whether to ignore transactions marked as conflicting when spending
	IgnoreConflicting bool

	// IgnoreLocked determines whether to ignore transactions marked as locked when spending
	IgnoreLocked bool

	// ParentMetadata provides pre-fetched metadata for parent transactions
	// When provided, the validator will check this map before calling utxoStore.Get()
	// This enables validation to proceed without UTXO store lookups for in-block parents
	// Key: parent transaction hash, Value: metadata (block height)
	ParentMetadata map[chainhash.Hash]*ParentTxMetadata
}

// Option defines a function type for setting options
// This follows the functional options pattern for flexible configuration
type Option func(*Options)

// NewDefaultOptions creates a new Options instance with default settings
// Default configuration:
//   - skipUtxoCreation: false (UTXOs will be created)
//   - addTXToBlockAssembly: true (transactions will be added to block assembly)
//
// Returns:
//   - *Options: New options instance with default settings
func NewDefaultOptions() *Options {
	return &Options{
		SkipUtxoCreation:     false,
		AddTXToBlockAssembly: true,
		SkipPolicyChecks:     false,
		CreateConflicting:    false,
	}
}

// ProcessOptions applies the provided options to a new Options instance
// Parameters:
//   - opts: Variable number of Option functions to apply
//
// Returns:
//   - *Options: Configured options instance
func ProcessOptions(opts ...Option) *Options {
	options := NewDefaultOptions()
	for _, o := range opts {
		o(options)
	}

	return options
}

// WithSkipUtxoCreation creates an option to control UTXO creation
// Parameters:
//   - skip: When true, UTXO creation will be skipped
//
// Returns:
//   - Option: Function that sets the skipUtxoCreation option
func WithSkipUtxoCreation(skip bool) Option {
	return func(o *Options) {
		o.SkipUtxoCreation = skip
	}
}

// WithAddTXToBlockAssembly creates an option to control block assembly integration (allows the transaction to be added to the block assembly or not)
// Parameters:
//   - add: When true, transactions will be added to block assembly
//
// Returns:
//   - Option: Function that sets the addTXToBlockAssembly option
func WithAddTXToBlockAssembly(add bool) Option {
	return func(o *Options) {
		o.AddTXToBlockAssembly = add
	}
}

// WithSkipPolicyChecks creates an option to control policy checks
// Parameters:
//   - skip: When true, policy checks will be skipped
//
// Returns:
//   - Option: Function that sets the skipPolicyChecks option
func WithSkipPolicyChecks(skip bool) Option {
	return func(o *Options) {
		o.SkipPolicyChecks = skip
	}
}

// WithCreateConflicting creates an option to control whether a conflicting transaction is created
// Parameters:
//   - create: When true, a conflicting transaction will be created
//
// Returns:
//   - Option: Function that sets the createConflicting option
func WithCreateConflicting(create bool) Option {
	return func(o *Options) {
		o.CreateConflicting = create
	}
}

// WithIgnoreConflicting creates an option to control whether a conflicting transaction is ignored
// Parameters:
//   - ignore: When true, a conflicting transaction will be ignored
//
// Returns:
//   - Option: Function that sets the ignoreConflicting option
func WithIgnoreConflicting(ignore bool) Option {
	return func(o *Options) {
		o.IgnoreConflicting = ignore
	}
}

// WithIgnoreLocked creates an option to control whether the locked flag will be ignored when spending UTXOs
// Parameters:
//   - ignoreLocked: When true, transactions marked as locked will also be processed
//
// Returns:
//   - Option: Function that sets the ignoreLocked option
func WithIgnoreLocked(ignoreLocked bool) Option {
	return func(o *Options) {
		o.IgnoreLocked = ignoreLocked
	}
}

// WithParentMetadata creates an option to provide pre-fetched parent transaction metadata
// Parameters:
//   - metadata: Map of parent transaction hashes to their metadata (block height, etc.)
//
// Returns:
//   - Option: Function that sets the parentMetadata option
func WithParentMetadata(metadata map[chainhash.Hash]*ParentTxMetadata) Option {
	return func(o *Options) {
		o.ParentMetadata = metadata
	}
}

// TxValidatorOptions defines configuration options specific to transaction validation
type TxValidatorOptions struct {
	skipPolicyChecks bool
}

// NewTxValidatorOptions creates a new TxValidatorOptions instance with the provided options applied.
func NewTxValidatorOptions(opts ...TxValidatorOption) *TxValidatorOptions {
	options := &TxValidatorOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

// TxValidatorOption defines a function type for setting transaction validator options
// This follows the functional options pattern for flexible configuration
type TxValidatorOption func(*TxValidatorOptions)

// WithTxValidatorSkipPolicyChecks creates an option to skip policy checks during validation.
func WithTxValidatorSkipPolicyChecks(skip bool) TxValidatorOption {
	return func(o *TxValidatorOptions) {
		o.skipPolicyChecks = skip
	}
}
